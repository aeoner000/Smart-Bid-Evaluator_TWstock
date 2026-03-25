import sys
import time
import random
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import date
import traceback
import logging

# --- 設定 Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parents[2]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.config_loader import config
from src.db_base.db_manager import get_db_manager

# 從 config 讀取設定
auction_cfg = config["crawlers"]["auction"]
API_URL = auction_cfg["api_url"]
YEAR_RANGE_URL = auction_cfg["year_range_url"]
DATE_COLS = auction_cfg["date_columns"]
FLOAT_COLS = auction_cfg["float_columns"]
INT_COLS = auction_cfg["int_columns"]

class AuctionCrawler:
    # 建立命名標準映射
    MASTER_RENAME_MAP = {
        "競拍數量(張)": "競拍數量_張",
        "最低投標價格(元)": "最低投標價格_元",
        "最低每標單位(張)": "最低每標單投標數量_張",
        "最低每標單投標數量(張)": "最低每標單投標數量_張",
        "最高投(得)標數量(張)": "最高投_得_標數量_張",
        "保證金成數(%)": "保證金成數_百分比",
        "每一投標單投標處理費(元)": "每一投標單投標處理費_元",
        "得標總金額(元)": "得標總金額_元",
        "得標手續費率(%)": "得標手續費率_百分比",
        "合格投標數量(張)": "合格投標數量_張",
        "最低得標價格(元)": "最低得標價格_元",
        "最高得標價格(元)": "最高得標價格_元",
        "得標加權平均價格(元)": "得標加權平均價格_元",
        "實際承銷價格(元)": "承銷價格_元",
        "取消競價拍賣(流標或取消)": "取消競價拍賣_流標或取消",
        "撥券日期(上市、上櫃日期)": "撥券日期_上市_上櫃日期",
    }

    def __init__(self):
        self.dao = get_db_manager()
        self.table_name = "bid_info"

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """根據 MASTER_RENAME_MAP 映射來重新命名欄位"""
        df.columns = df.columns.str.strip()
        return df.rename(columns=self.MASTER_RENAME_MAP)

    def format_conver(self, df: pd.DataFrame) -> pd.DataFrame:
        """依據 config 裡的欄位清單，自動轉換內容格式"""
        
        # 1. 處理數值欄位 (Float)
        for col in FLOAT_COLS:
            if col in df.columns:
                # 處理字串中的逗號，並強制轉為 float
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '').str.strip(), 
                                            errors='coerce').astype("float64")

        # 2. 處理整數欄位 (Int64) - 解決 669 str 轉 int 失敗問題
        for col in INT_COLS:
            if col in df.columns:
                # 先轉 numeric (處理掉字串/空值) 再轉可含空值的 Int64
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), 
                                            errors='coerce').astype('Int64')

        # 3. 處理日期欄位
        all_date_cols = DATE_COLS + ['update_time']
        for col in all_date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.floor('us').dt.tz_localize(None)

        # 4. 處理證券代號 (通常建議維持 STRING)
        if '證券代號' in df.columns:
            df['證券代號'] = df['證券代號'].astype(str).str.strip()

        return df

    def clean_and_prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """初步資料清洗模組"""
        if df.empty:
            return df

        # A. 先統一更名，避免後續邏輯找不到欄位
        df = self._rename_columns(df)

        # B. 過濾流標或取消 (使用更名後的欄位)
        cancel_col = "取消競價拍賣_流標或取消"
        if cancel_col in df.columns:
            df = df[df[cancel_col] != "Y"].copy()

        # C. 處理發行性質邏輯
        if "發行性質" not in df.columns:
            df.insert(loc=5, column="發行性質", value="")

        if "發行市場" in df.columns:
            df["發行性質"] = np.where(
                df["發行市場"].str.contains("初上市", na=False), "初上市",
                np.where(df["發行市場"].str.contains("初上櫃", na=False), "初上櫃", df["發行性質"])
            )

        condition = df["發行性質"].str.contains("初上市|初上櫃", na=False)
        df = df[condition].copy()


        # D. 補齊時間戳記與移除不必要欄位
        if "update_time" not in df.columns:
            df["update_time"] = pd.Timestamp.now(tz="Asia/Taipei").normalize()

        if "序號" in df.columns:
            df.drop(columns="序號", inplace=True)

        # F. 最後執行型別自動轉換 (對齊 Config)
        df = self.format_conver(df)

        return df

    def run(self):
        """核心執行邏輯 - 狀態歸零法"""
        logger.info("--- [AuctionCrawler] 啟動：執行「狀態歸零法」 ---")
        try:
            # 1. 清理舊任務 (確保 Dataset 路徑完整)
            logger.info("步驟 1/4: 清理所有未完成 (status != 'all_complete') 的舊任務...")
            qualified_table_name = f"`{self.dao.project_id}.{self.dao.dataset_id}.{self.table_name}`"
            sql = f"DELETE FROM {qualified_table_name} WHERE status != @status"
            self.dao.execute(sql, {"status": "all_complete"})
            logger.info("✅ 舊任務清理完成。")

            # 2. 抓取 API 資料
            logger.info("步驟 2/4: 從 API 抓取最新資料...")
            today = pd.Timestamp(date.today()).normalize()

            api_data_frames = []
            
            year_info_res = requests.get(YEAR_RANGE_URL, params={"response": "json"})
            year_info_res.raise_for_status()
            available_years = year_info_res.json()
            start_year = int(available_years.get("startYear", 2016))
            end_year = int(available_years.get("endYear", today.year))
   
            for year in range(start_year, end_year + 1):
                params = {"date": f"{year}0101", "response": "json"}
                try:
                    api_res = requests.get(API_URL, params=params)
                    api_res.raise_for_status()
                    api_data = api_res.json()

                    if "data" in api_data and api_data["data"]:
                        # 抓取後立即進行初步清洗與型別對齊
                        temp_df = pd.DataFrame(api_data["data"], columns=api_data["fields"])
                        temp_df = self.clean_and_prepare_data(temp_df)

                        api_data_frames.append(temp_df)
                    time.sleep(random.uniform(1, 2)) # 稍微增加延遲避免被 API 封鎖
                except Exception as e:
                    logger.warning(f"抓取年份 {year} 資料時發生錯誤: {e}")
            if not api_data_frames:
                logger.error("⚠️ API 未返回任何有效資料。")
                return

            all_api_cases_df = pd.concat(api_data_frames, ignore_index=True)
            all_api_cases_df = self._rename_columns(all_api_cases_df)
            all_api_cases_df = self.format_conver(all_api_cases_df)
            
            # 3. 過濾已完成案件
            logger.info("步驟 3/4: 過濾已標記為 'all_complete' 的歷史案件...")
            completed_cases_df = self.dao.fetch_all(self.table_name)

            if not completed_cases_df.empty:
                # 統一型別以便比對
                completed_cases_df = self.format_conver(completed_cases_df)
                
                completed_keys = set(zip(
                    completed_cases_df['證券代號'].astype(str),
                    pd.to_datetime(completed_cases_df['投標開始日']).dt.normalize()
                ))
                
                api_keys = zip(
                    all_api_cases_df['證券代號'].astype(str),
                    pd.to_datetime(all_api_cases_df['投標開始日']).dt.normalize()
                )

                new_tasks_df = all_api_cases_df[[k not in completed_keys for k in api_keys]].copy()

            else:
                new_tasks_df = all_api_cases_df.copy()
            # 4. 存檔
            if not new_tasks_df.empty:
                logger.info(f"步驟 4/4: 將 {len(new_tasks_df)} 筆新任務寫入資料庫...")
                new_tasks_df['status'] = 'crawling'
                # 寫入前最後一次型別檢查
                new_tasks_df = self.format_conver(new_tasks_df)
                self.dao.save_data(new_tasks_df, self.table_name, if_exists="append")
                logger.info("💾 新任務已成功存檔至 BigQuery。")
            else:
                logger.info("無任何新任務需要新增。")

            logger.info("--- [AuctionCrawler] 執行完畢 ---")
        except Exception as e:
            logger.error(f"❌ AuctionCrawler 嚴重錯誤: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    crawler = AuctionCrawler()
    crawler.run()