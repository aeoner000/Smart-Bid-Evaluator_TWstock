''' 
興櫃資料爬取_競拍資訊 (完整不省略版本)
'''
import sys
import time
import random
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import date

# --- 路徑補丁：確保可以從根目錄引入 src ---
root_dir = Path(__file__).resolve().parent.parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# 引入設定與工具
from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS
from src.db_base.db_manager import IPO_DAO

auction_cfg = cfg["crawlers"]["auction"]
API_URL = auction_cfg["api_url"]
YEAR_RANGE_URL = auction_cfg["year_range_url"]
RENAME_COL = auction_cfg["rename_columns"]
DATE_COLS = auction_cfg["date_columns"]
NUMERIC_COLS = auction_cfg["numeric_columns"]
INT_COLS = auction_cfg["int_columns"]


class AuctionCrawler:
    def __init__(self):
        self.dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        self.table_name = "bid_info"
        self.dao.ensure_table_exists(self.table_name)

    def format_conver(self, df):
        """轉換內容格式 (依照原始邏輯)"""
        # 1. 處理帶逗號的數字
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

        # 2. 處理日期
        all_date_cols = DATE_COLS + ['update_time']
        for col in all_date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # 3. 證券代號轉字串
        if '證券代號' in df.columns:
            df['證券代號'] = df['證券代號'].astype(str).str.strip()

        # 4. 確保整數欄位使用可容忍 NaN 的 Int64
        for col in INT_COLS:
            if col in df.columns:
                df[col] = df[col].astype('Int64')
        return df

    def clean_and_prepare_data(self, df):
        """初步資料清洗模組"""
        if df.empty:
            return df

        # A. 補全發行性質
        if "發行性質" not in df.columns:
            df.insert(loc=5, column="發行性質", value="")
            df["發行性質"] = np.where(
                df["發行市場"].str.contains("初上市", na=False), "初上市",
                np.where(df["發行市場"].str.contains("初上櫃", na=False), "初上櫃", "")
            )
        
        if "update_time" not in df.columns:
            df["update_time"] = pd.Timestamp.now(tz="Asia/Taipei")

        # C. 統一更名與刪除序號
        df = df.rename(columns=RENAME_COL)
        if "序號" in df.columns:
            df.drop(columns="序號", inplace=True)

        # D. 強制日期轉換
        for col in DATE_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%Y/%m/%d", errors="coerce")

        # E. 基本過濾
        condition = df["發行性質"].str.contains("初上市|初上櫃", na=False)
        df = df[condition].copy()
        
        # 執行格式轉換 (處理數字與細節)
        df = self.format_conver(df)

        # F. 排除未得標/流標 (得標金額 > 0)
        df = df[df["得標總金額(元)"] > 0]
        return df

    def run(self):
        """執行爬取與存檔主邏輯，具備中斷續存與增量更新功能。"""
        today = pd.Timestamp(date.today())
        params = {"response": "json"}
        new_data_list = []

        try:
            old_df = self.dao.fetch_all(self.table_name)

            if old_df.empty:
                print(">>> [模式：初次全量] 資料庫無資料，準備爬取歷史所有年份...")
                res_years = requests.get(YEAR_RANGE_URL, params=params)
                res_years.raise_for_status()
                year_info = res_years.json()
                start_year = int(year_info.get("startYear", 2016))
                end_year = int(year_info.get("endYear", today.year))

                for year in range(start_year, end_year + 1):
                    print(f"正在抓取 {year} 年...")
                    req = requests.get(API_URL, params={"date": f"{year}0101", "response": "json"})
                    req.raise_for_status()
                    data_json = req.json()
                    if "data" in data_json:
                        temp_df = pd.DataFrame(data_json["data"], columns=data_json["fields"])
                        cleaned_df = self.clean_and_prepare_data(temp_df)
                        # 過濾掉未來的上市日期
                        filtered_df = cleaned_df[pd.to_datetime(cleaned_df["撥券日期(上市、上櫃日期)"]) <= today].copy()
                        if not filtered_df.empty:
                            new_data_list.append(filtered_df)
                    time.sleep(random.uniform(1, 2))
            else:
                print(">>> [模式：增量更新] 讀取資料庫資料進行比對...")
                max_date = self.dao.get_max_date(self.table_name, "撥券日期(上市、上櫃日期)")
                print(f"資料庫最新日期為: {max_date.date()}")

                req = requests.get(API_URL, params=params)
                req.raise_for_status()
                data_json = req.json()
                if "data" not in data_json:
                    print("從 API 獲取新資料失敗。")
                    return

                new_raw_df = pd.DataFrame(data_json["data"], columns=data_json["fields"])
                cleaned_new_df = self.clean_and_prepare_data(new_raw_df)

                incremental_df = cleaned_new_df[
                    (pd.to_datetime(cleaned_new_df["撥券日期(上市、上櫃日期)"]) > max_date) &
                    (pd.to_datetime(cleaned_new_df["撥券日期(上市、上櫃日期)"]) <= today)
                ].copy()

                if not incremental_df.empty:
                    print(f"檢測到 {len(incremental_df)} 筆新資料，正在補進...")
                    new_data_list.append(incremental_df)
                else:
                    print("目前沒有符合條件的新資料。")
        except Exception as e:
            print(f"❌ 執行爬取時發生錯誤: {e}")
        
        finally:
            if new_data_list:
                print("\n🏁 執行最終存檔...")
                new_data_df = pd.concat(new_data_list, ignore_index=True)
                self.dao.save_data(new_data_df, self.table_name, if_exists="append")
                print(f"💾 存檔完成！本次新增 {len(new_data_df)} 筆資料至 '{self.table_name}'。")
            else:
                print("\nℹ️ 本次執行無新資料可供存檔。")

if __name__ == "__main__":
    # 單獨測試執行
    crawler = AuctionCrawler()
    crawler.run()