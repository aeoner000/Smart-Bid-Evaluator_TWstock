''' 
興櫃資料爬取_競拍資訊 (整合物理刪除與高效比對版本)
'''
import sys
import time
import random
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import date
import traceback

# --- 路徑補丁 ---
root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS
from src.db_base.db_manager import IPO_DAO

# 讀取設定
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
        """轉換內容格式"""
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

        all_date_cols = DATE_COLS + ['update_time']
        for col in all_date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if '證券代號' in df.columns:
            df['證券代號'] = df['證券代號'].astype(str).str.strip()

        for col in INT_COLS:
            if col in df.columns:
                df[col] = df[col].astype('Int64')
        return df

    def clean_and_prepare_data(self, df):
        """初步資料清洗模組"""
        if df.empty:
            return df

        if "發行性質" not in df.columns:
            df.insert(loc=5, column="發行性質", value="")
            df["發行性質"] = np.where(
                df["發行市場"].str.contains("初上市", na=False), "初上市",
                np.where(df["發行市場"].str.contains("初上櫃", na=False), "初上櫃", "")
            )
        
        if "update_time" not in df.columns:
            df["update_time"] = pd.Timestamp.now(tz="Asia/Taipei")

        df = df.rename(columns=RENAME_COL)
        if "序號" in df.columns:
            df.drop(columns="序號", inplace=True)

        for col in DATE_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%Y/%m/%d", errors="coerce")

        condition = df["發行性質"].str.contains("初上市|初上櫃", na=False)
        df = df[condition].copy()
        
        df = self.format_conver(df)
        df = df[df["取消競價拍賣(流標或取消)"] != "Y"]
        return df

    def run(self):
        """
        核心執行邏輯 - 採用「極簡模式：狀態歸零法」
        此方法清理所有未完成的舊任務，從API獲取最新數據，
        過濾掉已徹底完成的歷史案件，最後將新的待辦任務附加回資料庫。
        """
        print("--- [AuctionCrawler] 啟動：執行「狀態歸零法」 ---")
        try:
            # 步驟 1: 【清空舊任務】刪除所有未完成的案件
            print("步驟 1/4: 清理所有未完成 (status != 'all_complete') 的舊任務...")
            self.dao.execute(f"DELETE FROM {self.table_name} WHERE status != 'all_complete'")
            print("✅ 舊任務清理完成。")
            # 步驟 2: 【獲取最新情報】從 API 抓取資料
            print("步驟 2/4: 從 API 抓取最新資料...")
            today = pd.Timestamp(date.today()).normalize()
            api_data_frames = []
            
            # 獲取可用的年份範圍
            year_info_res = requests.get(YEAR_RANGE_URL, params={"response": "json"})
            year_info_res.raise_for_status()
            available_years = year_info_res.json()
            start_year = int(available_years.get("startYear", today.year - 8))
            end_year = int(available_years.get("endYear", today.year))
            
            for year in range(start_year, end_year + 1):
                params = {"date": f"{year}0101", "response": "json"}
                try:
                    api_res = requests.get(API_URL, params=params)
                    api_res.raise_for_status()
                    api_data = api_res.json()
                    if "data" in api_data and api_data["data"]:
                        api_df = pd.DataFrame(api_data["data"], columns=api_data["fields"])
                        api_df = self.clean_and_prepare_data(api_df)
                        api_data_frames.append(api_df)
                    time.sleep(random.uniform(0.5, 1))
                except requests.exceptions.RequestException as e:
                    print(f"警告：抓取年份 {year} 資料時發生錯誤: {e}")
            if not api_data_frames:
                print("⚠️ API 未返回任何有效資料，終止執行。")
                return
 
            # 合併並清理從API獲取的所有案件
            all_api_cases_df = pd.concat(api_data_frames, ignore_index=True)
            print("✅ API 資料抓取與清理完成。")
            # 步驟 3: 【過濾已完成】讀取已完成案件並過濾
            print("步驟 3/4: 過濾已標記為 'all_complete' 的歷史案件...")
            completed_cases_df = self.dao.fetch_all(self.table_name)
            
            if not completed_cases_df.empty:
                # 建立已完成案件的唯一鍵 Set，以提高過濾效率
                completed_keys_set = set(zip(
                    completed_cases_df['證券代號'].astype(str),
                    pd.to_datetime(completed_cases_df['投標開始日']).dt.normalize()
                ))
                
                # 建立當前 API 資料的唯一鍵
                api_cases_keys = zip(
                    all_api_cases_df['證券代號'].astype(str),
                    pd.to_datetime(all_api_cases_df['投標開始日']).dt.normalize()
                )
                # 建立一個布林遮罩，標記哪些是新案件
                is_new_case_mask = [key not in completed_keys_set for key in api_cases_keys]
                new_tasks_df = all_api_cases_df[is_new_case_mask].copy()
            else:
                # 如果沒有任何已完成的案件（例如首次執行），則所有抓取的案件都是新任務
                new_tasks_df = all_api_cases_df.copy()
            print(f"✅ 過濾完成，找到 {len(new_tasks_df)} 筆需要處理的新任務。")
            # 步驟 4: 【建立新任務】將新任務以 'crawling' 狀態附加回資料庫
            if not new_tasks_df.empty:
                print(f"步驟 4/4: 將 {len(new_tasks_df)} 筆新任務以 'crawling' 狀態附加至資料庫...")
                new_tasks_df['status'] = 'crawling'
                self.dao.save_data(new_tasks_df, self.table_name, if_exists="append")
                print("💾 新任務已成功存檔。")
            else:
                print("步驟 4/4: 無任何新任務需要新增。")
            print("--- [AuctionCrawler] 執行完畢 ---")
        except Exception as e:
            print(f"❌ AuctionCrawler 執行時發生嚴重錯誤: {e}")
            traceback.print_exc()
            
if __name__ == "__main__":
    crawler = AuctionCrawler()