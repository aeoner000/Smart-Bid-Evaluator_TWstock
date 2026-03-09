''' 
興櫃資料爬取_競拍資訊 (完整不省略版本)
'''
import sys
import time
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
from configs.auction_cfg import (
    API_URL, YEAR_RANGE_URL, RENAME_COL, 
    DATE_COLS, NUMERIC_COLS, INT_COLS
)

class AuctionCrawler:
    def __init__(self):
        self.save_folder = Path("./data/raw_table")
        self.file_path = self.save_folder / "bid_info2.csv"
        self.save_folder.mkdir(parents=True, exist_ok=True)

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

        # 4. 處理流標欄位
        if '取消競價拍賣(流標或取消)' in df.columns:
            print(df['取消競價拍賣(流標或取消)'])
            df['取消競價拍賣(流標或取消)'] = df['取消競價拍賣(流標或取消)'].str.strip().replace('', 'N')
            df['取消競價拍賣(流標或取消)'] = df['取消競價拍賣(流標或取消)'].fillna('N')
            print(df['取消競價拍賣(流標或取消)'])
            input()
        # 5. 確保整數欄位使用可容忍 NaN 的 Int64
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
        """執行爬取與存檔主邏輯"""
        today = pd.Timestamp(date.today())
        params = {"response": "json"}
        try:
            # A. 判斷模式
            if not self.file_path.exists():
                print(">>> [模式：初次全量] 準備爬取歷史所有年份...")
                res_years = requests.get(YEAR_RANGE_URL, params=params)
                year_info = res_years.json()
                start_year = int(year_info.get("startYear", 2016))
                end_year = int(year_info.get("endYear", today.year))

                all_data_list = []
                for y in range(start_year, end_year + 1):
                    print(f"正在抓取 {y} 年...")
                    req = requests.get(API_URL, params={"date": f"{y}0101", "response": "json"})
                    data_json = req.json()
                    if "data" in data_json:
                        temp_df = pd.DataFrame(data_json["data"], columns=data_json["fields"])
                        cleaned_df = self.clean_and_prepare_data(temp_df)
                        all_data_list.append(cleaned_df)
                    time.sleep(1)

                if not all_data_list:
                    print("❌ 抓取失敗，無資料。")
                    return

                raw_df = pd.concat(all_data_list, ignore_index=True)
                final_df = raw_df[raw_df["撥券日期(上市、上櫃日期)"] <= today]
            else:
                print(">>> [模式：增量更新] 讀取現有資料進行比對...")
                old_df = pd.read_csv(self.file_path, encoding="utf-8-sig", dtype={"證券代號":str})
                old_df["撥券日期(上市、上櫃日期)"] = pd.to_datetime(old_df["撥券日期(上市、上櫃日期)"])
                max_date = old_df["撥券日期(上市、上櫃日期)"].max()
                print(f"資料庫最新日期為: {max_date.date()}")

                req = requests.get(API_URL, params=params)
                data_json = req.json()
                new_raw_df = pd.DataFrame(data_json["data"], columns=data_json["fields"])
                cleaned_new_df = self.clean_and_prepare_data(new_raw_df)

                incremental_df = cleaned_new_df[
                    (cleaned_new_df["撥券日期(上市、上櫃日期)"] > max_date) &
                    (cleaned_new_df["撥券日期(上市、上櫃日期)"] <= today)
                ]

                if not incremental_df.empty:
                    print(f"檢測到 {len(incremental_df)} 筆新資料，正在補進...")
                    final_df = pd.concat([old_df, incremental_df], ignore_index=True)
                else:
                    print("目前沒有符合條件的新資料。")
                    return
        except Exception as e:
            print(e)
        finally:
            # B. 存檔輸出 (使用 utf-8-sig 以便 Excel 開啟)
            final_df.to_csv(self.file_path, index=False, encoding="utf-8-sig")
            print(f"✅ 任務完成！目前總筆數：{len(final_df)} | 存檔路徑: {self.file_path}")

if __name__ == "__main__":
    # 單獨測試執行
    crawler = AuctionCrawler()
    crawler.run()