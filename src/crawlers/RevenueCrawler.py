'''
興櫃資料爬蟲_歷史營收
'''
import os, sys, time, random
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests

# 修正路徑
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from configs.revenue_cfg import REVENUE_COLS, HEADERS
from src.utils.revenue import get_revenue_data, calculate_revenue_features
from src.utils.diff_index import search_index_list

class RevenueCrawler:
    def __init__(self):
        self.save_folder = root_path / "data" / "raw_table"
        self.raw_data_path = self.save_folder / "bid_info.csv"
        self.revenue_info_path = self.save_folder / "revenue_info.csv"
        self.session = requests.Session()
        # 初始化 Cookie
        self.session.get("https://mopsov.twse.com.tw/mops/web/t05st10_ifrs", headers=HEADERS, timeout=15)

    def run(self):
        curr_data, diff_index, _ = search_index_list(
            self.raw_data_path, self.revenue_info_path, "證券代號", "投標開始日", REVENUE_COLS
        )
        if diff_index is None or diff_index.empty:
            print("✅ 營收資料已最新")
            return

        newly_captured = []
        print(f"🚀 開始處理 {len(diff_index)} 筆新資料...")

        try:
            for idx in diff_index:
                code, start_date = idx[0], idx[1]
                print(f"🔍 處理代號: {code} | 日期: {start_date.date()}")
                
                # 計算基準月份 (若 10 號前則看前前月)
                y, m, d = start_date.year, start_date.month, start_date.day
                if d <= 10:
                    m -= 1
                    if m == 0: m, y = 12, y - 1

                # 抓取連續 5 個月
                monthly_results = []
                success_all_5 = True
                
                for i in range(5):
                    total_m = (y * 12 + (m - 1)) - i
                    cur_y, cur_m = total_m // 12, (total_m % 12) + 1
                    
                    this_m, last_y, yoy = get_revenue_data(code, cur_y, cur_m, self.session)
                    if this_m is None:
                        print(f"  ⚠️ {cur_y}/{cur_m} 抓取失敗")
                        success_all_5 = False
                        break
                    
                    monthly_results.append((this_m, yoy))
                    time.sleep(random.uniform(1, 2))

                if success_all_5:
                    rev_list = [r[0] for r in monthly_results]
                    latest_yoy = monthly_results[0][1]
                    features = calculate_revenue_features(rev_list, latest_yoy)
                    
                    if features:
                        row = {"證券代號": code, "投標開始日": start_date, **features}
                        newly_captured.append(row)
                        print(f"  ✅ 成功獲取 5 個月數據並計算指標")
                    
                time.sleep(random.uniform(3, 6)) # 避免被封 IP

        except Exception as e:
            print(f"‼️ 執行中斷: {e}")
        
        finally:
            if newly_captured:
                new_df = pd.DataFrame(newly_captured)
                final_df = pd.concat([curr_data, new_df], ignore_index=True)
                # 確保不重複
                final_df = final_df.drop_duplicates(subset=['證券代號', '投標開始日'], keep='last')
                final_df.to_csv(self.revenue_info_path, index=False, encoding="utf-8-sig")
                print(f"💾 已存檔 {len(newly_captured)} 筆資料至 {self.revenue_info_path.name}")
            else:
                print("ℹ️ 無新資料存檔")

if __name__ == "__main__":
    RevenueCrawler().run()