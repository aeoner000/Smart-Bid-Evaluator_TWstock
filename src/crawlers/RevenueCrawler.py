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

from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS
from src.utils.revenue import get_revenue_data, calculate_revenue_features
from src.database.db_manager import IPO_DAO

revenue_cfg = cfg["crawlers"]["revenue"]
REVENUE_COLS = revenue_cfg["revenue_cols"]
# config.yaml 中的 revenue.headers 格式為 list of dicts，不符合 requests 需求。
# 在此將其轉換為單一的 dict，才能正確設定 session headers。
HEADERS_LIST = revenue_cfg["headers"]
HEADERS = {k: v for d in HEADERS_LIST for k, v in d.items()}

class RevenueCrawler:
    def __init__(self):
        self.session = requests.Session()
        # 將 HEADERS 設為 session 的預設值，確保後續所有請求都帶上
        self.session.headers.update(HEADERS)
        # 初始化 Cookie
        self.session.get("https://mops.twse.com.tw/mops/#/web/t05st10_ifrs", timeout=15)

        # Database access (SQLite)
        self.dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        self.table_name = "revenue_info"
        self.dao.ensure_table_exists(self.table_name)

    def run(self):
        # 1. 讀取來源與目標資料
        raw_df = self.dao.fetch_all("bid_info")
        if raw_df.empty:
            print("❌ 找不到來源資料 bid_info，無法繼續。")
            return
        
        curr_data = self.dao.fetch_all(self.table_name)

        # 2. 決定抓取模式
        key_cols = ["證券代號", "投標開始日"]
        if curr_data.empty:
            print(f">>> [模式：初次全量] '{self.table_name}' 為空，準備進行首次完整抓取...")
            raw_df[key_cols[1]] = pd.to_datetime(raw_df[key_cols[1]])
            diff_index = [tuple(x) for x in raw_df[key_cols].to_numpy()]
        else:
            print(f">>> [模式：增量更新] '{self.table_name}' 已有資料，進行差異比對...")
            _, diff_index = self.dao.diff_index(
                raw_table="bid_info",
                target_table=self.table_name,
                key_cols=key_cols,
            )

        if not diff_index:
            print("✅ 營收資料已是最新。")
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
                print(f"\n💾 執行最終存檔...")
                new_df = pd.DataFrame(newly_captured)
                self.dao.save_data(new_df, self.table_name, if_exists="append")
                print(f"✅ 存檔完成！本次新增 {len(newly_captured)} 筆資料至 '{self.table_name}'。")
            else:
                print("\nℹ️ 本次執行無新資料可供存檔。")

if __name__ == "__main__":
    RevenueCrawler().run()