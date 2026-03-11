'''
興櫃資料爬蟲_上市櫃行情
'''
import sys, time, random
from pathlib import Path
from collections import deque
import pandas as pd

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

# 假設這些是你的自定義工具函數
from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS
from src.utils.market import (
    get_market_Inst_tw, get_margin, get_market_usa, 
    get_market_tw
)
from src.utils.finmind_manager import FinMindManager
from src.db_base.db_manager import IPO_DAO

market_cfg = cfg["crawlers"]["market"]
FEATURE_COLS = market_cfg["feature_cols"]
USA_TICKERS = market_cfg["usa_tickers"]
USA_NAME_MAP = market_cfg["usa_name_map"]

class MarketCrawler:
    def __init__(self):
        self.dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        self.table_name = "all_market_info"
        self.dao.ensure_table_exists(self.table_name)
        
        # 初始化 FinMind API 管理器
        self.fm = FinMindManager()

    def process_data(self, dl, code, target_time):
        """核心抓取邏輯，確保傳入特定的 DataLoader"""
        # 這裡會依序調用 API，請根據實際情況調整 usage 增加的數值
        tw_indices = get_market_Inst_tw(dl, target_time, 10)
        tw_margin = get_margin(dl, target_time, 10)
        usa_market = get_market_usa(target_time, USA_TICKERS, USA_NAME_MAP, 10)
        tw_market = get_market_tw(target_time, 10)
        
        key_info = pd.Series({"證券代號": code, "投標開始日": target_time})
        return pd.concat([key_info, tw_indices, tw_margin, usa_market, tw_market])

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
   
        if len(diff_index) == 0:
            print("✅ 資料已是最新，無需抓取。")
            return

        tasks = deque(diff_index) # 用貯列先進先出
        newly_captured = []
        
        print(f"🚀 啟動爬蟲，剩餘任務：{len(tasks)} 筆")

        try:
            # 開始迴圈
            while tasks:
                # 取出任務
                code, target_time = tasks.popleft()
                
                try:
                    print(f"正在抓取: {code} | {target_time.date()}", end=" ", flush=True)
                    
                    # 1. 從管理器獲取可用的 loader
                    loader = self.fm.get_loader()
                    
                    # 2. 執行抓取
                    combined = self.process_data(loader, code, target_time)
                    newly_captured.append(combined.to_dict())
                    
                    # 3. 增加使用計數 (此爬蟲一次消耗 2 個 API call)
                    self.fm.add_usage(2)
                    print("✅")

                    # 隨機延遲 (多 Token 可稍微加速，但仍建議保留 1 秒以上)
                    time.sleep(random.uniform(1.0, 2.0))

                except Exception as e:
                    print(f"❌ 出錯: {e}")
                    # 發生錯誤可考慮將任務放回隊列末尾重試，或記錄到錯誤日誌
                    # tasks.append((code, target_time)) 
                    time.sleep(5)
        except KeyboardInterrupt:
            print("\n🛑 偵測到使用者中斷，準備存檔...")
        finally:
            # 最終存檔（寫入 SQLite），確保即使中斷也能保存已抓取的資料
            if newly_captured:
                print("\n🏁 迴圈結束或中斷，執行最終存檔...")
                new_df = pd.DataFrame(newly_captured)
                self.dao.save_data(new_df, self.table_name, if_exists="append")
                # print(f" 存檔完成！本次新增 {len(newly_captured)} 筆資料至 '{self.table_name}'。")

        print("🏁 所有任務處理完成！")

if __name__ == "__main__":    
    crawler = MarketCrawler()
    crawler.run()