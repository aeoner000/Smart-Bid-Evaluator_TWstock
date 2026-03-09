'''
興櫃資料爬蟲_上市櫃行情
'''
import os, sys, time, random
from datetime import datetime, timedelta
from pathlib import Path
from collections import deque
import pandas as pd
from FinMind.data import DataLoader
from dotenv import load_dotenv

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

# 假設這些是你的自定義工具函數
from configs.market_cfg import FEATURE_COLS, USA_TICKERS, USA_NAME_MAP
from src.utils.diff_index import search_index_list
from src.utils.market import (
    get_market_Inst_tw, get_margin, get_market_usa, 
    get_market_tw
)

class MarketCrawler:
    def __init__(self):
        """
        :param tokens: list of strings, 你的多個 FinMind API Tokens
        """
        self.save_folder = root_path / "data" / "raw_table"
        self.raw_data_path = self.save_folder / "bid_info.csv"
        self.market_stmt_path = self.save_folder / "all_market_info.csv"
        load_dotenv()
        self.api_tokens = [os.getenv("FINMIND"), os.getenv("FINMIND2")]
        
        # 初始化多個 DataLoader 實例
        self.clients = []
        for t in self.api_tokens:
            if t:
                dl = DataLoader()
                dl.login_by_token(api_token=t)
                self.clients.append({"loader": dl, "token": t, "usage": 0})
        
        if not self.clients:
            raise ValueError("❌ 找不到有效的 API Token，請檢查 .env 檔案。")

        self.current_idx = 0
        self.max_safe_limit = 280  # 每個 Token 每小時的安全上限 (假設一組資料耗費 2 tokens)

    @property
    def current_client(self):
        return self.clients[self.current_idx]

    def switch_client(self):
        """切換到下一個可用 Token，若繞回第一個則回傳 True 表示全數用盡"""
        self.current_idx = (self.current_idx + 1) % len(self.clients)
        return self.current_idx == 0

    def sleep_until_next_hour(self):
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0)
        wait_sec = (next_hour - now).total_seconds()
        print(f"\n😴 所有 Token 額度皆已達標，休息至下個整點 {next_hour.strftime('%H:%M')}，需等待 {int(wait_sec/60)} 分鐘...")
        time.sleep(max(wait_sec, 1))
        # 重置所有 Token 的計數
        for c in self.clients:
            c["usage"] = 0

    def process_data(self, dl, code, target_time):
        """核心抓取邏輯，確保傳入特定的 DataLoader"""
        # 這裡會依序調用 API，請根據實際情況調整 usage 增加的數值
        tw_indices = get_market_Inst_tw(dl, target_time, 10)
        tw_margin = get_margin(dl, target_time, 10)
        usa_market = get_market_usa(target_time, USA_TICKERS, USA_NAME_MAP, 10)
        tw_market = get_market_tw(target_time, 10)
        
        key_info = pd.Series({"證券代號": code, "投標開始日": target_time})
        return pd.concat([key_info, tw_indices, tw_margin, usa_market, tw_market])

    def save_checkpoint(self, base_data, new_data_list):
        """分段存檔，防止程式崩潰導致資料遺失"""
        if not new_data_list:
            return base_data
        
        new_df = pd.DataFrame(new_data_list)
        # 確保新資料與舊資料欄位對齊
        combined_df = pd.concat([base_data, new_df], ignore_index=True).drop_duplicates(
            subset=["證券代號", "投標開始日"], keep="last"
        )
        combined_df.to_csv(self.market_stmt_path, index=False, encoding="utf-8-sig")
        print(f"💾 已同步存檔至本地，目前累計新抓取 {len(new_data_list)} 筆。")
        return combined_df

    def run(self):
        # 1. 取得需要抓取的差異清單
        curr_data, diff_index, _ = search_index_list(
            self.raw_data_path, self.market_stmt_path, "證券代號", "投標開始日", FEATURE_COLS
        )
        
        if diff_index is None or len(diff_index) == 0:
            print("✅ 資料已是最新，無需抓取。")
            return

        tasks = deque(diff_index) # 使用隊列管理任務
        newly_captured = []
        last_hour = datetime.now().hour
        
        print(f"🚀 啟動雙 Token 模式，剩餘任務：{len(tasks)} 筆")

        # 2. 開始迴圈
        while tasks:
            # 整點重置計數
            current_hour = datetime.now().hour
            if current_hour != last_hour:
                for c in self.clients: c["usage"] = 0
                last_hour = current_hour

            # 檢查當前 Token 額度
            if self.current_client["usage"] >= self.max_safe_limit:
                print(f"💡 Token {self.current_idx + 1} 額度耗盡，嘗試切換...")
                all_exhausted = self.switch_client()
                if all_exhausted:
                    self.sleep_until_next_hour()
                continue

            # 取出任務
            code, target_time = tasks.popleft()
            
            try:
                print(f"[{self.current_idx + 1}] 正在抓取: {code} | {target_time.date()}", end=" ", flush=True)
                
                # 執行抓取 (傳入當前的 loader)
                combined = self.process_data(self.current_client["loader"], code, target_time)
                newly_captured.append(combined.to_dict())
                
                # 更新使用量 (此處假設 process_data 總共消耗 2 tokens)
                self.current_client["usage"] += 2
                print("✅")

                # 每 10 筆強制存檔一次
                if len(newly_captured) % 10 == 0:
                    curr_data = self.save_checkpoint(curr_data, newly_captured)

                # 隨機延遲 (多 Token 可稍微加速，但仍建議保留 1 秒以上)
                time.sleep(random.uniform(1.0, 2.0))

            except Exception as e:
                print(f"❌ 出錯: {e}")
                # 發生錯誤可考慮將任務放回隊列末尾重試，或記錄到錯誤日誌
                # tasks.append((code, target_time)) 
                time.sleep(5)
            finally:
                # 3. 最終存檔
                self.save_checkpoint(curr_data, newly_captured)


        print("🏁 所有任務處理完成！")

if __name__ == "__main__":    
    crawler = MarketCrawler()
    crawler.run()