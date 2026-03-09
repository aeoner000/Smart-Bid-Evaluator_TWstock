'''
生成目標變數表
'''
import time, random, sys, os
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from FinMind.data import DataLoader
from dotenv import load_dotenv
import traceback

from configs.target_cfg import FEATURE_COLS 
from src.utils.diff_index import search_index_list
from src.utils.target import get_target_value, cal_y_feature

class TargetCrawler:
    def __init__(self):
        # --- 環境變數與路徑 ---
        root_path = Path(__file__).resolve().parent.parent.parent
        if str(root_path) not in sys.path:
            sys.path.insert(0, str(root_path))
        # 1. 完全依照你給的變數與路徑定義
        self.result_folder = root_path / "data" / "raw_table"
        self.raw_data_path = self.result_folder / "bid_info.csv"
        self.target_variable_path = self.result_folder / "target_variable.csv"
        
        self.code_col = "證券代號"
        self.time_col = ["投標開始日", "撥券日期(上市、上櫃日期)"] # 保持 List 傳入
        self.feature_cols = FEATURE_COLS

        # 2. Token 與 API 狀態 (雙 Token 自動切換)
        load_dotenv()
        self.api_tokens = [os.getenv("FINMIND"), os.getenv("FINMIND2")]
        self.tokens = [t for t in self.api_tokens if t]
        self.token_index = 0
        self.MAX_CALLS_PER_HOUR = 595
        self.call_count = 0
        self.current_hour = datetime.now().hour
        
        # 3. 初始化 DataLoader (排除 Log)
        self.dl = DataLoader()
        self._login()

    def _login(self):
        """登入當前的 Token"""
        if not self.tokens:
            print("❌ 錯誤：找不到任何有效的 API Token")
            return
        token = self.tokens[self.token_index]
        self.dl.login_by_token(api_token=token)
        print(f"🔑 已登入 Token {self.token_index + 1}")

    def run(self):
        """執行主邏輯，完全保留你的 main 結構"""
        start_time = time.time()

        # --- 解決 ValueError 預處理 ---
        # 為了讓 search_index_list 傳入 List 不報錯，我們先手動將 CSV 內的日期轉好
        if self.raw_data_path.exists():
            temp_df = pd.read_csv(self.raw_data_path, encoding="utf-8-sig")
            for col in self.time_col:
                if col in temp_df.columns:
                    temp_df[col] = pd.to_datetime(temp_df[col].astype(str), errors='coerce')
            temp_df.to_csv(self.raw_data_path, index=False, encoding="utf-8-sig")

        # 4. 呼叫 search_index_list (完全維持你給的參數)
        curr_data, diff_index, raw_data = search_index_list(
            self.raw_data_path, 
            self.target_variable_path, 
            self.code_col, 
            self.time_col, 
            self.feature_cols
        )

        if diff_index is None or diff_index.empty:
            print("✅ 目標變數資料已是最新。")
            return

        dflist = []
        print(f"🚀 開始抓取，待處理筆數: {len(diff_index)}")

        try:
            # 5. 主迴圈 (保留你的解構方式：code, bid_date, list_date)
            for code, bid_date, list_date in diff_index:
                now = datetime.now()
                
                # 整點重置
                if now.hour != self.current_hour:
                    self.current_hour = now.hour
                    self.call_count = 0
                    print(f"API 次數已重置")

                # API 額度控管與 Token 切換
                if self.call_count >= self.MAX_CALLS_PER_HOUR:
                    if self.token_index < len(self.tokens) - 1:
                        self.token_index += 1
                        self.call_count = 0
                        self._login()
                    else:
                        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0)
                        wait_seconds = (next_hour - now).total_seconds()
                        print(f"已達上限 {self.MAX_CALLS_PER_HOUR} 次，休息 {int(wait_seconds)} 秒...")
                        time.sleep(max(wait_seconds, 1))
                        self.token_index = 0
                        self.call_count = 0
                        self.current_hour = datetime.now().hour
                        self._login()

                self.call_count += 1
                
                try:
                    # 使用 .date() 格式化輸出，保持畫面乾淨
                    print(f"股票代號: {code}, 投標開始日期: {bid_date.date()}, 上市日期: {list_date.date()}")
                    
                    # 抓取資料 (變數名稱依據你的原始碼)
                    data = get_target_value(self.dl, code, list_date, self.feature_cols)
                    
                    if data is None:
                        print("===> 無資料")
                        continue
           
                    data = cal_y_feature(data, raw_data, code, bid_date)
                    data.insert(0, self.time_col[1], list_date)
                    data.insert(0, "投標開始日", bid_date)
                    data.insert(0, "證券代號", code)

                    # 保留原始的 dict 輸出
                    print(data.to_dict(orient='records')[0])

                    dflist.append(data)
                    time.sleep(random.uniform(1, 2))
                    
                except Exception as e:
                    # traceback.print_exc()
                    print(f"❌ 處理錯誤 ({code}): {e}")

        except Exception as e:
            print(f"💥 程式中斷: {e}")

        finally:
            # 6. 存檔邏輯 (完全保留你的處理方式)
            if dflist:
                new_df = pd.concat(dflist, ignore_index=True)
                if curr_data.empty:
                    curr_data = new_df
                else:
                    curr_data = pd.concat([curr_data, new_df], ignore_index=True)
                
                # 數值轉換與去重 (去重時使用完整 Key)
                curr_data[self.feature_cols] = curr_data[self.feature_cols].apply(pd.to_numeric, errors='coerce')
                curr_data = curr_data.drop_duplicates(subset=[self.code_col] + self.time_col, keep='last')
                
                self.target_variable_path.parent.mkdir(parents=True, exist_ok=True)
                curr_data.to_csv(self.target_variable_path, index=False, encoding="utf-8-sig")
                print(f"💾 存檔完成，新增 {len(dflist)} 筆。總筆數: {len(curr_data)}")
            else:
                print("**此次無資料**")

            print(f"**完成** --> 花費時間: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    # 從 .env 讀取 Token
    load_dotenv()
    tokens = [os.getenv("FINMIND"), os.getenv("FINMIND2")]
    
    # 執行 (路徑依照你的 data 資料夾位置)
    crawler = TargetCrawler()
    crawler.run()