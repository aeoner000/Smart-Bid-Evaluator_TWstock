'''
生成目標變數表
'''
import time, random, sys, os
import pandas as pd
from pathlib import Path

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS
from src.utils.target import get_target_value, cal_y_feature
from src.utils.finmind_manager import FinMindManager
from src.database.db_manager import IPO_DAO

target_cfg = cfg["crawlers"]["target"]
FEATURE_COLS = target_cfg["feature_cols"]

class TargetCrawler:
    def __init__(self):
        # --- 定義欄位 ---
        self.code_col = "證券代號"
        self.time_col = ["投標開始日", "撥券日期(上市、上櫃日期)"] # 保持 List 傳入
        self.feature_cols = FEATURE_COLS

        # Database access
        self.dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        self.table_name = "target_variable"
        self.dao.ensure_table_exists(self.table_name)

        # 初始化 FinMind API 管理器
        self.fm = FinMindManager()

    def run(self):
        """執行主邏輯，完全保留你的 main 結構"""
        start_time = time.time()

        # 1. 讀取來源與目標資料
        raw_data = self.dao.fetch_all("bid_info")
        if raw_data.empty:
            print("❌ 找不到來源資料 bid_info，無法繼續。")
            return
        
        curr_data = self.dao.fetch_all(self.table_name)

        # 2. 決定抓取模式
        key_cols = [self.code_col] + self.time_col
        if curr_data.empty:
            print(f">>> [模式：初次全量] '{self.table_name}' 為空，準備進行首次完整抓取...")
            for col in self.time_col:
                raw_data[col] = pd.to_datetime(raw_data[col])
            diff_index = [tuple(x) for x in raw_data[key_cols].to_numpy()]
        else:
            print(f">>> [模式：增量更新] '{self.table_name}' 已有資料，進行差異比對...")
            _, diff_index = self.dao.diff_index(
                raw_table="bid_info",
                target_table=self.table_name,
                key_cols=key_cols,
            )

        if not diff_index:
            print("✅ 目標變數資料已是最新。")
            return

        dflist = []
        print(f"🚀 開始抓取，待處理筆數: {len(diff_index)}")

        try:
            # 5. 主迴圈 (保留你的解構方式：code, bid_date, list_date)
            for code, bid_date, list_date in diff_index:
                try:
                    # 使用 .date() 格式化輸出，保持畫面乾淨
                    print(f"股票代號: {code}, 投標開始日期: {bid_date.date()}, 上市日期: {list_date.date()}")
                    
                    # 1. 從管理器獲取可用的 loader
                    loader = self.fm.get_loader()

                    # 抓取資料 (變數名稱依據你的原始碼)
                    data = get_target_value(loader, code, list_date, self.feature_cols)
                    
                    # 2. 增加使用計數 (此爬蟲一次消耗 1 個 API call)
                    self.fm.add_usage(1)
                    
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
                    print(f"❌ 處理錯誤 ({code}): {e}")

        except Exception as e:
            print(f"💥 程式中斷: {e}")

        finally:
            # 6. 存檔邏輯 (完全保留你的處理方式)
            if dflist:
                print(f"\n💾 執行最終存檔...")
                new_df = pd.concat(dflist, ignore_index=True)
                
                # 數值轉換
                new_df[self.feature_cols] = new_df[self.feature_cols].apply(pd.to_numeric, errors='coerce')

                # 直接 append 新資料
                self.dao.save_data(new_df, self.table_name, if_exists="append")
                print(f"✅ 存檔完成！本次新增 {len(dflist)} 筆資料至 '{self.table_name}'。")
            else:
                print("\n**此次無資料可存檔**")

            print(f"**完成** --> 花費時間: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    # 執行 (路徑依照你的 data 資料夾位置)
    crawler = TargetCrawler()
    crawler.run()