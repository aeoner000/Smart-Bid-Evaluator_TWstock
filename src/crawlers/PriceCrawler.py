import os, sys, time, random
import pandas as pd
from pathlib import Path

# 強制將專案根目錄加入路徑
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS
from src.utils.price import get_price_table, data_output
from src.database.db_manager import IPO_DAO

price_cfg = cfg["crawlers"]["price"]
PRICE_COLS = price_cfg["price_cols"]
HEADERS = price_cfg["headers"]

class PriceCrawler:
    def __init__(self):
        self.dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        self.table_name = "history_price_info"
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
            print("✅ 興櫃行情資料已是最新。")
            return

        newly_captured = []
        print(f"🚀 開始執行興櫃行情抓取，待處理筆數: {len(diff_index)}")

        try:
            for code, target_date in diff_index:
                print(f"🔍 處理代號: {code} | 基準日: {target_date.date()}", end=" ")
                
                # 抓取資料 (內部已完成清洗與日期轉換)
                df = get_price_table(code, target_date.year, target_date.month, HEADERS)
                
                # 計算指標
                re = data_output(df, target_date)
                
                if re:
                    row_dict = {"證券代號": code, "投標開始日": target_date, **re}
                    newly_captured.append(row_dict)
                    print("✅ 成功")
                else:
                    print("⚠️ 無法獲取足夠行情資料")

                # 禮貌爬蟲，避免被鎖
                time.sleep(random.uniform(4, 7))

        except KeyboardInterrupt:
            print("\n🛑 偵測到使用者中斷，準備存檔...")
        except Exception as e:
            print(f"\n❌ 程式發生非預期錯誤: {e}")
        
        finally:
            # 2. 存檔邏輯：只要有抓到東西就合併存檔
            if newly_captured:
                print("\n💾 執行最終存檔...")
                new_df = pd.DataFrame(newly_captured)
                # 確保數值欄位正確
                for col in PRICE_COLS:
                    if col in new_df.columns:
                        new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
                
                self.dao.save_data(new_df, self.table_name, if_exists="append")
                print(f"✅ 存檔成功！本次新增 {len(newly_captured)} 筆資料至 '{self.table_name}'。")
            else:
                print("ℹ️ 本次執行無任何新抓取到的資料。")

if __name__ == "__main__":
    PriceCrawler().run()