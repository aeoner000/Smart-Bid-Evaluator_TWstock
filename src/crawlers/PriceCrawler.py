import os, sys, time, random
import pandas as pd
from pathlib import Path

# 強制將專案根目錄加入路徑
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from configs.price_cfg import PRICE_COLS, HEADERS
from src.utils.price import get_price_table, data_output
from src.utils.diff_index import search_index_list

class PriceCrawler:
    def __init__(self):
        self.save_folder = root_path / "data" / "raw_table"
        self.raw_data_path = self.save_folder / "bid_info.csv"
        self.price_info_path = self.save_folder / "history_price_info.csv"

    def run(self):
        # 1. 取得待處理名單 (透過 diff_index 工具)
        curr_data, diff_index, _ = search_index_list(
            self.raw_data_path, self.price_info_path, "證券代號", "投標開始日", PRICE_COLS
        )
        
        if diff_index is None or diff_index.empty:
            print("✅ 興櫃行情資料已是最新狀態。")
            return

        newly_captured = []
        print(f"🚀 開始執行興櫃行情抓取，共計 {len(diff_index)} 筆資料...")

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
                new_df = pd.DataFrame(newly_captured)
                # 確保數值欄位正確
                for col in PRICE_COLS:
                    if col in new_df.columns:
                        new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
                
                final_df = pd.concat([curr_data, new_df], ignore_index=True)
                # 最後檢查一次不重複
                final_df = final_df.drop_duplicates(subset=['證券代號', '投標開始日'], keep='last')
                
                # 確保資料夾存在
                self.save_folder.mkdir(parents=True, exist_ok=True)
                final_df.to_csv(self.price_info_path, index=False, encoding="utf-8-sig")
                print(f"💾 存檔成功！新增 {len(newly_captured)} 筆，存檔路徑: {self.price_info_path.name}")
            else:
                print("ℹ️ 本次執行無任何新抓取到的資料。")

if __name__ == "__main__":
    PriceCrawler().run()