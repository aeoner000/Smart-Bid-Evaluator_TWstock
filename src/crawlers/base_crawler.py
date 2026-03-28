import sys
import time
import random
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parents[2]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

# 改造點：從新的 db_manager 和舊的 schemas 導入
from src.db_base.db_manager import get_db_manager
from src.db_base.schemas import TABLE_SCHEMAS

# 說明：不再需要從 config_loader 導入 DB_PATH 等，也不再需要 sys.path 補丁

class BaseCrawler(ABC):
    """
    爬蟲基礎類別，封裝了通用的執行、差異比對和存檔邏輯。
    """
    def __init__(self, table_name: str):
        if not table_name:
            raise ValueError("子類別必須提供 table_name")
        self.table_name = table_name
        self.key_cols = ["證券代號", "投標開始日"]
        
        # 改造點：使用新的工廠函式來獲取 DAO
        self.dao = get_db_manager()
        
        # 說明：ensure_table_exists 不再由此處調用。
        # 它應由 db_manager 內部或專門的 schema 管理腳本處理。
        
        # 待辦：TABLE_SCHEMAS 仍是硬式編碼，未來可改由 DAO 提供欄位資訊
        self.all_cols = [col[0] for col in TABLE_SCHEMAS[self.table_name]]
        
    @abstractmethod
    def process_task(self, code: str, start_date: pd.Timestamp) -> tuple[bool, dict | str]:
        """
        【子類別必須實作此方法】
        定義單一任務的具體抓取邏輯。
        - 回傳 (True, result_dict) 代表成功。
        - 回傳 (False, error_message) 代表失敗。
        """
        pass

    def _save(self, success_list: list, fail_list: list):
        """
        統一存檔邏輯 (保持不變)。
        """
        final_df = pd.DataFrame()
        if success_list:
            success_df = pd.DataFrame(success_list)
            if "證券代號" in success_df.columns:
                success_df["證券代號"] = success_df["證券代號"].astype(str)
            if hasattr(self, 'calculate_ratios'): #　檢查這個self裡面，有沒有一個叫做 'calculate_ratios' 的方法或屬性-->fin 中的
                cols_to_fix = [c for c in self.all_cols if c in success_df.columns and c not in self.key_cols]
                success_df[cols_to_fix] = success_df[cols_to_fix].apply(pd.to_numeric, errors='coerce')
                success_df = self.calculate_ratios(success_df)
            final_df = pd.concat([final_df, success_df], ignore_index=True)

        if fail_list:         
            fail_df = pd.DataFrame(fail_list, columns=self.key_cols)

            fail_df = fail_df.reindex(columns=self.all_cols)
            final_df = pd.concat([final_df, fail_df], ignore_index=True)
        if not final_df.empty:
            actual_date_cols = [c for c in final_df.columns 
                                if any(key in c for key in ["日期", "開始日", "結束日", "update_time"])]
            for col in final_df.columns:
                if col in actual_date_cols:
                    # 日期處理：強制截斷到微秒 [us] 精度
                    final_df[col] = pd.to_datetime(final_df[col], errors='coerce') \
                                      .dt.tz_localize(None).dt.floor('us').astype('datetime64[us]')
                
                elif col == "證券代號":
                    # 證券代號處理：絕對要是字串 (String)
                    final_df[col] = final_df[col].astype(str)
                
                elif col not in self.key_cols:
                    # 數值處理：其餘獲利率、成交量等才轉 Numeric
                    final_df[col] = pd.to_numeric(final_df[col], errors='coerce')

            self.dao.save_data(final_df, self.table_name, if_exists="append")
  
            success_count = len(success_list)
            fail_count = len(fail_list)
            print(f"✅ 存檔操作完成。處理成功: {success_count} 筆, 標記失敗: {fail_count} 筆。")
        else:
            print("⚠️ 本次無新資料或失敗標記需存檔。")

    def run(self, diff_index: list, max_rounds=5):
        """
        通用的主執行流程。
        """
        if not diff_index:
            print(f"✅ ({self.table_name}) 無任何新任務需要執行。")
            return

        successful_results = []
        fail_list = list(diff_index) # 預設都是失敗
        was_interrupted = False      # 是不是中斷
        print(f">>> ({self.table_name}) 準備處理 {len(fail_list)} 筆新資料...")

        try:
            round_num = 0
            today = pd.Timestamp.today().normalize()

            while fail_list and round_num < max_rounds:
                round_num += 1
                current_round_tasks = fail_list[:]
                print(f"\n--- 第 {round_num} 輪嘗試，剩餘 {len(current_round_tasks)} 筆 ---")
                
                for code, start_date in current_round_tasks:
                    bid_start_date = pd.to_datetime(start_date).normalize()

                    if today < bid_start_date:
                        base_date_for_crawling = today
                    else:
                        base_date_for_crawling = bid_start_date
                    
                    print(f"處理: {code} (投標日: {bid_start_date.date()}, 基準日: {base_date_for_crawling.date()})...", end=" ")
                    
                    success, result = self.process_task(code, base_date_for_crawling)

                    if success:
                        row_data = {"證券代號": code, "投標開始日": start_date}
                        if isinstance(result, dict):
                            row_data.update(result)
                        successful_results.append(row_data)
                        fail_list.remove((code, start_date))
                        print("成功")
                        if len(successful_results) >= 50:
                            print("\n--- 批次存檔 ---")
                            self._save(success_list=successful_results, fail_list=[])
                            successful_results.clear()
                            print("--- 繼續處理 ---")
                    else:
                        print(f"失敗: {result}")

                    time.sleep(random.uniform(10, 15))

        except KeyboardInterrupt:
            was_interrupted = True
            print("\n⚠️ 使用者中斷執行。")
        except Exception as e:
            was_interrupted = True
            print(f"\n❌ 執行中發生嚴重錯誤: {e}")
        
        finally:
            print("\n💾 執行最終存檔...")
            if was_interrupted:
                # 如果是中斷，只保存成功的
                self._save(success_list=successful_results, fail_list=[])
            else:
                # 如果是正常結束，保存成功和剩餘失敗的
                self._save(success_list=successful_results, fail_list=fail_list)
            
            if fail_list:
                print(f"❌ 最終未完成筆數: {len(fail_list)}")
