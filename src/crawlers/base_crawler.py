import time
import random
import pandas as pd
from abc import ABC, abstractmethod
from pathlib import Path
import sys

# --- 路徑處理 ---
root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from src.db_base.db_manager import IPO_DAO
from src.db_base.schemas import TABLE_SCHEMAS
from src.utils.config_loader import DB_PATH, TEST_DB_PATH, DB_CONNECT_KWARGS

class BaseCrawler(ABC):
    """
    爬蟲基礎類別，封裝了通用的執行、差異比對和存檔邏輯。
    【v3 - 精準智慧基準日】
    """
    def __init__(self, table_name: str):
        if not table_name:
            raise ValueError("子類別必須提供 table_name")
        self.table_name = table_name
        self.key_cols = ["證券代號", "投標開始日"]
        self.dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        self.dao.ensure_table_exists(self.table_name)
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
            if hasattr(self, 'calculate_ratios'):
                cols_to_fix = [c for c in self.all_cols if c in success_df.columns and c not in self.key_cols]
                success_df[cols_to_fix] = success_df[cols_to_fix].apply(pd.to_numeric, errors='coerce')
                success_df = self.calculate_ratios(success_df)
            final_df = pd.concat([final_df, success_df], ignore_index=True)
        if fail_list:
            fail_df = pd.DataFrame(fail_list, columns=self.key_cols)
            fail_df = fail_df.reindex(columns=self.all_cols)
            final_df = pd.concat([final_df, fail_df], ignore_index=True)
        if not final_df.empty:
            for col in self.all_cols:
                if col not in self.key_cols and col in final_df.columns:
                    final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
            self.dao.save_data(final_df, self.table_name, if_exists="append")
            success_count = len(success_list) if success_list else 0
            fail_count = len(fail_list) if fail_list else 0
            print(f"✅ 存檔操作完成。處理成功: {success_count} 筆, 標記失敗: {fail_count} 筆。")
        else:
            print("⚠️ 本次無新資料或失敗標記需存檔。")

    def run(self, diff_index: list, max_rounds=5):
        """
        通用的主執行流程 (僅加入智慧基準日邏輯)。
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
            # 【新增】在迴圈外獲取一次今天日期，以提高效率
            today = pd.Timestamp.today().normalize()

            while fail_list and round_num < max_rounds:
                round_num += 1
                current_round_tasks = fail_list[:]
                print(f"\n--- 第 {round_num} 輪嘗試，剩餘 {len(current_round_tasks)} 筆 ---")
                
                for code, start_date in current_round_tasks:
                    bid_start_date = pd.to_datetime(start_date).normalize()

                    # --- 智慧基準日邏輯 --- #
                    if today < bid_start_date:
                        # 未來案件：以「今天」為基準日
                        base_date_for_crawling = today
                    else:
                        # 歷史或當日案件：以「投標開始日」為基準日
                        base_date_for_crawling = bid_start_date
                    
                    print(f"處理: {code} (投標日: {bid_start_date.date()}, 基準日: {base_date_for_crawling.date()})...", end=" ")
                    
                    # 【唯一的修改】將正確的基準日傳給 process_task
                    # 注意：process_task 的參數名稱是 start_date，但我們傳入的是 base_date_for_crawling
                    success, result = self.process_task(code, base_date_for_crawling)

                    if success:
                        row_data = {"證券代號": code, "投標開始日": start_date} # 存檔時仍用原始的 start_date
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
                self._save(success_list=successful_results, fail_list=[])
            else:
                self._save(success_list=successful_results, fail_list=fail_list)
            
            if fail_list:
                print(f"❌ 最終未完成筆數: {len(fail_list)}")
