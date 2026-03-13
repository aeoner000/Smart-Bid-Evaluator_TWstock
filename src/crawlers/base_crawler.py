import time
import random
import pandas as pd
from abc import ABC, abstractmethod
from pathlib import Path
import sys

# --- 路徑處理 ---
root_dir = Path(__file__).resolve().parent.parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from src.db_base.db_manager import IPO_DAO
from src.db_base.schemas import TABLE_SCHEMAS
from src.utils.config_loader import DB_PATH, TEST_DB_PATH, DB_CONNECT_KWARGS

class BaseCrawler(ABC):
    """
    爬蟲基礎類別，封裝了通用的執行、差異比對和存檔邏輯。
    """
    def __init__(self, table_name: str):
        if not table_name:
            raise ValueError("子類別必須提供 table_name")
        self.table_name = table_name
        self.key_cols = ["證券代號", "投標開始日"]
        self.dao = IPO_DAO(TEST_DB_PATH, **DB_CONNECT_KWARGS)
        self.dao.ensure_table_exists(self.table_name)
        # 從 schema 取得此表格的完整欄位列表
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
        統一存檔邏輯，可同時處理成功與失敗的項目。
        """
        final_df = pd.DataFrame()

        # 處理成功項目
        if success_list:
            success_df = pd.DataFrame(success_list)
            # 在合併前進行數值轉換與比率計算 (如果子類別有實作)
            if hasattr(self, 'calculate_ratios'):
                cols_to_fix = [c for c in self.all_cols if c in success_df.columns and c not in self.key_cols]
                success_df[cols_to_fix] = success_df[cols_to_fix].apply(pd.to_numeric, errors='coerce')
                success_df = self.calculate_ratios(success_df)
            final_df = pd.concat([final_df, success_df], ignore_index=True)

        # 處理失敗項目
        if fail_list:
            fail_df = pd.DataFrame(fail_list, columns=self.key_cols)
            fail_df = fail_df.reindex(columns=self.all_cols)
            final_df = pd.concat([final_df, fail_df], ignore_index=True)

        # 執行存檔
        if not final_df.empty:
            # 在存檔前對數值欄位進行最後的格式化
            for col in self.all_cols:
                if col not in self.key_cols and col in final_df.columns:
                    final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
            
            self.dao.save_data(final_df, self.table_name, if_exists="append")
            success_count = len(success_list) if success_list else 0
            fail_count = len(fail_list) if fail_list else 0
            print(f"✅ 存檔操作完成。處理成功: {success_count} 筆, 標記失敗: {fail_count} 筆。")
        else:
            print("⚠️ 本次無新資料或失敗標記需存檔。")


    def run(self, max_rounds=2):
        """
        通用的主執行流程。
        """
        raw_df = self.dao.fetch_all("bid_info")
        if raw_df.empty:
            print("❌ 找不到來源資料 bid_info，無法繼續。")
            return
        
        _, diff_index = self.dao.diff_index(
            raw_table="bid_info",
            target_table=self.table_name,
            key_cols=self.key_cols,
        )

        if len(diff_index) == 0:
            print(f"✅ 資料已是最新 ({self.table_name})。")
            return

        successful_results = []
        tasks_to_run = list(diff_index)
        fail_list = list(diff_index) # 預設所有任務都可能失敗
        was_interrupted = False      # 是否為自行中斷，預設 False
        print(f">>> ({self.table_name}) 準備處理 {len(tasks_to_run)} 筆新資料...")

        try:
            # --- 多輪嘗試邏輯 (與 financialcrawler 相同) ---
            round_num = 0
            while fail_list and round_num < max_rounds:
                round_num += 1
                current_round_tasks = fail_list[:] # 複製一份本輪要跑的
                
                print(f"\n--- 第 {round_num} 輪嘗試，剩餘 {len(current_round_tasks)} 筆 ---")
                
                for code, start_date in current_round_tasks:
                    print(f"處理: {code} ({start_date.date()})...", end=" ")
                    
                    success, result = self.process_task(code, start_date)

                    if success:
                        row_data = {"證券代號": code, "投標開始日": start_date}
                        if isinstance(result, dict):
                            row_data.update(result)
                        successful_results.append(row_data)
                        fail_list.remove((code, start_date)) # 成功，從失敗列表中移除
                        print("成功")
                        
                        # 批次存檔邏輯 (可選)
                        if len(successful_results) >= 50:
                            print("\n--- 批次存檔 ---")
                            self._save(success_list=successful_results, fail_list=[]) # 只存成功的
                            successful_results.clear()
                            print("--- 繼續處理 ---")

                    else:
                        print(f"失敗: {result}")

                    time.sleep(random.uniform(3, 6))

        except KeyboardInterrupt:
            was_interrupted = True
            print("\n⚠️ 使用者中斷執行。")
        except Exception as e:
            was_interrupted = True
            print(f"\n❌ 執行中發生嚴重錯誤: {e}")
        
        finally:
            print("\n💾 執行最終存檔...")
            if was_interrupted:
                # 自行中斷不存fail
                self._save(success_list=successful_results, fail_list=[])
            else:
                # 最後存檔時，傳入剩餘的成功項目和最終的失敗列表
                self._save(success_list=successful_results, fail_list=fail_list)
            
            if fail_list:
                print(f"❌ 最終未完成筆數: {len(fail_list)}")