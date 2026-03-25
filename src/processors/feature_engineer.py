
import sys, joblib, logging
from pathlib import Path
import pandas as pd
import numpy as np
import warnings

# --- Import Logger Config ---
from src.utils.logger_config import init_logger

# --- 日誌與路徑標準化 ---
logger = logging.getLogger(__name__)

# 精準過濾：只針對包含 "BigQuery Storage module not found" 關鍵字的 UserWarning 進行忽略
warnings.filterwarnings("ignore", message=".*BigQuery Storage module not found.*")
# --- 基礎配置載入 ---
root_path = Path(__file__).resolve().parents[2]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.config_loader import config
from src.db_base.db_manager import get_db_manager
from src.utils.feature_utils import *
from src.processors.skew_transformer import SkewTransformer
from src.processors.feature_selector import FeatureSelector
from src.utils.storage_handler import get_storage_handler

FE_cfg = config["feature_engineer"]
CLEAN_TABLE = FE_cfg["clean_table"]
FC_cfg = FE_cfg["feature_cols"]
Y_COLS, X_COLS, ID_COLS = FC_cfg["target_variables"], FC_cfg["x_features"], FC_cfg["id_cols"]
TABLE_NAME_MAP = FC_cfg["target_variables_map"]

class FeatureEngineer:
    def __init__(self):
        self.dao = get_db_manager()
        self.storage = get_storage_handler()
        self.skew_path = config['paths']['skew_path']
        self.metadata_path = config['paths']['metadata_path']


    # 改造點：新增的內部方法，用 pandas 取代 SQL JOIN
    def _combine_features_in_pandas(self, config_dict: dict, merge_keys=['證券代號', '投標開始日']):
        table_names = list(config_dict.keys())
        main_table_name = table_names[0]
        other_tables_name = table_names[1:]

        # 1. 讀取主表
        main_df = self.dao.fetch_all(main_table_name)
        if main_df.empty:
            raise ValueError(f"主資料表 {main_table_name} 為空，無法進行特徵組合。")

        # 確保合併鍵的類型正確
        main_df['投標開始日'] = pd.to_datetime(main_df['投標開始日'])
        
        # 2. 依序合併其他特徵表
        merged_df = main_df.copy()
        for table_name in other_tables_name:
            feature_df = self.dao.fetch_all(table_name)
            if table_name == "target_variable": # 不要跟bid_info中重複
                feature_df = feature_df.drop(columns=['撥券日期_上市_上櫃日期'], errors='ignore')
            if feature_df.empty:
                logger.warning(f"Feature table {table_name} is empty, skipping merge.")
                continue
            
            # 確保合併鍵的類型正確
            if '投標開始日' in feature_df.columns:
                feature_df['投標開始日'] = pd.to_datetime(feature_df['投標開始日'])

            # 執行合併
            merged_df = pd.merge(merged_df, feature_df, on=merge_keys, how='left')
        return merged_df

    def run(self): 
        try:    
            # 1. Pipeline 資料清理
            # 改造點：呼叫新的內部方法
            df = (self._combine_features_in_pandas(CLEAN_TABLE)
                .pipe(set_type).pipe(sort_by_date).pipe(remove_duplicates)
                .pipe(apply_growth_cap).pipe(fill_nan, CLEAN_TABLE)
                .pipe(add_is_miss, CLEAN_TABLE).pipe(add_new_feature)
                .pipe(handle_missing_data, CLEAN_TABLE))

            self.dao.save_data(df, "all_features", if_exists="replace")
        except Exception as e:
            logger.error(f"Data cleaning pipeline failed: {e}", exc_info=True)
            return 

        # 切分開發集與待預測集
        df_dev = df[df["status"] == "all_complete"]  
        df_pred = df[df["status"] != "all_complete"]
        
        split_idx = int(len(df_dev) * 0.8)
        train_base, test_base = df_dev.iloc[:split_idx], df_dev.iloc[split_idx:]
        
        # --- 階段一：全域偏態轉換 (用於 Feature Selection) ---
        logger.info("Starting global skew transformation...")
        global_x_st = SkewTransformer().fit(train_base[X_COLS])
        X_train_ref = global_x_st.transform(train_base[X_COLS])

        all_selected_features, all_skew_transformer, all_y_skew_transformer = {}, {}, {}

        for y in Y_COLS:
            try:
                logger.info(f"Processing Target: {y}")
                y_eng = TABLE_NAME_MAP[y]

                # 1. 訓練目標變數 Y 的轉換器
                y_st = SkewTransformer().fit(train_base[[y]])
                y_train_trans = y_st.transform(train_base[[y]])
                y_test_trans = y_st.transform(test_base[[y]])

                # 2. 特徵選擇
                selector = FeatureSelector(n_selected=30)
                selector.fit(X_train_ref.join(y_train_trans), X_COLS, y) 
                f_selected = selector.selected_features

                # 3. 正式轉換器 (針對選出的 30 個特徵重新 Fit)
                final_x_st = SkewTransformer().fit(train_base[f_selected])
                
                # 4. 儲存訓練與測試集 (包含選中的 X 與 轉換後的 Y)
                train_out = final_x_st.transform(train_base[f_selected]).join(y_train_trans)
                test_out = final_x_st.transform(test_base[f_selected]).join(y_test_trans)
                
                self.dao.save_data(train_out, f"Train_{y_eng}", if_exists="replace")
                self.dao.save_data(test_out, f"Test_{y_eng}", if_exists="replace")

                # 5. 【核心邏輯】處理預測資料 (Predict Table)
                if not df_pred.empty:
                    # 如果有待預測資料，轉換後存入
                    df_pred = df_pred.copy()
                    df_pred[f_selected] = final_x_st.transform(df_pred[f_selected])
                    p_out = df_pred[ID_COLS + f_selected]
                    # 依據你的需求，預測表通常不需要 y，但如果需要空欄位可以 join
                    self.dao.save_data(p_out, f"Predict_{y_eng}", if_exists="replace")
                else:
                    # 如果沒有待預測資料，存一張只有欄位名稱的「空表」
                    empty_cols = f_selected + [y]
                    empty_df = pd.DataFrame(columns=empty_cols)
                    self.dao.save_data(empty_df, f"Predict_{y_eng}", if_exists="replace")
                    logger.warning(f"No prediction data found. Created an empty table for {y_eng}.")

                # 6. 紀錄轉換器物件與名單
                all_selected_features[y] = f_selected
                all_skew_transformer[y] = final_x_st
                all_y_skew_transformer[y] = y_st

            except Exception as e:
                logger.error(f"Failed to process target {y}: {e}", exc_info=True)
        if all_selected_features: # 至少有一個成功才存檔
            try:
                self.storage.save_file(all_selected_features, str(Path(self.skew_path).parent / "all_selected_features.joblib"))
                self.storage.save_file(all_skew_transformer, str(Path(self.skew_path).parent / "all_skew_transformer.joblib"))
                self.storage.save_file(all_y_skew_transformer, str(Path(self.skew_path).parent / "all_y_skew_transformer.joblib"))
                logger.info("All transformers and feature lists have been saved successfully.")
            except Exception as e:
                logger.error(f"Failed to save joblib files: {e}", exc_info=True)
        else:
            logger.error("No target variables were processed successfully, skipping save.")
            
    
        
        return self

if __name__ == "__main__":
    init_logger() # Initialize logging
    FeatureEngineer().run()
