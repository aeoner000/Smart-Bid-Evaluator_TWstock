import sys, joblib
from pathlib import Path
import pandas as pd
import numpy as np

# --- 基礎配置載入 ---
root_path = Path(__file__).resolve().parents[2]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS, SAVE_DIR
from src.db_base.db_manager import IPO_DAO
from src.utils.feature_utils import *
from .skew_transformer import SkewTransformer
from .feature_selector import FeatureSelector

FE_cfg = cfg["feature_engineer"]
CLEAN_TABLE = FE_cfg["clean_table"]
FC_cfg = FE_cfg["feature_cols"]
Y_COLS, X_COLS, ID_COLS = FC_cfg["target_variables"], FC_cfg["x_features"], FC_cfg["id_cols"]
TABLE_NAME_MAP = FC_cfg["target_variables_map"]

class FeatureEngineer:
    def __init__(self):
        self.dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        self.save_dir = SAVE_DIR

    def run(self): 
        try:    
            # 1. Pipeline 資料清理
            df = (self.dao.combine_all_feature(CLEAN_TABLE)
                .pipe(set_type).pipe(sort_by_date).pipe(remove_duplicates)
                .pipe(apply_growth_cap).pipe(fill_nan, CLEAN_TABLE)
                .pipe(add_is_miss, CLEAN_TABLE).pipe(add_new_feature)
                .pipe(handle_missing_data, CLEAN_TABLE))
            self.dao.save_data(df, "all_features", if_exists="replace")
        except Exception as e:
            print(f"❌ 清理失敗: {e}"); return 

        # 切分開發集與待預測集
        df_dev = df[df["status"] == "all_complete"]  
        df_pred = df[df["status"] != "all_complete"]
        
        split_idx = int(len(df_dev) * 0.8)
        train_base, test_base = df_dev.iloc[:split_idx], df_dev.iloc[split_idx:]
        
        # --- 階段一：全域偏態轉換 (用於 Feature Selection) ---
        print("🚀 執行全域偏態轉換...")
        global_x_st = SkewTransformer().fit(train_base[X_COLS])
        X_train_ref = global_x_st.transform(train_base[X_COLS])

        all_selected_features, all_skew_transformer, all_y_skew_transformer = {}, {}, {}

        for y in Y_COLS:
            try:
                print(f"\nProcessing Target: {y}")
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
                    # 欄位包含：選中的 30 個特徵名 + 目標變數名
                    empty_cols = f_selected + [y]
                    empty_df = pd.DataFrame(columns=empty_cols)
                    self.dao.save_data(empty_df, f"Predict_{y_eng}", if_exists="replace")
                    print(f"⚠️ 無預測資料，已建立 {y_eng} 的欄位空表。")

                # 6. 紀錄轉換器物件與名單
                all_selected_features[y] = f_selected
                all_skew_transformer[y] = final_x_st
                all_y_skew_transformer[y] = y_st

            except Exception as e:
                print(f"❌ 目標 {y} 處理失敗: {e}")

        # 序列化儲存
        joblib.dump(all_selected_features, self.save_dir / "all_selected_features.joblib")
        joblib.dump(all_skew_transformer, self.save_dir / "all_skew_transformer.joblib")
        joblib.dump(all_y_skew_transformer, self.save_dir / "all_y_skew_transformer.joblib")
        
        return self

if __name__ == "__main__":
    FeatureEngineer().run()