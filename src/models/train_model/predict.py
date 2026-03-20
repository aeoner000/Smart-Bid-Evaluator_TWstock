'''
載入已訓練好的模型，對新資料進行預測，並將結果存入獨立的資料表中。
'''
import sys
from pathlib import Path
import pandas as pd
import joblib
import logging

# --- 路徑處理 ---
# 確保無論從哪裡執行，都能找到專案根目錄
try:
    root_path = Path(__file__).resolve().parents[3]
except NameError:
    # 如果在交互式環境中執行，__file__ 可能未定義
    root_path = Path.cwd()

if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS, SAVE_DIR
from src.db_base.db_manager import IPO_DAO

# --- 日誌設定 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PredictPipeline")

# --- 載入設定 ---
FE_cfg = cfg["feature_engineer"]
FC_cfg = FE_cfg["feature_cols"]
Y_COLS = FC_cfg["target_variables"]          # 中文目標列表
TABLE_NAME_MAP = FC_cfg["target_variables_map"] # 中文 -> 英文表格名
ID_COLS = FC_cfg["id_cols"]                  # ['證券代號', '證券名稱', '投標開始日']


class Predictor:
    def __init__(self):
        self.dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        self.save_dir = SAVE_DIR
        self.models = {}
        self.y_transformers = {} # 儲存每個目標對應的轉換器
        self.all_selected_features = None

        self._load_artifacts()

    def _load_artifacts(self):
        logger.info("--- 正在載入預測所需元件 ---")
        try:
            # 1. 載入特徵列表
            features_path = self.save_dir / "all_selected_features.joblib"
            self.all_selected_features = joblib.load(features_path)
            
            # 2. 載入偏態轉換器字典 (內含各個目標變數的 SkewTransformer 實體)
            skew_path = self.save_dir / "all_y_skew_transformer.joblib"
            self.y_transformers = joblib.load(skew_path)
            logger.info("✅ 偏態轉換器 (all_y_skew_transformer.joblib) 載入成功。")
            
            # 3. 載入為每個目標訓練好的模型
            for y_chinese in Y_COLS:
                y_english = TABLE_NAME_MAP[y_chinese]
                model_path = self.save_dir / f"{y_english}_best_model.joblib"
                
                loaded_obj = joblib.load(model_path)
                self.models[y_chinese] = loaded_obj['model']
                
            logger.info(f"✅ 所有 {len(self.models)} 個預測模型載入成功。")

        except FileNotFoundError as e:
            logger.error(f"❌ 載入預測所需檔案時發生錯誤: {e}")
            raise 

    def run(self):
        logger.info("\n--- 開始執行預測流程 ---")

        for y_chinese in Y_COLS:
            try:
                y_english = TABLE_NAME_MAP[y_chinese]
                logger.info(f"\n>> 正在處理目標：{y_chinese}")
                
                # 1. 讀取待預測資料 (假設 Predict_ 表中的特徵已完成偏態處理)
                pred_table_name = f"Predict_{y_english}"
                pred_data = self.dao.fetch_all(pred_table_name)

                if pred_data.empty: continue
                
                # 2. 準備模型輸入 (X)
                model = self.models[y_chinese]
                selected_features = self.all_selected_features[y_chinese]
                X_pred = pred_data[selected_features]

                # 3. 執行預測 (得到的是轉換後的數值)
                preds_transformed = model.predict(X_pred)

                # 4. 【核心步驟】執行偏態還原
                # 將預測結果轉為帶有名稱的 Series，觸發 SkewTransformer 的還原邏輯
                preds_series = pd.Series(
                    preds_transformed.flatten(), 
                    index=pred_data.index, 
                    name=y_chinese
                )
                
                transformer = self.y_transformers.get(y_chinese)
                if transformer:
                    # 轉回原始尺度 (例如：從 0.5 轉回 1.25)
                    predictions_final = transformer.inverse_transform(preds_series)
                    logger.info("   - ✅ 已成功將預測結果還原至原始尺度。")
                else:
                    predictions_final = preds_series
                    logger.warning(f"   - ⚠️ 找不到 {y_chinese} 的轉換器，使用原始預測值。")

                # 5. 準備結果 DataFrame
                result_df = pred_data[ID_COLS].copy()
                result_df[f'predicted_{y_chinese}'] = predictions_final
                result_df[f'predicted_{y_chinese}'] = result_df[f'predicted_{y_chinese}'].astype('float64').round(4)
                # float64才會正確限制小數點
                
                # 6. 存入資料庫
                result_table_name = f"Result_{y_english}"
                self.dao.save_data(result_df, result_table_name, if_exists="replace")
                
                logger.info(f"   - ✅ 預測結果已存入: '{result_table_name}'")

            except Exception as e:
                logger.error(f"   - ❌ 處理目標 {y_chinese} 時發生錯誤: {e}")
                continue


if __name__ == "__main__":
    try:
        predictor = Predictor()
        predictor.run()
    except Exception as e:
        # 主要捕捉 _load_artifacts 中的 FileNotFoundError
        logger.error(f"\n程式執行失敗。請根據上方錯誤訊息修正問題後再試。")
