import sys
import logging
from pathlib import Path
import pandas as pd

# --- 路徑處理 ---
try:
    root_path = Path(__file__).resolve().parents[3]
except NameError:
    root_path = Path.cwd()

if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

# --- 導入專案模組 ---
from src.utils.logger_config import init_logger
from src.utils.config_loader import config
from src.db_base import get_db_manager
from src.utils.storage_handler import get_storage_handler

# --- 日誌設定 ---
logger = logging.getLogger(__name__)

# --- 載入設定 ---
WEIGHTS_DIR = config['paths']['weights_dir']
FE_cfg = config["feature_engineer"]
FC_cfg = FE_cfg["feature_cols"]
Y_COLS = FC_cfg["target_variables"]
TABLE_NAME_MAP = FC_cfg["target_variables_map"]
ID_COLS = FC_cfg["id_cols"]


class Predictor:
    '''執行預測流程，根據設定從 GCS 或本地載入模型。'''
    def __init__(self):
        logger.info("Initializing Predictor...")
        self.dao = get_db_manager()
        self.storage_handler = get_storage_handler()
        self.models = {}
        self.y_transformers = {}
        self.all_selected_features = None

        self._load_artifacts()

    def _load_artifacts(self):
        logger.info("Loading prediction artifacts...")
        try:
            features_path = f"{WEIGHTS_DIR}/all_selected_features.joblib"
            self.all_selected_features = self.storage_handler.load_file(features_path)

            skew_path = f"{WEIGHTS_DIR}/all_y_skew_transformer.joblib"
            self.y_transformers = self.storage_handler.load_file(skew_path)

            for y_chinese in Y_COLS:
                y_english = TABLE_NAME_MAP[y_chinese]
                model_path = f"{WEIGHTS_DIR}/{y_english}_best_model.joblib"

                loaded_obj = self.storage_handler.load_file(model_path)
                self.models[y_chinese] = loaded_obj['model']

            logger.info(f"Successfully loaded {len(self.models)} models.")

        except Exception as e:
            logger.error(f"Fatal error loading artifacts: {e}", exc_info=True)
            raise

    # =========================================================
    # ✅ NEW FUNCTION（與 training pipeline 完全一致）
    # =========================================================
    def _aggregate_train_test_to_dict(
        self,
        y_chinese,
        y_english,
        model,
        selected_features
    ):
        try:
            train_df = self.dao.fetch_all(f"Train_{y_english}")
            test_df = self.dao.fetch_all(f"Test_{y_english}")

            if train_df.empty and test_df.empty:
                logger.warning(f"No Train/Test data for {y_chinese}")
                return {}

            combined_df = pd.concat(
                [train_df, test_df],
                axis=0,
                ignore_index=True
            )

            # ===== actual =====
            if y_chinese in combined_df.columns:
                y_actual_series = pd.Series(
                    combined_df[y_chinese].values,
                    index=combined_df.index
                )
            else:
                y_actual_series = pd.Series([None] * len(combined_df))

            # ===== predict =====
            X_all = combined_df[selected_features]
            preds = model.predict(X_all)

            preds_series = pd.Series(
                preds.flatten(),
                index=combined_df.index
            )

            transformer = self.y_transformers.get(y_chinese)

            if transformer:
                # predict inverse
                y_pred = transformer.inverse_transform(preds_series)

                # actual inverse（防呆）
                try:
                    y_actual = transformer.inverse_transform(y_actual_series)
                except Exception:
                    y_actual = y_actual_series
            else:
                y_pred = preds_series
                y_actual = y_actual_series

            y_pred = y_pred.astype('float64').round(4)

            # ✅ 完全對齊 training pipeline 命名
            return {
                f"{y_chinese}_actual_value": y_actual.reset_index(drop=True),
                f"{y_chinese}_predicted_value": y_pred.reset_index(drop=True)
            }

        except Exception as e:
            logger.error(f"Aggregation failed for {y_chinese}: {e}", exc_info=True)
            return {}

    def run(self):
        logger.info("Starting prediction pipeline...")

        # ✅ collector（與 training pipeline 一致）
        evaluation_results_collector = {}

        for y_chinese in Y_COLS:
            try:
                y_english = TABLE_NAME_MAP[y_chinese]
                logger.info(f"Processing target: {y_chinese}")

                # =========================
                # 原本流程（完全不動）
                # =========================
                pred_table_name = f"Predict_{y_english}"
                pred_data = self.dao.fetch_all(pred_table_name)

                if pred_data.empty:
                    logger.info(f"No data in '{pred_table_name}', skipping.")
                    continue

                model = self.models[y_chinese]
                selected_features = self.all_selected_features[y_chinese]

                X_pred = pred_data[selected_features]
                preds_transformed = model.predict(X_pred)

                preds_series = pd.Series(
                    preds_transformed.flatten(),
                    index=pred_data.index,
                    name=y_chinese
                )

                transformer = self.y_transformers.get(y_chinese)
                if transformer:
                    predictions_final = transformer.inverse_transform(preds_series)
                else:
                    predictions_final = preds_series

                result_df = pred_data[ID_COLS].copy()
                result_df[f'predicted_{y_chinese}'] = (
                    predictions_final.astype('float64').round(4)
                )

                result_table_name = f"Result_{y_english}"
                self.dao.save_data(result_df, result_table_name, if_exists="replace")

                logger.info(f"Saved '{result_table_name}'.")

                # =========================
                # ✅ NEW FEATURE（統一 collector）
                # =========================
                agg_result = self._aggregate_train_test_to_dict(
                    y_chinese,
                    y_english,
                    model,
                    selected_features
                )

                evaluation_results_collector.update(agg_result)

            except Exception as e:
                logger.error(f"Error processing {y_chinese}: {e}", exc_info=True)
                continue

        # =========================
        # ✅ FINAL SAVE（完全對齊 training pipeline）
        # =========================
        if evaluation_results_collector:
            try:
                logger.info("Consolidating prediction results for all targets...")

                final_df = pd.concat(
                    evaluation_results_collector.values(),
                    axis=1
                )
                final_df.columns = list(evaluation_results_collector.keys())

                self.dao.save_data(
                    final_df,
                    "predict_all",
                    if_exists="replace"
                )

                logger.info("Successfully saved predict_all table.")

            except Exception as e:
                logger.error(f"Error saving predict_all: {e}", exc_info=True)
        else:
            logger.warning("No aggregated results to save.")


if __name__ == "__main__":
    init_logger()
    try:
        predictor = Predictor()
        predictor.run()
        logger.info("All prediction tasks completed.")
    except Exception as e:
        logger.error("Fatal error occurred.", exc_info=True)