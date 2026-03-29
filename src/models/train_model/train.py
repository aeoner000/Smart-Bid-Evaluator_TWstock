
import logging
import numpy as np
import pandas as pd
import sys
from pathlib import Path

# --- Path Setup ---
try:
    root_path = Path(__file__).resolve().parents[3]
except NameError:
    root_path = Path.cwd()

if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.logger_config import init_logger
from src.db_base.db_manager import get_db_manager
from src.utils.config_loader import config
from src.utils.storage_handler import get_storage_handler
from src.models.train_model.boost_automl import BoostAutoMLManager
from sklearn.metrics import mean_squared_error

logger = logging.getLogger(__name__)

# ==============================================================================
# Metadata Update Function
# ==============================================================================
def _update_training_metadata(metadata: dict, storage_handler):
    """Writes the final metadata back to training_metadata.json."""
    metadata_path = config['paths']["metadata_path"]
    logger.info(f"Writing final metadata to: {metadata_path}")
    try:
        storage_handler.save_file(metadata, metadata_path)
        logger.info("Training metadata updated successfully.")
    except Exception as e:
        logger.error(f"Fatal error writing metadata to file: {e}", exc_info=True)

# ==============================================================================
# Main Function
# ==============================================================================
def run_training_pipeline(new_total_count: int = None):
    # Initialize tools
    dao = get_db_manager()
    storage = get_storage_handler()
    
    # Load settings
    fe_cfg = config["feature_engineer"]["feature_cols"]
    target_map = fe_cfg["target_variables_map"]
    candidate_models = ['lgbm', 'xgb', 'cat']


    # Load transformers and metadata using storage_handler
    try:
        skew_path = config['paths']['skew_path'].replace('skew_transformer.joblib', 'all_y_skew_transformer.joblib')
        y_skew_trans = storage.load_file(skew_path)
        if y_skew_trans is None:
             y_skew_trans = {} # Use an empty dict if the file doesn't exist
             logger.warning(f"Could not find transformer file at {skew_path}, using empty transformers.")
    except FileNotFoundError:
        y_skew_trans = {}
        logger.warning(f"Transformer file not found at {skew_path}, using empty transformers.")

    try:
        metadata_path = config['paths']["metadata_path"]
        metadata = storage.load_file(metadata_path)
    except FileNotFoundError:
        metadata = {"last_training_count": 0, "champion_scores": {}}
        logger.warning("Metadata file not found, using default values.")

    # Pass y_skew_trans to the AutoMLManager
    automl = BoostAutoMLManager(y_skew_transformers=y_skew_trans)
    
    evaluation_results_collector = {}

    for y_chinese, y_english in target_map.items():
        logger.info(f"Processing target variable: {y_chinese} ({y_english})")
        
        train_df = dao.fetch_all(f"Train_{y_english}")
        test_df = dao.fetch_all(f"Test_{y_english}")

        if train_df.empty or test_df.empty:
            logger.warning(f"Train or test data for {y_english} is empty. Skipping.")
            continue
        
        y_train, X_train = train_df[y_chinese], train_df.drop(columns=[y_chinese])
        y_test, X_test = test_df[y_chinese], test_df.reindex(columns=X_train.columns)
        feature_list = X_train.columns

        model_scores = {m_type: automl.train_and_optimize(m_type, X_train, y_train, n_trials=40) for m_type in candidate_models}
        best_type = min(model_scores, key=lambda k: model_scores[k][0]) # mse最小
        best_cv_score, best_params = model_scores[best_type]
        
        # Retrain the final model with the best parameters
        final_model = automl._init_model(best_type, best_params)

        # ================ 模型建立 =========================
        final_model.fit(X_train, y_train)
        automl.models[best_type] = final_model # Update the model in the temporary store for saving

        preds_raw = final_model.predict(X_test)
        preds_series = pd.Series(preds_raw.flatten(), index=y_test.index, name=y_chinese)
        transformer = y_skew_trans.get(y_chinese)
        
        if transformer:
            preds_original = transformer.inverse_transform(preds_series)
            y_true_original = transformer.inverse_transform(y_test)
        else:
            preds_original, y_true_original = preds_series, y_test
        
        challenger_rmse = np.sqrt(mean_squared_error(y_true_original, preds_original))
        logger.info(f"Challenger RMSE for {y_chinese} on original scale test set: {challenger_rmse:.4f}")

        champion_scores = metadata.get("champion_scores", {})
        champion_rmse = champion_scores.get(y_english, float('inf'))

        logger.info(f"Starting model showdown (Test RMSE) for {y_chinese}")
        logger.info(f"    - Incumbent Champion: {champion_rmse:.4f}")
        logger.info(f"    - Current Challenger: {challenger_rmse:.4f}")

        if challenger_rmse < champion_rmse * 0.95:
            logger.info("修改模型")
            metadata["champion_scores"][y_english] = challenger_rmse
            
            evaluation_results_collector[f'{y_chinese}_actual_value'] = y_true_original
            evaluation_results_collector[f'{y_chinese}_predicted_value'] = preds_original

            automl.save_best_target_model(
                target_name=y_english, 
                best_model_type=best_type,
                best_params=best_params,
                best_score=challenger_rmse,
                feature_list=feature_list # 加入特徵有哪些
            )
            logger.info(f"Process for {y_chinese} finished. New champion model has been exported.")
        else:
            logger.info("Champion defended its title. Discarding challenger model.")

    if evaluation_results_collector:
        try:
            logger.info("Consolidating evaluation results for all targets...")
            final_eval_df = pd.concat(evaluation_results_collector.values(), axis=1)
            final_eval_df.columns = list(evaluation_results_collector.keys())
            
            table_name = "evaluation_results"
            logger.info(f"Saving consolidated evaluation results to database table: {table_name}")
            dao.save_data(final_eval_df, table_name, if_exists="replace")
            logger.info("Successfully saved consolidated evaluation results to database.")
        except Exception as e:
            logger.error("Error saving consolidated evaluation results to database: {e}", exc_info=True)

    if new_total_count is not None:
        metadata['last_training_count'] = new_total_count
        _update_training_metadata(metadata=metadata, storage_handler=storage)

    logger.info("Training pipeline for all target variables has completed.")

if __name__ == "__main__":
    init_logger()
    run_training_pipeline()
