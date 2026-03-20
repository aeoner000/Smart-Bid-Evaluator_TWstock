import logging
import numpy as np
import pandas as pd
import joblib
import json
from pathlib import Path
from sklearn.metrics import mean_squared_error

from src.db_base.db_manager import IPO_DAO
from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS, SAVE_DIR
from src.models.train_model.boost_automl import BoostAutoMLManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TrainPipeline")

# ==============================================================================
# 元數據更新函式
# ==============================================================================
def _update_training_metadata(metadata: dict):
    """將最終的元數據一次性寫回 training_metadata.json。"""
    # 【修改後】的路徑，指向 json/ 資料夾
    metadata_path = Path(__file__).resolve().parents[3] / 'json' / 'training_metadata.json'
    logger.info(f"準備將最終元數據寫回: {metadata_path}")
    try:
        # 【新增】確保目標資料夾存在
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ 訓練元數據更新成功。")
    except Exception as e:
        logger.error(f"❌ 將元數據寫回檔案時發生嚴重錯誤: {e}")

# ==============================================================================
# 主函式
# ==============================================================================
def run_training_pipeline(new_total_count: int = None):
    dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
    automl = BoostAutoMLManager()
    fe_cfg = cfg["feature_engineer"]["feature_cols"]
    target_map = fe_cfg["target_variables_map"]
    candidate_models = ['lgbm', 'xgb', 'cat']
    y_skew_trans = joblib.load(SAVE_DIR / "all_y_skew_transformer.joblib")

    # 【修改後】的路徑，從 json/ 資料夾讀取
    metadata_path = Path(__file__).resolve().parents[3] / 'json' / 'training_metadata.json'
    if metadata_path.exists():
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    else:
        metadata = {"last_training_count": 0, "champion_scores": {}}

    evaluation_results_collector = {}

    for y_chinese, y_english in target_map.items():
        logger.info(f"\n🚀 === 開始處理目標：{y_chinese} ({y_english}) ===")
        
        train_df = dao.fetch_all(f"Train_{y_english}")
        test_df = dao.fetch_all(f"Test_{y_english}")
        if train_df.empty or test_df.empty: continue
        y_train, X_train = train_df[y_chinese], train_df.drop(columns=[y_chinese])
        y_test, X_test = test_df[y_chinese], test_df.reindex(columns=X_train.columns)
        model_scores = {m_type: automl.train_and_optimize(m_type, X_train, y_train, n_trials=40) for m_type in candidate_models}
        best_type = min(model_scores, key=lambda k: model_scores[k][0])
        best_cv_score, best_params = model_scores[best_type]
        final_model = automl._init_model(best_type, best_params)
        final_model.fit(X_train, y_train)

        preds_raw = final_model.predict(X_test)
        preds_series = pd.Series(preds_raw.flatten(), index=y_test.index, name=y_chinese)
        transformer = y_skew_trans.get(y_chinese)
        if transformer:
            preds_original = transformer.inverse_transform(preds_series)
            y_true_original = transformer.inverse_transform(y_test)
        else:
            preds_original, y_true_original = preds_series, y_test
        
        challenger_rmse = np.sqrt(mean_squared_error(y_true_original, preds_original))
        logger.info(f"⭐ {y_chinese} 原始尺度測試集 RMSE (挑戰者分數): {challenger_rmse:.4f}")

        champion_scores = metadata.get("champion_scores", {})
        champion_rmse = champion_scores.get(y_english, float('inf'))

        logger.info(f"🥊 開始模型對決 (Test RMSE) - {y_chinese}")
        logger.info(f"    - 🏆 現任冠軍: {champion_rmse:.4f}")
        logger.info(f"    - ⚔️ 本次挑戰者: {challenger_rmse:.4f}")

        if challenger_rmse < champion_rmse*0.95:
            logger.info("    - 👑 新冠軍誕生！模型表現更佳。")
            metadata["champion_scores"][y_english] = challenger_rmse
            
            logger.info(f"📊 為 '{y_chinese}' 準備評估數據以供最終合併。")
            evaluation_results_collector[f'{y_chinese}_actual_value'] = y_true_original
            evaluation_results_collector[f'{y_chinese}_predicted_value'] = preds_original

            automl.models[best_type] = final_model
            automl.save_best_target_model(
                target_name=y_english, 
                best_model_type=best_type,
                best_params=best_params,
                best_score=challenger_rmse
            )
            logger.info(f"✅ {y_chinese} 流程結束，新冠軍模型已導出。")
        else:
            logger.info("    - 🛡️ 冠軍衛冕成功，予以捨棄。")

    if evaluation_results_collector:
        try:
            logger.info("\n--- 正在合併所有目標的評估結果 ---")
            final_eval_df = pd.concat(evaluation_results_collector.values(), axis=1)
            final_eval_df.columns = list(evaluation_results_collector.keys())
            
            table_name = "evaluation_results"
            logger.info(f"--- 準備將合併後的評估結果存入資料庫表格: {table_name} ---")
            dao.save_data(final_eval_df, table_name, if_exists="replace")
            logger.info(f"✅ 已成功將合併評估結果存入資料庫。")
        except Exception as e:
            logger.error(f"❌ 儲存合併評估結果至資料庫時出錯: {e}")

    if new_total_count is not None:
        metadata['last_training_count'] = new_total_count
        _update_training_metadata(metadata=metadata)

    logger.info("\n🎉🎉🎉 所有目標的模型訓練流程已全部完成。 🎉🎉🎉")

if __name__ == "__main__":
    run_training_pipeline()
