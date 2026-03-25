
import optuna
import numpy as np
import pandas as pd
import logging
import lightgbm as lgb
import xgboost as xgb
from catboost import CatBoostRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import TimeSeriesSplit

# --- 路徑處理 ---
# 舊的 sys.path 修改已被移除，因為新架構不再需要它

from src.utils.config_loader import config
from src.utils.storage_handler import get_storage_handler
from src.processors.skew_transformer import SkewTransformer

# 日誌配置
logger = logging.getLogger(__name__)


class BoostAutoMLManager:
    def __init__(self, y_skew_transformers: dict):
        """
        初始化 AutoML 管理器。
        現在它從外部接收 y_skew_transformers，而不是自己讀取檔案。
        """
        # 直接使用外部加載好的 config
        self.config = config["ml_model_environment"]
        self.models = {}               # 模型暫存
        self.y_skew_trans = y_skew_transformers # 從外部傳入，不再自己讀取
        self.storage = get_storage_handler() # 初始化儲存工具
        logger.info("BoostAutoMLManager initialized with y-skew transformers.")


    def _init_model(self, model_type: str, params: dict):
        ''' 初始化模型 '''
        if model_type == 'lgbm': return lgb.LGBMRegressor(**params)
        if model_type == 'xgb': return xgb.XGBRegressor(**params)
        if model_type == 'cat': return CatBoostRegressor(**params)
        raise ValueError(f"Unsupported model type: {model_type}")

    def _objective(self, trial, model_type, X, y):
        ''' Optuna 優化目標函數 '''
        model_cfg = self.config['models'][model_type]
        params = model_cfg['static_params'].copy()
        
        # 1. 動態生成搜尋空間
        for p_name, space in model_cfg['search_space'].items():
            method = getattr(trial, f"suggest_{space['type']}")
            params[p_name] = method(p_name, space['low'], space['high']) 
            
        tscv = TimeSeriesSplit(n_splits=5)
        errors = []
        
        # 2. 獲取該目標變數 (y) 的轉換器
        y_name = y.name
        y_transformer = self.y_skew_trans.get(y_name) # 使用 self.y_skew_trans

        if not y_transformer:
            # 如果這個 y 當初沒被加入轉換器字典，則不進行轉換
            logger.warning(f"Transformer for {y_name} not found. Calculating error on original scale.")

        # 3. 時間序列交叉驗證
        for train_idx, val_idx in tscv.split(X):
            # 切分資料
            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

            # 初始化並訓練模型
            model = self._init_model(model_type, params)
            model.fit(X_train, y_train)
            # 預測 (此時 preds 通常是 numpy array)
            preds_raw = model.predict(X_val)

            # 4. 關鍵修正：將 preds 封裝回 Series 以配合你的 SkewTransformer 邏輯
            if y_transformer:
                # 建立一個與 y_val 結構相同的 Series
                preds_series = pd.Series(preds_raw.flatten(), index=y_val.index, name=y_name)
                try:
                    # 執行反向轉換 (會檢查 name 是否在 high_skew_cols 內)
                    preds_original = y_transformer.inverse_transform(preds_series)
                    y_true_original = y_transformer.inverse_transform(y_val)
                except Exception as e:
                    # 萬一連轉換過程都噴錯（例如數值大到溢位），直接給懲罰分
                    logger.error(f"Trial {trial.number}: Critical error during inverse transformation: {e}")
                    return 9999.0
            else:
                preds_original = preds_raw
                y_true_original = y_val
            if np.isnan(preds_original).any() or np.isinf(preds_original).any():
                # 發現垃圾數值，不進 MSE 計算，直接回傳超大分數（懲罰）
                # 這樣 Optuna 就會學到「這組參數是死路」，以後自動避開
                logger.warning(f"Trial {trial.number}: Invalid values (NaN/Inf) generated. Assigning penalty score.")
                return 9999.0
            # 5. 計算誤差 (RMSE)
            mse = mean_squared_error(y_true_original, preds_original)
            rmse = np.sqrt(mse)
            
            # 防呆：處理模型爆炸產生的 inf 或 nan
            if np.isnan(rmse) or np.isinf(rmse):
                return 9999.0
                
            errors.append(rmse)

        return np.mean(errors)

    def train_and_optimize(self, model_type: str, X: pd.DataFrame, y: pd.Series, n_trials: int = 30):
        logger.info(f"Starting automated hyperparameter tuning for: {model_type}")
        
        study = optuna.create_study(direction="minimize") # 創建參數優化框架
        study.optimize(lambda t: self._objective(t, model_type, X, y), n_trials=n_trials)
        # optimize: 啟動，他一定會塞一個trail給()中的物件
        # Lambda 的角色：它像是一個「轉接頭」，接收 Optuna 傳來的 t
        
        # 複製原本 param
        final_params = self.config['models'][model_type]['static_params'].copy()
        final_params.update(study.best_params) # 將優化的參數更新到 final_params
        
        logger.info(f"Best parameters for {model_type}: {study.best_params}")

        # 將最後優化參數傳入初始化物件當作訓練模型的參數
        model = self._init_model(model_type, final_params)
        model.fit(X, y)
        self.models[model_type] = model
        
        return study.best_value, final_params


    def save_best_target_model(self, target_name: str, best_model_type: str, best_params: dict, best_score:float):
        """
        儲存字典中目標變數對應的模型，他是已經在外部比較確認過的
        """
        # 從 config 讀取儲存目錄，更有彈性
        weights_dir = config["paths"].get("weights_dir", "src/models/saved_weights")
        file_path = f"{weights_dir}/{target_name}_best_model.joblib"
        
        # 從字典中抽出冠軍模型
        winner_model = self.models.get(best_model_type)
        if not winner_model:
            logger.error(f"CRITICAL: Model type '{best_model_type}' not found. Cannot save.")
            return

        # 儲存包含模型實體、模型類型與參數的資訊
        model_payload = {
            'model': winner_model,
            'model_type': best_model_type,
            'best_params': best_params,
            'best_score':  best_score
        }
        
        try:
            # 使用 storage_handler 儲存
            self.storage.save_file(model_payload, file_path)
            logger.info(f"Best model for target saved to: {file_path} (Type: {best_model_type})")
        except Exception as e:
            logger.error(f"Failed to save model '{target_name}' to {file_path}: {e}")
            raise
        finally:
            # 重要：存完後清空暫存區，為下一個 y (目標變數) 做準備
            self.models = {} 
