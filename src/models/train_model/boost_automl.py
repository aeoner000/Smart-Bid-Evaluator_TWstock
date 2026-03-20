import joblib
import os, sys
import optuna
import numpy as np
import pandas as pd
from pathlib import Path
import logging
import lightgbm as lgb
import xgboost as xgb
from catboost import CatBoostRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import TimeSeriesSplit

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parents[3]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.config_loader import cfg, SAVE_DIR
from src.processors.skew_transformer import SkewTransformer

# 日誌配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)



class BoostAutoMLManager:
    def __init__(self):
        # 直接使用外部加載好的 cfg，不需要再讀一遍檔案
        self.config = cfg["ml_model_environment"]
        self.models = {}               # 模型暫存
        self.save_dir = Path(SAVE_DIR) # 確保是 Path 物件
        self.y_skew_trans = joblib.load(self.save_dir / "all_y_skew_transformer.joblib")
        
        if not self.save_dir.exists():
            self.save_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"建立儲存目錄: {self.save_dir}")

    def _init_model(self, model_type: str, params: dict):
        ''' 初始化模型 '''
        if model_type == 'lgbm': return lgb.LGBMRegressor(**params)
        if model_type == 'xgb': return xgb.XGBRegressor(**params)
        if model_type == 'cat': return CatBoostRegressor(**params)
        raise ValueError(f"不支援的模型: {model_type}")

    def _objective(self, trial, model_type, X, y):
        ''' Optuna 優化目標函數 '''
        model_cfg = self.config['models'][model_type]
        params = model_cfg['static_params'].copy()
        
        # 1. 動態生成搜尋空間
        for p_name, space in model_cfg['search_space'].items():
            method = getattr(trial, f"suggest_{space['type']}")
            # 建議：如果 YAML 有設定 log 參數則傳入，否則預設 False
            params[p_name] = method(p_name, space['low'], space['high']) 
            
        tscv = TimeSeriesSplit(n_splits=5)
        errors = []
        
        # 2. 獲取該目標變數 (y) 的轉換器
        y_name = y.name
        try:
            y_transformer = self.y_skew_trans[y_name]
        except KeyError:
            # 如果這個 y 當初沒被加入轉換器字典，則不進行轉換
            y_transformer = None
            logger.warning(f"⚠️ 找不到 {y_name} 的轉換器，將使用原始尺度計算誤差。")

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
                
                # 執行反向轉換 (會檢查 name 是否在 high_skew_cols 內)
                preds_original = y_transformer.inverse_transform(preds_series)
                y_true_original = y_transformer.inverse_transform(y_val)
            else:
                preds_original = preds_raw
                y_true_original = y_val

            # 5. 計算誤差 (RMSE)
            mse = mean_squared_error(y_true_original, preds_original)
            rmse = np.sqrt(mse)
            
            # 防呆：處理模型爆炸產生的 inf 或 nan
            if np.isnan(rmse) or np.isinf(rmse):
                return float('inf')
                
            errors.append(rmse)

        return np.mean(errors)

    def train_and_optimize(self, model_type: str, X: pd.DataFrame, y: pd.Series, n_trials: int = 30):
        logger.info(f"🚀 開始自動化調參: {model_type}")
        
        study = optuna.create_study(direction="minimize") # 創建參數優化框架
        study.optimize(lambda t: self._objective(t, model_type, X, y), n_trials=n_trials)
        # optimize: 啟動，他一定會塞一個trail給()中的物件
        # Lambda 的角色：它像是一個「轉接頭」，接收 Optuna 傳來的 t
        
        # 複製原本 param
        final_params = self.config['models'][model_type]['static_params'].copy()
        final_params.update(study.best_params) # 將優化的參數更新到 final_params
        
        logger.info(f"✨ {model_type} 最佳參數: {study.best_params}")
        
        # 將最後優化參數傳入初始化物件當作訓練模型的參數
        model = self._init_model(model_type, final_params)
        model.fit(X, y)
        self.models[model_type] = model
        
        # self.export_model(model_type, study.best_params)
        return study.best_value, final_params


    def save_best_target_model(self, target_name: str, best_model_type: str, best_params: dict, best_score:float):
        """
        儲存字典中目標變數對應的模型，他是已經在外部比較確認過的
        """
        file_path = self.save_dir / f"{target_name}_best_model.joblib"
        
        # 從字典中抽出冠軍模型
        winner_model = self.models[best_model_type]
        
        # 儲存包含模型實體、模型類型與參數的資訊
        joblib.dump({
            'model': winner_model,
            'model_type': best_model_type,
            'best_params': best_params,
            'best_score':  best_score
        }, file_path)
        
        # 重要：存完後清空暫存區，為下一個 y (目標變數) 做準備
        self.models = {} 
        logger.info(f"✨ 最佳模型已導出至: {file_path} (類型: {best_model_type})")




