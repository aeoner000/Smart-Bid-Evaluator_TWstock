
import joblib, sys
import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.inspection import permutation_importance
import lightgbm as lgb
from pathlib import Path

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parents[2]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.config_loader import cfg, SELECT_FEATURE_PATH
# 此處不再需要 SkewTransformer
# from .skew_transformer import SkewTransformer 

class FeatureSelector:
    def __init__(self, n_selected=20):
        '''
        特徵選擇流程 (註解完全保留)
        0. 刪除共線性高的特徵
        1. 先使用 LGBM 建立模型
        2. 使用時間交叉驗證方式將資料一時間分成設定折數之訓練、測試樣本
        3. 每一折都訓練並丟到permutation_importance去使用模型並一一打亂每一個特徵難他的預測結果更好或更壞
        4. 將每個特徵的重要性(預測分數因打亂而下降的平均)加入list
            -->他的重要性是數值越大越重要(ex.打亂後r2下降0.2，分數就是0.2)
        5. 平均每一折的結果
        6. 依分數排序特徵選出指定(ex.n_selected=20)數量特徵，但可能小於指定數量欄位，因為會排除共線高、重要性小於0的
        '''
        self.n_selected = n_selected
        self.selected_features = []
        
    def _get_low_correlation_features(self, df, x_cols):
        """檢查共線性，回傳過濾後的欄位名單 (註解完全保留)"""
        corr_matrix = df[x_cols].corr().abs()
        
        # 取得矩陣的上三角（避免重複檢查）
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        
        # 找出相關性大於門檻的欄位--> 將上面產生的 df 中哪一欄有大於 0.9 則取出他的欄名
        to_drop = [column for column in upper.columns if any(upper[column] > 0.9)]
        
        keep_features = [col for col in x_cols if col not in to_drop]
        print(f"共線性過濾：移除了 {len(to_drop)} 個高度相關欄位，剩餘 {len(keep_features)} 個。")
        return keep_features
        
    def fit(self, df, x_cols, y_col):
        reduced_x_cols = self._get_low_correlation_features(df, x_cols)
        # 基礎模型：小樣本建議使用較淺的樹 (註解完全保留)
        model = lgb.LGBMRegressor(n_estimators=100, max_depth=3, learning_rate=0.05, 
                                    random_state=42, verbose=-1, num_leaves=8, 
                                    min_child_samples=20)
        # 時序交叉驗證 (註解完全保留)
        tscv = TimeSeriesSplit(n_splits=5)
        importances = [] 

        for train_idx, val_idx in tscv.split(df): 
            X_train, X_val = df.iloc[train_idx][reduced_x_cols], df.iloc[val_idx][reduced_x_cols]
            y_train, y_val = df.iloc[train_idx][y_col], df.iloc[val_idx][y_col]
            
            # =====================================================================
            # 【核心修改】移除此處的偏態處理邏輯
            # 偏態處理已統一在 feature_engineer.py 中執行。
            # 此處假設傳入的 X_train, X_val 已經是轉換後的版本。
            # =====================================================================
            model.fit(X_train, y_train)
            
            # 使用排列重要性：看誰被打亂後最痛 (註解完全保留)
            result = permutation_importance(model, 
                                            X_val, y_val, # <--- 直接使用 X_val
                                            n_repeats=10, random_state=42,
                                            scoring="neg_mean_absolute_error") # MAE
            importances.append(result.importances_mean)

        # 計算平均重要性並排序 (註解完全保留)
        avg_imp = np.mean(importances, axis=0)
        importance_df = pd.DataFrame({'feature': reduced_x_cols, 'importance': avg_imp}).sort_values(by='importance', ascending=False)
        importance_df = importance_df[importance_df['importance'] > 0]
        
        # 挑選前 N 個最重要的特徵 (註解完全保留)
        self.selected_features = importance_df.head(self.n_selected)['feature'].tolist()
        print(f"挑選結束，選擇數量{len(self.selected_features)}，列表:")
        return self

    def save(self, dir, name="selected_features.joblib"):
        self.save_path = dir / name
        joblib.dump(self.selected_features, self.save_path)
        print(f"🎯 選定特徵已存至: {self.save_path}")
