import joblib, os
import pandas as pd
import numpy as np
from sklearn.preprocessing import PowerTransformer

from src.utils.feature_utils import identify_binary_columns


class SkewTransformer:
    def __init__(self, threshold=0.75):
        """
        偏態轉換器 (Skewness Transformer)
        --------------------------------
        1. 自動偵測高偏態連續型變數。
        2. 排除二元特徵 (Binary Features)。
        3. 具備自動對齊功能：即使 Transform 時欄位減少（特徵選擇後），也能正確對應。
        """
        self.threshold = threshold
        self.pt = PowerTransformer(method='yeo-johnson')
        self.high_skew_cols = []  # 儲存 fit 時判定需要轉換的欄位名稱

    def fit(self, data):
        """
        學習階段：偵測高偏態欄位並擬合 PowerTransformer。
        """
        # 統一轉為 DataFrame 處理
        df = data.to_frame() if isinstance(data, pd.Series) else data.copy()
        
        # 1. 識別並排除二元特徵 (0/1 等類別型不需轉偏態)
        if df.shape[1] == 1:
            # 如果只有一欄 (通常是目標變數 Y)，直接列入檢查
            check_cols = df.columns.tolist()
        else:
            binary_cols = identify_binary_columns(df)
            check_cols = [col for col in df.columns if col not in binary_cols]
        
        # 2. 篩選數值型且絕對偏態值 > threshold 的欄位
        numeric_df = df[check_cols].select_dtypes(include=[np.number])
        if numeric_df.empty:
            self.high_skew_cols = []
        else:
            skew_values = numeric_df.skew()
            self.high_skew_cols = skew_values[abs(skew_values) > self.threshold].index.tolist()

        # 3. 擬合 PowerTransformer (僅針對高偏態欄位)
        if self.high_skew_cols:
            self.pt.fit(df[self.high_skew_cols])
            
        return self

    def transform(self, data):
        """
        轉換階段：將資料投影至常態分佈空間。
        支援「局部欄位轉換」，這對特徵選擇後的資料非常重要。
        """
        if not self.high_skew_cols:
            return data

        df_out = data.copy()
        
        if isinstance(data, pd.DataFrame):
            # 找出當前資料中，有哪些是「需要轉」且「確實存在」的欄位
            existing_cols = [col for col in self.high_skew_cols if col in data.columns]
            
            if not existing_cols:
                return df_out

            # 狀況 A: 欄位完全對齊 (Fit 50 欄, Transform 50 欄) -> 快速批量轉換
            if len(existing_cols) == len(self.high_skew_cols):
                # 確保順序與 fit 時一致
                df_out[self.high_skew_cols] = self.pt.transform(df_out[self.high_skew_cols])
            
            # 狀況 B: 欄位部分缺失 (特徵選擇後只剩 30 欄) -> 逐欄對齊轉換
            else:
                for col in existing_cols:
                    # 找到該欄位在原 PowerTransformer 中的索引位置
                    col_idx = self.high_skew_cols.index(col)
                    # 建立符合 sklearn 維度要求的臨時矩陣 (填充 0)
                    temp_in = np.zeros((len(df_out), len(self.high_skew_cols)))
                    temp_in[:, col_idx] = df_out[col].values
                    # 轉換後僅取回該欄位對應的數值
                    df_out[col] = self.pt.transform(temp_in)[:, col_idx]
            
            return df_out

        elif isinstance(data, pd.Series):
            # 處理單一欄位 (常用於目標變數 y)
            if data.name in self.high_skew_cols:
                # 即使是 Series，也封裝成帶名字的 DF 以消除 sklearn 警告
                temp_df = pd.DataFrame(data.values.reshape(-1, 1), columns=[data.name])
                transformed = self.pt.transform(temp_df)
                return pd.Series(transformed.flatten(), index=data.index, name=data.name)
            return data
            
        return data

    def fit_transform(self, data):
        return self.fit(data).transform(data)
    
    def inverse_transform(self, data):
        """
        還原轉換：將常態空間的預測值拉回原始尺度。
        """
        if not self.high_skew_cols:
            return data

        if isinstance(data, pd.DataFrame):
            df_out = data.copy()
            existing_cols = [col for col in self.high_skew_cols if col in data.columns]
            
            if len(existing_cols) == len(self.high_skew_cols):
                df_out[self.high_skew_cols] = self.pt.inverse_transform(df_out[self.high_skew_cols])
            else:
                for col in existing_cols:
                    col_idx = self.high_skew_cols.index(col)
                    temp_in = np.zeros((len(df_out), len(self.high_skew_cols)))
                    temp_in[:, col_idx] = df_out[col].values
                    df_out[col] = self.pt.inverse_transform(temp_in)[:, col_idx]
            return df_out

        elif isinstance(data, pd.Series):
            # 預測端最常用的還原邏輯 (y_pred -> y_real)
            # 假設 y 是單一欄位且有被轉換過
            name = data.name if data.name else self.high_skew_cols[0]
            temp_df = pd.DataFrame(data.values.reshape(-1, 1), columns=[name])
            try:
                # 嘗試全矩陣還原
                recovered = self.pt.inverse_transform(temp_df)
            except:
                # 若維度不合，則補齊維度還原
                temp_full = np.zeros((len(data), len(self.high_skew_cols)))
                temp_full[:, 0] = data.values
                recovered = self.pt.inverse_transform(temp_full)[:, 0]
                
            return pd.Series(recovered.flatten(), index=data.index, name=data.name)

        return data

    def save(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump(self, path)
        print(f"✅ SkewTransformer 儲存完畢: {path}")

    @staticmethod
    def load(path):
        if os.path.exists(path):
            return joblib.load(path)
        print(f"⚠️ 找不到路徑: {path}")
        return None