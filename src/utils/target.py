import pandas as pd
import numpy as np

def get_target_value(api, code:str, datetime:pd.Timestamp, feature_cols:list):
    """
    得到完整上市當天股價，若當天無資料則往後最多找7天
    """
    for i in range(8):
        # 計算當前嘗試的日期
        current_date = datetime + pd.Timedelta(days=i)
        date_str = current_date.strftime('%Y-%m-%d')
        
        df = api.taiwan_stock_daily(stock_id=code, start_date=date_str, end_date=date_str)
        
        # 檢查資料是否存在且不為空
        if df is not None and not df.empty:
            print(current_date)
            # 取得資料後進行切片與處理
            df = df.iloc[:1, 2:]
            df.columns = feature_cols[:8]
            return df.round(3)
            
    # 若迴圈結束仍找不到資料，則返回 None
    return None

def cal_y_feature(curr_data, raw_data, code:str, bid_date:pd.Timestamp):
    """
    1. 門檻預測 : 最低得標加價率 => (最低得標價格 / 最低投標價格) - 1
    2. 行情預測 : 加權平均加價率 => (得標加權平均價 / 最低投標價格) - 1
    3. 獲利預估 : 預估獲利率 => (首日收盤價 / 最低投標價格) - 1
    """
    prices = [raw_data.loc[(raw_data["證券代號"] == code) & (raw_data["投標開始日"] == bid_date), col].squeeze()
        for col in ["最低投標價格(元)", "最低得標價格(元)", "得標加權平均價格(元)"]]
    bid_min, win_min, win_avg = [p if not isinstance(p, pd.Series) else np.nan for p in prices]

    close_p = curr_data["收盤價"].iloc[0]

    curr_data["預估獲利率"] = (close_p / bid_min) - 1
    curr_data["最低得標加價率"] = (win_min / bid_min) - 1
    curr_data["加權平均加價率"] = (win_avg / bid_min) - 1

    return curr_data.round(3)