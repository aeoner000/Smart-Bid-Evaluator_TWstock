import pandas as pd
import numpy as np

def set_type(df:pd.DataFrame)-> pd.DataFrame:
    '''設置數值格式'''
    cols_list = df.columns
    for col in cols_list:
        if col in ["證券代號", "證券名稱", "status"]:
            df[col] = df[col].astype(str)
        elif col in ["投標開始日", "撥券日期(上市、上櫃日期)"]:
            df[col] = pd.to_datetime(df[col], format="mixed")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def sort_by_date(df:pd.DataFrame)-> pd.DataFrame:
    '''排序'''
    return df.sort_values(by='投標開始日', ascending=True)

def remove_duplicates(df:pd.DataFrame)-> pd.DataFrame:
    '''去重'''
    return df.drop_duplicates(subset=['證券代號', '投標開始日'], keep='first')

def apply_growth_cap(df, cap=10.0):
    """
    針對訓練資料清洗成長率與獲利指標
    :param df: pandas.DataFrame
    :param cap: 蓋帽值 (預設 10.0 代表 1000%)
    """
    # 定義排除清單
    exclude_cols = {"預估獲利率", "最低得標加價率", "加權平均加價率"}

    # 2. 篩選出：結尾是 "率" 且 不在排除清單中 的欄位
    # 同時我們也把常見的獲利指標 ROE/ROA 納入 (如果它們結尾不是率)
    target_cols = [
        c for c in df.columns
        if (c.endswith("率") or "ROE" in c or "ROA" in c)
        and c not in exclude_cols
    ]

    if not target_cols:
        return df

    # 處理無限大 (inf -> 10, -inf -> -10)
    df[target_cols] = df[target_cols].replace(np.inf, cap)
    df[target_cols] = df[target_cols].replace(-np.inf, -cap)

    # 將所有數值限制在 [-10, 10] 之間，保護模型不被「億倍成長」摧毀梯度
    df[target_cols] = df[target_cols].clip(lower=-cap, upper=cap)

    return df

def fill_nan(df:pd.DataFrame, config_dict:dict)-> pd.DataFrame:
    '''盤勢欄位向前填補'''
    cols_list = config_dict["all_market_info"][2:]
    existing_cols = [c for c in cols_list if c in df.columns]
    df[existing_cols] = df[existing_cols].ffill(limit=10)
    return df

def add_is_miss(df:pd.DataFrame, config_dict:dict)-> pd.DataFrame:
    '''
    加入是否有缺值
    1. fin_stmts
    2. revenue_info
    3. history_price_info
    '''
    fin_stmts_cols = config_dict["fin_stmts"][2:]
    revenue_info = config_dict["revenue_info"][2:]
    history_price_info = config_dict["history_price_info"][2:]

    df['fin_stmts_missing'] = df[fin_stmts_cols].isnull().any(axis=1).astype(int)
    df['revenue_info_missing'] = df[revenue_info].isnull().any(axis=1).astype(int)
    df['history_price_info_missing'] = df[history_price_info].isnull().any(axis=1).astype(int)

    return df

def add_new_feature(df:pd.DataFrame)-> pd.DataFrame:
    '''
    新增競價拍賣預測模型特徵：
    1. 標的公司性質（KY股）
    2. 時間季節性（第四季效應）
    3. 資金凍結跨度（投標至上市天數）
    4. 籌碼動向（融資與法人相對大盤之參與度）
    5. 市場情緒（量能爆發熱度）
    6. 獲利品質動能（ROE與成長性之複合指標）
    '''
    df['is_ky'] = df['證券名稱'].str.upper().str.endswith('-KY', na=False).astype(int)
    df['is_Q4'] = (df['投標開始日'].dt.quarter == 4).astype(int)
    df['days_to_listing'] = (df['撥券日期(上市、上櫃日期)'] - df['投標開始日']).dt.days
    df['融資比'] = (df['融資張數增減'] / df['大盤_平均成交量']).round(3)
    df['法人比'] = ((df['外資平均增減'] + df['投信平均增減'] + df['自營商平均增減']) / df['大盤_平均成交量']).round(3)
    df['Market_Heat'] = (df['前一日成交金額'] / df['前十日內平均成交金額']).round(3)
    df['ROE_Quality'] = (df['ROE'] * df['ROE成長率']).round(3)

    return df

def handle_missing_data(df:pd.DataFrame, config_dict:dict)-> pd.DataFrame:
    '''
    刪除所有包含缺失值的列
    1. bid_info
    2. all_market_info
    3. target_variable
    '''
    bid_info_cols = config_dict.get("bid_info", [])[2:]
    all_market_info_cols = config_dict.get("all_market_info", [])[2:]
    target_variable_cols = config_dict.get("target_variable", [])[2:]

    drop_cols = bid_info_cols + all_market_info_cols + target_variable_cols
    actual_drop_cols = [c for c in drop_cols if c in df.columns]

    # 1. 找出 status == 'all_complete' 的資料列的 index
    dev_df_indices = df[df['status'] == 'all_complete'].index

    # 2. 在這些資料列中，找出 'actual_drop_cols' 有缺失值的那些列的 index
    rows_to_drop_indices = df.loc[dev_df_indices, actual_drop_cols].isnull().any(axis=1)
    
    # 3. 取得需要被刪除的行的實際 index
    indices_to_drop = dev_df_indices[rows_to_drop_indices]

    # 4. 從原始的 df 中刪除這些行
    df_cleaned = df.drop(indices_to_drop)

    return df_cleaned

def identify_binary_columns(df:pd.DataFrame)-> list:
    ''' 找出所有 二元(1/0) 欄位 '''
    return [col for col in df.columns if set(df[col].dropna().unique()).issubset({0, 1})]