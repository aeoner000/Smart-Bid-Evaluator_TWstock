import streamlit as st
import pandas as pd
# 引入你剛才寫好的數據引擎函數
from .data_engine import get_bq_table, list_gcs_files, download_gcs_file, load_joblib_from_gcs
from streamlit_unit.mappings import FIELD_NAME_MAP
# 如果你需要處理時間或路徑，也可以引入這些（選配）
from datetime import datetime
import io

project_id = "gen-lang-client-0590877921"
dataset_id = "bid_predict_table"

TABLE_MAP = {
    "bid_info": f"{project_id}.{dataset_id}.bid_info",
    "all_features": f"{project_id}.{dataset_id}.all_features",
    "predict_est_profit": f"{project_id}.{dataset_id}.Result_input_target_est_profit_margin",
    "predict_min_winning_markup": f"{project_id}.{dataset_id}.Result_input_target_min_winning_markup_rate",
    "weighted_avg_markup": f"{project_id}.{dataset_id}.Result_input_target_weighted_avg_markup_rate",
    "predict_all": f"{project_id}.{dataset_id}.predict_all"
}

@st.cache_data(ttl=86400)
def get_core_table(table_key: str):
    """
    只需傳入代號（如 'bid_list'），自動抓取對應的表
    """
    if table_key not in TABLE_MAP:
        st.error(f"❌ 找不到代號為 {table_key} 的配置")
        return pd.DataFrame()
        
    full_table_path = TABLE_MAP[table_key]
    query = f"SELECT * FROM `{full_table_path}`" # 直接抓全表
    
    # 呼叫我們之前寫好的 BQ Client 邏輯
    return get_bq_table(query)

@st.cache_data(ttl=86400)
def get_update_time():
    ''' 取得最新更新時間 '''
    df = get_core_table("bid_info")
    df['update_time'] = pd.to_datetime(df['update_time'], format='mixed')
    max_time = df['update_time'].max()
    return max_time.strftime
#=============================== 首頁 ===============================
@st.cache_data(ttl=86400)
def get_sample_size():
    '''首頁樣本數'''
    df = get_core_table("all_features")
    return len(df)

@st.cache_data(ttl=86400)
def get_all_avg_pred_diff():
    ''' 取得平均誤差 '''
    df = get_core_table("predict_all")
    actual_list = []
    pred_list = []
    for name in df.columns:
        name_ = name.split("_")[1]
        if name_ == "actual":
            actual_list.append(name)
        else:
            pred_list.append(name)
    result_df = pd.DataFrame(index=df.index)
    result_df['真實值平均'] = df[actual_list].mean(axis=1)
    result_df['預測值平均'] = df[pred_list].mean(axis=1)
    result_df['平均誤差'] = result_df['預測值平均'] - result_df['真實值平均']
    return result_df

#=============================== 第二頁 ===============================
@st.cache_data(ttl=86400)
def get_curr_ipo():
    ''' 預測結果 '''
    df = get_core_table("bid_info")
    condition = (df["status"]=="features_complete")|(df["status"]=="crawling")
    show_list = ["證券名稱", "證券代號", "投標開始日", "投標結束日", "最低投標價格_元", "開標日期", "撥券日期_上市_上櫃日期"]
    show_df = df[condition][show_list]
    time_col = ["投標開始日", "投標結束日", "開標日期", "撥券日期_上市_上櫃日期"]
    show_df[time_col] = show_df[time_col].apply(lambda x: pd.to_datetime(x.astype(str), errors='coerce').dt.date)
    show_df = show_df.rename(columns=FIELD_NAME_MAP, errors='ignore')
    return show_df

@st.cache_data(ttl=86400)
def get_predict_result(code):
    ''' 取得預測結果並計算'''
    bid_info = get_core_table("bid_info")
    df_1 = get_core_table("predict_est_profit")
    df_2 = get_core_table("predict_min_winning_markup")
    df_3 = get_core_table("weighted_avg_markup")
    print(df_1)
    merge_df = df_1.merge(df_2, on='證券代號', how='left').merge(df_3, on='證券代號', how='left')
    merge_df.columns = merge_df.columns.str.split('_').str.get(-1)
    big_df = merge_df.merge(bid_info, on='證券代號', how='left')
    
    res = big_df[["證券代號", "最低投標價格_元", "最低得標加價率", "加權平均加價率", "預估獲利率"]].copy()
    base_price = res["最低投標價格_元"].fillna(0)
    res["預估最低中標價格"] = (base_price * (1 + res["最低得標加價率"].fillna(0))).round(2)
    res["預估平均中標價格"] = (base_price * (1 + res["加權平均加價率"].fillna(0))).round(2)
    res["預估上市開盤價"] = (base_price * (1 + res["預估獲利率"].fillna(0))).round(2)

    final_cols = ["證券代號", "預估最低中標價格", "預估平均中標價格", "預估上市開盤價"]
    predict_df = res[final_cols]
    mask = (predict_df["證券代號"].astype(str) == code)
    filtered_data = predict_df[mask]
    if not filtered_data.empty:
        data = filtered_data.fillna("N/A").iloc[0]
    else:
        data = pd.Series("N/A", index=predict_df.columns)
    return data

@st.cache_data(ttl=86400)
def get_base_info(code):
    all_features = get_core_table("all_features")
    need_col = ["證券代號", "每股盈餘", "每股盈餘成長率", "營收成長率", "ROE成長率", "每股淨值", "負債比"]
    df = all_features[need_col]
    df = df.rename(columns=FIELD_NAME_MAP, errors='ignore')
    mask = (df["證券代號"].astype(str) == code)
    filtered_data = df[mask].copy()
    mask = (filtered_data["證券代號"].astype(str) == code)
    filtered_data = filtered_data[mask]
    filtered_data.drop(columns="證券代號", inplace=True, errors='ignore')
    if not filtered_data.empty:
        data = filtered_data.fillna("N/A").iloc[0]
    else:
        data = pd.Series("N/A", index=filtered_data.columns)
    return data

def get_feature_important():
    ''' 特徵重要性 '''
    features_path = "src/models/saved_weights/all_selected_features.joblib"
    obj = load_joblib_from_gcs(features_path)
    processed_data = {}
    for target, features in obj.items():
        s = pd.Series(features).reindex(range(5)) # 轉成 Series 並重新索引為 0~4，超出部分自動補 NaN
        processed_data[target] = s.fillna("N/A")
    df = pd.DataFrame(processed_data)
    df = df.replace(FIELD_NAME_MAP)
    return df
#=============================== 第三頁 ===============================
@st.cache_data(ttl=86400)
def get_history_predict(target_name):
    ''' 三個預測目標的歷史資料預測結果 '''
    df = get_core_table("predict_all")
    text = ["_actual_value", "_predicted_value"]
    name_list = []
    for t in text:
        name = f"{target_name}{t}"
        name_list.append(name)

    return df[name_list]

@st.cache_data(ttl=86400)
def get_all_feature_cols():
    '''' 取得自訂座標軸圖之完整Df、名稱映射結果 '''
    all_features = get_core_table("all_features")
    all_features = all_features.drop(columns=["證券代號", "投標開始日", "證券名稱", "撥券日期_上市_上櫃日期"])
    rename_df_cols = all_features.rename(columns=FIELD_NAME_MAP)
    col_map = dict(zip(rename_df_cols.columns, all_features.columns))
    cols = {
        "data": all_features,
        "ori_cols": list(all_features.columns),                      # 原始欄位清單
        "rename_cols": list(rename_df_cols.columns),                 # 介面顯示欄位對應清單
        "map": col_map
        }
    return cols

@st.cache_data(ttl=86400)
def get_contain_time_df():
    ''' 取得含時間欄位之完整df '''
    all_features = get_core_table("all_features")
    all_features = all_features.drop(columns=["證券代號", "證券名稱", "撥券日期_上市_上櫃日期"])
    all_features["投標開始日"] = pd.to_datetime(all_features["投標開始日"])
    all_features = all_features.set_index("投標開始日")
    rename_df_cols = all_features.rename(columns=FIELD_NAME_MAP)
    col_map = dict(zip(rename_df_cols.columns, all_features.columns))
    cols = {
        "data": all_features,
        "ori_cols": list(all_features.columns),                      # 原始欄位清單
        "rename_cols": list(rename_df_cols.columns),                 # 介面顯示欄位對應清單
        "map": col_map
        }
    return cols
