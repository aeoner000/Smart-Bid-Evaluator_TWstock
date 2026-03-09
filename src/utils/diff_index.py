import os
import pandas as pd

def search_index_list(raw_data_path, curr_data_path, code_col: str, time_col, feature_cols: list):
    """
    支援單一或多個時間欄位
    curr_data: 已存在的目標變數表, pd.DataFrame
    diff_index: 待處理的差異索引, pd.Index
    raw_data: 原始競拍資料, pd.DataFrame
    """
    if not os.path.exists(raw_data_path):
        return None, None, None

    # 1. 讀取原始資料
    raw_data = pd.read_csv(raw_data_path, encoding="utf-8-sig", dtype={code_col: str})
    
    # --- 判斷 time_col 類型並轉換日期 ---
    if isinstance(time_col, list):
        # 如果是列表，使用 apply 逐欄轉換，避免 DataFrame 直接轉日期的 ValueError
        raw_data[time_col] = raw_data[time_col].apply(pd.to_datetime, format='mixed', errors='coerce')
        index_cols = [code_col] + time_col
    else:
        # 如果是單一字串，直接轉換
        raw_data[time_col] = pd.to_datetime(raw_data[time_col], format='mixed', errors='coerce')
        index_cols = [code_col, time_col]

    # 2. 讀取現有資料
    if os.path.exists(curr_data_path):
        curr_data = pd.read_csv(curr_data_path, encoding="utf-8-sig", dtype={code_col: str})
        if isinstance(time_col, list):
            curr_data[time_col] = curr_data[time_col].apply(pd.to_datetime, format='mixed', errors='coerce')
        else:
            curr_data[time_col] = pd.to_datetime(curr_data[time_col], format='mixed', errors='coerce')
    else:
        # 建立空 DataFrame，動態組合欄位
        cols = [code_col] + (time_col if isinstance(time_col, list) else [time_col]) + feature_cols
        curr_data = pd.DataFrame(columns=cols)

    # 3. 執行索引比對 (使用動態生成的 index_cols)
    raw_indexed = raw_data.set_index(index_cols)

    if not curr_data.empty:
        # 確保 curr_data 也有正確的 index 進行比對
        curr_indexed = curr_data.set_index(index_cols)
        # difference 會回傳 raw 有但 curr 沒有的索引
        diff_index = raw_indexed.index.difference(curr_indexed.index, sort=False)
    else:
        diff_index = raw_indexed.index

    return curr_data, diff_index, raw_data