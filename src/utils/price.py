import requests as req
import pandas as pd
import time, random

def fix_date(date_str):
    """將民國日期字串 (114/01/01) 轉為 datetime 物件"""
    try:
        parts = date_str.split('/')
        year = int(parts[0]) + 1911
        date_s = f"{year}/{parts[1]}/{parts[2]}"
        return pd.to_datetime(date_s)
    except:
        return pd.NaT

def get_price_table(code, y, m, headers):
    """
    抓取興櫃行情。
    解決原本 main 邏輯中 df.str.replace 導致的 DataFrame 報錯。
    """
    y2 = y if m > 1 else y - 1
    m2 = m - 1 if m > 1 else 12
    url_curr = f"https://www.tpex.org.tw/www/zh-tw/emerging/historical?type=Monthly&date={y}/{m:02d}/01&code={code}&response=json"
    url_prev = f"https://www.tpex.org.tw/www/zh-tw/emerging/historical?type=Monthly&date={y2}/{m2:02d}/01&code={code}&response=json"

    for attempt in range(1, 6):
        try:
            r1 = req.get(url_curr, headers=headers, timeout=10)
            r2 = req.get(url_prev, headers=headers, timeout=10)

            if r1.status_code == 200 and r2.status_code == 200:
                json1 = r1.json()
                json2 = r2.json()

                if "tables" in json1 and "tables" in json2 and json1["tables"] and json2["tables"]:
                    df1 = pd.DataFrame(json1["tables"][0]["data"], columns=json1["tables"][0]["fields"])
                    df2 = pd.DataFrame(json2["tables"][0]["data"], columns=json2["tables"][0]["fields"])
                    df_combi = pd.concat([df1, df2], ignore_index=True)

                    # 1. 確保只取前 7 欄並統一命名
                    df_combi = df_combi.iloc[:, 0:7].copy()
                    df_combi.columns = ["日期", "成交股數", "成交金額(元)", "成交最高", "成交最低", "成交均價", "筆數"]
                    
                    # 2. 針對「每一欄 (Series)」個別使用 .str 處理
                    num_cols = ["成交股數", "成交金額(元)", "成交最高", "成交最低", "成交均價", "筆數"]
                    for col in num_cols:
                        # 先轉為字串以防萬一，再去掉逗號，最後轉數字
                        df_combi[col] = df_combi[col].astype(str).str.replace(',', '')
                        df_combi[col] = pd.to_numeric(df_combi[col], errors='coerce').fillna(0)
                    
                    # 3. 轉換日期
                    df_combi["日期"] = df_combi["日期"].apply(fix_date)
                    return df_combi
                else:
                    print(f"⚠️ {code} 資料表結構異常或無資料")
            else:
                print(f"⚠️ {code} 連線失敗 (Status: {r1.status_code})")

        except Exception as e:
            print(f"❌ 第 {attempt} 次嘗試發生錯誤: {e}")

        time.sleep(attempt * 2 + random.uniform(1, 3))
    return None

def data_output(df, target_date):
    """計算十日平均指標"""
    if df is None or df.empty:
        return None
    
    # 只取投標日之前的資料
    df = df[df["日期"] < target_date].sort_values("日期").reset_index(drop=True)
    if len(df) == 0:
        return None

    re = {}
    # 單日指標 (取最後一列)
    last_row = df.iloc[-1]
    re['前一日平均成交價'] = last_row['成交均價']
    re['前一日成交金額'] = last_row['成交金額(元)']
    re['前一日最高成交價'] = last_row['成交最高']
    re['前一日最低成交價'] = last_row['成交最低']
    re['前一日成交筆數'] = last_row['筆數']
    re['前一日成交股數'] = last_row['成交股數']

    # 十日平均指標 (取最後十列)
    last_10 = df.tail(10)
    re['前十日內平均成交價'] = round(last_10['成交均價'].mean(), 3)
    re['前十日內平均成交金額'] = round(last_10['成交金額(元)'].mean(), 0)
    re['前十日內平均成交筆數'] = round(last_10['筆數'].mean(), 0)
    re['前十日內平均成交股數'] = round(last_10['成交股數'].mean(), 0)

    # 漲幅計算
    first_price_10 = last_10['成交均價'].iloc[0]
    if first_price_10 != 0:
        re['前十日內漲幅'] = round((re['前一日平均成交價'] - first_price_10) / first_price_10, 3)
    else:
        re['前十日內漲幅'] = 0.0

    return re