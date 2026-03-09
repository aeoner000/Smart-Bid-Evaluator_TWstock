import pandas as pd
import yfinance as yf
from datetime import timedelta
import os

def get_near_n_day(df: pd.DataFrame, day=10, date_name="date"):
    df[date_name] = pd.to_datetime(df[date_name])
    unique_dates = sorted(df[date_name].unique())
    # 取最後 n 天 (不含當天，避免當天資料不全)
    near_n_day = unique_dates[-(day+1):-1]
    return df[df[date_name].isin(near_n_day)].copy()

def get_market_Inst_tw(dl, target_time, day=10):
    end_date = target_time - timedelta(days=1)
    start_date = end_date - timedelta(days=40)
    df_total = dl.taiwan_stock_institutional_investors_total(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d")
    )
    df_total["name"] = df_total["name"].str.strip()
    near_n_day_df = get_near_n_day(df_total, day)
    near_n_day_df["net"] = near_n_day_df["buy"].astype(float) - near_n_day_df["sell"].astype(float)
    df_pivot = near_n_day_df.pivot_table(index="date", columns="name", values="net", aggfunc='sum')
    df_pivot["Dealer_total"] = df_pivot["Dealer_Hedging"] + df_pivot["Dealer_self"]
    
    return pd.Series({
        "外資平均增減": df_pivot['Foreign_Investor'].mean(),
        "投信平均增減": df_pivot['Investment_Trust'].mean(),
        "自營商平均增減": df_pivot['Dealer_total'].mean()
    })

def get_margin(dl, target_time, day=10):
    end_date = target_time - timedelta(days=1)
    start_date = end_date - timedelta(days=40)
    df_total = dl.taiwan_stock_margin_purchase_short_sale_total(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d")
    )
    near_n_day_df = get_near_n_day(df_total, day)
    near_n_day_df["change"] = near_n_day_df["TodayBalance"] - near_n_day_df["YesBalance"]
    df_pivot = near_n_day_df.pivot_table(index="date", columns="name", values="change")
    return pd.Series({
        "融資張數增減": df_pivot['MarginPurchase'].mean().round(3),
        "融券張數增減": df_pivot['ShortSale'].mean().round(3),
        "融資金額增減": df_pivot['MarginPurchaseMoney'].mean().round(3)
    })

def get_market_usa(target_time, tickers, name_map, day=10):
    end_date = target_time - timedelta(days=1)
    start_date = end_date - timedelta(days=40)
    df_total = yf.download(tickers, start=start_date, end=end_date, progress=False)
    close_df = df_total["Close"].reset_index()
    near_n_day_df = get_near_n_day(close_df, day, "Date")
    diff = ((near_n_day_df.iloc[-1, 1:] - near_n_day_df.iloc[0, 1:]) / near_n_day_df.iloc[0, 1:]) * 100
    return diff.astype(float).round(3).rename(index=name_map)

def get_market_tw(target_time, days=10):
    end_date = target_time
    start_date = end_date - timedelta(days=40)
    re = {}
    for code in ["^TWII", "^TWOII"]:
        df = yf.download(code, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), progress=False)
        if df.empty: continue
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        final_df = df.tail(days).copy()
        price_col = 'Adj Close' if 'Adj Close' in df.columns else 'Close'
        total_change_pct = ((final_df[price_col].iloc[-1] - final_df[price_col].iloc[0]) / final_df[price_col].iloc[0]) * 100
        code_name = "大盤" if code == "^TWII" else "櫃買"
        re[f"{code_name}_10日漲幅(%)"] = round(total_change_pct, 3)
        re[f"{code_name}_平均成交量"] = round(final_df['Volume'].mean(), 0)
    return pd.Series(re)

