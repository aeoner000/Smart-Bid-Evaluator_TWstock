import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from scipy import stats
from streamlit_unit.query_func import get_history_predict, get_all_feature_cols, get_contain_time_df

# 1. 頁面配置
st.set_page_config(page_title="競拍歷史探索系統", layout="wide")

# 2. 核心 CSS (僅保留導航與容器樣式，不設定全域字體)
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: transparent !important;
        border: none !important;
        font-size: 1.1rem;
        font-weight: 700;
        color: #64748b;
    }
    .stTabs [aria-selected="true"] {
        color: #f97316 !important; 
        border-bottom: 3px solid #f97316 !important;
    }
    [data-testid="stVerticalBlock"] > div:has(div.stPlotlyChart) {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 24px;
        margin-top: 15px;
    }
    </style>
""", unsafe_allow_html=True)


st.title("歷史探索系統")

# --- 🟢 第一層：大頁簽 ---
tab_backtest, tab_macro = st.tabs(["模型回測驗證 (GLOBAL BACKTEST)", "市場宏觀統計 (MARKET MACRO)"])

# --- [模組 A：模型回測驗證] (保持原始邏輯不變) ---

with tab_backtest:
    st.subheader("歷史數據回顧：預測與實際對照")
    def plot_bt(title, color, col):
        his_pred_df = get_history_predict(col)
        fig = go.Figure()
        x = his_pred_df.index
        y_act = his_pred_df.iloc[:, 0]
        y_pre = his_pred_df.iloc[:, 1]
        fig.add_trace(go.Scatter(x=x, y=y_act, name='實際真實值', line=dict(color='#cbd5e1', width=1.5), mode='lines'))
        fig.add_trace(go.Scatter(x=x, y=y_pre, name='模型預測值', line=dict(color=color, width=1.5), mode='lines'))
        fig.update_layout(title=dict(text=title, x=0, y=0.98, xanchor='left'), 
                            height=400, template="plotly_white", margin=dict(l=10, r=10, t=20, b=10), 
                            legend=dict(orientation="h", yanchor="top", y=1.3, xanchor="center", x=0.7), 
                            autosize=True,)
        return fig
    c1, c2 = st.columns(2)
    with c1: st.plotly_chart(plot_bt("最低得標加價率", "#f97316", "最低得標加價率"), use_container_width=True)
    with c2: st.plotly_chart(plot_bt("平均加權得標率", "#2563eb", "最低得標加價率"), use_container_width=True)
    st.plotly_chart(plot_bt("預估獲利率", "#10b981", "預估獲利率"), use_container_width=True)

# --- [模組 B：市場宏觀統計] ---
with tab_macro:
    sub_mode = st.segmented_control(
        "MacroSelect",
        options=["特徵相關性分析", "時間序列聚合趨勢"],
        default="特徵相關性分析",
        label_visibility="collapsed"
    )
    
    st.markdown("<br>", unsafe_allow_html=True)

    if sub_mode == "特徵相關性分析":
        # 參數選擇區
        # --- 2. 氣泡圖渲染函數 ---
        def render_bubble_chart(df, x_col, y_col, size_col, x_label, y_label, s_label, label_col='股票名稱', profit_col='獲利空間'):
            """
            全動態氣泡圖：支援負數處理、重複欄位校正與專業統計標籤
            """
            # 1. 確保欄位唯一性 (防範 X, Y, Size 選到同一個欄位導致 Pandas 報錯)
            all_cols = [x_col, y_col, size_col, label_col]
            if profit_col in df.columns:
                all_cols.append(profit_col)
            
            unique_cols = list(set(all_cols))
            plot_df = df[unique_cols].copy()

            # 2. 數值型別轉換與空值剔除
            for col in [x_col, y_col, size_col]:
                plot_df[col] = pd.to_numeric(plot_df[col], errors='coerce')
            
            # 剔除關鍵數值缺失的列
            plot_df = plot_df.dropna(subset=[x_col, y_col, size_col])

            if len(plot_df) < 2:
                st.warning("⚠️ 有效數據不足（至少需 2 筆數字資料），無法進行分析。")
                return

            # 3. 處理氣泡大小 (小於 0 給 0，並進行視覺縮放)
            # 使用 clip 確保符合 Plotly 要求 [0, inf]
            raw_size_clipped = plot_df[size_col].clip(lower=0)
            
            if raw_size_clipped.max() <= 0:
                plot_df['display_size'] = 8  # 若全為負或0，給予預設小點以免圖表空白
            else:
                # 縮放至直徑 0~50 像素
                plot_df['display_size'] = (raw_size_clipped / raw_size_clipped.max()) * 50

            # 4. 統計計算 (R, R-squared, P-value)
            x_v = plot_df[x_col].values
            y_v = plot_df[y_col].values
            slope, intercept, r_val, p_val, _ = stats.linregress(x_v, y_v)
            r_sq = r_val ** 2

            # 5. 繪圖開始
            fig = go.Figure()

            # A. 氣泡散佈層
            fig.add_trace(go.Scatter(
                x=x_v, y=y_v, 
                text=plot_df[label_col],
                mode='markers',
                name='數值',
                marker=dict(
                    size=plot_df['display_size'],
                    sizemode='diameter',
                    color=y_v, 
                    colorscale='Viridis', 
                    showscale=True,
                    colorbar=dict(title=f"氣泡數值", thickness=15),
                    line=dict(width=0.5, color='white')
                ),
                # 懸停文字設定
                hovertemplate=(
                    f"<b>%{{text}}</b><br>"
                    f"{x_label}: %{{x:,.2f}}<br>"
                    f"{y_label}: %{{y:,.2f}}<br>"
                    f"{s_label}: %{{customdata:,.2f}}<extra></extra>"
                ),
                customdata=plot_df[size_col] # 傳入原始值(含負數)供 Hover 顯示
            ))

            # B. 回歸趨勢線層
            line_x = [x_v.min(), x_v.max()]
            line_y = [slope * x_v.min() + intercept, slope * x_v.max() + intercept]
            fig.add_trace(go.Scatter(
                x=line_x, y=line_y, 
                mode='lines', 
                name='回歸趨勢',
                line=dict(color='#ef4444', dash='dash', width=2)
            ))

            # 6. 統計資訊框 (Annotation)
            p_text = "顯著(p<0.05)" if p_val<0.05 else "不顯著(p>0.05)"
            fig.add_annotation(
                xref="paper", yref="paper", 
                x=1, y=1.05, # 定位在圖表右上角上方
                xanchor="center", yanchor="bottom",
                text=(
                    f"判定係數 <b>R² : {r_sq:.4f}</b><br>"
                    f"相關係數 <b>R : {r_val:.3f}</b><br>"
                    f"P-Value : <b>{p_text}</b>"
                ),
                showarrow=False, align="left",
                bgcolor="rgba(255, 255, 255, 0.8)",
                bordercolor="#cbd5e1", borderwidth=1, borderpad=8,
                font=dict(size=14, color="#334155")
            )

            # 7. 佈局配置
            fig.update_layout(
                height=700,
                template="plotly_white",
                title=dict(
                    text=f"<b>多維相關性分析：{x_label} vs {y_label}</b>",
                    font=dict(size=22),
                    y=0.95,           # 標題垂直位置
                    x=0.5,            # 標題水平居中
                    xanchor="center"
                ),
                legend=dict(
                    orientation="h",  # 橫向排列
                    yanchor="top", y=-0.15, 
                    xanchor="center", x=0.5,
                    itemsizing='constant', itemwidth=30
                ),
                xaxis=dict(title=dict(text=f"<b>{x_label}</b>"), gridcolor="#f1f5f9"),
                yaxis=dict(title=dict(text=f"<b>{y_label}</b>"), gridcolor="#f1f5f9"),
                # t=150 留出頂部空間給標題與統計框，b=100 留給下方的 Legend
                margin=dict(t=150, r=30, b=100, l=60) 
            )

            return st.plotly_chart(fig, use_container_width=True)
    # --- 3. 主程式 UI 邏輯 ---
        col_data = get_all_feature_cols()      # 是一個字典紀錄完整原始表、原始欄位名稱、轉換後欄位名稱、轉換的zip dict
        display_list = col_data["rename_cols"] # 轉換後名稱列表
        name_map = col_data["map"]             # 一個 zip(轉換後名稱, 原始名稱)的 dict

        if not col_data["data"].empty:
            c1, c2, c3 = st.columns(3) # 改成三欄
            with c1:
                x_display = st.selectbox("X 軸參數", display_list, index=0)
            with c2:
                y_display = st.selectbox("Y 軸參數", display_list, index=1)
            with c3:
                # 新增：氣泡大小選單，預設選第三個參數或是你指定的權重欄位
                s_display = st.selectbox("氣泡數值", display_list, index=2)

            x_f = name_map[x_display]
            y_f = name_map[y_display]
            s_f = name_map[s_display]
    
            render_bubble_chart(
                df=col_data["data"], 
                x_col=x_f, 
                y_col=y_f,
                size_col=s_f, # 傳入動態選擇的欄位
                x_label=x_display, # 顯示用：中文名稱
                y_label=y_display, # 顯示用：中文名稱
                s_label=s_display, # 顯示用：中文名稱
                label_col=s_f
            )

    else:
        # 時間序列聚合
        c1, c2, c3 = st.columns([1.5, 2.5, 2])
        col_map = get_contain_time_df()      # 是一個字典紀錄完整原始表、原始欄位名稱、轉換後欄位名稱、轉換的zip dict
        rename_cols = col_map["rename_cols"] # 轉換後名稱列表
        df = col_map["data"]                 # 資料庫取的all_feature
        col_map = col_map["map"]             # 一個 zip(轉換後名稱, 原始名稱)的 dict

        target_feat = c1.selectbox("目標特徵", rename_cols) # 使用者點擊
        time_dim = c2.radio("時間", ["年", "季", "月"], horizontal=True, index=1)
        op_type = c3.radio("聚合邏輯", ["平均", "加總", "中位數"], horizontal=True)
 
        freq_map = {"年": "YE", "季": "QE", "月": "ME"}
        agg_map = {"平均": "mean", "加總": "sum", "中位數": "median"}

        ts_data = df.resample(freq_map[time_dim])[col_map.get(target_feat)].agg(agg_map[op_type])
        unit = "%" if any(word in target_feat for word in ["率", "百分比", "ROE", "ROA"]) else ""
        if time_dim == "月":
            ts_data.index = ts_data.index.strftime('%Y-%m') # 變成 "2026-03"
        elif time_dim == "季":
            ts_data.index = ts_data.index.to_period("Q").astype(str) # 變成 "2026Q1"
        else: # 年
            ts_data.index = ts_data.index.strftime('%Y') # 變成 "2026"
        fig_ts = go.Figure(go.Bar(x=ts_data.index, y=ts_data.values, marker_color='#f97316', 
                                    hovertemplate=f"數值: %{{y:.2f}}{unit}<extra></extra>"))
        fig_ts.update_xaxes(
            type='category', # 強制轉為類別軸，這就不會出現「沒數據的月份」
            tickangle=-70
        )
        # 1. 定義 Plotly 控制按鈕與縮放設定
        chart_config = {
            'hovermode': "x unified",
            'displayModeBar': True,      # 是否顯示右上方工具列
            'displaylogo': False,        # 隱藏 Plotly Logo
            'modeBarButtonsToRemove': ['lasso2d', 'select2d'], # 移除不常用的工具
            'scrollZoom': False,         # 禁用滾輪縮放（避免捲動網頁時誤觸）
            'responsive': True           # 自動隨視窗縮放
        }
        st.plotly_chart(fig_ts, use_container_width=True, config=chart_config)