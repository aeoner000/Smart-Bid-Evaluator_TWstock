import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from streamlit_unit.query_func import get_curr_ipo, get_predict_result, get_base_info, get_feature_important
from streamlit_unit.data_engine import add_system_info

# 1. 頁面配置與進階 CSS
st.set_page_config(layout="wide", page_title="即時預測中心")
add_system_info(
    title="操作說明",
    content="1.點選資訊表左方小框開始預測<br>2.特徵重要性則告訴您哪些影響最大"
)
st.markdown("""
    <style>
    [data-testid="stDataFrameColHeaderCheckbox"] { display: none; }
    [data-testid="stDataFrameRowHeaderCheckbox"] { display: none; }
    
    .main-title {
        font-size: 1.4rem; font-weight: 800; color: #1e3a8a;
        border-left: 5px solid #3b82f6; padding-left: 12px; margin-top: 10px;
    }

    /* AI 智慧預測樣式 */
    .predict-card-gold {
        display: flex; justify-content: space-between; align-items: center;
        background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%); 
        border: 1px solid #f59e0b; border-radius: 8px;
        padding: 12px 20px; margin-bottom: 5px;
        box-shadow: 0 2px 4px rgba(245, 158, 11, 0.1);
    }
    .predict-label-gold { color: #92400e; font-size: 1rem; font-weight: 700; }
    .predict-value-gold { color: #b45309; font-size: 2rem; font-weight: 900; }
    .predict-value-red { color: #dc2626; font-size: 2rem; font-weight: 900; }

    /* 基本面卡片樣式 */
    .info-card {
        background-color: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px;
        padding: 18px 22px; margin-bottom: 12px; box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }
    .info-label { color: #64748b; font-size: 0.9rem; font-weight: 600; margin-bottom: 6px; }
    .info-value { color: #0f172a; font-size: 1.5rem; font-weight: 800; }
    
    .stTabs [data-baseweb="tab"] { font-size: 1rem; font-weight: 600; }
    </style>
""", unsafe_allow_html=True)


# 💡 新增：固定的全域權重資料 (不再隨選擇變動)
GLOBAL_WEIGHTS = {
    "features": {"vals": [0.45, 0.22, 0.15, 0.10, 0.08], "labs": ["法人籌碼面", "產業成長性", "財務穩定度", "市場情緒面", "技術指標面"]},
    "sentiment": {"vals": [90, 82, 75, 60, 45], "labs": ["新聞正向率", "社群熱烈度", "機構推薦度", "散戶關注度", "空頭平倉量"]},
    "industry": {"vals": [95, 88, 70, 65, 55], "labs": ["毛利領先度", "研發投入比", "市佔穩定率", "資產回報率", "現金流量比"]}
}


# 3. 第一區：列表
ipo_df = get_curr_ipo()
st.markdown('<div class="main-title">近期競拍標的</div>', unsafe_allow_html=True)
select_event = st.dataframe(
    ipo_df, use_container_width=True, hide_index=True,
    on_select="rerun", selection_mode="single-row", height=225
)

sel_idx = select_event.selection.rows[0] if select_event.selection.rows else 0
sel_stock = ipo_df.iloc[sel_idx] # 取得資料表的使用者點選的股票


target_code = str(sel_stock["證券代號"])
predict_series = get_predict_result(target_code) # 輸出點選的股票的預測 series

# 4. 第二區：AI 智慧預測 (與列表連動)
st.markdown('<div class="main-title">AI 智慧預測</div>', unsafe_allow_html=True)
p_cols = st.columns(3)

# 第一欄
with p_cols[0]:
    st.markdown(f'''<div class="predict-card-gold">
        <div class="predict-label-gold">預估最低中標價格</div>
        <div class="predict-value-gold">{predict_series["預估最低中標價格"]}</div>
    </div>''', unsafe_allow_html=True)

# 第二欄
with p_cols[1]:
    st.markdown(f'''<div class="predict-card-gold">
        <div class="predict-label-gold">預估平均中標價格</div>
        <div class="predict-value-gold">{predict_series["預估平均中標價格"]}</div>
    </div>''', unsafe_allow_html=True)

# 第三欄
with p_cols[2]:
    st.markdown(f'''<div class="predict-card-gold">
        <div class="predict-label-gold">預估上市開盤價</div>
        <div class="predict-value-red">{predict_series["預估上市開盤價"]}</div>
    </div>''', unsafe_allow_html=True)

# 5. 下方分欄
col_left, col_right = st.columns([7, 3], gap="large")
#target_code
base_info_series = get_base_info(target_code)

with col_left:
    st.markdown(f"### 基本面核心矩陣")
    m_cols = st.columns(3)
    
    # 這裡的迴圈邏輯完全不用動！
    # .items() 會依序吐出 (欄位名, 數值)
    for i, (label, val) in enumerate(base_info_series.items()):
        with m_cols[i % 3]:
            # 如果需要加上單位（如 %），可以在這裡判斷
            display_val = f"{val:.2f}%" if "率" in label or "比" in label else f"{val:.2f}"
            
            st.markdown(
                f'''
                <div class="info-card">
                    <div class="info-label">{label}</div>
                    <div class="info-value">{display_val}</div>
                </div>
                ''', 
                unsafe_allow_html=True
            )

imp_features = get_feature_important() # df
def create_rank_list(feature_list, color):
    # 確保只取前 5 個，不足補 N/A
    top_5 = (list(feature_list)[:5] + ["N/A"] * 5)[:5]
    
    html_content = ""
    for i, name in enumerate(top_5):
        rank = i + 1
        # 設定不同名次的透明度
        opacity = 1 - (i * 0.1) 
        
        # --- 僅針對第一名定義特殊文字樣式，其餘名次維持原樣 ---
        if i == 0:
            # 第一名：紅色、900粗度、1.3em大小
            text_color = "#FF0000"
            text_weight = "900"
            text_size = "1.3em"
        else:
            # 其他名次：原本的顏色、500粗度、1.1em大小
            text_color = color
            text_weight = "500"
            text_size = "1.1em"

        html_content += f"""
        <div style="
            display: flex; align-items: center; 
            margin-bottom: 8px; padding: 10px; 
            background-color: {color}30; border-left: 5px solid {color};
            border-radius: 4px; opacity: {opacity};
        ">
            <div style="
                font-weight: bold; color: {text_color}; 
                margin-right: 15px; width: 25px; font-size: {text_size};
            ">#{rank}</div>
            <div style="color: {text_color if i==0 else '#475569'}; font-weight: {text_weight};">
                {name}
            </div>
        </div>
        """
    return st.markdown(html_content, unsafe_allow_html=True)

with col_right:
    st.markdown('<div class="main-title">預測目標特徵重要性 (Top 5)</div>', unsafe_allow_html=True)
    tab1, tab2, tab3 = st.tabs(["最低得標加價率", "加權平均加價率", "預估獲利率"])
    
    with tab1:
        # 傳入該欄位的 list
        create_rank_list(imp_features["最低得標加價率"].tolist(), "#d97706")
        
    with tab2:
        create_rank_list(imp_features["加權平均加價率"].tolist(), "#3b82f6")
        
    with tab3:
        create_rank_list(imp_features["預估獲利率"].tolist(), "#10b981")