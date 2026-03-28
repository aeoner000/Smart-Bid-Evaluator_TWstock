
import streamlit as st
st.cache_data.clear()
# 1. 頁面配置
st.set_page_config(
    page_title="AI Auction v6.2",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. 頁面定義 (請確保 pages/ 資料夾檔案路徑正確)
p_home = st.Page("pages/00_Home.py", title="首頁總覽", icon=":material/home:")
p_predict = st.Page("pages/01_predict_view.py", title="及時預測", icon=":material/monitoring:")
p_history = st.Page("pages/02_history.py", title="歷史探索", icon=":material/analytics:")
p_source = st.Page("pages/f_00_source.py", title="資料來源", icon=":material/database:")
p_about = st.Page("pages/f_01_about.py", title="關於作者", icon=":material/person:")

# 3. 執行導覽
pg = st.navigation({
    "主要功能": [p_home, p_predict, p_history],
    "系統資訊": [p_source, p_about]
})

# 4. 核心 CSS 鎖定
sidebar_bg_color = "#1B263B"
accent_color = "#4CC9F0"

st.markdown(f"""
    <style>
    /* 側邊欄背景 */
    [data-testid="stSidebar"] {{
        background-color: {sidebar_bg_color} !important;
    }}
    [data-testid="stSidebar"] * {{ color: white !important; }}

    /* --- 強力選取器：分開控制標題文字 --- */

    /* 1. 主要功能 (第一個 Header) */
    [data-testid="stSidebarNavGroupHeader"]:nth-of-type(1) div[data-testid="stCaptionContainer"] {{
        font-size: 22px !important;  /* 故意調大到 22px 測試是否有變化 */
        color: {accent_color} !important;
        font-weight: 800 !important;
        line-height: 1.5 !important;
    }}

    /* 2. 系統資訊 (第二個 Header) */
    [data-testid="stSidebarNavGroupHeader"]:nth-of-type(2) div[data-testid="stCaptionContainer"] {{
        font-size: 10px !important;  /* 強制縮小到 10px */
        color: rgba(255,255,255,0.4) !important;
        font-weight: 400 !important;
        margin-top: 40px !important; /* 增加與上方區塊的距離 */
    }}

    /* 選單項目文字 (首頁、預測...) */
    [data-testid="stSidebarNavItems"] span {{ 
        font-size: 18px !important; 
    }}

    /* --- 頂部 Logo 偽元素釘選 --- */
    [data-testid="stSidebarContent"]::before {{
        content: "";
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 110px;
        background-color: {sidebar_bg_color};
        z-index: 10;
        border-bottom: 1px solid rgba(255,255,255,0.1);
        background-image: 
            url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="%234CC9F0" stroke-width="3"><path d="M3 3v18h18"/><path d="M7 14l3-3 4 4 5-5"/></svg>'),
            url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 60"><text x="0" y="40" fill="white" font-family="Arial" font-size="32" font-weight="bold">ML AUCTION</text></svg>'),
            url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 40"><text x="0" y="25" fill="rgba(255,255,255,0.6)" font-family="Arial" font-size="12">Smart Bidding v6.2</text></svg>');
        background-repeat: no-repeat;
        background-position: 20px 30px, 65px 25px, 65px 55px;
        background-size: 35px 35px, auto 60px, auto 40px;
    }}

    /* 選單位置調整 */
    [data-testid="stSidebarNav"] {{
        padding-top: 110px !important;
        padding-bottom: 60px !important;
    }}

    /* 底部版權偽元素釘選 */
    [data-testid="stSidebarContent"]::after {{
        content: "v1.0.0-stable | © 2026 AI Auction";
        position: absolute;
        bottom: 0; left: 0; right: 0;
        height: 50px;
        background-color: {sidebar_bg_color};
        z-index: 10;
        border-top: 1px solid rgba(255,255,255,0.1);
        display: flex; align-items: center; justify-content: center;
        font-size: 11px; color: rgba(255,255,255,0.3) !important;
    }}
    </style>
""", unsafe_allow_html=True)

pg.run()