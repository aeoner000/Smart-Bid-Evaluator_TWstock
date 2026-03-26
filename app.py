import streamlit as st
import streamlit.components.v1 as components
import datetime
st.cache_data.clear()
st.cache_resource.clear()
# 設定頁面配置
st.set_page_config(
    page_title="AI Auction v6.2",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# 定義你想要的導覽列大小
sidebar_font_size = "20px"
sidebar_icon_size = "23px"

# --- 顏色定義 ---
sidebar_bg_color = "#1B263B"  # 側邊欄：深藍灰
main_bg_color = "#F0F2F6"     # 主背景：淺藍灰 (更具質感)
accent_color = "#4CC9F0"      # 亮點綴色

st.markdown(f"""
    <style>
    /* 1. 側邊欄樣式 */
    [data-testid="stSidebar"] {{ 
        background-color: {sidebar_bg_color} !important; 
    }}
    [data-testid="stSidebar"] * {{ color: white !important; }}
    
    /* 2. 主背景改成淺藍灰 (重點修正) */
    .stApp {{
        background-color: {main_bg_color} !important;
    }}

    /* 調整 st.Page 導覽列的字體與圖示 (這部分無法用 HTML 標籤改) */
    [data-testid="stSidebarNavItems"] span {{ 
        font-size: {sidebar_font_size} !important; 
    }}
    [data-testid="stSidebarNavItems"] span[data-testid="stIconMaterial"] {{
        font-size: {sidebar_icon_size} !important;
    }}
    
    /* 調整內容排序，確保 HTML 內容在最上方 */
    [data-testid="stSidebarContent"] {{ display: flex; flex-direction: column; }}
    div[data-testid="stSidebarUserContent"] {{ order: 1 !important; }}
    [data-testid="stSidebarNav"] {{ order: 2 !important; }}

    /* 固定側邊欄最底部的文字樣式 */
    .sidebar-footer {{
        position: fixed;
        bottom: 15px;      /* 距離底部 15px */
        left: 20px;        /* 距離左側 20px */
        width: 260px;      /* 寬度需小於側邊欄寬度 */
        color: rgba(255,255,255,0.4); /* 半透明白色，顯得精緻 */
        font-size: 11px;   /* 小字級 */
        z-index: 99;       /* 確保在最上層 */
    }}
    </style>
    
    """, unsafe_allow_html=True)

# 1. 先定義頁面物件
p0 = st.Page("pages/00_Home.py", title="　首頁", icon=":material/home:")
p1 = st.Page("pages/01_presict_view.py", title="　及時預測", icon=":material/monitoring:") # 或用 bolt
p2 = st.Page("pages/02_history.py", title="　歷史探索", icon=":material/cloud_queue:")
p3 = st.Page("pages/f_00_source.py", title="　資料來源", icon=":material/language:")
# 2. 建立導覽物件
pg = st.navigation([p0, p1, p2, p3])
# 側邊欄品牌區

with st.sidebar:
    # 使用小括號將字串串接，這樣在編輯器可以換行，但在執行時是連貫的
    brand_html = (
        # 1. 整體位置：-70px 數字越大越往上衝
        '<div style="margin-top:-50px;padding-left:10px;display:flex;align-items:center;gap:12px;">'
            
            # 2. 圖示大小與粗細：
            # width/height 控制外框大小 (建議兩者一致)
            # stroke-width="3" 控制線條粗細 (數字越大越粗)
            '<svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="3" stroke-linecap="round" stroke-linejoin="round">'
                '<path d="M3 3v18h18"/>'
                '<path d="M7 14l3-3 4 4 5-5"/>'
            '</svg>'
            
            '<div>'
                # 3. 大標題文字大小 (ML AUCTION)：
                # font-size: 26px (建議範圍 24px~30px)
                # line-height: 1.1 (控制與下方小字的間距，數字越小越緊湊)
                '<div style="font-size:35px;font-weight:700;color:white;line-height:1.1;">ML AUCTION</div>'
                
                # 4. 版本號文字大小 (Smart Bidding)：
                # font-size: 13px (建議範圍 12px~14px)
                # color: rgba(255,255,255,0.7) (0.7 是透明度，數字越小越淡)
                '<div style="font-size:13px;color:rgba(255,255,255,0.7);margin-top:2px;">Smart Bidding</div>'
            '</div>'
        '</div>'
        
        # 5. 分隔線間距：
        # margin: 15px (上間距) 0px (左右) 10px (下間距)
        '<hr style="margin:15px 0px 10px 0px;border:0;border-top:1px solid rgba(255,255,255,0.2);">'
    )
    st.markdown(brand_html, unsafe_allow_html=True)
    st.markdown('<div class="sidebar-footer">v1.0.0-stable | © 2026 AI Auction streamlit</div>', unsafe_allow_html=True)


# 4. 最後才執行 run
pg.run()
