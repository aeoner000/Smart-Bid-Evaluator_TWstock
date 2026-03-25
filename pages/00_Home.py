import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots

from streamlit_unit.query_func import get_all_avg_pred_diff, get_sample_size, get_update_time
from streamlit_unit.data_engine import add_system_info


last_reboot = get_update_time()
st.markdown(f"""
    <div style="position: absolute; top: -50px; right: 60px; display: flex; gap: 15px; align-items: center;">
        <a href="https://github.com/aeoner000/Smart-Bid-Evaluator_TWstock.git" target="_blank" style="text-decoration: none; color: #666; font-size: 13px; display: flex; align-items: center; gap: 4px;">
            <span></span> GitHub連結
        </a>
        <a href="mailto:support@company.com" style="text-decoration: none; color: #666; font-size: 13px; display: flex; align-items: center; gap: 4px;">
            <span></span> 幫助中心
        </a>
        <div style="width: 1px; height: 12px; background-color: #ccc; margin: 0 5px;"></div>
    </div>
""", unsafe_allow_html=True)
add_system_info()
# 建立三欄佈局，第一欄放標題，後兩欄放數據
head_col, kpi1, kpi2 = st.columns([3, 1, 1])

with head_col:
    st.markdown(f"""
        <h1 style="margin-bottom: 0px; font-weight: 900;">系統首頁</h1>
        <p style="color: #666; font-size: 0.9rem;">最後更新：{last_reboot} | 每日爬蟲更新</p>
    """, unsafe_allow_html=True)

with kpi1:
    n = get_sample_size()
    st.metric(label="目前總樣本", value=n, delta="未上市樣本")

with kpi2:
    st.metric(label="使用模型", value="GBDT",  delta="LGBM、XGB、CAT 進行評估")

st.divider()

# 1. 定義按鈕的自定義 CSS (放在標頭或 CSS 區塊)
st.markdown("""
    <style>
    /* 左側：深藍色實心按鈕 (預設按鈕樣式) */
    div[data-testid="stColumn"]:nth-child(1) button {
        background-color: #1547A1 !important;
        color: white !important;
        border: none !important;
        padding: 20px !important;
        border-radius: 12px !important;
        font-weight: 700 !important;
        height: 70px !important;
    }

    /* 右側：白色底深藍框按鈕 (第二個欄位的按鈕) */
    div[data-testid="stColumn"]:nth-child(2) button {
        background-color: white !important;
        color: #1547A1 !important;
        border: 2px solid #1547A1 !important;
        padding: 20px !important;
        border-radius: 12px !important;
        font-weight: 700 !important;
        height: 70px !important;
    }
    
    /* 懸停效果 */
    button:hover {
        opacity: 0.8;
        transform: translateY(-2px);
        transition: 0.3s;
    }
    </style>
""", unsafe_allow_html=True)

# 2. 正常的 Streamlit 代碼 (完全不用動邏輯)
col1, col2 = st.columns(2)

with col1:
    # 這裡的文字會直接套用 CSS，你可以換成你要的 Icon
    if st.button("進入即時預測", use_container_width=True, key="btn_analysis"):
        st.switch_page("pages/01_presict_view.py")

with col2:
    if st.button("查看歷史數據", use_container_width=True, key="btn_gcs"):
        st.switch_page("pages/02_history.py")

st.markdown("---")
# 定義卡片組件函數
def core_feature_card(number, label, title, content):
    import uuid
    uid = str(uuid.uuid4())[:8]
    
    # CSS 部分：將功能相近的屬性寫在同一行，減少垂直空間
    style = f"""
    <style>
        #chk-{uid} {{ display: none; }}
        .box-{uid} {{ 
            color: #555; font-size: 14px; line-height: 1.6; 
            display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; 
            overflow: hidden; transition: all 0.3s ease; 
        }}
        #chk-{uid}:checked ~ .box-{uid} {{ 
            -webkit-line-clamp: unset; display: block; 
        }}
        .btn-{uid} {{ 
            display: inline-block; margin-top: 10px; 
            color: #1547A1; font-size: 12px; font-weight: bold; cursor: pointer; 
        }}
        #chk-{uid}:checked ~ .btn-{uid}::before {{ content: "▲ 收起內容"; }}
        #chk-{uid}:not(:checked) ~ .btn-{uid}::before {{ content: "▼ 展開更多"; }}
    </style>
    """

    # HTML 部分：標籤獨立換行，但 style 屬性保持緊湊
    content_html = f"""
    <div style="background: white; padding: 25px; border-radius: 12px; border: 1px solid #f0f2f6; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-bottom: 20px; min-height: 180px;">
        <div style="color: #1E3A8A; font-family: monospace; font-size: 12px; font-weight: 700; margin-bottom: 8px;">
            {number} / {label}
        </div>
        <h3 style="color: #0D1B2A; margin: 0 0 12px 0; font-size: 20px; font-weight: 800;">
            {title}
        </h3>
        <input type="checkbox" id="chk-{uid}">
        <div class="box-{uid}">{content}</div>
        <label for="chk-{uid}" class="btn-{uid}"></label>
    </div>
    """
    
    return style + content_html

# 建立 2x2 佈局
col1, col2 = st.columns(2)

pipeline_text = """
    <b>AI 數據煉金術：從監控到決策</b><br>
    本系統建構了一套全自動數據流水線，將零散資訊轉化為精準的投標指引：<br>
    <br>
    <b>1. 全自動數據監控（數據收集）</b><br>
    系統如同 24 小時偵測器，每日自動追蹤<b>證交所</b>與<b>公開資訊觀測站</b>。同步擷取新股公告、財務體質（獲利與營收）及市場熱度（股價與法人動向），並整合歷年歷史得標紀錄，確保資料權威且完整。<br>
    <br>
    <b>2. 深度特徵加工（數據提煉）</b><br>
    原始資料經由 AI 引擎深度加工，提煉出成長動能、獲利穩定性等上百項量化指標。透過自動化清洗與異常值修正，將數據轉化為高品質的「特徵燃料」，驅動 <b>XGBoost</b> 等先進模型，助您掌握科學化的競拍勝機。
    """

model_intro_text = """
    <b>系統預測目標：三大核心加價與獲利指標</b><br>
    本平台透過 AI 模型針對競拍案產出三項關鍵預測值，助您精準出價：<br>
    1. <b>最低得標加價率</b>：預測「最低得標價」相對於「最低投標價」的溢價百分比。<br>
    2. <b>加權平均加價率</b>：預測「平均得標價」相對於「最低投標價」的溢價百分比。<br>
    3. <b>預估獲利率</b>：預測「上市開盤價」相對於「投標日前一日成交價」的潛在獲利百分比。<br>
    <br>
    <b>模型選用：梯度提升 (GBDT) 與 AutoML 技術</b><br>
    系統採用強大的<b>梯度提升（Gradient Boosting）</b>模型，並透過 <b>AutoML</b> 技術進行自動化超參數調校。此架構擅長處理金融市場的複雜表格式數據，能精準捕捉財務指標與市場情緒間的非線性關係。藉由 AutoML 自動優化配置，確保模型在不同市場循環下均能維持高精準度與穩健性，為您的競拍決策提供最科學的數據支持。
    """

intro_text = """
    本平台將複雜的金融大數據轉化為直觀的投資指引，呈現三大核心視角：<br>
    <br>
    <b>1. 預測透明化：AI 準確度實證</b><br>
    首頁呈現「模型效能對照圖」，直觀展示歷史預測值與真實得標價的誤差，確保 AI 的可靠性與透明度，讓使用者在參考建議前先建立信任感。<br>
    <br>
    <b>2. 決策即時化：核心指標預測</b><br>
    針對當前競拍個股，系統即時呈現預測得標價、溢價率及潛在報酬率。配合「特徵重要性儀表板」，揭露影響本次出價的關鍵因子（如市場動能、財務品質），讓 AI 不再是黑盒子。<br>
    <br>
    <b>3. 歷史回測化：權威數據追蹤</b><br>
    完整收錄歷年競拍戰績，支援多維度篩選回測。所有資訊均源自 TWSE 與 MOPS 等官方渠道，呈現具備權威性、可追溯性的數據引擎架構，助您科學化掌握每一場競拍勝機。
    """
with col1:
    st.markdown(core_feature_card(
        "01", "MISSION", "核心定位", 
        "為台股投資人打造之AI競拍投標助理。它自動整合公司基本面與市場數據，利用機器學習精準預測競拍得標價，旨在輔助使用者制定數據驅動的最佳投標決策，克服資訊不對稱的挑戰，有效提升競拍成功率與投資回報。"
    ), unsafe_allow_html=True)
    
    st.markdown(core_feature_card(
        "03", "ANALYSIS", "模型選用及預測目標", 
        f"{model_intro_text}"
    ), unsafe_allow_html=True)

with col2:
    st.markdown(core_feature_card(
        "02", "ENGINE", "數據引擎", 
        f"{pipeline_text}"
    ), unsafe_allow_html=True)
    
    st.markdown(core_feature_card(
        "04", "FUTURE", "平台介紹", 
        f"{intro_text}"
    ), unsafe_allow_html=True)


# 模型效能層
# 1. 建立具有雙 Y 軸的畫布
fig = make_subplots(specs=[[{"secondary_y": True}]])
df = get_all_avg_pred_diff()
# 2. 加入預測值與真實值 (主 Y 軸：左邊)
fig.add_trace(
    go.Scatter(x=df.index, y=df['預測值平均'], name='預測值平均', 
               line=dict(color='#3b82f6', width=3)),
    secondary_y=False
)

fig.add_trace(
    go.Scatter(x=df.index, y=df['真實值平均'], name='真實值平均', 
               line=dict(color='#ef4444', width=2)),
    secondary_y=False
)

# 3. 加入誤差長條圖 (副 Y 軸：右邊)
# 設定半透明顏色，才不會遮住後面的線
fig.add_trace(
    go.Bar(x=df.index, y=df['平均誤差'], name='預測誤差',
           marker_color='rgba(156, 163, 175, 0.3)'), # 灰色半透明
    secondary_y=True
)

# 4. 更新佈局
fig.update_layout(
    title={
        'text': "<b>模型預測值與真實值誤差對照圖(模型預測為比率)</b>", # 標題文字，支援 HTML 加粗
        'y': 0.95,           # 標題高度位置 (0 到 1)
        'x': 0.5,            # 標題水平位置 (0.5 為置中)
        'xanchor': 'center', # 錨點置中
        'yanchor': 'top',
        'font': dict(size=18, color='#1e293b') # 字體大小與顏色
    },
    template="plotly_white",
    margin=dict(l=20, r=20, t=20, b=20),
    height=400, # 疊圖建議高度稍微拉高一點
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    hovermode="x unified" # 鼠標移上去時會同時顯示三條數據，超好用！
)

fig.update_xaxes(title_text="樣本序號 (Sample Index)")
# 設定左右 Y 軸的標題
fig.update_yaxes(title_text="比率", secondary_y=False)
fig.update_yaxes(title_text="比率誤差值", secondary_y=True)

st.plotly_chart(fig, use_container_width=True)
