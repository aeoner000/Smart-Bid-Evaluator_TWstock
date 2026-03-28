import streamlit as st

# 1. 注入自定義 CSS (鎖定類別，不影響側邊欄)
st.markdown("""
    <style>
    /* 僅針對自定義圖塊進行樣式設定 */
    .floating-card {
        /* 左上深 (#d9e2ec) 到 右下淺 (#ffffff) 的漸層 */
        background: linear-gradient(135deg, #d9e2ec 0%, #f0f4f8 50%, #ffffff 100%); 
        padding: 30px;
        border-radius: 10px;
        box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.05); 
        transition: all 0.4s cubic-bezier(0.165, 0.84, 0.44, 1);
        border: 1px solid #cfd8dc;
        margin-bottom: 25px;
        color: #263238;
    }

    /* 懸停效果：向上浮動並強化左上深色感 */
    .floating-card:hover {
        transform: translateY(-8px);
        box-shadow: 0 15px 35px rgba(144, 164, 174, 0.4);
        border: 1px solid #546e7a;
        background: linear-gradient(135deg, #cfd8e3 0%, #e1e9f0 100%);
    }

    /* 圖塊內的標題樣式 */
    .floating-card h1, .floating-card h2, .floating-card h3, .floating-card h4 {
        color: #102027;
        margin-top: 0;
        font-weight: 700;
        border: none; /* 防止與 Streamlit 預設標題線衝突 */
    }

    /* 圖塊內的水平線 */
    .floating-card hr {
        border: 0;
        border-top: 2px solid #546e7a;
        margin: 15px 0;
        opacity: 0.3;
    }

    /* 圖塊內的列表符號 */
    .floating-card ul {
        list-style-type: none;
        padding-left: 0;
    }
    .floating-card li:before {
        content: "■ ";
        color: #546e7a;
        font-size: 10px;
        margin-right: 8px;
    }
    </style>
    """, unsafe_allow_html=True)

# 1. 專業定位與核心簡介
st.markdown(f"""
<div class="floating-card">
    <h1>■ 關於開發者 | Data Engineering & Analysis</h1>
    <hr>
    <h3><strong>跨領域轉型：從非相關專業領域走向數據驅動開發</strong></h3>
    <p>具備碩士學位與持續的自我精進能力，未來將致力於數據工程與機器學習領域。<br>
    擅長透過 Python 建立自動化 ETL 流程，並將複雜的業務邏輯轉化為可預測的數據模型。</p>
</div>
""", unsafe_allow_html=True)

# 2. 轉職歷程與自學動機
st.markdown(f"""
<div class="floating-card">
    <h2>■ 轉職歷程與自學動機</h2>
    <p><strong>學經歷背景與轉折：</strong><br>
    我擁有<strong>都市計畫與空間資訊碩士</strong>學位，並於都市計畫與空間資訊領域累積了實務經驗。在處理複雜的法規、市場資訊及空間資料時，我觀察到傳統作業模式高度依賴人工整理，導致資訊零散且決策效率受限。因發現原本職業並不符合自身設想，期間也持續學習資料相關知識，因而產生踏入新領域的想法。</p>
    <p><strong>對資料領域的熱忱：</strong><br>
    就學時期接觸資料工程、科學領域，也將他運用於學位論文中，而過往經歷讓我開始投入對資料領域的的好奇與興趣，並藉由<strong>Python、SQL</strong> 的自學，從基礎數據清理到進階架構設計，發現自己深受數據背後的邏輯與規律吸引，享受將原始資料萃取為核心價值的過程。目前也持續藉由Cousera雲端學習平台、TibaMe在職課程提升自己。</p>
</div>
""", unsafe_allow_html=True)

# 3. 技術核心能力 (三欄配置)
st.markdown("<h2 style='color: #546e7a; padding-left: 5px;'>■ 技術核心能力</h2>", unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""<div class="floating-card">
        <h4>Programming</h4>
        <ul>
            <li><strong>Python</strong>: 自動化開發</li>
            <li><strong>SQL</strong>: 資料庫查詢</li>
            <li><strong>Development</strong>: Git 版本管理</li>
        </ul>
    </div>""", unsafe_allow_html=True)

with col2:
    st.markdown("""<div class="floating-card">
        <h4>Data Engineering</h4>
        <ul>
            <li><strong>BigQuery</strong>: 雲端數據倉庫</li>
            <li><strong>ETL Pipeline</strong>: 清洗整合</li>
            <li><strong>Migration</strong>: 架構優化</li>
        </ul>
    </div>""", unsafe_allow_html=True)

with col3:
    st.markdown("""<div class="floating-card">
        <h4>Data Science</h4>
        <ul>
            <li><strong>ML Modeling</strong>: XGBoost</li>
            <li><strong>Analysis</strong>: Pandas</li>
            <li><strong>Feature</strong>: 特徵處理</li>
        </ul>
    </div>""", unsafe_allow_html=True)

# 4. 專案實作
st.markdown(f"""
<div class="floating-card">
    <h2>■ 核心專案：Smart Bidding v6.2</h2>
    <h4><strong>端對端金融數據預測系統</strong></h4>
    <p><strong>開發目的：</strong><br>
    建立自動化整合多維度數據系統，透過機器學習模型提供客觀價格預測。</p>
    <hr>
    <p><strong>技術實踐：</strong></p>
    <ul>
        <li><strong>架構遷移</strong>：SQLite 遷移至 <strong>Google BigQuery</strong>。</li>
        <li><strong>自動化</strong>：開發爬蟲與清洗腳本，實現數據即時整合。</li>
        <li><strong>模型應用</strong>：運用 <strong>Boost 家族模型</strong> 進行競拍價格及未來上市價格預測。</li>
    </ul>
</div>
""", unsafe_allow_html=True)

# 5. 聯繫資訊
st.markdown(f"""
<div class="floating-card">
    <h2>■ 聯繫資訊</h2>
    <div style="display: flex; justify-content: space-between; flex-wrap: wrap;">
        <div style="flex: 1; min-width: 250px;">
            <p><strong>▸ Email:</strong> j341111@gmail.com</p>
            <p><strong>▸ GitHub:</strong> <a href="https://github.com/aeoner000/Smart-Bid-Evaluator_TWstock.git" target="_blank" style="color: #263238;">專案原始碼與文件</a></p>
        </div>
        <div style="flex: 1; min-width: 250px;">
            <p><strong>▸ Location:</strong> 台北市, 台灣</p>
            <p style="font-weight: bold; border-left: 4px solid #546e7a; padding-left: 10px; color: #546e7a;">Status: 積極尋求資料職位</p>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<center style='opacity:0.5; color: #546e7a;'>© 2026 Data Engineering Portfolio</center>", unsafe_allow_html=True)