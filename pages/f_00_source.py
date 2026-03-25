import streamlit as st

def show_source_page():
    # --- 1. 資料定義區 (包含五個來源) ---
    SOURCES = [
        {"name": "臺灣證券交易所 (TWSE)", "url": "https://www.twse.com.tw", 
         "desc": "獲取每日競拍公告、個股基本面及市場成交資訊。臺灣證券交易所負責股票上市審查與市場監督。"},
        {"name": "公開資訊觀測站", "url": "https://mops.twse.com.tw", 
         "desc": "公司財務報表與營收資訊來源。專供投資人查詢上市櫃公司的重大訊息、財務報表與營運數據。"},
        {"name": "Yahoo Finance API", "url": "https://finance.yahoo.com", 
         "desc": "蒐集上市日大盤資訊。提供即時股價、歷史走勢及財務數據，用於數據分析與模型校準。"},
        {"name": "FinMind API", "url": "https://finmindtrade.com/", 
         "desc": "蒐集股市資訊與籌碼面數據。提供涵蓋台股、美股及匯率的技術、基本與法人資料庫。"},
        {"name": "證券櫃檯買賣中心", "url": "https://www.tpex.org.tw/zh-tw/esb/trading/info/stock-pricing.html", 
         "desc": "由櫃買中心取得預競拍股票之歷史價格等資訊。它是提供上櫃、興櫃與債券交易的法定機構，旨在活絡中小型企業籌資與多元金融商品交易。"},
    ]

    # --- 2. 頁面標題與統一說明 ---
    st.markdown('<h1 style="font-weight: 900; margin-bottom: 0px;">數據來源說明</h1>', unsafe_allow_html=True)
    st.markdown("""
        <p style="color: #666; margin-bottom: 20px; line-height: 1.6;">
            本系統之預測模型高度整合下列來源之官方數據。所有資訊皆由後端爬蟲每日同步，
            並經過數據清洗與特徵工程處理，確保預測特徵之準確性。
        </p>
    """, unsafe_allow_html=True)
    
    st.divider()

    # --- 3. 提示語 (已移至圖塊上方) ---
    st.info("💡 提示：點擊標題即可跳轉至官方網站。所有預測結果僅供參考，請以官方最終公告為準。")
    st.markdown("<br>", unsafe_allow_html=True)

    # --- 4. 樣式定義 (保持整潔換行) ---
    st.markdown("""
        <style>
        .source-card {
            background: white; padding: 22px; border-radius: 12px;
            border: 1px solid #f0f2f6; box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            margin-bottom: 18px; transition: transform 0.2s; min-height: 165px;
        }
        .source-card:hover { 
            transform: translateY(-3px); border-color: #1547A1; 
        }
        .source-title { 
            color: #1547A1; font-size: 18px; font-weight: 800; text-decoration: none; 
        }
        .source-desc { 
            color: #555; font-size: 14px; margin-top: 10px; line-height: 1.5; 
        }
        .link-icon { font-size: 12px; margin-left: 5px; opacity: 0.6; }
        </style>
    """, unsafe_allow_html=True)

    # --- 5. 自動生成卡片 (2x2 + 1 佈局) ---
    cols = st.columns(2)
    for i, src in enumerate(SOURCES):
        target_col = cols[i % 2]
        with target_col:
            st.markdown(f"""
                <div class="source-card">
                    <a href="{src['url']}" target="_blank" class="source-title">
                        {src['name']} <span class="link-icon">↗</span>
                    </a>
                    <div class="source-desc">{src['desc']}</div>
                    <div style="margin-top: 15px;">
                        <code style="font-size: 10px; color: #999; background: #f8f9fa; padding: 3px 6px; border-radius: 4px;">
                            {src['url']}
                        </code>
                    </div>
                </div>
            """, unsafe_allow_html=True)

    # --- 6. 頁面底部註腳 ---
    st.markdown("""
        <div style="text-align: center; color: #bbb; font-size: 11px; margin-top: 40px;">
            最後同步時間：2026-03-25 | 系統版本 v6.2.4
        </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    show_source_page()