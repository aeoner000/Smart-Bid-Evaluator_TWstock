# Smart-Bid-Evaluator_TWstock

**版本：1.0.1**
**最後更新**: 2026年3月

台灣興櫃公司投標評估系統 – 利用多源數據與機器學習進行投標決策支持。

---

## 📋 項目概述

本專案由六類爬蟲收集台灣興櫃公司公開資料，存入本地 SQLite 資料庫，並提供統一的存取、處理與訓練管線。整體流程為：

爬取 → 清洗 → 儲存 → 特徵工程 → 模型訓練/預測。

核心模組：

- `src/crawlers/`：Auction、Financial、Market、Price、Revenue、Target 爬蟲。
- `src/db_base/`：`IPO_DAO` 資料庫存取層與表結構。
- `src/processors/data_clean.py`：清洗與特徵生成。
- `src/models/train_model/`：訓練與堆疊邏輯。
- `src/utils/`：配置、FinMind 管理、解析與其它輔助工具。

此版本不包含訓練或預測腳本內容，`train.py` 和 `predict.py` 為空白入口，待後續擴展。

---

## 📁 資料夾結構

```text
smart-bid-evaluator/
├── src/
│   ├── crawlers/          # 六種爬蟲實作
│   ├── db_base/          # DAO 層 (`IPO_DAO`、schemas)
│   ├── processors/        # 資料清洗、特徵工程
│   ├── models/            # ML 模型、預處理、堆疊
│   └── utils/             # 配置、API 管理、工具函數
│
├── data/                  # 由程式自動建立
│   ├── database/          # SQLite 檔案
│   └── example/           # 範例資料
│
├── notebooks/             # Jupyter 筆記本
├── config.yaml            # 全域設定
├── .env.example           # 環境變數樣板
├── requirements.txt       # Python 依賴
├── main.py                # 爬蟲驅動入口
├── train.py               # 訓練入口 (目前為空)
└── predict.py             # 預測入口 (目前為空)
```

---

## 🚀 快速起步

1. **環境準備**

    ```bash
    git clone https://github.com/aeoner000/Smart-Bid-Evaluator_TWstock.git
    cd Smart-Bid-Evaluator_TWstock
    pip install -r requirements.txt
    cp .env.example .env
    # 編輯 .env 填入 FinMind API Token
    ```

2. **設定 API Token**

    ```text
    FINMIND=your_token_here
    FINMIND2=another_token   # 可選，多 Token 支援自動切換
    ```

3. **執行爬蟲**

    ```bash
    # 預設按順序跑全部爬蟲
    python main.py

    # 或指定單一任務
    python main.py --task financial
    ```

---

## 🔄 資料流程速覽

AuctionCrawler 為基礎資料，其他爬蟲需其輸出。

```
Auction → {Financial,Market,Price,Revenue,Target} → data_clean → models
```

### Tables

| 爬蟲 | SQLite table | 主鍵 | 更新頻率 |
|------|--------------|------|----------|
| AuctionCrawler   | bid_info           | (證券代號, 投標開始日) | 月 |
| FinancialCrawler | fin_stmts          | same | 月 |
| MarketCrawler    | all_market_info    | same | 月 |
| PriceCrawler     | history_price_info | same | 月 |
| RevenueCrawler   | revenue_info       | same | 月 |
| TargetCrawler    | target_variable    | same | 月 |

---

## 🛠 配置要點

`config.yaml` 包含資料庫路徑、各爬蟲選項與欄位型別轉換。FinMind Token 從 `.env` 讀入。

重要項目：

```yaml
database:
  db_path: "data/database/database.sqlite3"
  connect:
    timeout: 30
    check_same_thread: false
```

---

## 📦 主要依賴

- pandas>=3.0.1
- requests>=2.32.5
- python-dotenv>=1.0.0
- FinMind>=2.0.5
- PyYAML>=6.0
- lxml>=6.0.2

（完整列表見 `requirements.txt`）

---

## 📝 補充說明

- 日誌使用標準 `logging`。
- `IPO_DAO.diff_index()` 支援增量更新檢查。
- `train.py` 及 `predict.py` 預留給未來模型實作。
- `.env` 不應提交至版本控制。

---

## 📄 貢獻與授權

歡迎 Issue、PR。請參照主要倉庫授權條款。
