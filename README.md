# Smart-Bid-Evaluator_TWstock

**版本：1.0.2**
**最後更新**: 2026年3月

台灣興櫃公司投標評估系統 – 利用多源數據與機器學習進行投標決策支持。

---

## 📋 項目概述

本專案透過多個網路爬蟲收集台灣興櫃公司的公開發行、財務、市場等多維度資料，將其儲存於本地 SQLite 資料庫，並建立了一套從資料抓取、特徵工程到模型訓練的完整管線。

### 核心模組

- **`src/crawlers/`**: 包含六種爬蟲，各自負責不同的資料來源。
- **`src/db_base/`**: 使用 DAO (Data Access Object) 模式，封裝所有資料庫操作。
- **`src/processors/feature_engineer.py`**: 負責將所有爬取的資料表進行合併與特徵生成。
- **`src/models/`**: 包含機器學習模型的訓練腳本 (例如 Stacking)。
- **`src/utils/`**: 提供設定檔讀取、API 金鑰管理、資料格式化等通用輔助工具。

---

## 📁 專案結構

```text
.
├── data/
│   └── database.db           # 由程式自動建立的 SQLite 資料庫
├── src/
│   ├── crawlers/             # 爬蟲實作
│   ├── db_base/            # 資料庫存取層 (DAO, Schemas)
│   ├── processors/         # 特徵工程
│   ├── models/             # 機器學習模型
│   └── utils/              # 通用工具函式
│
├── .env.example            # 環境變數範本
├── ARCHITECTURE.md         # 系統架構圖
├── config.yaml             # 全域設定檔
├── main.py                 # 程式主入口 (驅動爬蟲與特徵工程)
└── requirements.txt        # Python 依賴套件
```

---

## 🚀 快速起步

1.  **環境準備**

    ```bash
    # 1. 複製專案
    git clone https://github.com/aeoner000/Smart-Bid-Evaluator_TWstock.git
    cd Smart-Bid-Evaluator_TWstock

    # 2. 安裝依賴
    pip install -r requirements.txt

    # 3. 設定環境變數
    cp .env.example .env
    ```

2.  **設定 API Token**

    編輯專案根目錄下的 `.env` 檔案，填入您從 FinMind 申請的 API Token。支援多組 Token，程式會自動輪換使用。

    ```text
    # .env
    FINMIND_TOKEN="your_token_1"
    FINMIND_TOKEN_2="your_token_2"
    ```

3.  **執行資料抓取與處理**

    使用 `main.py` 作為程式主入口。

    ```bash
    # 執行所有爬蟲，按預設順序抓取全部資料 (建議首次執行使用)
    python main.py --crawler all

    # 也可以指定執行單一爬蟲
    python main.py --crawler financial

    # 當所有資料表都抓取完畢後，執行特徵工程
    python main.py --feature
    ```

---

## 🔄 資料流程

`AuctionCrawler` 抓取的 `bid_info` 是所有流程的基礎。其他爬蟲會根據此表的內容進行增量更新。

```
[AuctionCrawler] -> [Financial, Market, Price, Revenue, Target Crawlers] -> [Feature Engineer] -> [Model Training]
```

### 主要資料表

所有表格皆以 `("證券代號", "投標開始日")` 作為複合主鍵，儲存於 `data/database.db`。

| 爬蟲 (`--crawler` 參數) | 產生的 SQLite 表 | 主要內容 |
| :--- | :--- | :--- |
| `auction` | `bid_info` | 股票申購的基本資訊 (起始點) |
| `financial` | `fin_stmts` | 公司季度財務報表與比率 |
| `market` | `all_market_info` | 國內外市場關鍵指標 |
| `price` | `history_price_info` | 申購期間的歷史股價特徵 |
| `revenue` | `revenue_info` | 公司月營收與其成長特徵 |
| `target` | `target_variable` | 上市後的股價表現 (目標變數) |

---

## 🛠️ 配置詳解

-   **`config.yaml`**: 定義了資料庫路徑、各爬蟲的 URL、欄位名稱、以及資料型別等核心設定。
-   **`.env`**: 用於存放敏感資訊，如 FinMind API Token，此檔案不應被提交到版本控制中。

---

## 📦 主要依賴

-   `pandas`
-   `requests`
-   `python-dotenv`
-   `FinMind`
-   `PyYAML`
-   `lxml`
-   `loguru`

*完整的依賴列表請參見 `requirements.txt`。*

---

## 📝 補充說明

-   **增量更新**: 系統內建增量更新機制，再次執行爬蟲時，只會抓取 `bid_info` 表中新增的紀錄，避免重複抓取。
-   **日誌系統**: 使用 `Loguru` 函式庫記錄詳細的執行日誌，方便追蹤與除錯。
-   **API 管理**: `src/utils/finmind_manager.py` 負責管理 FinMind API Token，可自動切換並處理額度耗盡的情況。
