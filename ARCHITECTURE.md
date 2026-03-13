# 系統架構與數據流

**文檔版本**: 1.0.2
**最後更新**: 2026年3月

本文件旨在說明整個專案的結構、各模組職責以及核心數據流。

## 專案結構圖

```
.
├── data/
│   └── database.db           # SQLite 資料庫檔案
├── src/
│   ├── crawlers/               # --- 爬蟲層 ---
│   │   ├── base_crawler.py     # 爬蟲基底類別，定義通用 run() 與抽象 process_task()
│   │   ├── auctioncrawler.py   # 增量抓取 `bid_info` (主要資料源)
│   │   ├── financialcrawler.py # 抓取 `fin_stmts` (財務報表)
│   │   ├── marketcrawler.py    # 抓取 `all_market_info` (市場總覽)
│   │   ├── pricecrawler.py     # 抓取 `history_price_info` (歷史股價)
│   │   ├── revenuecrawler.py   # 抓取 `revenue_info` (營收)
│   │   └── targetcrawler.py    # 計算 `target_variable` (目標變數)
│   │
│   ├── db_base/                # --- 資料庫層 ---
│   │   ├── db_manager.py       # DAO 模式，封裝所有資料庫操作 (CRUD, diff)
│   │   └── schemas.py          # 定義資料庫所有表的 schema (欄位, 型別)
│   │
│   ├── processors/             # --- 資料處理層 ---
│   │   └── feature_engineer.py # 合併所有表，生成最終特徵
│   │
│   ├── models/                 # --- 模型層 ---
│   │   └── train_model/
│   │       ├── stacking.py     # Stacking 模型訓練
│   │       └── transformer.py  # Transformer 模型訓練 (可選)
│   │
│   └── utils/                  # --- 工具層 ---
│       ├── config_loader.py    # 讀取 config.yaml
│       ├── finmind_manager.py  # FinMind API Token 管理與輪換
│       ├── logger_config.py    # Loguru 日誌設定
│       ├── financial_format_utils.py # 財報數字、日期格式化
│       ├── market_utils.py     # 市場總覽資料抓取輔助函式
│       ├── price_utils.py      # 歷史股價資料抓取輔助函式
│       ├── revenue_utils.py    # 營收資料抓取輔助函式
│       └── target_utils.py     # 目標變數計算輔助函式
│
├── .env                        # 環境變數 (FinMind token)
├── ARCHITECTURE.md             # 本文件
├── config.yaml                 # 主要設定檔
├── main.py                     # 程式主入口 (解析參數、依序執行爬蟲)
├── README.md                   # 專案說明
└── requirements.txt            # Python 依賴套件
```

## 數據流 (Data Flow)

1.  **啟動**: `main.py` 解析命令行參數 (`--crawler` 或 `--feature`)。

2.  **爬蟲階段**:
    *   `AuctionCrawler` 首先執行，抓取最新的股票申購資訊 (`bid_info` table)，作為所有其他爬蟲的基礎。
    *   其餘爬蟲 (`Financial`, `Market`, `Price`, `Revenue`, `Target`) 執行時，會透過 `db_manager.py` 的 `diff_index` 方法，比對各自目標表與 `bid_info` 表的差異。
    *   它們只針對差異的股票代號與日期，透過各自的 `_utils.py` 模組抓取所需資料。
    *   所有資料最終都儲存在 `data/database.db` 的對應表中。

3.  **特徵工程階段**:
    *   當執行 `main.py --feature` 時，`feature_engineer.py` 會被觸發。
    *   它會從資料庫中讀取所有爬蟲產生的表。
    *   進行資料清理、合併 (join)，並計算衍生特徵，最終生成一個寬表 (wide table)，用於模型訓練。

4.  **模型訓練階段**:
    *   `models/train_model/` 中的腳本讀取特徵工程產生的寬表。
    *   執行模型訓練 (例如 Stacking)，並將訓練好的模型存檔。

## 核心模組詳述

-   **`base_crawler.py`**: 所有爬蟲的父類。它提供了一個統一的 `run()` 方法，該方法處理了資料庫連接、增量檢查 (`diff_index`) 和資料儲存的通用邏輯，讓子爬蟲只需要專注於實現 `process_task` 方法，即單一任務的抓取邏輯。

-   **`db_manager.py`**: Data Access Object (DAO) 設計模式的實現。它將所有 SQL 操作與爬蟲邏輯解耦。所有爬蟲都透過 `IPO_DAO` 物件來讀寫資料庫，而不是直接執行 SQL 語句。

-   **`finmind_manager.py`**: 解決 FinMind API 有每日使用次數限制的問題。它管理一個 Token 池，當某個 Token 的額度用完時，能自動切換到下一個可用的 Token，並在所有 Token 都用盡時，執行等待。

-   **`*_utils.py`**: 每個 `utils` 模組都服務於一個特定的爬蟲，將該爬蟲的資料抓取/計算的核心邏輯從爬蟲的流程控制中分離出來，使其更易於單獨測試和維護。
