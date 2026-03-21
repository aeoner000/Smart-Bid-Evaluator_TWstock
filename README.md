# 台股競拍價格預測系統

**專案願景:** 運用數據工程與機器學習技術，精準預測台股競拍價格，為投資者提供數據驅動的決策優勢。

---

## 主要工作流程

整個流程由 `main.py` 統一調度，其核心是**循序執行的工作流**。根據命令列參數，`main.py` 會依序執行爬蟲、資料處理、模型訓練與預測等階段，確保每個階段都在前一階段成功完成的基礎上進行。

graph TD
    A[啟動 main.py] --> B(執行爬蟲階段 crawl)
    
    subgraph "爬蟲階段 (Crawl Stage)"
        B1[AuctionCrawler: 更新任務清單]
        B2[FeatureCrawlers: 依任務補齊特徵]
        B3[TargetCrawler: 抓取競拍結果]
        B1 --> B2 --> B3
    end

    B --> C(執行資料處理階段 process)
    
    subgraph "資料處理階段 (Process Stage)"
        C1[FeatureEngineer: 資料清理與特徵工程]
    end

    C --> D{判斷是否滿足訓練條件}
    
    subgraph "模型訓練階段 (Train Stage)"
        D2[run_training_pipeline: 訓練並儲存新模型]
        D3[跳過訓練]
    end

    D -- 若滿足 --> D2
    D -- 若不滿足 --> D3

    D2 --> E(執行預測階段 predict)
    D3 --> E
    
    subgraph "預測階段 (Predict Stage)"
        E1[Predictor: 載入最新模型產生預測]
    end

    E --> E1
    E1 --> F[結束]

### 異常處理與日誌

- **階段級錯誤處理:** 在爬蟲階段，包含一個穩健的資料回滾機制。若任一特徵爬蟲失敗，系統將自動刪除該批次中已部分下載的資料，以確保資料的完整性。
- **日誌記錄:** 應用程式使用詳細的日誌記錄每個階段的關鍵步驟，使得對管線的監控和偵錯變得容易。

---

## 核心功能與架構優勢

- **自動化ETL:** 擁有從資料擷取(Crawl)、轉換(Process)到載入(Load to Model)的完整自動化流程。
- **多維度特徵整合:** 整合財務報表、歷史股價、市場數據和營收報告等多維度特徵。
- **智慧訓練觸發:** 僅在收集到足夠新資料時才觸發模型訓練，以優化計算資源。
- **模組化與可擴充性:** 專案採高內聚、低耦合的模組化結構。各模組（爬蟲、清理、訓練）之間透過資料庫進行解耦，互不直接依賴，為未來的擴展打下了堅實的基礎。

---

## 未來擴展方向

本專案的模組化架構為系統未來的發展提供了巨大的靈活性。主要的擴展方向包括：

1.  **水平擴展與雲端部署:** 
    - **容器化:** 將每個執行階段（`crawl`, `process`, `train`）打包成獨立的 Docker 容器。
    - **工作流編排:** 部署到 Kubernetes，並使用 Airflow 或 Argo Workflows 等工具進行排程，實現更複雜的依賴管理與自動化重試機制。

2.  **效能優化:**
    - **分散式計算:** 當資料量達到億級時，可將 Pandas 資料處理邏輯遷移至 Dask 或 Spark，以利用分散式計算的能力，突破單一節點的記憶體瓶頸。

3.  **模型與特徵實驗:**
    - **快速迭代:** 資料科學家可以專注於 `src/processors` 和 `src/models` 目錄，快速實驗新的特徵工程方法或不同的預測模型，而無需擔心影響數據的獲取（爬蟲）流程。

---

## 技術棧

- **Python 3.9+**
- **核心函式庫:**
    - **Pandas:** 用於資料操作與分析。
    - **google-cloud-bigquery:** 用於與 BigQuery 資料庫互動。
    - **scikit-learn, XGBoost, LightGBM:** 用於模型訓練與預測。
- **資料庫:** Google BigQuery

---

## 快速上手

### 1. 環境變數

在專案根目錄下建立 `.env` 檔案，並填入您的 BigQuery 憑證。

### 2. 安裝

```bash
pip install -r requirements.txt
```

### 3. 執行

可執行整個管線或單獨階段。

```bash
# 執行整個管線 (依序 crawl -> process -> train -> predict)
python main.py

# 僅執行爬蟲和資料處理階段
python main.py crawl process
```

---

## 檔案結構

```
.
├── ARCHITECTURE.md
├── README.md
├── config.yaml
├── main.py
├── requirements.txt
├── src
│   ├── __init__.py
│   ├── crawlers
│   │   ├── __init__.py
│   │   ├── auctioncrawler.py
│   │   ├── base_crawler.py
│   │   ├── financialcrawler.py
│   │   ├── marketcrawler.py
│   │   ├── pricecrawler.py
│   │   ├── revenuecrawler.py
│   │   └── targetcrawler.py
│   ├── db_base
│   │   ├── __init__.py
│   │   ├── bigquery_dao.py
│   │   ├── bigquery_schemas.py
│   │   ├── db_manager.py
│   │   ├── schemas.py
│   │   └── sqlite_dao.py
│   ├── models
│   │   ├── __init__.py
│   │   └── train_model
│   │       ├── __init__.py
│   │       ├── boost_automl.py
│   │       ├── predict.py
│   │       └── train.py
│   ├── processors
│   │   ├── __init__.py
│   │   ├── feature_engineer.py
│   │   ├── feature_selector.py
│   │   └── skew_transformer.py
│   └── utils
│       ├── __init__.py
│       ├── config_loader.py
│       ├── feature_utils.py
│       ├── financial_format_utils.py
│       ├── finmind_manager.py
│       ├── logger_config.py
│       ├── market_utils.py
│       ├── price_utils.py
│       ├── revenue_utils.py
│       ├── storage_handler.py
│       └── target_utils.py
├── data
│   └── example
│       ├── all_feature_table.csv
│       ├── all_market_info.csv
│       ├── bid_info.csv
│       ├── fin_stmts.csv
│       ├── history_price_info.csv
│       ├── revenue_info.csv
│       └── target_variable.csv
└── json
    └── training_metadata.json
```
