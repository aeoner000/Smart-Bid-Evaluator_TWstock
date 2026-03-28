# 系統架構設計

**版本:** 1.0  
**最後更新:** 2026-03-28

---

## 1. 架構總體設計

本系統採用模組化架構，各功能單元獨立執行並通過數據庫表作為縱向接口進行通訊。設計目標為提高代碼可維護性、降低模組間耦合度，並確保訓練與預測過程的一致性。

### 1.1 核心原則

| 原則 | 實踐方式 |
|------|--------|
| **模組獨立性** | 各模組可獨立開發、測試、重新分發 |
| **介面清晰** | 模組間唯一通訊方式為數據庫表，無直接依賴 |
| **數據透明** | 檢查點表記錄各階段轉換結果，支持審計與排查 |
| **可重現性** | 轉換器物件序列化存儲，消除訓練-預測偏差 |

### 1.2 關鍵概念

- **模組**: 獨立執行的功能單元（爬蟲、處理、訓練、預測）
- **表**: 模組間的通訊介面（原始表、檢查點表、輸入表）
- **轉換器**: 不可逆邏輯的有狀態物件（偏態轉換、特徵選擇），序列化為 `.joblib` 文件以確保一致性
- **工件**: 已訓練的模型和轉換器物件，在預測階段再利用

---

## 2. 系統架構圖

```
┌─────────────────────────────────────────────────────────────────────┐
│                            數據源層                                   │
│  ┌──────────────────┐              ┌──────────────────┐              │
│  │  FinMind API     │              │  網頁爬蟲        │              │
│  │  (股票數據)      │              │  (競拍信息)      │              │
│  └──────────────────┘              └──────────────────┘              │
└────────────┬─────────────────────────────────────┬───────────────────┘
             │                                     │
┌────────────▼──────────────────────────────────────▼──────────────────┐
│                      爬蟲層 (Crawl Layer)                             │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌────────────────┐ │
│  │ Auction │ │ Price   │ │ Market  │ │Financial│ │ Revenue/Target │ │
│  │ Crawler │ │ Crawler │ │ Crawler │ │Crawler  │ │  Crawlers      │ │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └────────────────┘ │
│  異常回滾機制：任意爬蟲失敗則清除該批次部分數據                        │
└────────────┬──────────────────────────────────────────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────────────┐
│              數據庫層 - 原始表 (raw tables)                        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│  │bid_info  │ │ history_ │ │ market_  │ │fin_stmts │ │ revenue_ │ │
│  │          │ │  price   │ │  info    │ │          │ │  info    │ │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
└────────────┬──────────────────────────────────────────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────────────┐
│            處理層 (Process Layer)                                   │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 1. 讀取原始表 → 2. Pandas 合併 → 3. 數據清理流水線            │  │
│  │    (set_type → fill_nan → add_is_miss → add_new_feature)    │  │
│  │                                                              │  │
│  │ 4. Per-target 偏態轉換 → 5. Per-target 特徵選擇             │  │
│  │                                                              │  │
│  │ 6. 轉換器序列化存儲 (.joblib)                                │  │
│  └──────────────────────────────────────────────────────────────┘  │
└────────────┬───────────────────────────────────────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────────────┐
│         數據庫層 - 檢查點與待訓練表                                 │
│  ┌──────────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ all_features │  │ Train_*  │  │  Test_*  │  │Predict_* │       │
│  │   (寬表)      │  │ (訓練集)  │  │(測試集)  │  │(預測集)  │       │
│  └──────────────┘  └──────────┘  └──────────┘  └──────────┘       │
│  [存儲轉換器工件]                                                     │
│  - all_y_skew_transformer.joblib                                   │
│  - all_selected_features.joblib                                    │
└────────────┬───────────────────────────────────────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────────────┐
│            訓練判定層 (Decision Layer)                              │
│            檢查新數據筆數是否 ≥ 門檻值 (默認 20)                     │
│  是 → 執行訓練 │ 否 → 保留現有模型                                  │
└────────────┬─────────────────────────┬───────────────────────────────┘
             │                         │
     ┌───────▼────────────┐    ┌──────▼────────────┐
     │ 訓練層 (Train)     │    │ 預測層 (Predict) │
     │ - 模型訓練         │    │ - 載入工件        │
     │ - 驗證評估         │    │ - 應用轉換        │
     │ - 序列化儲存       │    │ - 推理計算        │
     └──────┬─────────────┘    └──────┬───────────┘
            │                         │
     ┌──────▼─────────────────────────▼────────────┐
     │  數據庫層 - 預測結果                         │
     │  [已訓練模型工件]                           │
     │  - {y_english}_best_model.joblib            │
     │  - training_metadata.json                   │
     │  - [預測結果表]                             │
     └─────────────────────────────────────────────┘
```

---

## 3. 模組職責分解

### 3.1 爬蟲模組 (src/crawlers/)

**職責**: 采集外部數據源，寫入原始表

| 爬蟲類 | 數據源 | 輸出表 | 關鍵方法 |
|--------|--------|--------|---------|
| `AuctionCrawler` | 網頁爬蟲 | auction_tasks | 初始化任務清單 |
| `PriceCrawler` | FinMind API | history_price | 股價歷史 |
| `MarketCrawler` | 網頁爬蟲 | market_info | 市場行情 |
| `FinancialCrawler` | 網頁爬蟲 | fin_stmts | 財務報表 |
| `RevenueCrawler` | 網頁爬蟲 | revenue_info | 營收數據 |
| `TargetCrawler` | 網頁爬蟲 | target_variable | 競拍結果（目標變量） |

**執行方式**:
- `AuctionCrawler` 初始化任務清單
- 四個特徵爬蟲並行執行，各自將數據寫入對應表
- `TargetCrawler` 最後收集目標變量
- 若任何爬蟲失敗，觸發數據回滾機制

### 3.2 處理模組 (src/processors/)

**職責**: 轉換原始表為模型可用的特徵矩陣

**執行流水線**:

```
1. _combine_features_in_pandas()
   └─ 讀取原始表 (bid_info 為主表)
      └─ 依序用 left join 合併其他特徵表
         └─ 合併鍵: [證券代號, 投標開始日]
         └─ 產出合並表

2. set_type()
   └─ 轉換數據型態 (str → numeric, datetime 等)

3. fill_nan()
   └─ 缺失值補填（中位數、眾數、前向填充）
   └─ 配置中 CLEAN_TABLE 定義每個字段的策略

4. add_is_miss()
   └─ 標記缺失狀態（缺失本身可能為預測信號）
   └─ 產生 is_missing_* 特徵列

5. add_new_feature()
   └─ 衍生特徵計算 (成長率、比例、交互項)

6. SkewTransformer (per-target)
   └─ 檢測並修正特徵分佈偏態
   └─ 金融數據常見偏態，目的為提高模型穩定性
   └─ 產出 all_y_skew_transformer.joblib

7. FeatureSelector (per-target)
   └─ 特徵篩選演算法 (如 mutual_info_regression, recursive elimination)
   └─ 每個目標變量獨立選擇，因為不同預測目標所需特徵組合不同
   └─ 產出 all_selected_features.joblib

8. 分割數據集
   └─ Train_* (80%): 訓練集
   └─ Test_* (20%): 測試/驗證集
   └─ Predict_*: 新數據預測集（僅特徵，無目標值）
```

| 類 | 職責 |
|---|---|
| `FeatureEngineer` | 主編排，呼叫各處理函數，管理數據輸入/輸出 |
| `SkewTransformer` | 偏態檢測與轉換（Power, Box-Cox, Yeo-Johnson） |
| `FeatureSelector` | 特徵選擇算法 (filter, wrapper, embedded 等) |

### 3.3 訓練模組 (src/models/train_model/)

**職責**: 訓練機器學習模型，評估性能，儲存工件

**執行流程**:

```
1. 決策層檢查 (should_trigger_training)
   └─ 讀取 training_metadata.json 取得上次訓練的數據量
   └─ 查詢 bid_info 表計算當前完整數據量
   └─ 新增數據筆數 = 當前 - 上次
   └─ 若 新增筆數 ≥ 門檻值 (default 20) → 觸發訓練
      否 → 保留現有最佳模型，跳過本次訓練

2. 模型訓練管道 (run_training_pipeline)
   └─ 對每個目標變量 (per-target):
      ├─ 讀取 Train_* 和 Test_*
      ├─ 初始化 BoostAutoMLManager
      │  ├─ 候選模型: LightGBM, XGBoost, CatBoost
      │  ├─ 超參數搜尋空間定義在 config.yaml
      │  ├─ 使用 Optuna 進行貝葉斯優化
      │  └─ 早停機制 (早停回合數: 50)
      ├─ 訓練多個模型，比較測試集 RMSE
      ├─ 選擇性能最佳的模型為 champion
      ├─ 儲存模型為 {y_english}_best_model.joblib
      └─ 更新 training_metadata.json
         ├─ last_training_count = 當前完整數據量
         ├─ champion_scores[y_english] = {model_type, rmse, ...}
         └─ 時間戳、特徵數量等元數據
```

| 類 | 職責 |
|---|---|
| `BoostAutoMLManager` | 模型訓練與超參數優化 |
| 訓練管道函數 | 決策、迭代、元數據管理 |

**模型配置** (config.yaml):
- LightGBM: 控制樹葉數與正則化強度，適合大規模特徵
- XGBoost: 限制樹深與抽樣比，穩定性佳
- CatBoost: 強力 L2 正則化，天生抗過擬合

### 3.4 預測模組 (src/models/train_model/predict.py)

**職責**: 應用訓練工件進行推理，生成預測結果

**執行流程**:

```
1. _load_artifacts()
   └─ 從儲存系統 (GCS 或本地) 載入:
      ├─ all_selected_features.joblib (特徵列表)
      ├─ all_y_skew_transformer.joblib (偏態轉換器)
      ├─ {y_english}_best_model.joblib (每個目標的模型)

2. run_prediction()
   └─ 對每個目標變量:
      ├─ 讀取 Predict_* 表
      ├─ 應用 SkewTransformer (特徵變換)
      ├─ 篩選 all_selected_features 相關列
      ├─ 模型推理 (model.predict)
      ├─ 產出預測值與置信度 (若支持)
      └─ 寫回數據庫預測結果表
```

---

## 4. 數據流詳細說明

### 4.1 端到端數據轉換

| 階段 | 輸入 | 輸出 | 存儲位置 |
|------|------|------|---------|
| **Crawl** | (無) | bid_info, history_price, market_info, fin_stmts, revenue_info, target_variable | 數據庫原始表 |
| **Process** | 原始表 + 配置 | all_features (寬表檢查點) Train_*/Test_*/Predict_* | 數據庫 + .joblib 工件 |
| **Decision** | 數據量查詢 | 訓練決策 (True/False) | — |
| **Train** | Train_*/Test_* + 工件 | 已訓練模型 + 元數據 | .joblib 文件 + training_metadata.json |
| **Predict** | Predict_* + 工件 | 預測結果 | 數據庫預測表 |

### 4.2 關鍵設計決策

| 決策項 | 實踐方式 | 原因 |
|--------|----------|------|
| **數據合併方式** | Pandas in-memory (應用層) | 降低數據庫負載，提高靈活性；中等規模數據下性能優於 SQL JOIN |
| **缺失值標記** | add_is_miss 生成特徵列 | 缺失狀態本身可能為預測信號 |
| **偏態轉換** | Per-target 獨立轉換器 | 金融數據常見偏態分佈，per-target 提高針對性 |
| **特徵選擇** | Per-target 獨立選擇 | 不同預測目標所需特徵組合存在差異 |
| **轉換器序列化** | .joblib 格式 | 確保訓練與預測過程一致，消除 Training-Serving Skew；joblib 為 scikit-learn 標準格式 |
| **訓練觸發閾值** | 新數據筆數 ≥ 20 | 積累足夠新樣本，衡量計算成本與模型新鮮度的平衡 |

---

## 5. 數據庫層

### 5.1 表結構配置

表定義在 `src/db_base/schemas.py`:

```
TABLE_SCHEMAS = {
    "bid_info": [
        ("證券代號", VARCHAR), ("投標開始日", DATE), 
        ("預期上市價格", FLOAT), ("實際上市價格", FLOAT),
        ...
    ],
    "history_price": [
        ("證券代號", VARCHAR), ("投標開始日", DATE),
        ("開盤價", FLOAT), ("收盤價", FLOAT), 
        ...
    ],
    ...
}
```

### 5.2 支持的數據庫後端

| 後端 | 配置 | 適用場景 |
|------|------|--------|
| **SQLite** | `database.type: sqlite` (config.yaml) | 本地開發、小規模部署 |
| **BigQuery** | `database.type: bigquery` (config.yaml) | 生產環境、大規模數據 |

**數據庫訪問接口**:
- `IPO_DAO_SQLite`: 本地 SQLite 操作
- `IPO_DAO_BigQuery`: Google BigQuery 操作
- 工廠函數 `get_db_manager()`: 根據配置自動選擇

### 5.3 儲存系統

| 儲存系統 | 配置 | 文件類型 |
|---------|------|--------|
| **GCS** | `storage.type: gcs` (config.yaml) | .joblib 模型、元數據 JSON |
| **本地** | `storage.type: local` (config.yaml) | 同上，存儲於 src/models/saved_weights/ |

---

## 6. 異常處理機制

### 6.1 爬蟲層異常處理

若特徵爬蟲中任意一個失敗，系統觸發回滾機制:

```
爬蟲異常 → 捕獲異常
         → 讀取已保存的檢查點
         → 刪除該批次部分下載的數據
         → 記錄失敗原因
         → 系統中止或重試
```

**設計意圖**: 避免汙染數據庫，保證交易數據的一致性。

### 6.2 訓練層異常處理

若訓練過程失敗（無法達到收斂等），系統保留現有最佳模型，避免回歸。

---

## 7. 配置管理

所有配置集中在 `config.yaml`，支持動態切換:

```yaml
storage:
  type: "gcs"  # 或 "local"
  gcs:
    bucket_name: "..."

database:
  type: "bigquery"  # 或 "sqlite"
  
paths:
  weights_dir: "src/models/saved_weights"
  metadata_path: "json/training_metadata.json"

ml_model_environment:
  models:
    lgbm:
      static_params: {...}
      search_space: {...}
```

配置通過 `src/utils/config_loader.py` 加載，所有模組共享。

---

## 8. 擴展點

| 擴展點 | 實現位置 | 說明 |
|--------|---------|------|
| **新爬蟲** | src/crawlers/ | 繼承 BaseCrawler，實現 process_task() |
| **新特徵** | src/processors/feature_engineer.py | modiy add_new_feature() |
| **新目標變量** | config.yaml + src/db_base/schemas.py | 新增表定義與配置 |
| **新模型算法** | src/models/train_model/boost_automl.py | 新增模型類與超參數空間 |

