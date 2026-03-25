# AI Auction v6.2 — Streamlit App

## 專案結構

```
ai_auction/
├── app.py                    # 系統首頁（Plotly 效能圖）
├── utils.py                  # BigQuery & GCS 共用邏輯
├── requirements.txt
├── pages/
│   ├── 01_Analysis.py        # ⚡ 即時預測（標的列表 + 黃金外框預測卡）
│   └── 02_GCS_Files.py       # 📊 歷史探索 + GCS 檔案瀏覽
└── .streamlit/
    └── secrets.toml          # GCP 金鑰（不要 commit）
```

---

## 安裝與啟動

```bash
pip install -r requirements.txt
streamlit run app.py
```

---

## GCP 設定步驟

### 1. 建立服務帳號

在 GCP Console → IAM → 服務帳號，建立一個帳號並授予：
- `BigQuery Data Viewer` + `BigQuery Job User`（讀取 BQ）
- `Storage Object Viewer`（讀取 GCS）

若需要上傳 GCS，另加：`Storage Object Creator`

### 2. 下載 JSON 金鑰

下載服務帳號的 JSON 金鑰，**不要放進 git**。

### 3. 填入 secrets.toml

將 JSON 內容對應填入 `.streamlit/secrets.toml`：

```toml
[gcp_service_account]
type                        = "service_account"
project_id                  = "my-project-123"
private_key_id              = "abc123..."
private_key                 = "-----BEGIN RSA PRIVATE KEY-----\n..."
client_email                = "my-sa@my-project.iam.gserviceaccount.com"
client_id                   = "1234567890"
auth_uri                    = "https://accounts.google.com/o/oauth2/auth"
token_uri                   = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url        = "https://www.googleapis.com/robot/v1/metadata/x509/..."

[gcp]
project_id  = "my-project-123"
bq_dataset  = "auction_data"       # 你的 BQ dataset 名稱
gcs_bucket  = "my-auction-bucket"  # 你的 GCS bucket 名稱
```

### 4. .gitignore

```
.streamlit/secrets.toml
*.json
__pycache__/
.env
```

---

## BigQuery 資料表結構（參考）

### `auction_targets`（即時預測頁面）

| 欄位 | 型別 | 說明 |
|------|------|------|
| stock_id | STRING | 股票代碼 |
| name | STRING | 公司名稱 |
| price | FLOAT64 | 最新成交價 |
| days_left | INT64 | 距競拍天數 |
| status | STRING | CRAWLING / WAITING |
| weighted_price | FLOAT64 | AI 預測加權均價 |
| min_price | FLOAT64 | AI 預測最低得標價 |
| profit_rate | FLOAT64 | 預估獲利率 |
| auction_date | DATE | 競拍日期 |

### `model_backtest`（歷史探索 - 回測驗證）

| 欄位 | 型別 | 說明 |
|------|------|------|
| date | DATE | 日期 |
| predicted_weighted | FLOAT64 | 預測加權均價 |
| actual_weighted | FLOAT64 | 實際加權均價 |
| predicted_min | FLOAT64 | 預測最低價 |
| actual_min | FLOAT64 | 實際最低價 |
| predicted_profit | FLOAT64 | 預測獲利率 |
| actual_profit | FLOAT64 | 實際獲利率 |

### `auction_history`（歷史探索 - 宏觀統計）

| 欄位 | 型別 | 說明 |
|------|------|------|
| auction_date | DATE | 競拍日期 |
| eps | FLOAT64 | EPS |
| profit_rate | FLOAT64 | 實際獲利率 |
| pe_ratio | FLOAT64 | PE 比率 |
| market_cap_b | FLOAT64 | 市值（億） |

---

## 從 Demo 切換到真實 BQ 資料

`utils.py` 的 `run_query()` 已封裝好，在各 `pages/` 裡找到
`_load_*_data()` 函式，把 `TODO` 那段的 DataFrame 轉換補完即可。

---

## Streamlit Cloud 部署

在 Streamlit Cloud 的 App Settings → Secrets 貼上
`.streamlit/secrets.toml` 的完整內容即可，不需上傳金鑰檔。
