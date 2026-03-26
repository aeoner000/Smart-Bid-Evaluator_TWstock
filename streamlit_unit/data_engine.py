import streamlit as st
import pandas as pd
import io
import joblib
import os
import json
from google.cloud import bigquery, storage
from google.oauth2 import service_account

# ==========================================
# 0️⃣ 環境變數設定說明
# ==========================================
# Cloud Run / GCE 建議設定：
PROJECT_ID="gen-lang-client-0590877921"
GCS_BUCKET="bid-predict-gcs-bucket"
#
# 本地開發可選：
#   GCP_CREDENTIALS={Service Account JSON字串}
#   或 GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json

# ==========================================
# 1️⃣ 環境變數取得
# ==========================================
def get_env(name: str, default=None, required=False):
    """
    統一取得環境變數
    - required=True -> 沒設會直接報錯
    """
    val = os.environ.get(name, default)
    if required and not val:
        raise ValueError(f"❌ 缺少必要環境變數: {name}")
    return val

# ==========================================
# 2️⃣ GCP 認證 (ADC 優先，JSON fallback)
# ==========================================
@st.cache_resource(ttl=86400)
def get_gcp_credentials():
    """
    認證策略（優先順序）：
    1️⃣ ADC（Cloud Run / GCE IAM）
    2️⃣ 環境變數 JSON (GCP_CREDENTIALS)
    3️⃣ JSON 檔案 (GOOGLE_APPLICATION_CREDENTIALS)
    """
    # 1️⃣ 嘗試 ADC
    try:
        from google.auth import default
        creds, project = default()
        if creds:
            print("✅ 使用 ADC 認證 (Cloud Run / GCE IAM)")
            return creds
    except Exception:
        pass

    # 2️⃣ 環境變數 JSON
    json_str = os.environ.get("GCP_CREDENTIALS")
    if json_str:
        try:
            info = json.loads(json_str)
            print("✅ 使用 GCP_CREDENTIALS JSON 字串")
            return service_account.Credentials.from_service_account_info(info)
        except Exception as e:
            raise ValueError(f"❌ GCP_CREDENTIALS 解析失敗: {e}")

    # 3️⃣ JSON 檔案
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path and os.path.exists(key_path):
        try:
            print("✅ 使用 GOOGLE_APPLICATION_CREDENTIALS 檔案")
            return service_account.Credentials.from_service_account_file(key_path)
        except Exception as e:
            raise ValueError(f"❌ 憑證檔案讀取失敗: {e}")

    print("⚠️ 未找到 ADC 或 JSON，回傳 None（Client 會自動嘗試 ADC）")
    return None

# ==========================================
# 3️⃣ BigQuery Client
# ==========================================
@st.cache_resource(ttl=86400)
def get_bq_client():
    creds = get_gcp_credentials()
    project = get_env("PROJECT_ID", default=PROJECT_ID, required=True)
    return bigquery.Client(credentials=creds, project=project)

# ==========================================
# 4️⃣ BigQuery 查詢
# ==========================================
@st.cache_data(ttl=86400)
def get_bq_table(query: str) -> pd.DataFrame:
    """
    執行 SQL 並回傳 DataFrame
    """
    try:
        client = get_bq_client()
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"❌ BigQuery 查詢失敗: {e}")
        return pd.DataFrame()

# ==========================================
# 5️⃣ GCS Client
# ==========================================
@st.cache_resource(ttl=86400)
def get_gcs_client():
    creds = get_gcp_credentials()
    project = get_env("PROJECT_ID", default=PROJECT_ID, required=True)
    return storage.Client(credentials=creds, project=project)

# ==========================================
# 6️⃣ 取得 Bucket
# ==========================================
def get_bucket():
    bucket_name = get_env("GCS_BUCKET", default=GCS_BUCKET, required=True)
    return get_gcs_client().bucket(bucket_name)

# ==========================================
# 7️⃣ 從 GCS 下載檔案
# ==========================================
@st.cache_data(ttl=86400)
def download_gcs_file(blob_path: str) -> bytes:
    """
    從 GCS 下載檔案（bytes）
    """
    try:
        bucket = get_bucket()
        blob = bucket.blob(blob_path)
        return blob.download_as_bytes()
    except Exception as e:
        st.error(f"❌ GCS 下載失敗: {e}")
        return None

# ==========================================
# 8️⃣ 載入 joblib 模型（GCS）
# ==========================================
@st.cache_resource(ttl=86400)
def load_joblib_from_gcs(path: str):
    """
    從 GCS 載入 joblib 模型
    """
    data = download_gcs_file(path)
    if data:
        try:
            buffer = io.BytesIO(data)
            return joblib.load(buffer)
        except Exception as e:
            st.error(f"❌ 模型載入失敗: {e}")
    return None

# ==========================================
# 9️⃣ 測試用
# ==========================================
if __name__ == "__main__":
    print("=" * 50)
    print("🧪 測試 GCP 連線...")

    # BigQuery 測試
    df = get_bq_table("SELECT CURRENT_TIMESTAMP() AS time")
    if not df.empty:
        print("✅ BigQuery 連線成功")
        print(df.head())
    else:
        print("❌ BigQuery 無資料或連線失敗")

    # GCS 測試
    try:
        bucket = get_bucket()
        print(f"✅ GCS Bucket: {bucket.name}")
    except Exception as e:
        print(f"❌ GCS 測試失敗: {e}")

    print("=" * 50)