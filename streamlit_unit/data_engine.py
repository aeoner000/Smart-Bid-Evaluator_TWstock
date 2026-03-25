import streamlit as st
import pandas as pd
import io
import joblib
import os
from google.cloud import bigquery, storage
from google.oauth2 import service_account

# ==========================================
# 0. 輔助函式：安全取得配置 (環境變數優先)
# ==========================================
def get_config(env_name, secret_path, default=None):
    """
    1. 優先從環境變數抓 (Cloud Run 模式)
    2. 如果檔案存在，才從 st.secrets 抓 (本地模式)
    """
    # 優先嘗試環境變數 (例如 PROJECT_ID)
    val = os.environ.get(env_name)
    if val:
        return val
    
    # 只有當 secrets.toml 存在時才去碰 st.secrets，避免框架閃退
    if os.path.exists(".streamlit/secrets.toml"):
        try:
            # 支援路徑如 "gcp_service_account.project_id"
            keys = secret_path.split(".")
            target = st.secrets
            for k in keys:
                target = target[k]
            return target
        except:
            pass
    return default

# ==========================================
# 1. 核心認證 (自動適應：ADC vs Service Account)
# ==========================================
@st.cache_resource(ttl=86400) 
def get_gcp_credentials():
    """
    本地：讀取 TOML 內的 private_key
    雲端：回傳 None 啟動 ADC (刷臉模式)
    """
    if os.path.exists(".streamlit/secrets.toml"):
        try:
            if "gcp_service_account" in st.secrets:
                info = st.secrets["gcp_service_account"]
                if "private_key" in info and info["private_key"]:
                    return service_account.Credentials.from_service_account_info(info)
        except:
            pass
    return None

# ==========================================
# 2. BigQuery 數據提取
# ==========================================
@st.cache_data(ttl=86400) 
def get_bq_table(query: str):
    creds = get_gcp_credentials()
    # 安全取得 Project ID
    project = get_config("PROJECT_ID", "gcp_service_account.project_id", "gen-lang-client-0590877921")
    
    try:
        client = bigquery.Client(credentials=creds, project=project)
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"❌ BigQuery 提取失敗: {e}")
        return pd.DataFrame()

# ==========================================
# 3. GCS 操作 (模型與檔案載入)
# ==========================================
@st.cache_resource(ttl=86400) 
def get_gcs_client():
    creds = get_gcp_credentials()
    project = get_config("PROJECT_ID", "gcp_service_account.project_id", "gen-lang-client-0590877921")
    return storage.Client(credentials=creds, project=project)

def get_bucket():
    bucket_name = get_config("GCS_BUCKET", "gcp.gcs_bucket", "your-default-bucket")
    return get_gcs_client().bucket(bucket_name)

@st.cache_data(ttl=86400) 
def download_gcs_file(source_blob_name: str):
    blob = get_bucket().blob(source_blob_name)
    return blob.download_as_bytes()

@st.cache_resource(ttl=86400)
def load_joblib_from_gcs(path: str):
    buffer = io.BytesIO(download_gcs_file(path))
    return joblib.load(buffer)

# ==========================================
# 4. 本地端測試測試區塊 (NAME == MAIN)
# ==========================================
if __name__ == "__main__":
    print("="*50)
    print("🧪 開始本地連線測試...")
    
    # 模擬環境變數 (如果你本地沒 TOML 也可以測試)
    # os.environ["PROJECT_ID"] = "gen-lang-client-0590877921"
    
    TEST_QUERY = "SELECT current_timestamp() as time"
    
    try:
        df = get_bq_table(TEST_QUERY)
        if not df.empty:
            print(f"✅ 連線成功！雲端時間: {df['time'][0]}")
        else:
            print("⚠️ 連線成功但無資料。")
    except Exception as e:
        print(f"❌ 測試失敗: {e}")
    print("="*50)