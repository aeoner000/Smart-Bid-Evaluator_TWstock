import streamlit as st
import pandas as pd
import io
import joblib
import os
from google.cloud import bigquery, storage
from google.oauth2 import service_account

# ==========================================
# 0. 輔助函式：安全取得配置 (完全繞過 Streamlit 報錯)
# ==========================================
def get_config(env_name, secret_path, default=None):
    """
    1. 優先從環境變數抓 (Cloud Run 模式，最快也最穩)
    2. 如果檔案真的存在，才試圖讀 st.secrets (本地模式)
    """
    # 優先嘗試環境變數 (Cloud Run 設定)
    val = os.environ.get(env_name)
    if val:
        return val
    
    # 只有實體檔案存在，我們才允許呼叫 st.secrets
    # 這是為了避免 Streamlit 發現沒檔案時直接拋出 StreamlitSecretNotFoundError
    if os.path.exists(".streamlit/secrets.toml"):
        try:
            # 這裡不直接寫 st.secrets[path]，避免任何潛在的初始化錯誤
            keys = secret_path.split(".")
            target = st.secrets
            for k in keys:
                if k in target:
                    target = target[k]
                else:
                    return default
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
    雲端部署實務：在 Cloud Run 不需要金鑰檔案，回傳 None 讓 Client 自動找身分。
    """
    if os.path.exists(".streamlit/secrets.toml"):
        try:
            # 只有在本地開發環境才讀取 Service Account info
            # 確保不會在雲端環境觸發 st.secrets 檢查
            if "gcp_service_account" in st.secrets:
                info = st.secrets["gcp_service_account"]
                if info.get("private_key"):
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
    # 雲端環境優先讀取環境變數 PROJECT_ID
    project = get_config("PROJECT_ID", "gcp_service_account.project_id", "gen-lang-client-0590877921")
    
    try:
        # 當 creds 為 None 時，Google Cloud SDK 會自動使用 Cloud Run 的身分 (ADC)
        client = bigquery.Client(credentials=creds, project=project)
        return client.query(query).to_dataframe()
    except Exception as e:
        # 這裡的錯誤會顯示在網頁上，方便排錯
        st.error(f"❌ BigQuery 提取失敗: {e}")
        return pd.DataFrame()

# ==========================================
# 3. GCS 操作
# ==========================================
@st.cache_resource(ttl=86400) 
def get_gcs_client():
    creds = get_gcp_credentials()
    project = get_config("PROJECT_ID", "gcp_service_account.project_id", "gen-lang-client-0590877921")
    return storage.Client(credentials=creds, project=project)

def get_bucket():
    # 雲端環境優先讀取環境變數 GCS_BUCKET
    bucket_name = get_config("GCS_BUCKET", "gcp.gcs_bucket")
    if not bucket_name:
        st.error("❌ 找不到 GCS_BUCKET 設定，請檢查環境變數或 secrets.toml")
        return None
    return get_gcs_client().bucket(bucket_name)

@st.cache_data(ttl=86400) 
def download_gcs_file(source_blob_name: str):
    bucket = get_bucket()
    if bucket:
        blob = bucket.blob(source_blob_name)
        return blob.download_as_bytes()
    return None

@st.cache_resource(ttl=86400)
def load_joblib_from_gcs(path: str):
    data = download_gcs_file(path)
    if data:
        buffer = io.BytesIO(data)
        return joblib.load(buffer)
    return None

# ==========================================
# 4. 本地端測試區塊
# ==========================================
if __name__ == "__main__":
    print("="*50)
    print("🧪 啟動脫離框架連線測試 (Standalone Test)...")
    # 此時 os.path.exists 會偵測到你本地的檔案，測試會通過
    TEST_QUERY = "SELECT current_timestamp() as time"
    df = get_bq_table(TEST_QUERY)
    if not df.empty:
        print(f"✅ 連線成功！專案: {get_config('PROJECT_ID', 'gcp_service_account.project_id')}")
        print(f"雲端時間: {df['time'][0]}")
    print("="*50)