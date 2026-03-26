import streamlit as st
import pandas as pd
import io
import joblib
import os
from google.cloud import bigquery, storage
from google.auth import default as google_auth_default
from google.oauth2 import service_account

# ==========================================
# 0️⃣ 環境變數 (由 Cloud Build / Cloud Run 注入)
# ==========================================
PROJECT_ID = os.environ.get("PROJECT_ID", "gen-lang-client-0590877921")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "bid-predict-gcs-bucket")

# ==========================================
# 1. 核心認證 (只改拿到權限的方式，其餘不變)
# ==========================================
@st.cache_resource(ttl=86400) 
def get_gcp_credentials():
    """
    支援 ADC 與 Service Account。
    在 Cloud Run 環境下自動透過 google_auth_default 取得權限。
    """
    try:
        # 本地開發：優先檢查 st.secrets
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            if "private_key" in info:
                return service_account.Credentials.from_service_account_info(info)
        
        # 雲端環境：使用 ADC (Application Default Credentials)
        creds, _ = google_auth_default()
        return creds
    except Exception:
        return None

# ==========================================
# 2. BigQuery 數據提取 (100% 原始邏輯，不管 Location)
# ==========================================
@st.cache_data(ttl=86400) 
def get_bq_table(query: str):
    """
    提取 BigQuery 資料。
    保持原始邏輯：直接執行傳入的 query 字串，不進行任何 SQL 處理，也不管 Location。
    """
    creds = get_gcp_credentials()
    
    try:
        # 完全回到原本的 Client 初始化方式，僅傳入 credentials 與 project
        client = bigquery.Client(credentials=creds, project=PROJECT_ID)
        
        # 直接執行原始傳入的 SQL
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"❌ BigQuery 提取失敗: {e}")
        return pd.DataFrame()

# ==========================================
# 3. GCS 操作 (函數名稱與功能嚴格保留)
# ==========================================
@st.cache_resource(ttl=86400) 
def get_gcs_client():
    creds = get_gcp_credentials()
    return storage.Client(credentials=creds, project=PROJECT_ID)

@st.cache_data(ttl=86400) 
def list_gcs_files(prefix: str = ""):
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]

@st.cache_data(ttl=86400) 
def download_gcs_file(source_blob_name: str):
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_bytes()

@st.cache_resource(ttl=86400)
def load_joblib_from_gcs(path: str):
    data = download_gcs_file(path)
    if data:
        buffer = io.BytesIO(data)
        return joblib.load(buffer)
    return None

# ==========================================
# 4. UI 元件 (保持原樣)
# ==========================================
def add_system_info(title="說明", content="數據每日更新，是一個全自動化且能預測競拍的平台"):
    st.markdown(f"""
    <style>
        .hover-info-container {{
            position: fixed; top: 80px; right: 20px; z-index: 1000;
            background: rgba(255, 255, 255, 0.9); backdrop-filter: blur(8px);
            border: 1px solid rgba(21, 71, 161, 0.15);
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
            width: 45px; height: 45px; border-radius: 50%;
            transition: all 0.4s ease-in-out; overflow: hidden;
            cursor: pointer; display: flex; align-items: center; justify-content: center;
        }}
        .hover-info-container:hover {{
            width: 240px; height: auto; border-radius: 12px;
            justify-content: flex-start; padding: 15px;
        }}
        .hover-info-content {{ opacity: 0; transition: opacity 0.3s; width: 0; }}
        .hover-info-container:hover .hover-info-content {{ opacity: 1; width: 100%; margin-left: 10px; }}
    </style>
    <div class="hover-info-container">
        <div class="hover-info-icon">💡</div>
        <div class="hover-info-content">
            <div style="color: #1547A1; font-size: 14px; font-weight: 800; margin-bottom: 5px;">{title}</div>
            <div style="color: #444; font-size: 12px; line-height: 1.6;">{content}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)