import streamlit as st
import pandas as pd
import io
import joblib
from google.cloud import bigquery, storage
from google.oauth2 import service_account

# ==========================================
# 1. 核心認證 (自動適應環境：本地 vs 雲端)
# ==========================================
@st.cache_resource(ttl=86400) 
def get_gcp_credentials():
    """
    憑證處理邏輯：
    - 本地開發：從 st.secrets["gcp_service_account"] 讀取完整 JSON 資訊。
    - Cloud Run：若 secrets 中無私鑰，回傳 None，觸發 ADC (Application Default Credentials)。
    """
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            # 檢查是否具備私鑰（代表是完整的 Service Account JSON）
            if "private_key" in info:
                return service_account.Credentials.from_service_account_info(info)
        return None
    except Exception:
        return None

# ==========================================
# 2. BigQuery 數據提取
# ==========================================
@st.cache_data(ttl=86400) 
def get_bq_table(query: str):
    """提取 BigQuery 資料並轉換為 DataFrame"""
    creds = get_gcp_credentials()
    # 確保 Project ID 存在於 secrets 中
    project = st.secrets.get("gcp_service_account", {}).get("project_id")
    
    try:
        # 當 creds 為 None 時，Client 會自動尋找環境中的服務帳戶 (Cloud Run 模式)
        client = bigquery.Client(credentials=creds, project=project)
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"❌ BigQuery 提取失敗: {e}")
        return pd.DataFrame()

# ==========================================
# 3. GCS 操作 (Client / List / Download)
# ==========================================
@st.cache_resource(ttl=86400) 
def get_gcs_client():
    """建立 GCS 連線物件"""
    creds = get_gcp_credentials()
    project = st.secrets.get("gcp_service_account", {}).get("project_id")
    return storage.Client(credentials=creds, project=project)

@st.cache_data(ttl=86400) 
def list_gcs_files(prefix: str = ""):
    """列出 Bucket 內的檔案清單"""
    bucket_name = st.secrets["gcp"]["gcs_bucket"]
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]

@st.cache_data(ttl=86400) 
def download_gcs_file(source_blob_name: str):
    """從 GCS 下載原始位元組資料"""
    bucket_name = st.secrets["gcp"]["gcs_bucket"]
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_bytes()

@st.cache_resource(ttl=86400)
def load_joblib_from_gcs(path: str):
    """從 GCS 載入模型或標籤器 (joblib 物件)"""
    bucket_name = st.secrets["gcp"]["gcs_bucket"]
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    
    # 使用 BytesIO 在記憶體中處理，不產生暫存檔
    buffer = io.BytesIO(blob.download_as_bytes())
    obj = joblib.load(buffer)
    return obj

# ==========================================
# 4. UI 元件：說明方塊
# ==========================================
def add_system_info(title="說明", content="數據每日更新，是一個全自動化且能預測競拍的平台"):
    """右上角懸停說明燈泡"""
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