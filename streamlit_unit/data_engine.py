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
    自動判斷環境並回傳憑證：
    - 本地開發：讀取 st.secrets["gcp_service_account"] (需包含 private_key)
    - Cloud Run：若無 secrets，回傳 None 觸發環境預設憑證 (ADC)
    """
    try:
        # 檢查 Streamlit Secrets 是否存在該區塊
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            
            # 如果有私鑰，代表是本地開發用的 Service Account JSON
            if "private_key" in info and info["private_key"]:
                return service_account.Credentials.from_service_account_info(info)
        
        # 若在 Cloud Run 環境，通常找不到實體 secrets 檔案或不含私鑰
        # 此時回傳 None，BigQuery Client 會自動使用 Cloud Run 執行階段的身分
        return None
    except Exception as e:
        st.warning(f"憑證載入提醒: {e}")
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

# --- 2. 測試主程式 ---
if __name__ == "__main__":
    print("="*50)
    print("🔍 開始進行『無 TOML 檔案』連線測試...")
    print("="*50)

    # 設定你的 Project ID
    project_id = "gen-lang-client-0590877921"
    
    try:
        # 取得憑證 (此時應回傳 None)
        credentials = get_gcp_credentials()
        
        # 初始化 Client
        # 當 credentials=None 時，Google Cloud SDK 會自動去找環境中的身分
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # 執行一個極簡查詢測試連線
        TEST_SQL = f"SELECT current_timestamp() as check_time"
        
        print(f"📡 正在嘗試連線至專案: {project_id}...")
        df = client.query(TEST_SQL).to_dataframe()
        
        print("\n✅ 【測試成功！】")
        print(f"目前 BigQuery 時間: {df['check_time'][0]}")
        print("\n💡 結論：即使沒有 TOML 檔案，只要環境身分正確即可連線。")

    except Exception as e:
        print("\n❌ 【測試失敗】")
        print(f"錯誤訊息: {str(e)}")
        print("\n🛠️ 排錯建議：")
        print("1. 如果是在本地測試，請先執行: gcloud auth application-default login")
        print("2. 如果是在 Cloud Run，請確認服務帳號具備 BigQuery Viewer 權限。")
    
    print("="*50)