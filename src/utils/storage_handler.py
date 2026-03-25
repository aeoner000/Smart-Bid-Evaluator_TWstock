import joblib
import os
import json
import logging
import io, sys
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account

root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)
    
from src.utils.config_loader import config

logger = logging.getLogger(__name__)

# --- BaseStorageHandler 與 LocalStorageHandler 保持原樣不變 ---
class BaseStorageHandler:
    """Abstract base class for storage handlers."""
    def save_file(self, obj: object, path: str) -> None:
        if path.endswith('.json'):
            self._save_json(obj, path)
        else:
            self._save_joblib(obj, path)

    def load_file(self, path: str) -> object:
        if path.endswith('.json'):
            return self._load_json(path)
        else:
            return self._load_joblib(path)

    def _save_joblib(self, obj: object, path: str) -> None: raise NotImplementedError
    def _load_joblib(self, path: str) -> object: raise NotImplementedError
    def _save_json(self, obj: dict, path: str) -> None: raise NotImplementedError
    def _load_json(self, path: str) -> dict: raise NotImplementedError


class LocalStorageHandler(BaseStorageHandler):
    """Handles saving and loading files on the local filesystem."""
    def _save_joblib(self, obj: object, path: str) -> None:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            joblib.dump(obj, path)
            logger.info(f"Joblib file successfully saved to local path: {path}")
        except Exception as e:
            logger.error(f"Failed to save joblib file locally to {path}", exc_info=True)
            raise

    def _load_joblib(self, path: str) -> object:
        try:
            obj = joblib.load(path)
            logger.info(f"Joblib file successfully loaded from local path: {path}")
            return obj
        except Exception as e:
            logger.error(f"Failed to load joblib file locally from {path}", exc_info=True)
            raise

    def _save_json(self, obj: dict, path: str) -> None:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(obj, f, indent=4, ensure_ascii=False)
            logger.info(f"JSON file successfully saved to local path: {path}")
        except Exception as e:
            logger.error(f"Failed to save JSON file locally to {path}", exc_info=True)
            raise

    def _load_json(self, path: str) -> dict:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"JSON file successfully loaded from local path: {path}")
            return data
        except Exception as e:
            logger.error(f"Failed to load JSON file locally from {path}", exc_info=True)
            raise


# --- 修改後的 GCSStorageHandler ---
class GCSStorageHandler(BaseStorageHandler):
    """Handles saving and loading files to/from Google Cloud Storage."""

    def __init__(self, bucket_name: str, credentials_path: str = None):
        if not bucket_name:
            logger.error("GCS bucket_name must be configured")
            raise ValueError("GCS bucket_name must be configured")
        
        try:
            # 關鍵修改：檢查 credentials_path 是否存在於實體路徑
            if credentials_path and os.path.exists(credentials_path):
                # 本地開發模式：有找到 JSON 金鑰
                logger.info(f"Using local service account key: {credentials_path}")
                self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
                self.client = storage.Client(credentials=self.credentials)
            else:
                # 雲端部署模式：找不到金鑰，自動改用 ADC (如 Cloud Run 的服務帳戶)
                logger.info("Credentials file not found or not provided. Switching to ADC (Cloud Run mode).")
                self.client = storage.Client()
            
            self.bucket = self.client.bucket(bucket_name)
            logger.info(f"GCS handler initialized for bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise

    def _save_joblib(self, obj: object, path: str) -> None:
        try:
            buffer = io.BytesIO()
            joblib.dump(obj, buffer)
            buffer.seek(0)
            blob = self.bucket.blob(path)
            blob.upload_from_file(buffer)
            logger.info(f"Joblib file successfully uploaded to GCS: gs://{self.bucket.name}/{path}")
        except Exception as e:
            logger.error(f"Failed to upload joblib to GCS at {path}", exc_info=True)
            raise

    def _load_joblib(self, path: str) -> object:
        try:
            blob = self.bucket.blob(path)
            if not blob.exists():
                raise FileNotFoundError(f"File not found in GCS: gs://{self.bucket.name}/{path}")
            buffer = io.BytesIO(blob.download_as_bytes())
            obj = joblib.load(buffer)
            logger.info(f"Joblib file successfully loaded from GCS: gs://{self.bucket.name}/{path}")
            return obj
        except Exception as e:
            logger.error(f"Failed to load joblib from GCS at {path}", exc_info=True)
            raise

    def _save_json(self, obj: dict, path: str) -> None:
        try:
            json_str = json.dumps(obj, indent=4, ensure_ascii=False)
            blob = self.bucket.blob(path)
            blob.upload_from_string(json_str, content_type='application/json')
            logger.info(f"JSON file successfully uploaded to GCS: gs://{self.bucket.name}/{path}")
        except Exception as e:
            logger.error(f"Failed to upload JSON to GCS at {path}", exc_info=True)
            raise

    def _load_json(self, path: str) -> dict:
        try:
            blob = self.bucket.blob(path)
            if not blob.exists():
                raise FileNotFoundError(f"File not found in GCS: gs://{self.bucket.name}/{path}")
            json_str = blob.download_as_string()
            data = json.loads(json_str)
            logger.info(f"JSON file successfully loaded from GCS: gs://{self.bucket.name}/{path}")
            return data
        except Exception as e:
            logger.error(f"Failed to load JSON from GCS at {path}", exc_info=True)
            raise

    def _upload_to_gcs(self, local_path: Path, gcs_path: str):
        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
        except Exception as e:
            logger.error(f"Failed to upload to GCS at {gcs_path}", exc_info=True)
            raise

    def _download_from_gcs(self, gcs_path: str, local_path: Path):
        try:
            blob = self.bucket.blob(gcs_path)
            if not blob.exists():
                raise FileNotFoundError(f"File not found in GCS: gs://{self.bucket.name}/{gcs_path}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob.download_to_filename(local_path)
        except Exception as e:
            logger.error(f"Failed to download from GCS at {gcs_path}", exc_info=True)
            raise


# --- 修改後的 get_storage_handler ---
def get_storage_handler():
    """
    Factory function that returns the appropriate storage handler based on config.
    """
    storage_config = config.get('storage', {})
    storage_type = storage_config.get('type', 'local')

    if storage_type == 'gcs':
        logger.info("Storage mode: Google Cloud Storage")
        gcs_config = storage_config.get('gcs', {})
        
        # 解析路徑字串
        raw_cred_path = gcs_config.get('credentials_path')
        if raw_cred_path:
            # 轉換為絕對路徑，方便 os.path.exists 判斷
            credentials_path = str(Path(__file__).resolve().parent.parent.parent / raw_cred_path)
        else:
            credentials_path = None

        return GCSStorageHandler(
            bucket_name=gcs_config.get('bucket_name'),
            credentials_path=credentials_path
        )
    elif storage_type == 'local':
        logger.info("Storage mode: Local")
        return LocalStorageHandler()
    else:
        logger.error(f"Unsupported storage type: '{storage_type}'. Check config.yaml.")
        raise ValueError(f"Unsupported storage type: '{storage_type}'. Check config.yaml.")


# ==========================================
# 簡單測試區塊 (if __name__ == "__main__")
# ==========================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("\n" + "="*50)
    print("🚀 Storage Handler Connection Test")
    print("="*50)

    try:
        handler = get_storage_handler()
        print(f"✅ Handler type: {type(handler).__name__}")
        
        # 測試資料
        test_data = {"status": "ok", "msg": "connection_test"}
        test_path = "test/connection_check.json"
        
        # 儲存測試
        print(f"📡 Uploading test file to {test_path}...")
        handler.save_file(test_data, test_path)
        
        # 讀取測試
        print(f"📥 Loading test file back...")
        loaded = handler.load_file(test_path)
        
        if loaded == test_data:
            print("✨ Success! Storage read/write cycle verified.")
        else:
            print("⚠️ Data mismatch after load.")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
    
    print("="*50 + "\n")