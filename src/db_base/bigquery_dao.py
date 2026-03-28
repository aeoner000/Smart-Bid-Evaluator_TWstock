import logging
import traceback
import datetime
import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union, Tuple
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# --- 路徑外掛 ---
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from src.utils.config_loader import config
# 這裡對應你 BigQuery 版的 schema 取得方式
from src.db_base.bigquery_schemas import get_table_schema 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IPO_DAO_BigQuery:
    """專業級 BigQuery DAO，與 SQLite 版介面完全對齊。"""
    
    def __init__(self):
        db_config = config['database']['bigquery']
        self.project_id = db_config['project_id']
        self.dataset_id = db_config['dataset_id']
        raw_cred_path = db_config.get('credentials_path')
        
        if raw_cred_path:
            self.credentials_path = str(Path(__file__).resolve().parent.parent.parent / raw_cred_path)
        else:
            self.credentials_path = None
        
        try:
            if self.credentials_path and os.path.exists(self.credentials_path):
                logger.info(f"🔑 使用本地金鑰啟動 BigQuery: {self.credentials_path}")
                self.client = bigquery.Client.from_service_account_json(self.credentials_path)
            else:
                logger.info(f"☁️ 使用 ADC 模式，Project: {self.project_id}")
                self.client = bigquery.Client(project=self.project_id)
            logger.info(f"✅ BigQuery client 啟動成功")
        except Exception as e:
            logger.error(f"❌ Client 初始化失敗: {e}")
            raise

        self.bid_info_pk = ['證券代號', '投標開始日']
        self._ensure_dataset_exists()

    # --- 內部輔助函數 (與 SQLite 邏輯一致) ---

    def _ensure_dataset_exists(self):
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            logger.info(f"資料集 {self.dataset_id} 不存在，進行創建...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset, timeout=30)

    def show_df(self, df, n):
        if "撥券日期_上市_上櫃日期" in df.columns or "最低得標加價率" in df.columns:
            print(n)
            print(df)
        return

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """複刻 SQLite 版的清理邏輯"""
        if df is None or df.empty: return df
        df = df.copy()

        ghost_strings = ['nan', 'none', 'n/a', '<na>', 'nat', 'null', 'NaN', 'None', 'N/A', 'NULL', '-', ' ', '']
        df = df.replace(ghost_strings, np.nan).infer_objects(copy=False)
        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        datetime_cols = df.select_dtypes(include=['datetime64', 'datetime']).columns

        for col in datetime_cols:
            # dt.floor('us') 會將奈秒捨去，保留至微秒，並統一去除時區
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(None).dt.floor('us').astype('datetime64[us]')

        df = df.replace(np.nan, None)
        return df.where(pd.notnull(df), None)

    # --- 核心存取介面 (與 SQLite 名稱與參數完全一致) ---

    def save_data(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> None:
        if df is None or df.empty:
            logger.warning(f"無資料可寫入表: {table_name}")
            return

        df = self._clean_dataframe(df)
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        bq_schema = get_table_schema(table_name)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if if_exists == "replace" 
            else bigquery.WriteDisposition.WRITE_APPEND,
            schema=bq_schema,
            autodetect=True if not bq_schema else False
        )

        try:
            # 寫入前根據 schema 重新排序列 (確保順序與 BQ 一致)
            if bq_schema:
                schema_cols = [field.name for field in bq_schema]
                df = df.reindex(columns=schema_cols)

            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            print(f"略過錯誤筆數 (Bad Records): {job.error_result if job.error_result else '0'}")
            logger.info(f"✅ 成功寫入 {len(df)} 筆資料至 BigQuery: {table_name}")
        except Exception as e:
            logger.error(f"❌ 寫入 BigQuery 失敗: {e}")
            raise

    def fetch_all(self, table_name: str) -> pd.DataFrame:
        """複刻 SQLite 版 fetch_all: 依據 Schema 指定順序讀取"""
        try:
            bq_schema = get_table_schema(table_name)
            if bq_schema:
                ordered_cols = [field.name for field in bq_schema]
                cols_str = ", ".join([f"`{col}`" for col in ordered_cols])
                sql = f"SELECT {cols_str} FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
            else:
                sql = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
            
            return self.query(sql)
        except Exception as e:
            logger.error(f"❌ fetch_all 失敗: {e}")
            return pd.DataFrame()

    def query(self, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
        """執行查詢並回傳 DataFrame"""
        job_config = bigquery.QueryJobConfig()
        # 注意: BigQuery 參數化查詢使用 @name，與 SQLite 的 ? 不同
        if params:
            query_params = []
            for k, v in params.items():
                query_params.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
            job_config.query_parameters = query_params

        try:
            return self.client.query(sql, job_config=job_config).to_dataframe()
        except Exception as e:
            logger.error(f"❌ 查詢失敗: {e}\nSQL: {sql}")
            return pd.DataFrame()

    def execute(self, sql: str, params: Optional[Union[List, Dict]] = None) -> None:
        """執行 DML (Delete/Update/Insert)，支援參數化查詢。"""
        job_config = bigquery.QueryJobConfig()
        
        if params:
            query_params = []
            # 判斷是字典還是列表，並轉換為 BigQuery 參數格式
            param_items = params.items() if isinstance(params, dict) else enumerate(params)
            for k, v in param_items:
                param_name = k if isinstance(params, dict) else None
                # 這裡統一轉成 STRING，BigQuery 會自動處理常見型別轉換
                query_params.append(
                    bigquery.ScalarQueryParameter(param_name, "STRING", str(v))
                )
            job_config.query_parameters = query_params

        try:
            # 關鍵修正：必須傳入 job_config
            query_job = self.client.query(sql, job_config=job_config)
            query_job.result()  # 等待執行完成
            logger.info(f"✅ SQL 執行成功")
        except Exception as e:
            logger.error(f"❌ SQL 執行失敗: {e}\nSQL: {sql}")
            raise

    def ensure_table_exists(self, table_name: str) -> None:
        """複刻 SQLite 邏輯：不存在則建表"""
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        try:
            self.client.get_table(table_id)
        except NotFound:
            logger.info(f"資料表 '{table_name}' 不存在，正在建立...")
            bq_schema = get_table_schema(table_name)
            table = bigquery.Table(table_id, schema=bq_schema)
            self.client.create_table(table)
            logger.info(f"✅ 資料表 {table_name} 已建立")

    def get_max_date(self, table_name: str, date_col: str) -> Optional[pd.Timestamp]:
        sql = f"SELECT MAX(`{date_col}`) AS max_date FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
        df = self.query(sql)
        if df.empty or pd.isnull(df.iloc[0, 0]): return None
        return pd.to_datetime(df.iloc[0, 0])

    def delete_by_keys(self, table_name: str, keys_to_delete: list[tuple[str, pd.Timestamp]]):
        if not keys_to_delete: return 0
        
        # BigQuery 的多欄位 IN 語法與 SQLite 不同，通常建議用 OR 串接或臨時表，這裡採 OR 串接以維持單一 SQL
        conditions = []
        for code, start_date in keys_to_delete:
            date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
            conditions.append(f"(`證券代號` = '{code}' AND `投標開始日` = '{date_str}')")
        
        where_clause = " OR ".join(conditions)
        sql = f"DELETE FROM `{self.project_id}.{self.dataset_id}.{table_name}` WHERE {where_clause}"
        
        try:
            logger.info(f"正在從 {table_name} 刪除 {len(keys_to_delete)} 筆資料...")
            self.execute(sql)
            return len(keys_to_delete)
        except Exception as e:
            logger.error(f"❌ delete_by_keys 失敗: {e}")
            raise

    def update_status_by_keys(self, keys: list[tuple], new_status: str):
        if not keys: return
        # 同樣使用 OR 串接處理多主鍵更新
        conditions = []
        for k in keys:
            date_str = pd.to_datetime(k[1]).strftime('%Y-%m-%d %H:%M:%S')
            conditions.append(f"(`{self.bid_info_pk[0]}` = '{k[0]}' AND `{self.bid_info_pk[1]}` = '{date_str}')")
        
        where_clause = " OR ".join(conditions)
        sql = f"UPDATE `{self.project_id}.{self.dataset_id}.bid_info` SET `status` = '{new_status}' WHERE {where_clause}"
        
        try:
            self.execute(sql)
            logger.info(f"成功將 {len(keys)} 筆狀態更新為 '{new_status}'")
        except Exception as e:
            logger.error(f"❌ update_status_by_keys 失敗: {e}")
            raise