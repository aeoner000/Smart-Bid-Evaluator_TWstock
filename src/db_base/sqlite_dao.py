import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd
import traceback
import logging

from src.utils.config_loader import config
# 修正: 同時引入 get_table_schema 以獲取欄位順序
from src.db_base.schemas import create_table_sql, get_table_schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Database:
    """提供 SQLite 連線管理（可擴充至其他 DB）。"""

    def __init__(self, db_path: str, **connect_kwargs: Any):
        self.db_path = db_path
        self.connect_kwargs = connect_kwargs

    @contextmanager
    def connect(self):
        """以 context manager 提供連線使用，確保連線會被正確關閉。"""
        conn = sqlite3.connect(self.db_path, **self.connect_kwargs)
        try:
            yield conn
        finally:
            conn.close()

class IPO_DAO_SQLite:
    """統一資料存取層（DAO）：提供讀 / 寫 / 查詢的共用介面。"""

    def __init__(self):
        db_config = config['database']['sqlite']
        self.db = Database(db_config['db_path'], **db_config['connect'])
        self.bid_info_pk = ['證券代號', '投標開始日']

    @staticmethod
    def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        ghost_strings = [
            'nan', 'none', 'n/a', '<na>', 'nat', 'null',
            'NaN', 'None', 'N/A', 'NULL', '-', ' ', ''
        ]
        
        df = df.replace(ghost_strings, np.nan)

        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

        df = df.where(pd.notnull(df), None)

        return df

    def save_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
    ) -> None:
        df_cleaned = self._clean_dataframe(df.copy())

        if df_cleaned is None or df_cleaned.empty:
            logger.warning(f"表 {table_name} 清理後無資料可供存入。")
            return

        try:
            with self.db.connect() as conn:
                df_cleaned.to_sql(table_name, conn, if_exists=if_exists, index=False)
                logger.info(f"成功將 {len(df_cleaned)} 筆資料存入 SQL 表: {table_name}")
        except Exception as e:
            err_msg = traceback.format_exc()
            logger.error(f"❌ 存入表 {table_name} 失敗！原因: {e}\n{err_msg}")

    def fetch_all(self, table_name: str) -> pd.DataFrame:
        """
        v4 修正版: 不再使用 'SELECT *'，而是根據 schema 明確指定欄位順序讀取，
        從根本上杜絕 SQLite 驅動造成的欄位順序錯亂問題。
        """
        try:
            # 1. 從 schema.py 取得該表所有欄位的正確順序
            schema = get_table_schema(table_name)
            ordered_cols = [col[0] for col in schema]
            
            # 2. 為了與 SQLite 語法兼容，用雙引號 "" 包裹欄位名
            cols_for_select = ", ".join([f'"{col}"' for col in ordered_cols])
            
            # 3. 建立明確指定欄位順序的查詢語句
            query = f'SELECT {cols_for_select} FROM "{table_name}"'
            
            # 4. 使用既有的 query 方法執行
            df = self.query(query)

            # 5. 【雙重保險】確保返回的 DataFrame 欄位順序與 schema 一致
            return df.reindex(columns=ordered_cols)

        except Exception as e:
            logger.error(f"❌ 在 fetch_all 中讀取表 {table_name} 時發生嚴重錯誤: {e}")
            logger.warning("⚠️ 將退回使用 'SELECT *' 模式，這可能導致欄位順序不一致！")
            # Fallback to the old, potentially unsafe method if the new one fails
            query = f'SELECT * FROM "{table_name}"'
            return self.query(query)

    def query(
        self, sql: str, params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None
    ) -> pd.DataFrame:
        """執行查詢並回傳 pandas.DataFrame。"""
        try:
            with self.db.connect() as conn:
                return pd.read_sql(sql, conn, params=params)
        except Exception as e:
            logger.error(f"執行查詢失敗: {e}\nSQL: {sql}")
            return pd.DataFrame()

    def execute(self, sql: str, params: Any = None) -> None:
        try:
            with self.db.connect() as conn:
                if params and isinstance(params, list) and isinstance(params[0], (list, tuple)):
                    conn.executemany(sql, params)
                else:
                    conn.execute(sql, params or [])
                conn.commit()
        except Exception as e:
            logger.error(f"執行 SQL 失敗: {e}\nSQL: {sql}")
            raise

    def ensure_table_exists(self, table_name: str) -> None:
        """
        檢查資料表是否存在，若不存在則根據 schemas.py 的定義建立它。
        """
        check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        try:
            with self.db.connect() as conn:
                cursor = conn.cursor()
                cursor.execute(check_sql, (table_name,))
                if cursor.fetchone() is None:
                    logger.info(f"資料表 '{table_name}' 不存在，正在根據 schema 建立...")
                    sql_create = create_table_sql(table_name)
                    self.execute(sql_create)
        except Exception as e:
            logger.error(f"檢查或建立資料表 '{table_name}' 時發生錯誤: {e}")

    def get_max_date(self, table_name: str, date_col: str) -> Optional[pd.Timestamp]:
        """取得指定欄位的最大日期。"""
        query = f"SELECT MAX([{date_col}]) AS max_date FROM {table_name}"
        df = self.query(query)
        if df.empty or df.iloc[0, 0] is None:
            return None
        return pd.to_datetime(df.iloc[0, 0])

    def delete_by_keys(self, table_name: str, keys_to_delete: list[tuple[str, pd.Timestamp]]):
        if not keys_to_delete:
            print(f"[{table_name}] 無需回滾，任務列表為空。")
            return 0

        formatted_keys = []
        for code, start_date in keys_to_delete:
            date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
            formatted_keys.append((code, date_str))
        
        values_clause = ", ".join([str(k) for k in formatted_keys])
        
        sql_delete = (
            f'DELETE FROM "{table_name}" WHERE ("證券代號", "投標開始日") IN ({values_clause})'
        )
        
        try:
            print(f"準備從 [{table_name}] 刪除 {len(formatted_keys)} 筆資料...")
            self.execute(sql_delete)
            print(f"已向 [{table_name}] 發送刪除命令。")
            return len(formatted_keys)
        except Exception as e:
            print(f"!!! 在表格 [{table_name}] 執行回滾 SQL 時發生嚴重錯誤: {e}")
            raise

    def update_status_by_keys(self, keys: list[tuple], new_status: str):
        if not keys:
            return
        pk1, pk2 = self.bid_info_pk
        sql = f'UPDATE "bid_info" SET "status" = ? WHERE "{pk1}" = ? AND "{pk2}" = ?'
        
        params_list = [
            (new_status, str(k[0]), pd.to_datetime(k[1]).strftime('%Y-%m-%d %H:%M:%S'))
            for k in keys
        ]
        
        try:
            self.execute(sql, params_list)
            logger.info(f"成功將 {len(keys)} 筆任務狀態更新為 '{new_status}'。")
        except Exception as e:
            logger.error(f"!!! 執行狀態更新時失敗: {e}")
            raise
