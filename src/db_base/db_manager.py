import sqlite3
from contextlib import contextmanager
# from datetime import datetime
# from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd
import traceback

import logging

# from src.utils.config_loader import cfg
from src.db_base.schemas import create_table_sql

# 設定 Logging，方便在 GCP 執行時追蹤錯誤
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


class IPO_DAO:
    """統一資料存取層（DAO）：提供讀 / 寫 / 查詢的共用介面。"""

    def __init__(self, db_path: str, **connect_kwargs: Any):
        self.db = Database(db_path, **connect_kwargs)

    # ---------------------------------------------------------------------
    # 內部工具
    # ---------------------------------------------------------------------
    @staticmethod
    def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """把字串型態欄位做 trim、把空字串轉為 NaN。"""
        if df is None or df.empty:
            return df

        string_cols = [c for c, t in df.dtypes.items()
                        if t == "object" or pd.api.types.is_string_dtype(t)]
        if string_cols:
            for col in string_cols:
                # 這種寫法內建處理了非字串的狀況，效能比 lambda map 快
                df[col] = df[col].astype(str).str.strip()
            
            df[string_cols] = df[string_cols].replace(["", "nan", "None"], np.nan)

        return df

    @staticmethod
    def _try_convert_dates(df: pd.DataFrame) -> pd.DataFrame:
        """嘗試將包含 'date', 'time', '日', '期' 的欄位轉為 datetime。"""
        if df is None or df.empty:
            return df
        
        target_keywords = ['date', 'time', '日', '期']
        for col in df.columns:
            # 僅針對 object (字串) 欄位且名稱符合關鍵字者進行轉換
            if pd.api.types.is_object_dtype(df[col]):
                if any(k in col.lower() for k in target_keywords):
                    try:
                        df[col] = pd.to_datetime(df[col], errors='ignore')
                    except Exception:
                        pass
        return df

    # ---------------------------------------------------------------------
    # 主要 CRUD / 查詢
    # ---------------------------------------------------------------------
    def save_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",  # 新增模式 (append, replace, fail)
    ) -> None:
        """將 DataFrame 存入指定資料表。"""
        if df is None or df.empty:
            logger.warning(f"表 {table_name} 無新資料可供存入。")
            return

        df = self._clean_dataframe(df)

        try:
            with self.db.connect() as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
                logger.info(f"成功將 {len(df)} 筆資料存入 SQL 表: {table_name}")
        except Exception as e:
            err_msg = traceback.format_exc()
            logger.error(f"❌ 存入表 {table_name} 失敗！原因: {e}\n{err_msg}")

    def fetch_all(self, table_name: str) -> pd.DataFrame:
        """讀取整張表（通常用於特徵工程、資料整併）。"""
        query = f"SELECT * FROM {table_name}"
        df = self.query(query)
        # 自動轉換日期格式，確保後續操作方便
        return self._try_convert_dates(df)

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

    def execute(
        self, sql: str, params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None
    ) -> None:
        """執行非查詢 SQL (DDL / DML)。"""
        try:
            with self.db.connect() as conn:
                conn.execute(sql, params or [])
                conn.commit()
        except Exception as e:
            logger.error(f"執行 SQL 失敗: {e}\nSQL: {sql}")

    # ---------------------------------------------------------------------
    # 常用查詢
    # ---------------------------------------------------------------------
    def ensure_table_exists(self, table_name: str) -> None:
        """
        檢查資料表是否存在，若不存在則根據 schemas.py 的定義建立它。
        """
        check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
        try:
            with self.db.connect() as conn:
                cursor = conn.cursor()
                cursor.execute(check_sql, (table_name,))
                if cursor.fetchone() is None:
                    # 資料表不存在，建立它
                    logger.info(f"資料表 '{table_name}' 不存在，正在根據 schema 建立...")
                    sql_create = create_table_sql(table_name)
                    self.execute(sql_create)
        except Exception as e:
            logger.error(f"檢查或建立資料表 '{table_name}' 時發生錯誤: {e}")

    def get_max_date(self, table_name: str, date_col: str) -> Optional[pd.Timestamp]:
        """取得指定欄位的最大日期，用於增量爬取比對。"""
        query = f"SELECT MAX([{date_col}]) AS max_date FROM {table_name}"
        df = self.query(query)
        if df.empty or df.iloc[0, 0] is None:
            return None
        return pd.to_datetime(df.iloc[0, 0])

    def diff_index(
        self,
        raw_table: str,
        target_table: str,
        key_cols: List[str],
    ) -> Tuple[pd.DataFrame, pd.Index]:
        """取得 raw_table 中尚未存到 target_table 的 key index。

        使用 SQL 直接比對，避免整張表載入記憶體。
        回傳：
          - raw_df：raw_table 的完整 DataFrame（供後續使用）
          - diff_index：以 key_cols 作為索引的 MultiIndex（tuple list）
        """
        # 1) 讀取 raw_table（如果沒有資料則直接回傳）
        raw_df = self.fetch_all(raw_table)
        # fetch_all 現在已經會自動轉換日期，所以 raw_df 裡的日期是 datetime 物件
        if raw_df.empty:
            return raw_df, pd.MultiIndex.from_tuples([], names=key_cols)

        # 2) 以 SQL 方式計算差異索引（避免載入 target_table 全表）
        key_cols_quoted = [f"[{c}]" for c in key_cols]
        key_cols_expr = ", ".join(key_cols_quoted)
        join_condition = " AND ".join([f"r.{c} = t.{c}" for c in key_cols_quoted])

        sql = f"""
            SELECT DISTINCT {key_cols_expr}
            FROM {raw_table} AS r
            WHERE NOT EXISTS (
                SELECT 1 FROM {target_table} AS t
                WHERE {join_condition}
            )
        """

        diff_df = self.query(sql)
        # 確保 diff_df (用於生成 index) 的日期欄位也被轉換
        diff_df = self._try_convert_dates(diff_df)

        if diff_df.empty:
            return raw_df, pd.Index([])

        # 轉成 MultiIndex，使迭代時可回傳 tuple，並轉成時間格式
        tuples = [
            tuple(pd.to_datetime(x) if i >= 1 else x for i, x in enumerate(row))  # i 控制第二個以後的都轉為 datetime
            for row in diff_df[key_cols].itertuples(index=False, name=None)
        ]
        multi_idx = pd.MultiIndex.from_tuples(tuples, names=key_cols)
        return raw_df, multi_idx
