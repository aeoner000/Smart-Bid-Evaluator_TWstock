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
        self.bid_info_pk = ['證券代號', '投標開始日']
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
                        df[col] = pd.to_datetime(df[col], format="mixed",errors='ignore')
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
        if df.empty:
            if if_exists == 'replace':
                logger.info(f"傳入空 DataFrame 且 if_exists='replace'，將清除並重建資料表: {table_name}")
                # 允許程式繼續往下執行，讓 to_sql 處理清空與重建的邏輯
            else:
                # 對於 'append' 或 'fail' 模式，無資料可存，行為不變，直接返回
                logger.warning(f"表 {table_name} 無新資料可供存入 (模式: {if_exists})。")
                return
        df = self._clean_dataframe(df)

        try:
            with self.db.connect() as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
                if not df.empty:
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

    def execute(self, sql: str, params: Any = None) -> None:
        try:
            with self.db.connect() as conn:
                # 如果 params 是一個列表，且裡面第一個元素也是列表/元組，就用 executemany
                if params and isinstance(params, list) and isinstance(params[0], (list, tuple)):
                    conn.executemany(sql, params)
                else:
                    conn.execute(sql, params or [])
                conn.commit()
        except Exception as e:
            logger.error(f"執行 SQL 失敗: {e}\nSQL: {sql}")
            raise

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

    def combine_all_feature(self, config_dict:dict, merge_keys=['證券代號', '投標開始日']):
        """
        config_dict: key 為表名, value 為欄位 list
        merge_keys: 在每一張表中名稱都一樣的關聯欄位
        """
        table_names = list(config_dict.keys()) # 取出要清理的所有表名
        main_table_name = table_names[0]       # 主表名
        other_tables_name = table_names[1:]    # 其他表名

        # 1. 主表的欄位
        main_cols = [f'"{main_table_name}"."{c}"' for c in config_dict[main_table_name]]

        # 2. 其他表的欄位 (為了避免衝突，加上表名.欄位名稱)
        other_cols = [] # 完整欄位
        for t in other_tables_name:
            # 僅選取不在 merge_keys 中的欄位，且不加前綴，是為了要組成 select .... 去建立正確欄位有哪些
            cols = [f'"{t}"."{c}"' for c in config_dict[t] if c not in merge_keys]
            other_cols.extend(cols)
        all_select_cols = ", ".join(main_cols + other_cols)

        # 3. 構建 JOIN 條件
        join_condition = [] # join條件
        for t in other_tables_name:
                          #  ex. "bid_info"."證券代號" = "fin_stmts"."證券代號" AND "bid_info"."投標開始日" = "fin_stmts"."投標開始日"
            on_condition = " AND ".join([f'"{main_table_name}"."{k}" = "{t}"."{k}"' for k in merge_keys])
            join_condition.append(f'LEFT JOIN "{t}" ON {on_condition}') # LEFT JOIN "bid_info" on on_condition

        final_sql = f"""
            SELECT {all_select_cols}
            FROM "{main_table_name}"
            {" ".join(join_condition)}
        """
        df = self.query(final_sql)

        if '投標開始日' in df.columns:
            df['投標開始日'] = pd.to_datetime(df['投標開始日'])
        return df


    def delete_by_keys(self, table_name: str, keys_to_delete: list[tuple[str, pd.Timestamp]]):
        """
        使用 SQL DELETE 命令，根據主鍵元組列表，高效地刪除指定表格中的多筆資料。
        此方法會將 Timestamp 格式化為 'YYYY-MM-DD HH:MM:SS' 以匹配資料庫中的 TEXT 儲存格式。

        Args:
            table_name: 要操作的表格名稱。
            keys_to_delete: 一個包含 (證券代號, pd.Timestamp) 元組的列表。
        """
        if not keys_to_delete:
            print(f"[{table_name}] 無需回滾，任務列表為空。")
            return 0

        # 1. 為了匹配 TEXT 欄位的儲存格式，將 Timestamp 轉換為 'YYYY-MM-DD HH:MM:SS'
        formatted_keys = []
        for code, start_date in keys_to_delete:
            # 這是關鍵：產生與資料庫 TEXT 欄位完全匹配的字串格式
            date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
            formatted_keys.append((code, date_str))
        
        # 2. 建立 SQL 的 IN (...) 子句內容
        # repr() 會自動處理字串的引號，例如 '1234' -> "'1234'"
        # str(tuple) 會產生 '(\'1234\', \'2023-10-26 00:00:00\')'
        values_clause = ", ".join([str(k) for k in formatted_keys])
        
        # 3. 組合完整的 SQL DELETE 語句
        # DuckDB/SQLite 支持使用元組進行 IN 比較
        sql_delete = (
            f'DELETE FROM "{table_name}" WHERE ("證券代號", "投標開始日") IN ({values_clause})'
        )
        
        try:
            # 4. 執行命令
            # 假設 self.dao.execute() 是執行 SQL 的方法
            print(f"準備從 [{table_name}] 刪除 {len(formatted_keys)} 筆資料...")
            self.execute(sql_delete) # 假設這是您 DAO 中執行 SQL 的方法
            print(f"已向 [{table_name}] 發送刪除命令。")
            
            # 精確回傳嘗試刪除的數量
            return len(formatted_keys)

        except Exception as e:
            print(f"!!! 在表格 [{table_name}] 執行回滾 SQL 時發生嚴重錯誤: {e}")
            # 向上拋出例外，讓 main.py 的交易協調器知道回滾失敗
            raise e

    def revert_task_status_by_keys(self, keys: list[tuple], new_status: str):
        """根據主鍵列表批量更新 bid_info 表的狀態。"""
        if not keys:
            return
        pk1, pk2 = self.bid_info_pk
        sql = f'UPDATE "bid_info" SET "status" = ? WHERE "{pk1}" = ? AND "{pk2}" = ?'
        
        params_list = [
            (new_status, str(k[0]), pd.to_datetime(k[1]).strftime('%Y-%m-%d %H:%M:%S'))
            for k in keys
        ]
        
        try:
            # 【關鍵修正】統一使用 self.execute 處理所有資料庫執行操作
            self.execute(sql, params_list)
            logger.info(f"成功將 {len(keys)} 筆任務狀態更新為 '{new_status}'。")
        except Exception as e:
            logger.error(f"!!! 執行狀態更新時失敗: {e}")
            raise