"""
資料庫模塊 - 提供統一的 SQLite 資料存取介面 (DAO 模式)

核心類別：
- Database: SQLite 連線管理（context manager 全自動關閉）
- IPO_DAO: 資料存取物件（讀/寫/查詢操作）

使用示例：
    from src.database.db_manager import IPO_DAO
    from src.utils.config_loader import DB_PATH, DB_CONNECT_KWARGS
    
    dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
    df = dao.fetch_all("bid_info")
    dao.save_data(df, "processed_table", if_exists="replace")
"""

from .db_manager import Database, IPO_DAO
from .schemas import (
    TABLE_SCHEMAS,
    get_table_schema,
    create_table_sql,
)

__all__ = [
    "Database",
    "IPO_DAO",
    "TABLE_SCHEMAS",
    "get_table_schema",
    "create_table_sql",
]
