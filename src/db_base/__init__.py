'''
資料庫模塊 - 提供統一的資料存取介面 (DAO 模式)

核心函數：
- get_db_manager: 根據配置文件返回對應的資料庫管理器 (SQLite 或 BigQuery)

使用示例：
    from src.db_base import get_db_manager
    
    dao = get_db_manager()
    df = dao.fetch_all("bid_info")
    dao.save_data(df, "processed_table", if_exists="replace")
'''

from .db_manager import get_db_manager
from .schemas import (
    TABLE_SCHEMAS,
    get_table_schema,
    create_table_sql,
)

__all__ = [
    "get_db_manager",
    "TABLE_SCHEMAS",
    "get_table_schema",
    "create_table_sql",
]
