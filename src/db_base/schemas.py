"""
** 資料結構定義 **
schemas.py 定義 SQLite 表格的結構，包含欄位名稱、格式、約束
Note: SQLite 沒有時間格式，目前都以TEXT替代
"""

from typing import Dict, List, Tuple

# Table schema definition: (column_name, sqlite_type, constraints)
TableSchema = List[Tuple[str, str, str]]

# Database table schemas
TABLE_SCHEMAS: Dict[str, TableSchema] = {
    "bid_info": [
        # Auction bid information
        ("開標日期", "TEXT", "NOT NULL"),  # DATE stored as TEXT in SQLite
        ("證券名稱", "TEXT", ""),
        ("證券代號", "TEXT", "NOT NULL"),
        ("發行市場", "TEXT", ""),
        ("發行性質", "TEXT", ""),
        ("競拍方式", "TEXT", ""),
        ("投標開始日", "TEXT", ""),  # DATE stored as TEXT in SQLite
        ("投標結束日", "TEXT", ""),  # DATE stored as TEXT in SQLite
        ("競拍數量(張)", "REAL", ""),
        ("最低投標價格(元)", "REAL", ""),
        ("最低每標單投標數量(張)", "INTEGER", ""),
        ("最高投(得)標數量(張)", "REAL", ""),
        ("保證金成數(%)", "INTEGER", ""),
        ("每一投標單投標處理費(元)", "INTEGER", ""),
        ("撥券日期(上市、上櫃日期)", "TEXT", ""),  # DATE stored as TEXT in SQLite
        ("主辦券商", "TEXT", ""),
        ("得標總金額(元)", "REAL", ""),
        ("得標手續費率(%)", "REAL", ""),
        ("總合格件", "REAL", ""),
        ("合格投標數量(張)", "REAL", ""),
        ("最低得標價格(元)", "REAL", ""),
        ("最高得標價格(元)", "REAL", ""),
        ("得標加權平均價格(元)", "REAL", ""),
        ("承銷價格(元)", "REAL", ""),
        ("取消競價拍賣(流標或取消)", "TEXT", ""),
        ("update_time", "TEXT", ""),  # DATETIME stored as TEXT in SQLite
        ("status", "TEXT", ""),       # no_list、all_complete
    ],

    "fin_stmts": [
        # Financial statements
        ("證券代號", "TEXT", "NOT NULL"),
        ("投標開始日", "TEXT", "NOT NULL"),
        ("營業收入", "REAL", ""),
        ("前一期營業收入", "REAL", ""),
        ("本期淨利", "REAL", ""),
        ("前一期本期淨利", "REAL", ""),
        ("每股盈餘", "REAL", ""),
        ("前一期每股盈餘", "REAL", ""),
        ("歸屬於母公司業主之權益合計", "REAL", ""),
        ("前一期歸屬於母公司業主之權益合計", "REAL", ""),
        ("已發行股份總數", "REAL", ""),
        ("前一期已發行股份總數", "REAL", ""),
        ("負債總計", "REAL", ""),
        ("前一期負債總計", "REAL", ""),
        ("資產總計", "REAL", ""),
        ("前一期資產總計", "REAL", ""),
        ("營收成長率", "REAL", ""),
        ("本期淨利成長率", "REAL", ""),
        ("每股盈餘成長率", "REAL", ""),
        ("ROE", "REAL", ""),
        ("前一期ROE", "REAL", ""),
        ("ROE成長率", "REAL", ""),
        ("ROA", "REAL", ""),
        ("前一期ROA", "REAL", ""),
        ("ROA成長率", "REAL", ""),
        ("每股淨值", "REAL", ""),
        ("前一期每股淨值", "REAL", ""),
        ("每股淨值成長率", "REAL", ""),
        ("負債比", "REAL", ""),
        ("前一期負債比", "REAL", ""),
        ("負債比成長率", "REAL", ""),
    ],

    "all_market_info": [
        # Market information
        ("證券代號", "TEXT", "NOT NULL"),
        ("投標開始日", "TEXT", "NOT NULL"),  # DATE stored as TEXT in SQLite
        ("外資平均增減", "REAL", ""),
        ("投信平均增減", "REAL", ""),
        ("自營商平均增減", "REAL", ""),
        ("融資張數增減", "REAL", ""),
        ("融券張數增減", "REAL", ""),
        ("融資金額增減", "REAL", ""),
        ("道瓊工業_10日漲幅(%)", "REAL", ""),
        ("標普500_10日漲幅(%)", "REAL", ""),
        ("那斯達克_10日漲幅(%)", "REAL", ""),
        ("費城半導體_10日漲幅(%)", "REAL", ""),
        ("大盤_10日漲幅(%)", "REAL", ""),
        ("大盤_平均成交量", "REAL", ""),
        ("櫃買_10日漲幅(%)", "REAL", ""),
        ("櫃買_平均成交量", "REAL", ""),
    ],

    "history_price_info": [
        # Historical price information
        ("證券代號", "TEXT", "NOT NULL"),
        ("投標開始日", "TEXT", "NOT NULL"),
        ("前一日平均成交價", "REAL", ""),
        ("前十日內平均成交價", "REAL", ""),
        ("前十日內漲幅", "REAL", ""),
        ("前一日成交金額", "REAL", ""),
        ("前十日內平均成交金額", "REAL", ""),
        ("前一日最高成交價", "REAL", ""),
        ("前一日最低成交價", "REAL", ""),
        ("前一日成交筆數", "REAL", ""),
        ("前一日成交股數", "REAL", ""),
        ("前十日內平均成交筆數", "REAL", ""),
        ("前十日內平均成交股數", "REAL", ""),
    ],

    "revenue_info": [
        # Revenue information
        ("證券代號", "TEXT", "NOT NULL"),
        ("投標開始日", "TEXT", "NOT NULL"),
        ("近一月營收", "REAL", ""),
        ("近一月營收年增率", "REAL", ""),
        ("近一月營收月增率", "REAL", ""),
        ("營收增長規律性_R2", "REAL", ""),
        ("營收風險波動率_cv", "REAL", ""),
        ("近五月成長次數比率", "REAL", ""),
    ],

    "target_variable": [
        # Target variable for prediction
        ("證券代號", "TEXT", "NOT NULL"),
        ("投標開始日", "TEXT", "NOT NULL"),
        ("撥券日期(上市、上櫃日期)", "TEXT", "NOT NULL"),
        ("成交量", "REAL", ""),
        ("成交金額", "REAL", ""),
        ("開盤價", "REAL", ""),
        ("最高價", "REAL", ""),
        ("最低價", "REAL", ""),
        ("收盤價", "REAL", ""),
        ("價差", "REAL", ""),
        ("成交筆數", "REAL", ""),
        ("預估獲利率", "REAL", ""),
        ("最低得標加價率", "REAL", ""),
        ("加權平均加價率", "REAL", ""),
    ],

    "all_features": [
        # 主鍵
        ("證券代號", "TEXT", "NOT NULL"),
        ("投標開始日", "TEXT", "NOT NULL"),
        ("撥券日期(上市、上櫃日期)", "TEXT", "NOT NULL"),

        # bid_info (競價拍賣基本資訊)
        ("證券名稱", "TEXT", "NOT NULL"),
        ("最低投標價格(元)", "REAL", ""),
        ("最高投(得)標數量(張)", "INTEGER", ""),

        # fin_stmts (財務報表特徵 - 基本面)
        ("營收成長率", "REAL", ""),
        ("本期淨利成長率", "REAL", ""),
        ("每股盈餘成長率", "REAL", ""),
        ("ROE", "REAL", ""),
        ("ROE成長率", "REAL", ""),
        ("ROA", "REAL", ""),
        ("ROA成長率", "REAL", ""),
        ("每股淨值", "REAL", ""),
        ("每股淨值成長率", "REAL", ""),
        ("負債比", "REAL", ""),
        ("負債比成長率", "REAL", ""),
        ("每股盈餘", "REAL", ""),

        # revenue_info (營收詳細資訊)
        ("近一月營收", "REAL", ""),
        ("近一月營收年增率", "REAL", ""),
        ("近一月營收月增率", "REAL", ""),
        ("營收增長規律性_R2", "REAL", ""),
        ("營收風險波動率_cv", "REAL", ""),
        ("近五月成長次數比率", "REAL", ""),

        # history_price_info (歷史價格與量能 - 技術面)
        ("前一日平均成交價", "REAL", ""),
        ("前十日內平均成交價", "REAL", ""),
        ("前十日內漲幅", "REAL", ""),
        ("前一日成交金額", "REAL", ""),
        ("前十日內平均成交金額", "REAL", ""),
        ("前一日最高成交價", "REAL", ""),
        ("前一日最低成交價", "REAL", ""),
        ("前一日成交筆數", "INTEGER", ""),
        ("前一日成交股數", "INTEGER", ""),
        ("前十日內平均成交筆數", "INTEGER", ""),
        ("前十日內平均成交股數", "INTEGER", ""),

        # all_market_info (市場籌碼與大盤環境)
        ("外資平均增減", "REAL", ""),
        ("投信平均增減", "REAL", ""),
        ("自營商平均增減", "REAL", ""),
        ("融資張數增減", "INTEGER", ""),
        ("融券張數增減", "INTEGER", ""),
        ("融資金額增減", "REAL", ""),
        ("道瓊工業_10日漲幅(%)", "REAL", ""),
        ("標普500_10日漲幅(%)", "REAL", ""),
        ("那斯達克_10日漲幅(%)", "REAL", ""),
        ("費城半導體_10日漲幅(%)", "REAL", ""),
        ("大盤_10日漲幅(%)", "REAL", ""),
        ("大盤_平均成交量", "REAL", ""),
        ("櫃買_10日漲幅(%)", "REAL", ""),
        ("櫃買_平均成交量", "REAL", ""),

        # target_variable (模型訓練目標變數)
        ("預估獲利率", "REAL", ""),
        ("最低得標加價率", "REAL", ""),
        ("加權平均加價率", "REAL", ""),

        ("status", "TEXT", ""), # no_list、all_complete
    ]
}


def get_table_schema(table_name: str) -> TableSchema:
    """
    Get the schema definition for a specific table.

    Args:
        table_name: Name of the table

    Returns:
        List of (column_name, sqlite_type, constraints) tuples

    Raises:
        KeyError: If table_name is not found in TABLE_SCHEMAS
    """
    if table_name not in TABLE_SCHEMAS:
        available_tables = list(TABLE_SCHEMAS.keys())
        raise KeyError(f"Table '{table_name}' not found. Available tables: {available_tables}")

    return TABLE_SCHEMAS[table_name]


def create_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL statement for a given table.

    Args:
        table_name: Name of the table

    Returns:
        SQL CREATE TABLE statement
    """
    schema = get_table_schema(table_name)

    columns_sql = []
    for col_name, col_type, constraints in schema:
        col_def = f"[{col_name}] {col_type}"
        if constraints:
            col_def += f" {constraints}"
        columns_sql.append(col_def)

    columns_str = ",\n    ".join(columns_sql)

    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_str}
        );
    """.strip()

    return sql