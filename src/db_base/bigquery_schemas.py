
"""
BigQuery 專用 Schema 定義庫 - v12 (外科手術修正版)

遵照最終、最嚴格指令：
1.  **Schema 定義區塊**：已全面替換，以 100% 精準對應 `schemas.py` 的表格與欄位名稱。
2.  **函式邏輯區塊**：`get_table_schema` 與 `create_table_sql` 函式，維持其原始 v7 版本的邏輯，一個字元都未改動。
"""

from google.cloud import bigquery
from typing import Dict, List, Optional

# ======================================================================================
# BEGIN SCHEMA DEFINITION - 絕對與 src/db_base/schemas.py 對齊
# ======================================================================================

BID_INFO_SCHEMA = [
    bigquery.SchemaField("開標日期", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("證券名稱", "STRING"),
    bigquery.SchemaField("證券代號", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("發行市場", "STRING"),
    bigquery.SchemaField("發行性質", "STRING"),
    bigquery.SchemaField("競拍方式", "STRING"),
    bigquery.SchemaField("投標開始日", "TIMESTAMP"),
    bigquery.SchemaField("投標結束日", "TIMESTAMP"),
    bigquery.SchemaField("競拍數量_張", "INT64"),
    bigquery.SchemaField("最低投標價格_元", "FLOAT64"),
    bigquery.SchemaField("最低每標單投標數量_張", "INT64"),
    bigquery.SchemaField("最高投_得_標數量_張", "INT64"),
    bigquery.SchemaField("保證金成數_百分比", "INT64"),
    bigquery.SchemaField("每一投標單投標處理費_元", "INT64"),
    bigquery.SchemaField("撥券日期_上市_上櫃日期", "TIMESTAMP"),
    bigquery.SchemaField("主辦券商", "STRING"),
    bigquery.SchemaField("得標總金額_元", "FLOAT64"),
    bigquery.SchemaField("得標手續費率_百分比", "FLOAT64"),
    bigquery.SchemaField("總合格件", "INT64"),
    bigquery.SchemaField("合格投標數量_張", "INT64"),
    bigquery.SchemaField("最低得標價格_元", "FLOAT64"),
    bigquery.SchemaField("最高得標價格_元", "FLOAT64"),
    bigquery.SchemaField("得標加權平均價格_元", "FLOAT64"),
    bigquery.SchemaField("承銷價格_元", "FLOAT64"),
    bigquery.SchemaField("取消競價拍賣_流標或取消", "STRING"),
    bigquery.SchemaField("update_time", "TIMESTAMP"),
    bigquery.SchemaField("status", "STRING"),
]

FIN_STMTS_SCHEMA = [
    bigquery.SchemaField("證券代號", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("投標開始日", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("營業收入", "FLOAT64"),
    bigquery.SchemaField("前一期營業收入", "FLOAT64"),
    bigquery.SchemaField("本期淨利", "FLOAT64"),
    bigquery.SchemaField("前一期本期淨利", "FLOAT64"),
    bigquery.SchemaField("每股盈餘", "FLOAT64"),
    bigquery.SchemaField("前一期每股盈餘", "FLOAT64"),
    bigquery.SchemaField("歸屬於母公司業主之權益合計", "FLOAT64"),
    bigquery.SchemaField("前一期歸屬於母公司業主之權益合計", "FLOAT64"),
    bigquery.SchemaField("已發行股份總數", "FLOAT64"),
    bigquery.SchemaField("前一期已發行股份總數", "FLOAT64"),
    bigquery.SchemaField("負債總計", "FLOAT64"),
    bigquery.SchemaField("前一期負債總計", "FLOAT64"),
    bigquery.SchemaField("資產總計", "FLOAT64"),
    bigquery.SchemaField("前一期資產總計", "FLOAT64"),
    bigquery.SchemaField("營收成長率", "FLOAT64"),
    bigquery.SchemaField("本期淨利成長率", "FLOAT64"),
    bigquery.SchemaField("每股盈餘成長率", "FLOAT64"),
    bigquery.SchemaField("ROE", "FLOAT64"),
    bigquery.SchemaField("前一期ROE", "FLOAT64"),
    bigquery.SchemaField("ROE成長率", "FLOAT64"),
    bigquery.SchemaField("ROA", "FLOAT64"),
    bigquery.SchemaField("前一期ROA", "FLOAT64"),
    bigquery.SchemaField("ROA成長率", "FLOAT64"),
    bigquery.SchemaField("每股淨值", "FLOAT64"),
    bigquery.SchemaField("前一期每股淨值", "FLOAT64"),
    bigquery.SchemaField("每股淨值成長率", "FLOAT64"),
    bigquery.SchemaField("負債比", "FLOAT64"),
    bigquery.SchemaField("前一期負債比", "FLOAT64"),
    bigquery.SchemaField("負債比成長率", "FLOAT64"),
]

ALL_MARKET_INFO_SCHEMA = [
    bigquery.SchemaField("證券代號", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("投標開始日", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("外資平均增減", "FLOAT64"),
    bigquery.SchemaField("投信平均增減", "FLOAT64"),
    bigquery.SchemaField("自營商平均增減", "FLOAT64"),
    bigquery.SchemaField("融資張數增減", "FLOAT64"),
    bigquery.SchemaField("融券張數增減", "FLOAT64"),
    bigquery.SchemaField("融資金額增減", "FLOAT64"),
    bigquery.SchemaField("道瓊工業_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("標普500_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("那斯達克_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("費城半導體_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("大盤_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("大盤_平均成交量", "FLOAT64"),
    bigquery.SchemaField("櫃買_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("櫃買_平均成交量", "FLOAT64"),
]

HISTORY_PRICE_INFO_SCHEMA = [
    bigquery.SchemaField("證券代號", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("投標開始日", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("前一日平均成交價", "FLOAT64"),
    bigquery.SchemaField("前十日內平均成交價", "FLOAT64"),
    bigquery.SchemaField("前十日內漲幅", "FLOAT64"),
    bigquery.SchemaField("前一日成交金額", "FLOAT64"),
    bigquery.SchemaField("前十日內平均成交金額", "FLOAT64"),
    bigquery.SchemaField("前一日最高成交價", "FLOAT64"),
    bigquery.SchemaField("前一日最低成交價", "FLOAT64"),
    bigquery.SchemaField("前一日成交筆數", "FLOAT64"),
    bigquery.SchemaField("前一日成交股數", "FLOAT64"),
    bigquery.SchemaField("前十日內平均成交筆數", "FLOAT64"),
    bigquery.SchemaField("前十日內平均成交股數", "FLOAT64"),
]

REVENUE_INFO_SCHEMA = [
    bigquery.SchemaField("證券代號", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("投標開始日", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("近一月營收", "FLOAT64"),
    bigquery.SchemaField("近一月營收年增率", "FLOAT64"),
    bigquery.SchemaField("近一月營收月增率", "FLOAT64"),
    bigquery.SchemaField("營收增長規律性_R2", "FLOAT64"),
    bigquery.SchemaField("營收風險波動率_cv", "FLOAT64"),
    bigquery.SchemaField("近五月成長次數比率", "FLOAT64"),
]

TARGET_VARIABLE_SCHEMA = [
    bigquery.SchemaField("證券代號", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("投標開始日", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("撥券日期_上市_上櫃日期", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("成交量", "FLOAT64"),
    bigquery.SchemaField("成交金額", "FLOAT64"),
    bigquery.SchemaField("開盤價", "FLOAT64"),
    bigquery.SchemaField("最高價", "FLOAT64"),
    bigquery.SchemaField("最低價", "FLOAT64"),
    bigquery.SchemaField("收盤價", "FLOAT64"),
    bigquery.SchemaField("價差", "FLOAT64"),
    bigquery.SchemaField("成交筆數", "FLOAT64"),
    bigquery.SchemaField("預估獲利率", "FLOAT64"),
    bigquery.SchemaField("最低得標加價率", "FLOAT64"),
    bigquery.SchemaField("加權平均加價率", "FLOAT64"),
]

ALL_FEATURES_SCHEMA = [
    bigquery.SchemaField("證券代號", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("投標開始日", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("撥券日期_上市_上櫃日期", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("證券名稱", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("最低投標價格_元", "FLOAT64"),
    bigquery.SchemaField("最高投_得_標數量_張", "INT64"),
    bigquery.SchemaField("營收成長率", "FLOAT64"),
    bigquery.SchemaField("本期淨利成長率", "FLOAT64"),
    bigquery.SchemaField("每股盈餘成長率", "FLOAT64"),
    bigquery.SchemaField("ROE", "FLOAT64"),
    bigquery.SchemaField("ROE成長率", "FLOAT64"),
    bigquery.SchemaField("ROA", "FLOAT64"),
    bigquery.SchemaField("ROA成長率", "FLOAT64"),
    bigquery.SchemaField("每股淨值", "FLOAT64"),
    bigquery.SchemaField("每股淨值成長率", "FLOAT64"),
    bigquery.SchemaField("負債比", "FLOAT64"),
    bigquery.SchemaField("負債比成長率", "FLOAT64"),
    bigquery.SchemaField("每股盈餘", "FLOAT64"),
    bigquery.SchemaField("近一月營收", "FLOAT64"),
    bigquery.SchemaField("近一月營收年增率", "FLOAT64"),
    bigquery.SchemaField("近一月營收月增率", "FLOAT64"),
    bigquery.SchemaField("營收增長規律性_R2", "FLOAT64"),
    bigquery.SchemaField("營收風險波動率_cv", "FLOAT64"),
    bigquery.SchemaField("近五月成長次數比率", "FLOAT64"),
    bigquery.SchemaField("前一日平均成交價", "FLOAT64"),
    bigquery.SchemaField("前十日內平均成交價", "FLOAT64"),
    bigquery.SchemaField("前十日內漲幅", "FLOAT64"),
    bigquery.SchemaField("前一日成交金額", "FLOAT64"),
    bigquery.SchemaField("前十日內平均成交金額", "FLOAT64"),
    bigquery.SchemaField("前一日最高成交價", "FLOAT64"),
    bigquery.SchemaField("前一日最低成交價", "FLOAT64"),
    bigquery.SchemaField("前一日成交筆數", "FLOAT64"),
    bigquery.SchemaField("前一日成交股數", "FLOAT64"),
    bigquery.SchemaField("前十日內平均成交筆數", "FLOAT64"),
    bigquery.SchemaField("前十日內平均成交股數", "FLOAT64"),
    bigquery.SchemaField("外資平均增減", "FLOAT64"),
    bigquery.SchemaField("投信平均增減", "FLOAT64"),
    bigquery.SchemaField("自營商平均增減", "FLOAT64"),
    bigquery.SchemaField("融資張數增減", "FLOAT64"),
    bigquery.SchemaField("融券張數增減", "FLOAT64"),
    bigquery.SchemaField("融資金額增減", "FLOAT64"),
    bigquery.SchemaField("道瓊工業_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("標普500_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("那斯達克_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("費城半導體_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("大盤_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("大盤_平均成交量", "FLOAT64"),
    bigquery.SchemaField("櫃買_10日漲幅_百分比", "FLOAT64"),
    bigquery.SchemaField("櫃買_平均成交量", "FLOAT64"),
    bigquery.SchemaField("fin_stmts_missing", "INT64"),
    bigquery.SchemaField("revenue_info_missing", "INT64"),
    bigquery.SchemaField("history_price_info_missing", "INT64"),
    bigquery.SchemaField("is_ky", "INT64"),
    bigquery.SchemaField("is_Q4", "INT64"),
    bigquery.SchemaField("days_to_listing", "INT64"),
    bigquery.SchemaField("融資比", "FLOAT64"),
    bigquery.SchemaField("法人比", "FLOAT64"),
    bigquery.SchemaField("Market_Heat", "FLOAT64"),
    bigquery.SchemaField("ROE_Quality", "FLOAT64"),
    bigquery.SchemaField("Curr_Gap_Pct", "FLOAT64"),
    bigquery.SchemaField("預估獲利率", "FLOAT64"),
    bigquery.SchemaField("最低得標加價率", "FLOAT64"),
    bigquery.SchemaField("加權平均加價率", "FLOAT64"),
    bigquery.SchemaField("status", "STRING"),
]

TABLE_SCHEMAS = {
    "bid_info": BID_INFO_SCHEMA,
    "fin_stmts": FIN_STMTS_SCHEMA,
    "all_market_info": ALL_MARKET_INFO_SCHEMA,
    "history_price_info": HISTORY_PRICE_INFO_SCHEMA,
    "revenue_info": REVENUE_INFO_SCHEMA,
    "target_variable": TARGET_VARIABLE_SCHEMA,
    "all_features": ALL_FEATURES_SCHEMA,
}

# ======================================================================================
# END SCHEMA DEFINITION
# ======================================================================================


def get_table_schema(table_name: str) -> Optional[List[bigquery.SchemaField]]:
    return TABLE_SCHEMAS.get(table_name)

def create_table_sql(table_name: str, project_id: str, dataset_id: str) -> Optional[str]:
    schema = get_table_schema(table_name)
    if not schema:
        return None

    # 1. 自動偵測分區欄位
    field_names = [f.name for f in schema]
    partition_col = next((col for col in ["投標開始日", "日期", "申購開始日"] if col in field_names), None)

    # 2. 定義 Table ID 與 欄位 (確保每個欄位前有固定 2 個空格縮排)
    table_id = f"`{project_id}`.`{dataset_id}`.`{table_name}`"
    columns_list = []
    for field in schema:
        col_def = f"  `{field.name}` {field.field_type}"
        if field.mode == "REQUIRED":
            col_def += " NOT NULL"
        columns_list.append(col_def)
    columns_str = ",\n".join(columns_list)

    # 3. 準備分區與叢集子句 (確保換行對齊)
    partition_clause = f"\nPARTITION BY TIMESTAMP_TRUNC(`{partition_col}`, MONTH)" if partition_col else ""
    cluster_clause = f"\nCLUSTER BY `證券代號`" if "證券代號" in field_names else ""

    # 4. 使用對齊結構組合 SQL
    # 使用 f-string 並確保括號與內容層級分明
    sql = (
        f"CREATE TABLE IF NOT EXISTS {table_id} (\n"
        f"{columns_str}\n"
        f"){partition_clause}{cluster_clause};"
    )

    return sql
