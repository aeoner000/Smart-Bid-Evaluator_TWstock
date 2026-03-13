'''
資料清理及模型特徵表建立
'''
import sys, time, random
from pathlib import Path
import pandas as pd

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.utils.config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS
from src.db_base.db_manager import IPO_DAO

FE_cfg = cfg["feature_engineer"]
CLEAN_TABLE = FE_cfg["clean_table"]


class FeatureEngineer:
    """資料清理、模型特徵建立"""
    def __init__(self):
        self.dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        self.table_name = "all_features"
        self.dao.ensure_table_exists(self.table_name)


    def run(self):
    
        df = self.dao.combine_all_feature(CLEAN_TABLE) # 將爬蟲結果表並選定欄位合併
        print(df)
        return df




if __name__ == "__main__":
    fe = FeatureEngineer()
    df = fe.run()
    # df.to_csv("v1.csv", encoding="utf-8-sig")




