from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "config.yaml"


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"config.yaml not found at {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


cfg = _load_config()

# convenience exports (same values as configs/db_cfg.py used to expose)
_database = cfg.get("database", {})
_paths = cfg.get("paths", {})
_common = cfg.get("common", {})

# 正式資料庫-->_database.get("db_path"-->優先由yaml抓路徑，否則用下方 "," 之後的路徑
DB_PATH = Path(_database.get("db_path", "data/database/database.sqlite3"))
# 測試資料庫
TEST_DB_PATH = Path(_database.get("test_db_path", "data/database/database_test.sqlite3"))

DATABASE_DIR = DB_PATH.parent
DB_CONNECT_KWARGS = _database.get("connect", {})

RAW_TABLE_DIR = Path(_paths.get("raw_table_dir", "data/raw_table"))

CSV_ENCODING = _common.get("csv_encoding", "utf-8-sig")
USER_AGENT = _common.get("user_agent")

SKEW_PATH = Path(_paths.get("skew_path", "src/models/saved_weights/skew_transformer.joblib"))
SELECT_FEATURE_PATH = Path(_paths.get("select_feature", "src/models/saved_weights/selected_features.joblib"))
 
SAVE_DIR = Path(_paths.get("weights_dir", "src/models/saved_weights"))
