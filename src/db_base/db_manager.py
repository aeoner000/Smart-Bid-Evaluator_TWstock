from src.utils.config_loader import config
from src.db_base.sqlite_dao import IPO_DAO_SQLite
from src.db_base.bigquery_dao import IPO_DAO_BigQuery
import logging
# 加入這兩行定義 logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_manager():
    """
    Factory function that reads the configuration and returns the appropriate
    Database Access Object (DAO).

    Returns:
        An instance of IPO_DAO_SQLite or IPO_DAO_BigQuery.
    """
    db_type = config.get('database', {}).get('type', 'sqlite')

    if db_type == 'bigquery':
        logger.info("Database mode: BigQuery")
        return IPO_DAO_BigQuery()
    
    elif db_type == 'sqlite':
        logger.info("Database mode: SQLite")
        return IPO_DAO_SQLite()
        
    else:
        raise ValueError(f"Unsupported database type: {db_type}. Please check your config.yaml.")

