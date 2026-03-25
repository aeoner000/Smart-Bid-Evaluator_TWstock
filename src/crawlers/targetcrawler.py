
# -*- coding: utf-8 -*-
import sys
import pandas as pd
from pathlib import Path
import logging
import json

# --- 日誌標準化 ---
logger = logging.getLogger(__name__)

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parents[2]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.crawlers.base_crawler import BaseCrawler
from src.utils.config_loader import config
from src.utils.target_utils import get_target_value, cal_y_feature
from src.utils.finmind_manager import FinMindManager

# 讀取配置
target_cfg = config["crawlers"]["target"]
FEATURE_COLS = target_cfg["feature_cols"]

class TargetCrawler(BaseCrawler):
    """
    目標變數爬蟲 (Target Variable Crawler)
    採用延遲載入 (Lazy-loading) 模式處理查找表。
    """

    def __init__(self):
        super().__init__(table_name="target_variable")
        self.fm = FinMindManager()
        self.list_date_lookup = None
        self.raw_data_cache = None

    def _init_resources(self):
        """內部方法：實際執行資料庫讀取與查找表建立，僅執行一次。"""
        logger.info(f"Initializing bid_info lookup resources for {self.table_name}...")
        source_df = self.dao.fetch_all("bid_info")
        
        if source_df.empty:
            logger.warning("Source table bid_info is empty.")
            self.list_date_lookup = {}
            self.raw_data_cache = pd.DataFrame()
            return

        # 修正：使用標準化的欄位名稱
        source_df['投標開始日'] = pd.to_datetime(source_df['投標開始日']).dt.tz_localize(None)
        source_df['撥券日期_上市_上櫃日期'] = pd.to_datetime(source_df['撥券日期_上市_上櫃日期'], 
                                                        errors='coerce').dt.tz_localize(None)

        # 修正：使用標準化的欄位名稱
        valid_df = source_df.dropna(subset=['證券代號', '投標開始日', '撥券日期_上市_上櫃日期'])
        self.list_date_lookup = pd.Series(
            valid_df['撥券日期_上市_上櫃日期'].values,
            index=pd.MultiIndex.from_frame(valid_df[["證券代號", "投標開始日"]])
        ).to_dict()

        self.raw_data_cache = source_df
        logger.info(f"Lookup resources initialized with {len(self.list_date_lookup)} entries.")

    def process_task(self, code: str, start_date: pd.Timestamp) -> tuple[bool, dict | str]:
        """
        執行單一任務抓取與計算。
        """
        if self.list_date_lookup is None:
            try:
                self._init_resources()
            except Exception as e:
                return False, f"資源初始化失敗: {e}"

        list_date = self.list_date_lookup.get((code, start_date))

        if pd.isna(list_date):
            return False, f"bid_info 遺失撥券日期"

        try:
            y_df = get_target_value(
                api=self.fm.get_loader(),
                code=code,
                datetime=pd.Timestamp(list_date),
                feature_cols=FEATURE_COLS
            )
            self.fm.add_usage(1)

            if y_df is None or y_df.empty:
                return False, "API 查無資料"

            y_cal_df = cal_y_feature(y_df, self.raw_data_cache, code, start_date)
            
            # 修正：使用標準化的欄位名稱
            y_cal_df['撥券日期_上市_上櫃日期'] = list_date

            result_dict = y_cal_df.iloc[0].to_dict()
            final_dict = {k: v for k, v in result_dict.items() if k not in self.key_cols}
            
            return True, final_dict

        except Exception as e:
            return False, str(e)

if __name__ == "__main__":
    # Set up basic logging for standalone script execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )

    # 1. 初始化爬蟲
    crawler = TargetCrawler()
    
    # 2. 定義測試任務 (請確保這些資料在你的 BigQuery bid_info 中存在且 status='features_complete')
    # 範例：測試「及其自動 (7795)」
    test_code = "7795"
    test_date = pd.Timestamp("2025-12-31") # 替換為該檔實際的投標開始日
    
    logger.info("Starting TargetCrawler test...")
    logger.info(f"Testing target: {test_code}, Bid Date: {test_date}")

    # 方式 A：直接測試單一任務邏輯 (不影響資料庫狀態)
    success, result = crawler.process_task(test_code, test_date)
    
    if success:
        logger.info("Fetch and calculation successful!")
        logger.info("Result summary:")
        # 使用 json.dumps 讓字典印出來比較漂亮 (處理日期格式需轉字串)
        logger.info(json.dumps(result, indent=4, ensure_ascii=False, default=str))
    else:
        logger.error(f"Execution failed: {result}")

    # 方式 B：如果你想測試完整的自動化流程 (會觸發比對、抓取並寫入資料庫)
    # 注意：這會真實更動到你的 target_variable 資料表
    # crawler.run(diff_index=[(test_code, test_date)])
