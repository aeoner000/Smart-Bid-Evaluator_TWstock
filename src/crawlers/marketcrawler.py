import sys, time, random
from pathlib import Path
import pandas as pd

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

# --- 引入 ---
from src.crawlers.base_crawler import BaseCrawler # 引入 BaseCrawler
from src.utils.config_loader import cfg
from src.utils.market_utils import (
    get_market_Inst_tw, get_margin, get_market_usa, 
    get_market_tw
)
from src.utils.finmind_manager import FinMindManager

# --- 設定 ---
market_cfg = cfg["crawlers"]["market"]
FEATURE_COLS = market_cfg["feature_cols"]
USA_TICKERS = market_cfg["usa_tickers"]
USA_NAME_MAP = market_cfg["usa_name_map"]


class MarketCrawler(BaseCrawler):
    def __init__(self):
        # 呼叫父類別的 __init__ 並傳入自己的 table_name
        super().__init__(table_name="all_market_info")
        # 初始化 FinMind API 管理器 (這是此爬蟲特有的)
        self.fm = FinMindManager()

    def _process_data_internal(self, dl, code, target_time):
        """
        輔助函式：保持原有的核心資料抓取與合併邏輯。
        """
        tw_indices = get_market_Inst_tw(dl, target_time, 10)
        tw_margin = get_margin(dl, target_time, 10)
        usa_market = get_market_usa(target_time, USA_TICKERS, USA_NAME_MAP, 10)
        tw_market = get_market_tw(target_time, 10)
        
        # 移除 '證券代號' 和 '投標開始日'，因為 BaseCrawler 會自動處理
        key_info = pd.Series() 
        combined_series = pd.concat([key_info, tw_indices, tw_margin, usa_market, tw_market])
        
        # 檢查是否所有值都為 None 或 NaN
        if combined_series.isnull().all():
            return None
        
        return combined_series.to_dict()

    def process_task(self, code: str, start_date: pd.Timestamp) -> tuple[bool, dict | str]:
        """
        【實作 BaseCrawler 的抽象方法】
        定義單一市場資料的抓取邏輯，包含與 FinMindManager 的互動。
        """
        try:
            # 1. 從管理器獲取可用的 loader，同時token管理都在這裡
            loader = self.fm.get_loader()
            
            # 2. 執行抓取
            result_dict = self._process_data_internal(loader, code, start_date)
            
            # 3. 如果抓取結果為空，視為失敗
            if result_dict is None:
                return False, "All market data fields are null"

            # 4. 增加使用計數 (此爬蟲一次消耗 2 個 API call)
            self.fm.add_usage(2)
            
            return True, result_dict

        except Exception as e:
            # 任何錯誤，都回傳失敗與原因
            # 例如：FinMindManager('No available loaders')
            return False, str(e)


if __name__ == "__main__":    
    crawler = MarketCrawler()
    crawler.run()