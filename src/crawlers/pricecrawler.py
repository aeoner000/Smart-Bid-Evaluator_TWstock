
import sys
from pathlib import Path
import pandas as pd

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

# --- 引入 ---
from src.crawlers.base_crawler import BaseCrawler # 引入 BaseCrawler
from src.utils.config_loader import cfg
from src.utils.price_utils import get_price_table, data_output

# --- 設定 ---
price_cfg = cfg["crawlers"]["price"]
HEADERS = price_cfg["headers"]

class PriceCrawler(BaseCrawler): # 繼承 BaseCrawler
    def __init__(self):
        # 呼叫父類別的 __init__ 並傳入自己的 table_name
        super().__init__(table_name="history_price_info")

    def process_task(self, code: str, start_date: pd.Timestamp) -> tuple[bool, dict | str]:
        """
        實作單一價格資料的抓取與計算邏輯。
        """
        try:
            # 這就是原本在 for 迴圈裡的邏輯
            df = get_price_table(code, start_date.year, start_date.month, HEADERS)
            re = data_output(df, start_date)
            
            if re:
                # 成功，回傳結果字典
                return True, re 
            else:
                # 失敗，回傳原因
                return False, "無法獲取足夠行情資料" 

        except Exception as e:
            # 任何抓取或處理過程中的意外都視為失敗
            return False, str(e)

if __name__ == "__main__":
    crawler = PriceCrawler()
    crawler.run() # 呼叫的是 BaseCrawler 中定義好的 run()
