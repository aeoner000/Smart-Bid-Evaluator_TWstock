
import sys, time, random
from pathlib import Path
import pandas as pd
import requests

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

# --- 引入 ---
from src.crawlers.base_crawler import BaseCrawler # 引入 BaseCrawler
from src.utils.config_loader import cfg
from src.utils.revenue_utils import get_revenue_data, calculate_revenue_features

# --- 設定 ---
revenue_cfg = cfg["crawlers"]["revenue"]
HEADERS_LIST = revenue_cfg["headers"]
HEADERS = {k: v for d in HEADERS_LIST for k, v in d.items()}

class RevenueCrawler(BaseCrawler):
    def __init__(self):
        # 呼叫父類別的 __init__
        super().__init__(table_name="revenue_info")
        
        # 初始化 requests.Session (此爬蟲特有)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        try:
            # 取得 Cookie
            self.session.get("https://mops.twse.com.tw/mops/#/web/t05st10_ifrs", timeout=15)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ RevenueCrawler 初始化 session cookie 失敗: {e}")

    def process_task(self, code: str, start_date: pd.Timestamp) -> tuple[bool, dict | str]:
        """
        【實作】抓取連續 5 個月營收資料的邏輯。
        """
        try:
            # 計算基準月份 (若 10 號前則看前前月)
            y, m, d = start_date.year, start_date.month, start_date.day
            if d <= 10:
                m -= 1
                if m == 0: m, y = 12, y - 1

            # 抓取連續 5 個月
            monthly_results = []
            for i in range(5):
                total_m = (y * 12 + (m - 1)) - i
                cur_y, cur_m = total_m // 12, (total_m % 12) + 1
                
                # get_revenue_data 內部已包含重試機制
                this_m, last_y, yoy = get_revenue_data(code, cur_y, cur_m, self.session)
                
                if this_m is None:
                    # 只要有任何一個月失敗，整個任務就失敗
                    return False, f"Failed to fetch: {cur_y}/{cur_m}"
                
                monthly_results.append((this_m, yoy))
                time.sleep(random.uniform(1, 2))

            # 如果 5 個月都成功
            rev_list = [r[0] for r in monthly_results]
            latest_yoy = monthly_results[0][1]
            features = calculate_revenue_features(rev_list, latest_yoy)
            
            if features:
                return True, features
            else:
                return False, "Failed to calculate revenue features"

        except Exception as e:
            return False, str(e)

if __name__ == "__main__":
    crawler = RevenueCrawler()
    crawler.run() # 呼叫 BaseCrawler 的 run()
