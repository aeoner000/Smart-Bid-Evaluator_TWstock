
import sys, time, random
from pathlib import Path
import pandas as pd
import requests

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parents[2]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

# --- 引入 ---
from src.crawlers.base_crawler import BaseCrawler # 引入 BaseCrawler
from src.utils.config_loader import cfg
from src.utils.revenue_utils import get_revenue_data, calculate_revenue_features

# --- 設定 ---
revenue_cfg = cfg["crawlers"]["revenue"]
HEADERS = revenue_cfg["headers"]

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
            base_date = pd.Timestamp(start_date).normalize()

            # 核心邏輯：營收落後一個月，且 10 號前再多退一個月
            if base_date.day <= 10:
                # 10 號前：從「前前個月」開始抓 (退 2 個月)
                start_point = base_date - pd.DateOffset(months=2)
            else:
                # 10 號後：從「上個月」開始抓 (退 1 個月)
                start_point = base_date - pd.DateOffset(months=1)

            # 抓取連續 5 個月
            monthly_results = []
            for i in range(5):
                # 直接用 DateOffset 減去 i 個月，完全不用寫數學公式
                target_date = start_point - pd.DateOffset(months=i)
                
                cur_y = target_date.year
                cur_m = target_date.month

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
