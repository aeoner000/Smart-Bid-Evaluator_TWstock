import time
import random
import requests
import numpy as np
import pandas as pd
from lxml import etree
from pathlib import Path
import sys

# --- 路徑處理 ---
root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# --- 引入 ---
from src.crawlers.base_crawler import BaseCrawler # 引入 BaseCrawler
from src.utils.config_loader import cfg
from src.utils.financial_format_utils import to_number, to_datetime

# --- 設定 ---
financial_cfg = cfg["crawlers"]["financial"]
URL_STMT = financial_cfg["url_stmt"]
URL_DOC = financial_cfg["url_doc"]
HEADERS = financial_cfg["headers"]
FALLBACK_DICT = financial_cfg["fallback_dict"]
NUM_COLS = financial_cfg["num_cols"] # BaseCrawler 的 _save 會用到


class FinancialCrawler(BaseCrawler):
    def __init__(self):
        # 呼叫父類別的建構子，並傳入此爬蟲對應的 table_name
        super().__init__(table_name="fin_stmts")

    def make_xpath(self, label, col_index):
        """輔助函式：建立 XPath"""
        return f"//tr[td[normalize-space(translate(., '　', ' '))='{label}']]/td[{col_index}]//text()"

    def search_year_season(self, code, date: pd.Timestamp):
        """輔助函式：查詢最接近投標日的財報季度"""
        print(f"查詢股號: {code}, 基準日期: {date.date()}")
        try:
            if code is None or date is None: return None
            re_list = []
            for i in range(2):
                params = {"step": 1, "colorchg": 1, "co_id": code, "year": date.year - 1911 - i, "mtype": "A"}
                req = requests.get(URL_DOC, params=params, headers=HEADERS, timeout=15)
                req.encoding = 'big5'
                if "查詢過量" in req.text:
                    print("⚠️ 查詢過量，稍後重試...")
                    time.sleep(10)
                    continue
                
                tree = etree.HTML(req.text)
                for f_name, f_key in [("合併", "合併"), ("個別", "個別")]:
                    xp_d = f"//tr[td[contains(text(), 'IFRSs{f_key}財報')]]/td[10]/text()"
                    xp_s = f"//tr[td[contains(text(), 'IFRSs{f_key}財報')]]/td[2]/text()"
                    update_date, season = tree.xpath(xp_d), tree.xpath(xp_s)
                    if update_date:
                        date_l = to_datetime(update_date)
                        season_conv = [s.replace(' 年 ', '/s').replace('第一季', '1').replace('第二季', '2')
                                       .replace('第三季', '3').replace('第四季', '4').strip() for s in season]
                        re_list.extend([(d, s, f_name) for d, s in zip(date_l, season_conv)])
                        break
            
            valid_reports = [p for p in re_list if p[0] < date]
            return max(valid_reports, key=lambda x: x[0], default=None)
        except Exception as e:
            print(f"❌ search_year_season 出錯: {e}")
            return None

    def get_report(self, tree):
        """輔助函式：從網頁原始碼中抓取財報欄位數據"""
        report = {}
        for key, possible_labels in FALLBACK_DICT.items():
            found_data = False
            for label in possible_labels:
                val_curr_raw = tree.xpath(self.make_xpath(label, 2))
                if val_curr_raw:
                    a = 100 if key == "已發行股份總數" else 1
                    report[key] = round(to_number(val_curr_raw[0]) * a, 3)
                    val_prev_raw = tree.xpath(self.make_xpath(label, 3))
                    report[f"前一期{key}"] = round(to_number(val_prev_raw[0]) * a, 3) if val_prev_raw else None
                    found_data = True
                    break
            if not found_data:
                report[key] = report[f"前一期{key}"] = None
        return report

    def calculate_ratios(self, df, dec=3):
        """
        計算財務比率。
        這個方法會被 BaseCrawler 的 _save 方法自動呼叫。
        """
        def growth(cur_col, pri_col):
            return np.where((pri_col != 0) & (pri_col.notna()), (cur_col - pri_col) / pri_col.abs(), 0)

        df["營收成長率"] = growth(df["營業收入"], df["前一期營業收入"]).round(dec)
        df["本期淨利成長率"] = growth(df["本期淨利"], df["前一期本期淨利"]).round(dec)
        df["每股盈餘成長率"] = growth(df["每股盈餘"], df["前一期每股盈餘"]).round(dec)
        df['ROE'] = (df['本期淨利'] / df['歸屬於母公司業主之權益合計']).round(dec)
        df['前一期ROE'] = (df['前一期本期淨利'] / df['前一期歸屬於母公司業主之權益合計']).round(dec)
        df['ROE成長率'] = growth(df['ROE'], df['前一期ROE']).round(dec)
        df['ROA'] = (df['本期淨利'] / df['資產總計']).round(dec)
        df['前一期ROA'] = (df['前一期本期淨利'] / df['前一期資產總計']).round(dec)
        df["ROA成長率"] = growth(df['ROA'], df['前一期ROA']).round(dec)
        df['每股淨值'] = ((df['歸屬於母公司業主之權益合計'] * 1000) / df['已發行股份總數']).round(dec)
        df['前一期每股淨值'] = ((df['前一期歸屬於母公司業主之權益合計'] * 1000) / df['前一期已發行股份總數']).round(dec)
        df['每股淨值成長率'] = growth(df['每股淨值'], df['前一期每股淨值']).round(dec)
        df["負債比"] = (df['負債總計'] / df['資產總計']).round(dec)
        df["前一期負債比"] = (df['前一期負債總計'] / df['前一期資產總計']).round(dec)
        df["負債比成長率"] = growth(df['負債比'], df['前一期負債比']).round(dec)

        return df

    def process_task(self, code: str, start_date: pd.Timestamp) -> tuple[bool, dict | str]:
        """
        【實作 BaseCrawler 的抽象方法】
        定義單一股票財報的抓取邏輯。
        """
        try:
            search = self.search_year_season(code, start_date)
            if search is None: 
                return False, "Search result is None"
            
            print(f"股號: {code}, 最接近季度: {search}")
            y, s, t = search[1].split('/s')[0], search[1].split('/s')[1], search[2]
            
            params = {
                "step": 1, "CO_ID": code, "SYEAR": y, "SSEASON": s,
                "REPORT_ID": "C" if t == "合併" else "A",
            }

            req = requests.get(URL_STMT, params=params, headers=HEADERS, timeout=15)
            req.encoding = req.apparent_encoding
            current_enc = req.encoding.lower() if req.encoding else ""
            if current_enc in ['iso-8859-1', 'ascii', 'windows-1252']:
                req.encoding = 'big5'
            
            if req.status_code != 200 or "查詢過量" in req.text:
                return False, "Rate Limited / Overload"

            tree = etree.HTML(req.text)
            report = self.get_report(tree)
            
            # 檢查 report 是否為空或只包含 None
            if not report or all(value is None for value in report.values()):
                return False, "Empty Report"
            
            return True, report

        except Exception as e:
            return False, str(e)


if __name__ == "__main__":
    crawler = FinancialCrawler()
    crawler.run() # 呼叫的是 BaseCrawler 中定義好的 run()