import time
import random
import requests
import numpy as np
import pandas as pd
from lxml import etree
from pathlib import Path
import sys

# --- 路徑補丁 ---
root_dir = Path(__file__).resolve().parent.parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# 引入自定義模組
from src.utils.financial import to_number, to_datetime
from src.utils.diff_index import search_index_list
from configs.financial_cfg import (
    URL_STMT, URL_DOC, HEADERS, FALLBACK_DICT, 
    NEW_COL
)

class FinancialCrawler:
    def __init__(self):
        self.save_folder = Path("./data/raw_table")
        self.raw_data_path = self.save_folder / "bid_info.csv"
        self.fina_stmt_path = self.save_folder / "fin_stmts.csv"
        self.save_folder.mkdir(parents=True, exist_ok=True)

    def make_xpath(self, label, col_index):
        return f"//tr[td[normalize-space(translate(., '　', ' '))='{label}']]/td[{col_index}]//text()"

    def search_year_season(self, code, date: pd.Timestamp):
        """查詢最接近投標日的財報季度"""
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
                # 判定合併或個別
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
        """抓取財報欄位數據"""
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
        """計算指標邏輯"""
        def growth(cur_col, pri_col):
            return np.where((pri_col != 0) & (pri_col.notna()), (cur_col - pri_col) / pri_col.abs(), 0)

        # --- 獲利效率 ---
        df["營收成長率"] = growth(df["營業收入"], df["前一期營業收入"]).round(dec)
        df["本期淨利成長率"] = growth(df["本期淨利"], df["前一期本期淨利"]).round(dec)
        df["每股盈餘成長率"] = growth(df["每股盈餘"], df["前一期每股盈餘"]).round(dec)
        df['ROE'] = (df['本期淨利'] / df['歸屬於母公司業主之權益合計']).round(dec)
        df['前一期ROE'] = (df['前一期本期淨利'] / df['前一期歸屬於母公司業主之權益合計']).round(dec)
        df['ROE成長率'] = growth(df['ROE'], df['前一期ROE']).round(dec)
        df['ROA'] = (df['本期淨利'] / df['資產總計']).round(dec)
        df['前一期ROA'] = (df['前一期本期淨利'] / df['前一期資產總計']).round(dec)
        df["ROA成長率"] = growth(df['ROA'], df['前一期ROA']).round(dec)

        # --- 價值指標 (處理你問的每股淨值) ---
        # 分子(千元*1000) / 分母(股數)
        df['每股淨值'] = ((df['歸屬於母公司業主之權益合計'] * 1000) / df['已發行股份總數']).round(dec)
        df['前一期每股淨值'] = ((df['前一期歸屬於母公司業主之權益合計'] * 1000) / df['前一期已發行股份總數']).round(dec)
        df['每股淨值成長率'] = growth(df['每股淨值'], df['前一期每股淨值']).round(dec)
        df["負債比"] = (df['負債總計'] / df['資產總計']).round(dec)
        df["前一期負債比"] = (df['前一期負債總計'] / df['前一期資產總計']).round(dec)
        df["負債比成長率"] = growth(df['負債比'], df['前一期負債比']).round(dec)

        return df

    def process_single_stock(self, code, start):
        """單一股票抓取流程 (你原本 main 裡的邏輯)"""
        try:
            search = self.search_year_season(code, start)
            if search is None: return False, "Search result is None"
            
            print(f"股號: {code}, 最接近季度: {search}")
            y, s, t = search[1].split('/s')[0], search[1].split('/s')[1], search[2]
            
            params = {
                "step": 1, "CO_ID": code, "SYEAR": y, "SSEASON": s,
                "REPORT_ID": "C" if t == "合併" else "A",
            }

            req = requests.get(URL_STMT, params=params, headers=HEADERS, timeout=15)
            req.encoding = req.apparent_encoding
            if req.encoding.lower() in ['iso-8859-1', 'ascii', 'windows-1252']:
                req.encoding = 'big5'
            
            if req.status_code != 200 or "查詢過量" in req.text:
                return False, "Rate Limited / Overload"

            tree = etree.HTML(req.text)
            report = self.get_report(tree)
            return (True, report) if report else (False, "Empty Report")
        except Exception as e:
            return False, str(e)
    def run(self):
        """主程式入口：改為動態新增 (Incremental Concat) 模式"""
        # 1. 取得現有資料與缺失索引
        fs_df, diff_index, _ = search_index_list(
            self.raw_data_path, self.fina_stmt_path, "證券代號", "投標開始日", NEW_COL
        )

        if diff_index is None:
            print("❌ 找不到來源檔 bid_info.csv")
            return
        
        if diff_index.empty:
            print("✅ 資料已是最新。")
            return

        # 這裡不預建 Full DataFrame，只準備儲存「新抓到成功」的列表
        successful_results = []
        fail = list(diff_index) # 將差異索引轉為待處理清單

        print(f">>> 準備處理 {len(fail)} 筆新資料...")

        try:
            # --- 兩輪嘗試邏輯 ---
            n = 0
            fail_time = 2 # 可以根據需求調整總輪數
            
            while fail and n < fail_time:
                n += 1
                current_round_list = fail[:] # 複製一份本輪要跑的
                print(f"\n--- 第 {n} 輪嘗試，剩餘 {len(current_round_list)} 筆 ---")
                
                for code, start in current_round_list:
                    print(f"處理股票代號 {code} ({start.date()})...", end=" ")
                    success, result = self.process_single_stock(code, start)

                    if success:
                        # 抓取成功：建立單筆 Dict 並加入成功清單
                        row_data = {"證券代號": code, "投標開始日": start}
                        row_data.update(result)
                        successful_results.append(row_data)
                        
                        # 從失敗清單移除
                        fail.remove((code, start))
                        print("成功")
                        time.sleep(random.uniform(3, 6))
                    else:
                        print(f"失敗: {result}")
                        time.sleep(2)

        except Exception as e:
            print(f"\n⚠️ 執行中斷: {e}")
        
        finally:
            # --- 存檔邏輯：僅處理成功抓到的部分 ---
            if successful_results:
                # 1. 將成功抓到的資料轉成 DataFrame
                new_data_df = pd.DataFrame(successful_results)
                
                # 2. 進行數值轉換與比率計算
                # 確保 NEW_COL 裡的欄位是數值
                cols_to_fix = [c for c in NEW_COL if c in new_data_df.columns]
                new_data_df[cols_to_fix] = new_data_df[cols_to_fix].apply(pd.to_numeric, errors='coerce')
                
                # 自動計算比率
                new_data_df = self.calculate_ratios(new_data_df)
                
                # 3. Concat 回原始資料
                final_df = pd.concat([fs_df, new_data_df], ignore_index=True)
                
                # 4. 存檔
                final_df.to_csv(self.fina_stmt_path, index=False, encoding="utf-8-sig")
                print(f"\n💾 已成功存檔 {len(successful_results)} 筆新資料。")
            else:
                print("\nℹ️ 沒有新抓到的成功資料，未更新檔案。")
                
            if fail:
                print(f"❌ 最終未完成筆數: {len(fail)}")

if __name__ == "__main__":
    crawler = FinancialCrawler()
    crawler.run()