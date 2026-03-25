import requests
import pandas as pd
import numpy as np
from lxml import etree
import time, random
import logging

logger = logging.getLogger(__name__)

def get_revenue_data(code, y, m, session):
    """從 MOPS 抓取單月營收"""
    if y > 1911: 
        y -= 1911
    m = str(m).zfill(2)
    url = "https://mopsov.twse.com.tw/mops/web/ajax_t05st10_ifrs"
    params = {
        "encodeURIComponent": "1", "step": "1", "firstin": "1",
        "off": "1", "isnew": "false", "TYPEK": "all",
        "co_id": code, "year": y, "month": m
    }

    for i in range(3): # 失敗重試 3 次
        try:
            resp = session.post(url, data=params, timeout=10)
            if resp.status_code == 200:
                html = etree.HTML(resp.text)
                target = html.xpath('//table[@class="hasBorder"]')
                if not target: return None, None, None

                # 解析邏輯 (依據標籤結構判斷)
                raw_this = html.xpath("//table[@class='hasBorder']//tr[2]/td/text()")
                if raw_this and raw_this[0].strip() == "新台幣":
                    r_this = html.xpath("//table[@class='hasBorder']//tr[3]/td[2]/text()")
                    r_last = html.xpath("//table[@class='hasBorder']//tr[4]/td[2]/text()")
                    r_yoy = html.xpath("//table[@class='hasBorder']//tr[6]/td[2]/text()")
                else:
                    r_this = raw_this
                    r_last = html.xpath("//table[@class='hasBorder']//tr[3]/td/text()")
                    r_yoy = html.xpath("//table[@class='hasBorder']//tr[5]/td/text()")

                # 清理與轉換
                def clean(v): return float(v[0].strip().replace(',', '')) if v else None
                
                this_m = clean(r_this)
                last_y = clean(r_last)
                yoy = round(clean(r_yoy) / 100, 3) if r_yoy else None
                return this_m, last_y, yoy
            time.sleep(2)
        except Exception as e:
            logger.error(f"網路異常: {e}", exc_info=True)
            time.sleep(5)
    return None, None, None

def calculate_revenue_features(rev_list, yoy):
    """計算 R2, CV, Monotonicity 等指標"""
    try:
        if None in rev_list or len(rev_list) < 2:
            return {k: None for k in ["近一月營收","近一月營收年增率","近一月營收月增率","營收增長規律性_R2","營收風險波動率_cv","近五月成長次數比率"]}
        
        rev_array = np.array(rev_list)[::-1] # 轉為舊到新
        n = len(rev_array)
        
        mom = round((rev_array[-1] - rev_array[-2]) / abs(rev_array[-2]), 3)
        cv = round(np.std(rev_array) / abs(np.mean(rev_array)), 3) if np.mean(rev_array) != 0 else 0
        consistency = round(np.sum(np.diff(rev_array) > 0) / (n - 1), 3)

        if np.any(rev_array <= 0):
            regularity = 0.0
        else:
            log_y = np.log(rev_array)
            x = np.arange(n)
            regularity = round(np.corrcoef(x, log_y)[0, 1]**2, 3)

        return {
            "近一月營收": float(rev_array[-1]),
            "近一月營收年增率": float(yoy),
            "近一月營收月增率": float(mom),
            "營收增長規律性_R2": float(regularity),
            "營收風險波動率_cv": float(cv),
            "近五月成長次數比率": float(consistency)
        }
    except Exception as e: 
        logger.error(f"計算 revenue features 時發生錯誤: {e}", exc_info=True)
        return None