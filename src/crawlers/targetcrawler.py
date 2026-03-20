# -*- coding: utf-8 -*-
import sys
import pandas as pd
from pathlib import Path

# --- 路徑處理 ---
root_path = Path(__file__).resolve().parents[2]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.crawlers.base_crawler import BaseCrawler
from src.utils.config_loader import cfg
from src.utils.target_utils import get_target_value
from src.utils.target_utils import cal_y_feature
from src.utils.finmind_manager import FinMindManager

# 讀取配置
target_cfg = cfg["crawlers"]["target"]
FEATURE_COLS = target_cfg["feature_cols"]

class TargetCrawler(BaseCrawler):
    """
    目標變數爬蟲 (Target Variable Crawler)
    採用延遲載入 (Lazy-loading) 模式處理查找表。
    """

    def __init__(self):
        # 1. 呼叫父類別 __init__，初始化 self.dao
        super().__init__(table_name="target_variable")
        
        self.fm = FinMindManager()
        
        # 2. 初始化為 None，不立即讀取資料庫
        self.list_date_lookup = None
        self.raw_data_cache = None

    def _init_resources(self):
        """內部方法：實際執行資料庫讀取與查找表建立，僅執行一次。"""
        print(f">>> [{self.table_name}] 正在初始化 bid_info 查找資源...")
        source_df = self.dao.fetch_all("bid_info")
        
        if source_df.empty:
            print("⚠️ 警告：來源表 bid_info 為空。")
            self.list_date_lookup = {}
            self.raw_data_cache = pd.DataFrame()
            return

        # 格式化日期
        source_df['投標開始日'] = pd.to_datetime(source_df['投標開始日'])
        source_df['撥券日期(上市、上櫃日期)'] = pd.to_datetime(
            source_df['撥券日期(上市、上櫃日期)'], errors='coerce'
        )
        
        # 建立快取：(證券代號, 投標開始日) -> 撥券日期
        valid_df = source_df.dropna(subset=['證券代號', '投標開始日', '撥券日期(上市、上櫃日期)'])
        self.list_date_lookup = pd.Series(
            valid_df['撥券日期(上市、上櫃日期)'].values,
            index=pd.MultiIndex.from_frame(valid_df[["證券代號", "投標開始日"]])
        ).to_dict()
        
        self.raw_data_cache = source_df
        print(f"查找資源初始化完成，共 {len(self.list_date_lookup)} 筆對照。")

    def process_task(self, code: str, start_date: pd.Timestamp) -> tuple[bool, dict | str]:
        """
        執行單一任務抓取與計算。
        """
        # --- 延遲載入邏輯 ---
        # 只有在第一次進入 process_task 且 self.list_date_lookup 為 None 時才會執行
        if self.list_date_lookup is None:
            try:
                self._init_resources()
            except Exception as e:
                return False, f"資源初始化失敗: {e}"

        # 1. 獲取上市日期
        list_date = self.list_date_lookup.get((code, start_date))
        if pd.isna(list_date):
            return False, f"bid_info 遺失撥券日期"

        try:
            # 2. 抓取股價 (回傳 1-row DataFrame 或 None)
            y_df = get_target_value(
                api=self.fm.get_loader(),
                code=code,
                datetime=pd.Timestamp(list_date),
                feature_cols=FEATURE_COLS
            )
            self.fm.add_usage(1)

            if y_df is None or y_df.empty:
                return False, "API 查無資料"

            # 3. 執行特徵計算
            y_cal_df = cal_y_feature(y_df, self.raw_data_cache, code, start_date)
            
            # 4. 補上必要欄位
            y_cal_df['撥券日期(上市、上櫃日期)'] = list_date

            # 5. 轉換為字典並過濾 Key
            result_dict = y_cal_df.iloc[0].to_dict()
            final_dict = {k: v for k, v in result_dict.items() if k not in self.key_cols}
            
            return True, final_dict

        except Exception as e:
            return False, str(e)
