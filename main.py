#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
專案總指揮官 (main.py) - v8 (流水線模式)

負責協調整個資料科學流程，採用「兩站式流水線」模型進行爬蟲作業。
職責清晰，流程穩健。

"""

# --- 標準函式庫導入 ---
import sys
import argparse
from pathlib import Path
import pandas as pd
import json
# --- 專案路徑設定 ---
root_dir = Path(__file__).resolve().parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# --- 本地應用程式導入 ---
from src.db_base.db_manager import IPO_DAO
from src.utils.config_loader import DB_PATH, DB_CONNECT_KWARGS

# 導入所有爬蟲工人
from src.crawlers import (
    TargetCrawler, 
    AuctionCrawler, 
    PriceCrawler, 
    MarketCrawler, 
    RevenueCrawler, 
    FinancialCrawler
)

# 導入特徵工程模組 (此處路徑依您現有設定)
from src.processors import FeatureEngineer

# 【新導入】導入模型訓練與預測模組
from src.models.train_model.train import run_training_pipeline
from src.models.train_model.predict import Predictor

def should_trigger_training(threshold: int = 20) -> bool:
    """
    (決策層) 檢查新資料量是否達到觸發門檻。
    
    回傳:
        bool: True 表示應觸發訓練，False 則否。
    """
    print("\n--- [階段 3: 模型訓練 (條件檢查)] ---")
    try:
        with open('json/training_metadata.json', 'r') as f:
            metadata = json.load(f)
        last_training_count = metadata.get('last_training_count', 0)
    except (FileNotFoundError, json.JSONDecodeError):
        last_training_count = 0
        print("⚠️ 訓練元數據檔案不存在或格式錯誤，將從 0 開始計算。")

    dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
    completed_df = dao.query("SELECT `證券代號` FROM bid_info WHERE status = 'all_complete'")
    current_completed_count = len(completed_df)
    new_data_count = current_completed_count - last_training_count

    print(f"上次訓練時資料數: {last_training_count}, 目前資料數: {current_completed_count}")
    print(f"新增資料筆數: {new_data_count} / {threshold}")

    if new_data_count >= threshold:
        print(f"✅ 新資料已累積 {new_data_count} 筆，達到門檻。")
        return True
    else:
        print("🔵 新資料量未達訓練門檻，本次跳過模型訓練。")
        return False
# --- 階段 1: 爬蟲工作 (流水線模式) ---
def run_crawling_stage():
    """
    執行所有定義的爬蟲，採用「兩站式流水線」模型。
    """
    print("\n--- [階段 1: 爬蟲工作 - 流水線模式] ---")
    dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)

    # 0. 執行 AuctionCrawler，建立全新的待辦清單 (狀態歸零法)
    print("步驟 0/3: 執行 AuctionCrawler 以建立全新的任務清單...")
    try:
        auction_worker = AuctionCrawler()
        auction_worker.run()
    except Exception as e:
        print(f"❌ AuctionCrawler 執行失敗，無法建立任務清單: {e}")
        return

    # =====================================================================
    # 第一站：特徵爬取 (Feature Crawling Station)
    # =====================================================================
    print("\n--- 第一站：特徵爬取 --- pivotal_keys in ['證券代號', '投標開始日']")

    # 1. 搜尋需要爬取特徵的任務
    feature_tasks_df = dao.query("SELECT * FROM bid_info WHERE status = 'crawling'")

    if feature_tasks_df.empty:
        print("🔵 目前沒有需要處理的新特徵任務。")
    else:
        feature_tasks_df['投標開始日'] = pd.to_datetime(feature_tasks_df['投標開始日'])
        pivotal_keys = list(feature_tasks_df[['證券代號', '投標開始日']].itertuples(index=False, name=None))
        print(f"🟢 任務鎖定成功！本次共需為 {len(pivotal_keys)} 個標的進行特徵補齊。")

        feature_workers = [ 
            FinancialCrawler(), PriceCrawler(),
            MarketCrawler(), RevenueCrawler(),
        ]

        try:
            # 2. 遍歷特徵工人，執行 清理 -> 爬取
            for worker in feature_workers:
                print(f"\n--- 交由 `{worker.table_name}` 爬蟲處理 ---")
                dao.delete_by_keys(table_name=worker.table_name, keys_to_delete=pivotal_keys)
                worker.run(diff_index=pivotal_keys)

            print("\n✅ 所有特徵爬蟲均已成功完成本批次任務。")

            # 3. 狀態晉升：將完成特徵爬取的任務更新為 features_complete
            today = pd.Timestamp.today().normalize()
            keys_to_promote = feature_tasks_df[feature_tasks_df['投標開始日'] <= today]
            if not keys_to_promote.empty:
                promote_pivotal_keys = list(keys_to_promote[['證券代號', '投標開始日']].itertuples(index=False, name=None))
                dao.revert_task_status_by_keys(keys=promote_pivotal_keys, new_status="features_complete")
            else:
                print("今日無任何任務達到投標日，無需晉升狀態。")

        except Exception as e:
            print(f"\n❌ 第一站執行失敗: `{e}` !!!")
            print("--- 偵測到錯誤，正在啟動資料回滾程序 ---")
            table_names_to_rollback = [worker.table_name for worker in feature_workers]
            try:
                for table in table_names_to_rollback:
                    dao.delete_by_keys(table_name=table, keys_to_delete=pivotal_keys)
                print("--- 資料回滾成功 ---")
            except Exception as rollback_e:
                print(f"❌ CRITICAL: 資料回滾程序本身也失敗了: {rollback_e} !!!")

    # =====================================================================
    # 第二站：目標爬取 (Target Crawling Station)
    # =====================================================================
    print("\n--- 第二站：目標爬取 --- pivotal_keys in ['證券代號', '投標開始日']")

    # 4. 搜尋需要爬取目標的任務
    today_str = pd.Timestamp.today().normalize().strftime('%Y-%m-%d %H:%M:%S')
    target_sql = f"""SELECT * FROM bid_info 
                      WHERE status = 'features_complete' 
                      AND `撥券日期(上市、上櫃日期)` IS NOT NULL
                      AND `撥券日期(上市、上櫃日期)` <= '{today_str}'"""
    target_tasks_df = dao.query(target_sql)

    if target_tasks_df.empty:
        print("🔵 目前沒有需要處理的目標任務。")
    else:
        target_tasks_df['投標開始日'] = pd.to_datetime(target_tasks_df['投標開始日'])
        target_pivotal_keys = list(target_tasks_df[['證券代號', '投標開始日']].itertuples(index=False, name=None))
        print(f"🟢 任務鎖定成功！本次共需為 {len(target_pivotal_keys)} 個標的抓取目標變數。")

        target_worker = TargetCrawler()

        try:
            # 5. 清理舊目標 & 執行爬取
            dao.delete_by_keys(table_name=target_worker.table_name, keys_to_delete=target_pivotal_keys)
            target_worker.run(diff_index=target_pivotal_keys)
            print(f"\n✅ {target_worker.table_name} 已成功完成本批次任務。")

            # 6. 狀態晉升：將完成目標爬取的任務更新為 all_complete
            dao.revert_task_status_by_keys(keys=target_pivotal_keys, new_status="all_complete")

        except Exception as e:
            print(f"\n❌ 第二站執行失敗: `{e}` !!!")

    print("\n--- [階段 1: 爬蟲工作] 執行完畢 ---")


# --- 階段 2: 資料處理 ---
def run_data_processing_stage():
    print("\n--- [階段 2: 資料清理與特徵工程] ---")
    # 此階段的實作保持不變
    try:
        fe = FeatureEngineer()
        fe.run()
        print("✅ [階段 2: 資料清理與特徵工程] 執行完畢。")
    except Exception as e:
        print(f"❌ [階段 2: 資料清理與特徵工程] 失敗: {e}")
        raise # 拋出錯誤，讓主流程決定是否中斷


# --- 階段 3: 模型訓練 ---
def run_model_training_stage():
    """(執行層 - Corrected) 執行模型訓練流程並傳遞必要參數。"""
    print("\n--- [階段 3: 模型訓練 (執行)] ---")
    try:
        # 新增 1: 建立資料庫連線，並查詢目前的總筆數
        dao = IPO_DAO(DB_PATH, **DB_CONNECT_KWARGS)
        current_completed_count = len(dao.query("SELECT `證券代號` FROM bid_info WHERE status = 'all_complete'"))
        
        print(f"🟢 啟動訓練流程，使用目前 {current_completed_count} 筆已完成資料...")
        
        # 新增 2: 將查到的總筆數，當作參數傳遞下去
        run_training_pipeline(new_total_count=current_completed_count)
        
        print("✅ [階段 3: 模型訓練] 執行完畢。")
    except Exception as e:
        print(f"❌ [階段 3: 模型訓練] 失敗: {e}")
        raise


# --- 階段 4: 產生預測 ---
def run_prediction_stage():
    print("\n--- [階段 4: 產生預測] ---")
    # 【注入新程式碼】
    try:
        predictor = Predictor() # 呼叫 predict.py 中的類別
        predictor.run()
        print("✅ [階段 4: 產生預測] 執行完畢。")
    except Exception as e:
        print(f"❌ [階段 4: 產生預測] 失敗: {e}")
        raise


def main():
    # 此 main 函式的 argparse 邏輯保持不變
    parser = argparse.ArgumentParser(description="專案總指揮官，管理資料科學流程。")
    parser.add_argument(
        'stages',
        nargs='*',
        default=["crawl", "process", "train", "predict"], # 預設執行所有階段
        help="要執行的階段。可選: crawl, process, train, predict。若不指定，則依序執行所有階段。"
    )
    args = parser.parse_args()
    print(f"--- 準備執行階段: {args.stages} ---")

    # 使用 try...except 包裹，確保任何階段出錯都能被捕獲
    try:
        if 'crawl' in args.stages:
            run_crawling_stage()
        if 'process' in args.stages:
            run_data_processing_stage()
        if 'train' in args.stages:
            is_manual_train = (len(args.stages) == 1 and args.stages[0] == 'train')
            # 簡潔的決策邏輯
            if is_manual_train:
                print("\n💡 偵測到手動訓練指令，將強制執行訓練。")
                run_model_training_stage()
            elif should_trigger_training():
                run_model_training_stage()
        if 'predict' in args.stages:
            run_prediction_stage()
        
        print("\n🎉🎉🎉 所有指定階段均已成功執行完畢！ 🎉🎉🎉")

    except Exception as e:
        print(f"\n💥 主流程因未處理的錯誤而中斷: {e}")
        print("請檢查上方日誌以了解詳細錯誤原因。")


if __name__ == "__main__":
    main()
