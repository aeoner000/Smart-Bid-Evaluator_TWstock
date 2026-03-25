#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
負責協調整個資料科學流程，採用「兩站式流水線」模型進行爬蟲作業。

"""

# --- 標準函式庫導入 ---
import sys
import argparse
from pathlib import Path
import pandas as pd
import json
import logging

# --- Logger 設定 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 專案路徑設定 ---
root_dir = Path(__file__).resolve().parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# --- 本地應用程式導入 ---
from src.db_base.db_manager import get_db_manager
from src.utils.storage_handler import get_storage_handler

# 導入所有爬蟲工人
from src.crawlers import (
    TargetCrawler, 
    AuctionCrawler, 
    PriceCrawler, 
    MarketCrawler, 
    RevenueCrawler, 
    FinancialCrawler
)

# 導入特徵工程模組
from src.processors import FeatureEngineer

# 導入模型訓練與預測模組
from src.models.train_model.train import run_training_pipeline
from src.models.train_model.predict import Predictor

def should_trigger_training(threshold: int = 20) -> bool:
    """
    (決策層) 檢查新資料量是否達到觸發門檻。
    """
    logger.info("--- [階段 3: 模型訓練 (條件檢查)] ---")
    try:
        storage_handler = get_storage_handler()
        metadata = storage_handler.load_file('json/training_metadata.json')
        last_training_count = metadata.get('last_training_count', 0)
    except (FileNotFoundError, json.JSONDecodeError):
        last_training_count = 0
        logger.warning("訓練元數據檔案不存在或格式錯誤，將從 0 開始計算。")

    dao = get_db_manager()
    query_sql = f"SELECT `證券代號` FROM `{dao.project_id}.{dao.dataset_id}.bid_info` WHERE status = 'all_complete'"
    try:
        completed_df = dao.query(query_sql)
        current_completed_count = len(completed_df)
    except Exception as e:
        if 'not found' in str(e).lower():
            logger.warning(f"`bid_info` 資料表似乎不存在或查詢出錯，將資料數計為 0。錯誤: {e}")
            current_completed_count = 0
        else:
            raise e
            
    new_data_count = current_completed_count - last_training_count

    logger.info(f"上次訓練時資料數: {last_training_count}, 目前資料數: {current_completed_count}")
    logger.info(f"新增資料筆數: {new_data_count} / {threshold}")

    if new_data_count >= threshold:
        logger.info(f"新資料已累積 {new_data_count} 筆，達到門檻。")
        return True
    else:
        logger.info("新資料量未達訓練門檻，本次跳過模型訓練。")
        return False

# --- 階段 1: 爬蟲工作 (流水線模式) ---
def run_crawling_stage():
    """
    執行所有定義的爬蟲，採用「兩站式流水線」模型。
    """
    logger.info("--- [階段 1: 爬蟲工作 - 流水線模式] ---")
    dao = get_db_manager()

    # 0. 執行 AuctionCrawler，建立全新的待辦清單 (狀態歸零法)
    logger.info("步驟 0/3: 執行 AuctionCrawler 以建立全新的任務清單...")
    try:
        auction_worker = AuctionCrawler()
        auction_worker.run()

    except Exception as e:
        logger.error(f"AuctionCrawler 執行失敗，無法建立任務清單: {e}")
        return

    # =====================================================================
    # 第一站：特徵爬取 (Feature Crawling Station)
    # =====================================================================
    logger.info("--- 第一站：特徵爬取 --- pivotal_keys in ['證券代號', '投標開始日']")

    # 1. 搜尋需要爬取特徵的任務
    feature_query_sql = f"SELECT * FROM `{dao.project_id}.{dao.dataset_id}.bid_info` WHERE status = 'crawling'"
    feature_tasks_df = dao.query(feature_query_sql)

    if feature_tasks_df.empty:
        logger.info("目前沒有需要處理的新特徵任務。")
    else:
        feature_tasks_df['投標開始日'] = pd.to_datetime(feature_tasks_df['投標開始日']).dt.tz_localize(None)
        pivotal_keys = list(feature_tasks_df[['證券代號', '投標開始日']].itertuples(index=False, name=None))
        logger.info(f"任務鎖定成功！本次共需為 {len(pivotal_keys)} 個標的進行特徵補齊。")

        feature_workers = [ 
            FinancialCrawler(), PriceCrawler(),
            MarketCrawler(), RevenueCrawler(),
        ]

        try:
            # 2. 遍歷特徵工人，執行 清理 -> 爬取
            for worker in feature_workers:
                logger.info(f"--- 交由 `{worker.table_name}` 爬蟲處理 ---")
                
                # 修正: 加入一個內層 try-except 來處理 Table Not Found 的情況
                try:
                    dao.delete_by_keys(table_name=worker.table_name, keys_to_delete=pivotal_keys)
                except Exception as e:
                    if 'not found' in str(e).lower():
                        logger.info(f"資料表 `{worker.table_name}` 尚不存在，跳過刪除步驟 (此為首次執行的正常現象)。")
                    else:
                        # 如果是其他錯誤，則重新拋出，觸發回滾
                        raise e
                
                worker.run(diff_index=pivotal_keys)

            logger.info("所有特徵爬蟲均已成功完成本批次任務。")

            # 3. 狀態晉升：將完成特徵爬取的任務更新為 features_complete
            today = pd.Timestamp.today().normalize()
            keys_to_promote = feature_tasks_df[feature_tasks_df['投標開始日'] <= today]
            if not keys_to_promote.empty:
                promote_pivotal_keys = list(keys_to_promote[['證券代號', '投標開始日']].itertuples(index=False, name=None))
                dao.update_status_by_keys(table_name="bid_info", keys=promote_pivotal_keys, new_status="features_complete")
            else:
                logger.info("今日無任何任務達到投標日，無需晉升狀態。")

        except Exception as e:
            logger.error(f"第一站執行失敗: `{e}` !!!")
            logger.info("--- 偵測到錯誤，正在啟動資料回滾程序 ---")
            table_names_to_rollback = [worker.table_name for worker in feature_workers]
            try:
                for table in table_names_to_rollback:
                    # 在回滾中也同樣處理 Table Not Found 的情況
                    try:
                        dao.delete_by_keys(table_name=table, keys_to_delete=pivotal_keys)
                    except Exception as rollback_del_e:
                        if 'not found' in str(rollback_del_e).lower():
                            logger.info(f"(回滾中) 資料表 `{table}` 不存在，無需刪除。")
                        else:
                            raise rollback_del_e
                logger.info("--- 資料回滾成功 ---")
            except Exception as rollback_e:
                logger.critical(f"CRITICAL: 資料回滾程序本身也失敗了: {rollback_e} !!!")

    # =====================================================================
    # 第二站：目標爬取 (Target Crawling Station)
    # =====================================================================
    logger.info("--- 第二站：目標爬取 --- pivotal_keys in ['證券代號', '投標開始日']")

    # 4. 搜尋需要爬取目標的任務
    today_str = pd.Timestamp.today().normalize().strftime('%Y-%m-%d %H:%M:%S')
    target_sql = f"""SELECT * FROM `{dao.project_id}.{dao.dataset_id}.bid_info`
                      WHERE status = 'features_complete'
                      AND `撥券日期_上市_上櫃日期` IS NOT NULL
                      AND `撥券日期_上市_上櫃日期` <= '{today_str}'"""
    target_tasks_df = dao.query(target_sql)

    if target_tasks_df.empty:
        logger.info("目前沒有需要處理的目標任務。")
    else:
        target_tasks_df['投標開始日'] = pd.to_datetime(target_tasks_df['投標開始日']).dt.tz_localize(None)
        target_pivotal_keys = list(target_tasks_df[['證券代號', '投標開始日']].itertuples(index=False, name=None))
        logger.info(f"任務鎖定成功！本次共需為 {len(target_pivotal_keys)} 個標的抓取目標變數。")

        target_worker = TargetCrawler()

        try:
            # 5. 清理舊目標 & 執行爬取
            dao.delete_by_keys(table_name=target_worker.table_name, keys_to_delete=target_pivotal_keys)
            target_worker.run(diff_index=target_pivotal_keys)
            logger.info(f"{target_worker.table_name} 已成功完成本批次任務。")

            # 6. 狀態晉升：將完成目標爬取的任務更新為 all_complete
            dao.update_status_by_keys(table_name="bid_info", keys=target_pivotal_keys, new_status="all_complete")
        except KeyboardInterrupt:
            logger.warning("偵測到使用者手動中斷 (Ctrl+C)！正在緊急執行資料回滾...")
            # 這裡放你的回滾代碼，例如刪除已寫入的部分資料
            dao.delete_by_keys(table_name="target_variable", keys_to_delete=target_pivotal_keys)
            logger.info("--- 緊急回滾完成 ---")
            sys.exit(1) # 讓程式正式退出
        except Exception as e:
            logger.error(f"第二站執行失敗: `{e}` !!!")

    logger.info("--- [階段 1: 爬蟲工作] 執行完畢 ---")


# --- 階段 2: 資料處理 ---
def run_data_processing_stage():
    logger.info("--- [階段 2: 資料清理與特徵工程] ---")
    try:
        fe = FeatureEngineer()
        fe.run()
        logger.info("[階段 2: 資料清理與特徵工程] 執行完畢。")
    except Exception as e:
        logger.error(f"[階段 2: 資料清理與特徵工程] 失敗: {e}")
        raise 

# --- 階段 3: 模型訓練 ---
def run_model_training_stage():
    """(執行層) 執行模型訓練流程並傳遞必要參數。"""
    logger.info("--- [階段 3: 模型訓練 (執行)] ---")
    try:
        dao = get_db_manager()
        query_sql = f"SELECT `證券代號` FROM `{dao.project_id}.{dao.dataset_id}.bid_info` WHERE status = 'all_complete'"
        try:
            current_completed_count = len(dao.query(query_sql))
        except Exception as e:
            if 'not found' in str(e).lower():
                current_completed_count = 0
            else:
                raise e
        
        logger.info(f"啟動訓練流程，使用目前 {current_completed_count} 筆已完成資料...")
        
        run_training_pipeline(new_total_count=current_completed_count)
        
        logger.info("[階段 3: 模型訓練] 執行完畢。")
    except Exception as e:
        logger.error(f"[階段 3: 模型訓練] 失敗: {e}")
        raise

# --- 階段 4: 產生預測 ---
def run_prediction_stage():
    logger.info("--- [階段 4: 產生預測] ---")
    try:
        predictor = Predictor()
        predictor.run()
        logger.info("[階段 4: 產生預測] 執行完畢。")
    except Exception as e:
        logger.error(f"[階段 4: 產生預測] 失敗: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="專案總指揮官，管理資料科學流程。")
    parser.add_argument(
        'stages',
        nargs='*',
        default=["crawl", "process", "train", "predict"], 
        help="要執行的階段。可選: crawl, process, train, predict。若不指定，則依序執行所有階段。"
    )
    args = parser.parse_args()
    logger.info(f"--- 準備執行階段: {args.stages} ---")

    try:
        dao = get_db_manager()
        storage = get_storage_handler()

        if 'crawl' in args.stages:
            run_crawling_stage()
        if 'process' in args.stages:
            run_data_processing_stage()
        if 'train' in args.stages:
            is_manual_train = (len(args.stages) == 1 and args.stages[0] == 'train')
            if is_manual_train:
                logger.info("偵測到手動訓練指令，將強制執行訓練。")
                run_model_training_stage()
            elif should_trigger_training():
                run_model_training_stage()
        if 'predict' in args.stages:
            run_prediction_stage()
        
        logger.info("所有指定階段均已成功執行完畢！")

    except Exception as e:
        logger.critical(f"主流程因未處理的錯誤而中斷: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # --- 資源清理邏輯，僅增加在最後 ---
        logger.info("--- 正在清理系統連線資源 ---")
        try:
            if dao and hasattr(dao, 'client'):
                dao.client.close()
                logger.info("✅ BigQuery 連線已釋放。")
        except Exception as e:
            logger.warning(f"釋放 BigQuery 資源時出錯: {e}")

        try:
            if storage and hasattr(storage, 'client'):
                storage.client.close()
                logger.info("✅ GCS 連線已釋放。")
        except Exception as e:
            logger.warning(f"釋放 GCS 資源時出錯: {e}")
if __name__ == "__main__":
    main()