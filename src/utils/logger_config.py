import logging
import os
from logging.handlers import RotatingFileHandler

def init_logger(log_dir="logs", log_name="app.log"):
    """初始化全域 Logger 設定"""
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_name)

    # 建立一個基礎的配置
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            # RotatingFileHandler: 檔案滿 5MB 會自動換新的，保留 5 個舊檔，避免硬碟爆掉
            RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("Logging 系統已啟動，輸出至: %s", log_path)

# 這裡不呼叫 init_logger，交給入口點決定何時啟動