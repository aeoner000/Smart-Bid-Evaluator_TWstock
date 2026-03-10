"""
工具模塊 - 包含配置載入、數據解析、市場行情等工具函數

各子模塊：
- config_loader: 加載 config.yaml 配置及數據庫設置
- parsing_utils: 通用解析工具（to_number, to_datetime）
- market: 大盤行情相關工具
- price: 股價相關工具
- revenue: 營收相關工具
- target: 目標變數計算工具
- finmind_manager: 統一管理 FinMind API
"""

from .config_loader import cfg, DB_PATH, DB_CONNECT_KWARGS, CSV_ENCODING, USER_AGENT
from .finmind_manager import FinMindManager

__all__ = [
    "cfg",
    "DB_PATH",
    "DB_CONNECT_KWARGS",
    "CSV_ENCODING",
    "USER_AGENT",
    "FinMindManager",
]
