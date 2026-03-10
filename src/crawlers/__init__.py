"""
爬蟲模塊 - 負責數據爬取與初步處理

各爬蟲數據流：
1. AuctionCrawler: 爬取興櫃競拍信息 → bid_info 表
2. FinancialCrawler: 爬取財務報表 → fin_stmts 表
3. MarketCrawler: 爬取大盤行情 → all_market_info 表
4. PriceCrawler: 爬取歷史股價 → history_price_info 表
5. RevenueCrawler: 爬取營收信息 → revenue_info 表
6. TargetCrawler: 計算目標變數 → target_variable 表
"""

from .AuctionCrawler import AuctionCrawler
from .FinancialCrawler import FinancialCrawler
from .MarketCrawler import MarketCrawler
from .PriceCrawler import PriceCrawler
from .RevenueCrawler import RevenueCrawler
from .TargetCrawler import TargetCrawler

__all__ = [
    "AuctionCrawler",
    "FinancialCrawler",
    "MarketCrawler",
    "PriceCrawler",
    "RevenueCrawler",
    "TargetCrawler",
]
