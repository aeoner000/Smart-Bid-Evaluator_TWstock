# pip freeze > requirements.txt
import time
from src.crawlers.AuctionCrawler import AuctionCrawler
from src.crawlers.FinancialCrawler import FinancialCrawler
from src.crawlers.MarketCrawler import MarketCrawler
from src.crawlers.PriceCrawler import PriceCrawler
from src.crawlers.RevenueCrawler import RevenueCrawler
from src.crawlers.TargetCrawler import TargetCrawler

if __name__ == "__main__":
    # 把它們放進清單，方便管理順序
    crawlers = [
        ("競拍", AuctionCrawler()), 
        ("財報", FinancialCrawler()),
        ("大盤", MarketCrawler()),
        ("股價", PriceCrawler()),
        ("營收", RevenueCrawler()),
        ("目標", TargetCrawler())
    ]
    start = time.time()
    for name, worker in crawlers:
        print(f"正在執行: {name} 資料抓取...")
        try:
            worker.run() # 這裡假設你的 class 裡有定義 def run(self):
            print(f"{name} 完成。")
            
        except Exception as e:
            print(f"{name} 錯誤！原因: {e}")
            continue
    end = time.time()
    print(f"結束，總花{end-start}秒")