import argparse
import sys
import time
from pathlib import Path

# 強制將專案根目錄加入路徑，確保執行時能找到 src
root_path = Path(__file__).resolve().parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.crawlers.AuctionCrawler import AuctionCrawler
from src.crawlers.FinancialCrawler import FinancialCrawler
from src.crawlers.MarketCrawler import MarketCrawler
from src.crawlers.PriceCrawler import PriceCrawler
from src.crawlers.RevenueCrawler import RevenueCrawler
from src.crawlers.TargetCrawler import TargetCrawler

def main():
    # 設定參數解析，方便單獨測試某個爬蟲
    # 使用方式: python main.py --task price
    parser = argparse.ArgumentParser(description="IPO Data Crawler Manager") # 定義參數解析器
    parser.add_argument(
        "--task", 
        type=str, 
        default="all", 
        help="指定要執行的爬蟲: auction, financial, market, price, revenue, target, all (預設)"
    ) # 解析參數，預設為 "all" 執行全部爬蟲
    args = parser.parse_args() # 解析參數

    # 定義爬蟲清單 (使用 lambda 延遲實例化，節省資源)
    crawlers_map = {
        "auction":   ("競拍基本資料", lambda: AuctionCrawler()), # lambda 用於延遲實例化，只有在執行到該爬蟲時才會創建對象
        "financial": ("財報資料",     lambda: FinancialCrawler()),
        "market":    ("大盤/籌碼資料", lambda: MarketCrawler()),
        "price":     ("興櫃股價資料", lambda: PriceCrawler()),
        "revenue":   ("歷史營收資料", lambda: RevenueCrawler()),
        "target":    ("目標變數",     lambda: TargetCrawler()),
    }

    # 定義執行順序 (Auction 必須第一)
    execution_order = ["auction", "financial", "market", "price", "revenue", "target"]

    # 決定要跑哪些任務
    if args.task == "all":
        tasks = execution_order
    elif args.task in crawlers_map:
        tasks = [args.task]
    else:
        print(f" 錯誤：找不到任務 '{args.task}'")
        print(f"ℹ 可用任務：{', '.join(execution_order)}")
        return

    print(f"🚀 開始執行流程，預計執行：{tasks}")
    start_time = time.time()

    for key in tasks:
        name, factory = crawlers_map[key]  # 從映射中獲取爬蟲名稱和函數
        print(f"\n{'='*50}")
        print(f"▶  正在執行 [{key}] {name} ...")
        print(f"{'='*50}")

        try:
            # 實例化並執行
            crawler = factory() # 只有在這裡才會創建爬蟲對象，節省資源
            crawler.run()       # 執行爬蟲的主邏輯
            print(f"✅ [{key}] 執行完成")
        except Exception as e:
            print(f"❌ [{key}] 執行失敗: {e}")
            # 如果是 auction 失敗，後續依賴它的爬蟲可能也會有問題，但這裡選擇繼續嘗試

    total_time = time.time() - start_time
    print(f"\n 流程結束，總耗時: {total_time:.2f} 秒")

if __name__ == "__main__":
    main()