import os
import time
from datetime import datetime, timedelta
from FinMind.data import DataLoader
from dotenv import load_dotenv

class FinMindManager:
    def __init__(self, max_calls_per_hour=595):
        """
        統一管理 FinMind API Tokens、使用量計數與自動切換。

        :param max_calls_per_hour: 每個 Token 每小時的安全使用上限 (官方為 600)。
        """
        load_dotenv()
        api_tokens = [os.getenv("FINMIND"), os.getenv("FINMIND2")]
        self.tokens = [t for t in api_tokens if t]

        if not self.tokens:
            raise ValueError("❌ 找不到任何有效的 FinMind API Token，請檢查 .env 檔案。")

        self.max_calls = max_calls_per_hour
        self.clients = []
        for i, token_str in enumerate(self.tokens):
            loader = DataLoader()
            loader.login_by_token(api_token=token_str)
            print(f"🔑 FinMindManager: 已登入 Token {i + 1}")
            self.clients.append({"loader": loader, "usage": 0})

        self.current_idx = 0
        self.last_reset_hour = datetime.now().hour

    def _check_and_reset_usage(self):
        """檢查是否跨小時，若是則重置所有 token 的使用計數。"""
        current_hour = datetime.now().hour
        if current_hour != self.last_reset_hour:
            print(f"\n🕒 新的一小時，重置所有 Token 使用計數。")
            for client in self.clients:
                client["usage"] = 0
            self.last_reset_hour = current_hour
            # 重置後，一律從第一個 token 開始
            if self.current_idx != 0:
                self.current_idx = 0
                print("🔄 切換回主要 Token 1。")

    def _switch_token(self):
        """切換到下一個 Token。如果所有 Token 都已用盡，返回 False。"""
        if self.current_idx < len(self.clients) - 1:
            self.current_idx += 1
            print(f"\n💡 Token 額度耗盡，自動切換至 Token {self.current_idx + 1}。")
            return True  # 切換成功
        else:
            return False # 所有 token 都用完了，切換失敗

    def _sleep_until_next_hour(self):
        """當所有 Token 額度用盡時，計算等待時間並休眠。"""
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0)
        wait_sec = (next_hour - now).total_seconds()
        print(f"\n😴 所有 Token 額度皆已達上限，將休眠至 {next_hour.strftime('%H:%M:%S')}...")
        time.sleep(max(wait_sec, 1))
        # 睡醒後重置計數
        self._check_and_reset_usage()

    def get_loader(self):
        """
        獲取當前可用的 DataLoader 實例。
        此方法會自動處理計數重置、Token 切換和休眠。
        """
        self._check_and_reset_usage()

        while self.clients[self.current_idx]["usage"] >= self.max_calls:
            if not self._switch_token():
                self._sleep_until_next_hour()

        return self.clients[self.current_idx]["loader"]

    def add_usage(self, count=1):
        """為當前的 Token 增加使用次數。"""
        self.clients[self.current_idx]["usage"] += count
        usage = self.clients[self.current_idx]['usage']
        print(f"(Token {self.current_idx + 1} 用量: {usage}/{self.max_calls})", end=" ")