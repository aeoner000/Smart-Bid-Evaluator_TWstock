import os
import time
import logging
from datetime import datetime, timedelta
from FinMind.data import DataLoader
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class FinMindManager:
    def __init__(self, max_calls_per_hour=595):
        """
        Manages FinMind API tokens, usage counting, and automatic switching.

        :param max_calls_per_hour: The safe usage limit for each token per hour (official limit is 600).
        """
        load_dotenv()
        api_tokens = [os.getenv("FINMIND"), os.getenv("FINMIND2")]
        self.tokens = [t for t in api_tokens if t]

        if not self.tokens:
            logger.error("No valid FinMind API Tokens found. Please check the .env file.")
            raise ValueError("No valid FinMind API Tokens found.")

        self.max_calls = max_calls_per_hour
        self.clients = []
        for i, token_str in enumerate(self.tokens):
            loader = DataLoader()
            loader.login_by_token(api_token=token_str)
            logger.info(f"FinMindManager: Logged in with Token {i + 1}")
            self.clients.append({"loader": loader, "usage": 0})

        self.current_idx = 0
        self.last_reset_hour = datetime.now().hour

    def _check_and_reset_usage(self):
        """Checks if the hour has changed and resets usage counts for all tokens if it has."""
        current_hour = datetime.now().hour
        if current_hour != self.last_reset_hour:
            logger.info("New hour detected. Resetting usage counts for all tokens.")
            for client in self.clients:
                client["usage"] = 0
            self.last_reset_hour = current_hour
            if self.current_idx != 0:
                self.current_idx = 0
                logger.info("Switching back to primary Token 1.")

    def _switch_token(self):
        """Switches to the next token. Returns False if all tokens are exhausted."""
        if self.current_idx < len(self.clients) - 1:
            self.current_idx += 1
            logger.info(f"Token quota exhausted. Automatically switching to Token {self.current_idx + 1}.")
            return True  # Switch successful
        else:
            return False # All tokens used, switch failed

    def _sleep_until_next_hour(self):
        """Calculates the wait time and sleeps until the next hour when all token quotas are exhausted."""
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0)
        wait_sec = (next_hour - now).total_seconds()
        logger.info(f"All token quotas have been reached. Sleeping until {next_hour.strftime('%H:%M:%S')}.")
        time.sleep(max(wait_sec, 1))
        self._check_and_reset_usage()

    def get_loader(self):
        """
        Retrieves the currently available DataLoader instance.
        This method automatically handles count resets, token switching, and sleeping.
        """
        self._check_and_reset_usage()

        while self.clients[self.current_idx]["usage"] >= self.max_calls:
            if not self._switch_token():
                self._sleep_until_next_hour()

        return self.clients[self.current_idx]["loader"]

    def add_usage(self, count=1):
        """Increments the usage count for the current token."""
        self.clients[self.current_idx]["usage"] += count
        usage = self.clients[self.current_idx]['usage']
        logger.debug(f"Token {self.current_idx + 1} usage: {usage}/{self.max_calls}")
