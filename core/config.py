# backend/core/config.py

import os
import json
import random
from logzero import logger
from dotenv import load_dotenv

# Find the absolute path to the .env file to ensure it's always found
basedir = os.path.abspath(os.path.dirname(__file__))
dotenv_path = os.path.join(basedir, '..', '.env')
load_dotenv(dotenv_path=dotenv_path)


def load_scannable_stocks():
    """
    Loads the daily watchlist. If it doesn't exist, falls back to the full list.
    """
    try:
        with open('daily_watchlist.json', 'r') as f:
            token_map = json.load(f)
            logger.info(f"SUCCESS: Loaded {len(token_map)} stocks from daily_watchlist.json")
            return token_map
    except FileNotFoundError:
        logger.warning("daily_watchlist.json not found. Falling back to the full scannable_stocks.json list.")
        try:
            with open('scannable_stocks.json', 'r') as f:
                token_map = json.load(f)
                stock_list = list(token_map.items())
                random.shuffle(stock_list)
                limited_token_map = dict(stock_list[:100])
                logger.info(f"Loaded {len(limited_token_map)} random stocks from the full list.")
                return limited_token_map
        except FileNotFoundError:
            logger.error("scannable_stocks.json also not found! Please run instrument_downloader.py.")
            return {"2885": {"symbol": "RELIANCE-EQ", "bias": "Bullish"}}
    except Exception as e:
        logger.exception(f"Error loading stock list: {e}")
        return {}

class Settings:
    # --- Securely load credentials from Environment Variables ---
    API_KEY = os.getenv("API_KEY")
    CLIENT_CODE = os.getenv("CLIENT_CODE")
    CLIENT_PASSWORD = os.getenv("CLIENT_PASSWORD")
    TOTP_SECRET = os.getenv("TOTP_SECRET")

    TOKEN_MAP = load_scannable_stocks()
    
    INDEX_TOKENS = {
        "26000": "NIFTY 50",
        "26009": "BANK NIFTY"
    }
    
    @property
    def INSTRUMENT_TOKENS_TO_SCAN(self):
        stock_tokens = list(self.TOKEN_MAP.keys())
        index_tokens = list(self.INDEX_TOKENS.keys())
        return stock_tokens + index_tokens

settings = Settings()