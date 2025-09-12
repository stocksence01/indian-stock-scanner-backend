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
    Returns a dict mapping token (as string) to stock info.
    """
    try:
        with open('daily_watchlist.json', 'r') as f:
            stock_list = json.load(f)
            token_map = {str(s["token"]): s for s in stock_list}
            logger.info(f"SUCCESS: Loaded {len(token_map)} stocks from daily_watchlist.json")
            return token_map
    except FileNotFoundError:
        logger.warning("daily_watchlist.json not found. Falling back to the full scannable_stocks.json list.")
        try:
            with open('scannable_stocks.json', 'r') as f:
                stock_list = json.load(f)
                random.shuffle(stock_list)
                limited_list = stock_list[:100]
                token_map = {str(s["token"]): s for s in limited_list}
                logger.info(f"Loaded {len(token_map)} random stocks from the full list.")
                return token_map
        except FileNotFoundError:
            logger.error("scannable_stocks.json also not found! Please run instrument_downloader.py.")
            return {"2885": {"symbol": "RELIANCE-EQ", "bias": "Bullish", "token": "2885"}}
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