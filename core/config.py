import os
import json
import random
from logzero import logger
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
dotenv_path = os.path.join(basedir, '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

def load_scannable_stocks():
    """
    Loads the daily watchlist. If it doesn't exist, falls back to a random
    selection from the full scannable_stocks.json list.
    Always returns a dict mapping token (as string) to stock info.
    """
    try:
        with open('daily_watchlist.json', 'r') as f:
            data = json.load(f)
            # If it's a dict of dicts, convert to token-keyed dict with token as string
            if isinstance(data, dict):
                token_map = {}
                for token, info in data.items():
                    # info is a dict, token is a string
                    token_map[str(token)] = {"token": str(token), **info}
                logger.info(f"SUCCESS: Loaded {len(token_map)} stocks from daily_watchlist.json (dict format)")
                return token_map
            # If it's a list of dicts, convert to token-keyed dict
            elif isinstance(data, list):
                token_map = {str(s["token"]): s for s in data if isinstance(s, dict) and "token" in s}
                logger.info(f"SUCCESS: Loaded {len(token_map)} stocks from daily_watchlist.json (list format)")
                return token_map
            else:
                logger.error("daily_watchlist.json is not in a recognized format.")
                return {}
    except FileNotFoundError:
        logger.warning("daily_watchlist.json not found. Falling back to the full scannable_stocks.json list.")
        try:
            with open('scannable_stocks.json', 'r') as f:
                data = json.load(f)
                # If it's a dict, convert to list of dicts
                if isinstance(data, dict):
                    stock_list = [{"token": str(token), "symbol": symbol, "bias": "Neutral"} for token, symbol in data.items()]
                elif isinstance(data, list):
                    stock_list = data
                else:
                    logger.error("scannable_stocks.json is not in a recognized format.")
                    return {}
                random.shuffle(stock_list)
                limited_list = stock_list[:20]
                token_map = {str(s["token"]): s for s in limited_list if isinstance(s, dict) and "token" in s}
                logger.info(f"Loaded {len(token_map)} random stocks from the full list.")
                return token_map
        except FileNotFoundError:
            logger.error("scannable_stocks.json also not found! Please run instrument_downloader.py.")
            return {"2885": {"symbol": "RELIANCE-EQ", "bias": "Bullish", "token": "2885"}}
    except Exception as e:
        logger.exception(f"Error loading stock list: {e}")
        return {}

class Settings:
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