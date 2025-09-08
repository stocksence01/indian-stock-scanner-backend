# backend/premarket_scanner.py

import json
import time
from datetime import datetime, timedelta
import pandas as pd
import ta
from logzero import logger

from services.smartapi_service import smartapi_service

def fetch_historical_data(smart_api, token):
    """Fetches daily data for a stock. Includes a retry mechanism for rate limiting."""
    retries = 3
    delay = 2
    for i in range(retries):
        try:
            to_date = datetime.now()
            from_date = to_date - timedelta(days=60) 
            historic_param = {
                "exchange": "NSE", "symboltoken": token, "interval": "ONE_DAY",
                "fromdate": from_date.strftime("%Y-%m-%d %H:%M"),
                "todate": to_date.strftime("%Y-%m-%d %H:%M")
            }
            data = smart_api.getCandleData(historic_param)
            if data.get("message") and "exceeding access rate" in data["message"]:
                logger.warning(f"Rate limit hit for {token}. Retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2
                continue
            if data.get("status") and data.get("data"):
                df = pd.DataFrame(data["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                return df.iloc[-30:] 
            else:
                return None
        except Exception as e:
            logger.error(f"An exception occurred for {token}: {e}. Retrying...")
            time.sleep(delay)
            delay *= 2
    logger.error(f"Failed to fetch data for token {token} after {retries} retries.")
    return None

def analyze_stock(df):
    """Analyzes a DataFrame to calculate scores and volatility."""
    if df is None or len(df) < 27:
        return None, None
    df['atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], window=14)
    last_day_for_atr = df.iloc[-1]
    atr_percentage = (last_day_for_atr['atr'] / last_day_for_atr['close']) * 100 if last_day_for_atr['close'] > 0 else 0
    if atr_percentage < 1.5:
        return None, None
    df['rsi'] = ta.momentum.rsi(df['close'], window=14)
    macd = ta.trend.MACD(df['close'])
    df['macd'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
    last_day = df.iloc[-1]
    prev_day = df.iloc[-2]
    bull_score, bear_score = 0, 0
    if last_day['rsi'] > 60: bull_score += 50
    if prev_day['macd'] < prev_day['macd_signal'] and last_day['macd'] > last_day['macd_signal']: bull_score += 50
    if last_day['rsi'] < 40: bear_score += 50
    if prev_day['macd'] > prev_day['macd_signal'] and last_day['macd'] < last_day['macd_signal']: bear_score += 50
    return bull_score, bear_score

def create_daily_watchlist():
    """Analyzes all stocks using a two-pass system and saves the top 50/50."""
    logger.info("Starting unified Bullish/Bearish pre-market watchlist creation...")
    if not smartapi_service.login():
        logger.error("Could not log in to SmartAPI. Aborting.")
        return
    try:
        with open('scannable_stocks.json', 'r') as f:
            full_stock_list = json.load(f)
    except FileNotFoundError:
        logger.error("scannable_stocks.json not found.")
        return

    bullish_stocks, bearish_stocks, failed_stocks = [], [], []
    
    logger.info(f"--- Starting First Pass: Analyzing {len(full_stock_list)} stocks ---")
    for i, (token, symbol) in enumerate(full_stock_list.items()):
        logger.info(f"Analyzing [{i+1}/{len(full_stock_list)}]: {symbol}")
        df = fetch_historical_data(smartapi_service.smart_api, token)
        bull_score, bear_score = analyze_stock(df)
        if bull_score is None:
            failed_stocks.append((token, symbol))
        else:
            if bull_score > 0: bullish_stocks.append({"token": token, "symbol": symbol, "score": bull_score})
            if bear_score > 0: bearish_stocks.append({"token": token, "symbol": symbol, "score": bear_score})
        time.sleep(0.3)

    if failed_stocks:
        logger.info(f"\n--- Starting Second Pass: Retrying {len(failed_stocks)} failed stocks ---")
        for i, (token, symbol) in enumerate(failed_stocks):
            logger.info(f"Retrying [{i+1}/{len(failed_stocks)}]: {symbol}")
            df = fetch_historical_data(smartapi_service.smart_api, token)
            bull_score, bear_score = analyze_stock(df)
            if bull_score is not None:
                if bull_score > 0: bullish_stocks.append({"token": token, "symbol": symbol, "score": bull_score})
                if bear_score > 0: bearish_stocks.append({"token": token, "symbol": symbol, "score": bear_score})
            else:
                logger.error(f"Failed to retrieve data for {symbol} on second attempt. Skipping permanently.")
            time.sleep(0.3)

    logger.info(f"Found {len(bullish_stocks)} total bullish candidates and {len(bearish_stocks)} total bearish candidates.")
    
    bullish_stocks.sort(key=lambda x: x['score'], reverse=True)
    bearish_stocks.sort(key=lambda x: x['score'], reverse=True)
    
    top_50_bullish = bullish_stocks[:50]
    top_50_bearish = bearish_stocks[:50]
    logger.info(f"Taking the top {len(top_50_bullish)} bullish and {len(top_50_bearish)} bearish stocks.")

    final_watchlist = {}
    for stock in top_50_bullish:
        final_watchlist[stock['token']] = {"symbol": stock['symbol'], "bias": "Bullish"}
    for stock in top_50_bearish:
        final_watchlist[stock['token']] = {"symbol": stock['symbol'], "bias": "Bearish"}

    logger.info(f"Final watchlist will contain {len(final_watchlist)} unique stocks.")

    output_filename = 'daily_watchlist.json'
    with open(output_filename, 'w') as f:
        json.dump(final_watchlist, f, indent=2)
        
    logger.info(f"\nSUCCESS! Saved {len(final_watchlist)} stocks to {output_filename}.")

if __name__ == "__main__":
    create_daily_watchlist()