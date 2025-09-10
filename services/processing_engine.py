import pandas as pd
import ta
from logzero import logger
import asyncio
from datetime import time, datetime

from services.smartapi_service import smartapi_service
from services.websocket_client import websocket_client
from core.config import settings

class ProcessingEngine:
    def __init__(self):
        self.data_store = {}
        self.scan_results = {}
        self.opening_ranges = {}
        self.index_data = {}
        logger.info("Processing Engine initialized with all features.")

    def calculate_final_score(self, token):
        """
        This is the final confirmation step. It's only called AFTER a stock
        has passed the ORB and liquidity filters.
        """
        df = self.data_store.get(token)
        if df is None or len(df) < 30: return 0

        stock_info = settings.TOKEN_MAP.get(token, {})
        bias = stock_info.get("bias")
        
        try:
            df['rsi'] = ta.momentum.rsi(df['close'], window=14)
            macd = ta.trend.MACD(df['close'])
            df['macd'] = macd.macd()
            df['macd_signal'] = macd.macd_signal()
            df['vwap'] = ta.volume.volume_weighted_average_price(
                high=df['high'], low=df['low'], close=df['close'], volume=df['volume'], window=len(df)
            )
            
            score = 0
            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]

            if bias == "Bullish":
                if last_row['rsi'] > 65: score += 40
                if prev_row['macd'] < prev_row['macd_signal'] and last_row['macd'] > last_row['macd_signal']: score += 40
                if last_row['close'] > last_row['vwap']: score += 20
            
            elif bias == "Bearish":
                if last_row['rsi'] < 35: score += 40
                if prev_row['macd'] > prev_row['macd_signal'] and last_row['macd'] < last_row['macd_signal']: score += 40
                if last_row['close'] < last_row['vwap']: score += 20
            
            return score
        except Exception:
            return 0

    async def get_opening_range_retroactively(self, token):
        """
        If the server starts after 9:30, this function fetches the 1-minute data
        from 9:15 to 9:30 to calculate the opening range.
        """
        try:
            logger.info(f"Retroactively fetching opening range for token {token}...")
            now = datetime.now()
            from_date = now.replace(hour=9, minute=15, second=0, microsecond=0)
            to_date = now.replace(hour=9, minute=30, second=0, microsecond=0)

            historic_param = {
                "exchange": "NSE", "symboltoken": token, "interval": "ONE_MINUTE",
                "fromdate": from_date.strftime("%Y-%m-%d %H:%M"),
                "todate": to_date.strftime("%Y-%m-%d %H:%M")
            }
            data = smartapi_service.smart_api.getCandleData(historic_param)
            
            if data.get("status") and data.get("data"):
                df_orb = pd.DataFrame(data["data"])
                orb_high = df_orb[2].max() / 100.0
                orb_low = df_orb[3].min() / 100.0
                self.opening_ranges[token] = {'high': orb_high, 'low': orb_low}
                logger.info(f"Successfully calculated retroactive ORB for {token}: High={orb_high}, Low={orb_low}")
            else:
                logger.error(f"Could not fetch retroactive ORB for {token}. It will not be scanned.")
        except Exception as e:
            logger.exception(f"Exception while fetching retroactive ORB for {token}: {e}")

    async def start_processing_loop(self):
        logger.info("Starting the advanced processing loop...")
        
        opening_range_start = time(9, 15)
        opening_range_end = time(9, 30)

        while True:
            try:
                tick_data = await websocket_client.data_queue.get()
                now_utc = datetime.utcnow()
                now_time = pd.to_datetime(now_utc).tz_localize('UTC').tz_convert('Asia/Kolkata').time()

                token = tick_data.get('token')
                ltp = tick_data.get('last_traded_price')
                open_price = tick_data.get('open_price')
                volume = tick_data.get('volume_traded_today')

                if not all([token, ltp, open_price, volume]):
                    continue

                price = ltp / 100.0

                if token in settings.INDEX_TOKENS:
                    opening = open_price / 100.0
                    change = price - opening
                    percent_change = (change / opening) * 100 if opening > 0 else 0
                    self.index_data[token] = {
                        "name": settings.INDEX_TOKENS[token],
                        "ltp": price, "change": change, "percent_change": percent_change
                    }
                    continue

                if token not in self.data_store:
                    self.data_store[token] = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'last_volume'])
                    self.data_store[token].index.name = 'timestamp'
                    self.data_store[token]['last_volume'] = 0

                df = self.data_store[token]
                current_bar_timestamp = pd.to_datetime(now_utc).floor('T')
                last_total_volume = df['last_volume'].iloc[-1] if not df.empty else 0
                minute_volume = volume - last_total_volume

                if current_bar_timestamp not in df.index:
                    new_bar = pd.DataFrame([{'open': price, 'high': price, 'low': price, 'close': price, 'volume': minute_volume, 'last_volume': volume}], index=[current_bar_timestamp])
                    self.data_store[token] = pd.concat([df, new_bar])
                else:
                    df.loc[current_bar_timestamp, 'high'] = max(df.loc[current_bar_timestamp, 'high'], price)
                    df.loc[current_bar_timestamp, 'low'] = min(df.loc[current_bar_timestamp, 'low'], price)
                    df.loc[current_bar_timestamp, 'close'] = price
                    df.loc[current_bar_timestamp, 'volume'] += minute_volume
                    df.loc[current_bar_timestamp, 'last_volume'] = volume

                best_bid = tick_data.get('best_5_buy_price_and_quantity', [{}])[0].get('price', 0)
                best_ask = tick_data.get('best_5_sell_price_and_quantity', [{}])[0].get('price', 0)
                if not all([best_bid, best_ask]): continue
                
                bid = best_bid / 100.0
                ask = best_ask / 100.0
                
                spread_percentage = ((ask - bid) / price) * 100 if price > 0 else 0
                if spread_percentage > 0.5:
                    if token in self.scan_results: del self.scan_results[token]
                    continue

                if opening_range_start <= now_time < opening_range_end:
                    if token not in self.opening_ranges:
                        self.opening_ranges[token] = {'high': price, 'low': price}
                    else:
                        self.opening_ranges[token]['high'] = max(self.opening_ranges[token]['high'], price)
                        self.opening_ranges[token]['low'] = min(self.opening_ranges[token]['low'], price)
                    continue

                if now_time >= opening_range_end:
                    if token not in self.opening_ranges:
                        await self.get_opening_range_retroactively(token)

                    orb = self.opening_ranges.get(token)
                    if not orb: continue

                    stock_info = settings.TOKEN_MAP.get(token, {})
                    bias = stock_info.get("bias")
                    symbol = stock_info.get("symbol")
                    
                    is_breakout = False
                    if bias == "Bullish" and price > orb['high']: is_breakout = True
                    elif bias == "Bearish" and price < orb['low']: is_breakout = True

                    if is_breakout:
                        # --- THE LOGIC IS NOW RESTORED ---
                        final_score = 100 + self.calculate_final_score(token)
                        self.scan_results[token] = {
                            "symbol": symbol, "score": final_score,
                            "price": price, "bias": bias
                        }
                    else:
                        if token in self.scan_results: del self.scan_results[token]

            except Exception as e:
                logger.exception(f"Error in processing loop: {e}")

processing_engine = ProcessingEngine()

