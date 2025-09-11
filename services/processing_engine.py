from __future__ import annotations

import asyncio
from datetime import datetime, time
from typing import Dict, Optional

import pandas as pd
import pytz
import ta
from logzero import logger

from services.smartapi_service import smartapi_service
from services.websocket_client import websocket_client
from core.config import settings


class ProcessingEngine:
    """Aggregate ticks into 1-minute bars, calculate opening range, and score breakouts."""

    def __init__(self):
        self.data_store: Dict[str, pd.DataFrame] = {}
        self.scan_results: Dict[str, Dict] = {}
        self.opening_ranges: Dict[str, Dict[str, float]] = {}
        self.index_data: Dict[str, Dict] = {}
        logger.info("Processing Engine initialized with all features.")

    # ---------- Helpers ----------
    def _safe_get_best_price(self, tick: dict, key: str) -> float:
        """Return first price from an array of price/qty objects, or 0."""
        arr = tick.get(key) or []
        if isinstance(arr, list) and arr:
            first = arr[0] or {}
            return float(first.get("price", 0) or 0)
        return 0.0

    def _compute_vwap_per_day(self, df: pd.DataFrame) -> pd.Series:
        """Compute VWAP per day using cumulative typical_price*volume / cumulative volume."""
        if df.empty:
            return pd.Series(dtype=float)
        tmp = df.copy()
        # Ensure correct data types before calculation
        tmp['high'] = pd.to_numeric(tmp['high'], errors='coerce')
        tmp['low'] = pd.to_numeric(tmp['low'], errors='coerce')
        tmp['close'] = pd.to_numeric(tmp['close'], errors='coerce')
        tmp['volume'] = pd.to_numeric(tmp['volume'], errors='coerce')
        tmp.dropna(subset=['high', 'low', 'close', 'volume'], inplace=True)
        
        typical_price = (tmp["high"] + tmp["low"] + tmp["close"]) / 3.0
        pv = typical_price * tmp["volume"]
        
        vwap = pv.groupby(tmp.index.date).cumsum() / tmp["volume"].groupby(tmp.index.date).cumsum()
        vwap.index = tmp.index
        return vwap

    # ---------- Scoring ----------
    def calculate_final_score(self, token: str) -> int:
        """Robust final-scoring combining RSI, MACD cross, and VWAP position."""
        df = self.data_store.get(token)
        if df is None or len(df) < 30:
            return 0

        stock_info = settings.TOKEN_MAP.get(token, {})
        bias = stock_info.get("bias")

        try:
            df = df.sort_index()
            calc_df = df[["open", "high", "low", "close", "volume"]].copy()
            calc_df["rsi"] = ta.momentum.rsi(calc_df["close"], window=14)
            macd = ta.trend.MACD(calc_df["close"])
            calc_df["macd"] = macd.macd()
            calc_df["macd_signal"] = macd.macd_signal()
            vwap_series = self._compute_vwap_per_day(calc_df)
            calc_df["vwap"] = vwap_series
            
            # --- FIX: Handle NaN values from indicators ---
            temp_df = calc_df.dropna()
            if len(temp_df) < 2:
                return 0

            score = 0
            last_row = temp_df.iloc[-1]
            prev_row = temp_df.iloc[-2]

            if bias == "Bullish":
                if last_row["rsi"] > 65: score += 40
                if prev_row["macd"] < prev_row["macd_signal"] and last_row["macd"] > last_row["macd_signal"]: score += 40
                if last_row["close"] > last_row["vwap"]: score += 20
            elif bias == "Bearish":
                if last_row["rsi"] < 35: score += 40
                if prev_row["macd"] > prev_row["macd_signal"] and last_row["macd"] < last_row["macd_signal"]: score += 40
                if last_row["close"] < last_row["vwap"]: score += 20

            return int(score)
        except Exception as e:
            logger.exception(f"Error in final scoring for token {token}: {e}")
            return 0

    # ---------- Retro ORB fetch ----------
    async def get_opening_range_retroactively(self, token: str, symbol: Optional[str], open_price_of_day: float) -> None:
        """Fetch 1-minute candles from 09:15 to 09:30 IST and compute ORB."""
        try:
            logger.info(f"Retroactively fetching opening range for {symbol} ({token})...")
            ist = pytz.timezone("Asia/Kolkata")
            now_ist = datetime.now(ist)
            from_date = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
            to_date = now_ist.replace(hour=9, minute=30, second=0, microsecond=0)
            historic_param = {
                "exchange": "NSE", "symboltoken": token, "interval": "ONE_MINUTE",
                "fromdate": from_date.strftime("%Y-%m-%d %H:%M"),
                "todate": to_date.strftime("%Y-%m-%d %H:%M"),
            }
            data = smartapi_service.smart_api.getCandleData(historic_param)
            if data.get("status") and data.get("data"):
                df_orb = pd.DataFrame(data["data"], columns=["time", "open", "high", "low", "close", "volume"])
                df_orb["high"] = pd.to_numeric(df_orb["high"]) / 100.0
                df_orb["low"] = pd.to_numeric(df_orb["low"]) / 100.0
                orb_high = float(df_orb["high"].max())
                orb_low = float(df_orb["low"].min())
                self.opening_ranges[token] = {"high": orb_high, "low": orb_low}
                logger.info(f"Calculated retro ORB for {symbol}: High={orb_high}, Low={orb_low}")
            else:
                logger.error(f"Could not fetch retro ORB for {symbol}. Using a failsafe range.")
                self.opening_ranges[token] = {"high": open_price_of_day * 1.002, "low": open_price_of_day * 0.998}
        except Exception as e:
            logger.exception(f"Exception while fetching retro ORB for {symbol}: {e}")
            self.opening_ranges[token] = {"high": open_price_of_day * 1.002, "low": open_price_of_day * 0.998}

    # ---------- Main processing loop ----------
    async def start_processing_loop(self) -> None:
        """Consume ticks from websocket_client.data_queue and maintain state."""
        logger.info("Starting the advanced processing loop...")
        ist = pytz.timezone("Asia/Kolkata")
        opening_range_start = time(9, 15)
        opening_range_end = time(9, 30)

        while True:
            try:
                tick_data = await websocket_client.data_queue.get()
                now_ist = datetime.now(ist)
                now_time = now_ist.time()

                token = str(tick_data.get("token"))
                ltp = tick_data.get("last_traded_price")
                open_price_day = tick_data.get("open_price_of_the_day")
                cumulative_volume = tick_data.get("volume_trade_for_the_day")

                if not all([token, ltp is not None, open_price_day is not None, cumulative_volume is not None]):
                    continue

                price = float(ltp) / 100.0
                if token in settings.INDEX_TOKENS:
                    opening = float(open_price_day) / 100.0
                    change = price - opening
                    percent_change = (change / opening) * 100 if opening > 0 else 0
                    self.index_data[token] = {"name": settings.INDEX_TOKENS[token], "ltp": price, "change": change, "percent_change": percent_change}
                    continue

                if token not in self.data_store:
                    self.data_store[token] = pd.DataFrame(columns=["open", "high", "low", "close", "volume", "last_volume"])
                    self.data_store[token].index.name = "timestamp"

                df = self.data_store[token]
                current_bar_timestamp = now_ist.replace(second=0, microsecond=0)
                last_total_volume = float(df["last_volume"].iloc[-1]) if not df.empty else 0.0
                minute_volume = float(cumulative_volume) - last_total_volume
                minute_volume = max(minute_volume, 0.0)

                if current_bar_timestamp not in df.index:
                    df.loc[current_bar_timestamp] = [price, price, price, price, minute_volume, float(cumulative_volume)]
                else:
                    row = df.loc[current_bar_timestamp]
                    df.at[current_bar_timestamp, "high"] = max(float(row["high"]), price)
                    df.at[current_bar_timestamp, "low"] = min(float(row["low"]), price)
                    df.at[current_bar_timestamp, "close"] = price
                    df.at[current_bar_timestamp, "volume"] = float(row["volume"]) + minute_volume
                    df.at[current_bar_timestamp, "last_volume"] = float(cumulative_volume)

                # --- FIX: Safer best bid/ask access ---
                bid_info = tick_data.get('best_5_buy_price_and_quantity', [])
                ask_info = tick_data.get('best_5_sell_price_and_quantity', [])
                best_bid = bid_info[0].get('price', 0) if bid_info else 0
                best_ask = ask_info[0].get('price', 0) if ask_info else 0
                if not (best_bid and best_ask):
                    continue

                bid, ask = best_bid / 100.0, best_ask / 100.0
                spread_percentage = ((ask - bid) / price) * 100 if price > 0 else 0
                if spread_percentage > 0.5:
                    self.scan_results.pop(token, None)
                    continue

                if opening_range_start <= now_time < opening_range_end:
                    if token not in self.opening_ranges:
                        self.opening_ranges[token] = {"high": price, "low": price}
                    else:
                        self.opening_ranges[token]["high"] = max(self.opening_ranges[token]["high"], price)
                        self.opening_ranges[token]["low"] = min(self.opening_ranges[token]["low"], price)
                    continue

                if now_time >= opening_range_end:
                    stock_info = settings.TOKEN_MAP.get(token, {})
                    symbol = stock_info.get("symbol")
                    if token not in self.opening_ranges:
                        day_open = float(open_price_day) / 100.0
                        await self.get_opening_range_retroactively(token, symbol, day_open)

                    orb = self.opening_ranges.get(token)
                    if not orb:
                        continue

                    bias = stock_info.get("bias")
                    is_breakout = False
                    if bias == "Bullish" and price > orb["high"]:
                        is_breakout = True
                    elif bias == "Bearish" and price < orb["low"]:
                        is_breakout = True

                    if is_breakout:
                        final_score = 100 + self.calculate_final_score(token)
                        if final_score >= 100:
                            self.scan_results[token] = {"symbol": symbol, "score": final_score, "price": price, "bias": bias}
                    else:
                        # "Scout" logic for non-breakout momentum
                        confirmation_score = self.calculate_final_score(token)
                        if confirmation_score > 0:
                            self.scan_results[token] = {"symbol": symbol, "score": confirmation_score, "price": price, "bias": bias}
                        else:
                            self.scan_results.pop(token, None)

            except asyncio.CancelledError:
                logger.info("Processing loop cancelled.")
                raise
            except Exception as e:
                logger.exception(f"Error in processing loop: {e}")


processing_engine = ProcessingEngine()

