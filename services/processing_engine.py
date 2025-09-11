from __future__ import annotations

import asyncio
from typing import Dict

import pytz
from logzero import logger

from services.smartapi_service import smartapi_service
from services.websocket_client import websocket_client
from core.config import settings


class ProcessingEngine:
    """A simple, robust engine to track live data for the daily watchlist."""

    def __init__(self):
        self.live_stock_data: Dict[str, Dict] = {}
        self.index_data: Dict[str, Dict] = {}
        logger.info("Processing Engine initialized in Live Tracker mode.")

    async def start_processing_loop(self) -> None:
        """Consume ticks from the queue and update the live data dictionary."""
        logger.info("Starting the live tracking loop...")
        
        while True:
            try:
                tick_data = await websocket_client.data_queue.get()

                token = str(tick_data.get("token"))
                ltp = tick_data.get("last_traded_price")
                open_price_day = tick_data.get("open_price_of_the_day")

                if not all([token, ltp is not None, open_price_day is not None]):
                    continue

                price = float(ltp) / 100.0
                opening = float(open_price_day) / 100.0
                change = price - opening
                percent_change = (change / opening) * 100 if opening > 0 else 0

                # Handle Index Ticks
                if token in settings.INDEX_TOKENS:
                    self.index_data[token] = {
                        "name": settings.INDEX_TOKENS[token],
                        "ltp": price,
                        "change": change,
                        "percent_change": percent_change
                    }
                    continue

                # Handle Stock Ticks
                stock_info = settings.TOKEN_MAP.get(token)
                if stock_info:
                    self.live_stock_data[token] = {
                        "symbol": stock_info.get("symbol"),
                        "price": price,
                        "bias": stock_info.get("bias"),
                        "token": token, # Pass token for frontend use
                        "change": change,
                        "percent_change": percent_change
                    }

            except asyncio.CancelledError:
                logger.info("Processing loop cancelled.")
                raise
            except Exception as e:
                logger.exception(f"Error in processing loop: {e}")


processing_engine = ProcessingEngine()

