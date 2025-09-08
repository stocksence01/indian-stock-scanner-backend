# backend/services/websocket_client.py

from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from logzero import logger
import asyncio

from services.smartapi_service import smartapi_service
from core.config import settings

class WebSocketClient:
    def __init__(self):
        self.sws = None
        self.data_queue = asyncio.Queue()

    def on_open(self, wsapp):
        logger.info("WebSocket connection opened. Subscribing to instruments...")
        correlation_id = "scanner_subscription"
        action = 1
        
        # --- THE KEY CHANGE IS HERE ---
        # Mode 2 provides the full data packet (Quote), which includes bid/ask prices and volume.
        mode = 2

        token_list = [
            {"exchangeType": 1, "tokens": settings.INSTRUMENT_TOKENS_TO_SCAN}
        ]

        self.sws.subscribe(correlation_id, mode, token_list)
        logger.info(f"Subscribed to {len(settings.INSTRUMENT_TOKENS_TO_SCAN)} tokens in Quote mode.")

    def on_data(self, wsapp, message):
        try:
            self.data_queue.put_nowait(message)
        except asyncio.QueueFull:
            logger.warning("Data queue is full, tick was dropped.")
        except Exception as e:
            logger.exception(f"Error putting tick into queue: {e}")

    def on_error(self, wsapp, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, wsapp):
        logger.info("WebSocket connection closed.")

    def connect(self):
        if not smartapi_service.jwt_token or not smartapi_service.feed_token:
            logger.error("Cannot connect to WebSocket, tokens are missing.")
            return

        auth_token = smartapi_service.jwt_token
        api_key = settings.API_KEY
        client_code = settings.CLIENT_CODE
        feed_token = smartapi_service.feed_token

        self.sws = SmartWebSocketV2(auth_token, api_key, client_code, feed_token)

        self.sws.on_open = self.on_open
        self.sws.on_data = self.on_data
        self.sws.on_error = self.on_error
        self.sws.on_close = self.on_close

        logger.info("Connecting to SmartAPI WebSocket...")
        self.sws.connect()

websocket_client = WebSocketClient()