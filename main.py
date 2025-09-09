import asyncio
import json
import random
import uvicorn
import os
import pandas as pd
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from logzero import logger

from services.smartapi_service import smartapi_service
from services.websocket_client import websocket_client
from services.processing_engine import processing_engine
from ws_connection.connection_manager import manager
from core.config import settings
from services.database_service import database_service

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles application startup and shutdown events."""
    logger.info("Application starting up...")
    
    run_mode = os.getenv("RUN_MODE", "MOCK").upper()

    if run_mode == "LIVE":
        logger.info("Starting in LIVE DATA mode.")
        if smartapi_service.login():
            logger.info("Successfully logged into SmartAPI.")
            
            # --- THIS IS THE FINAL FIX ---
            # We start the processing loop as a separate, long-running asyncio task.
            # And we run the WebSocket connection in a separate thread.
            # This is the most stable pattern for this type of application.
            asyncio.create_task(processing_engine.start_processing_loop())
            
            ws_thread = threading.Thread(target=websocket_client.connect, daemon=True)
            ws_thread.start()
            
            asyncio.create_task(broadcast_realtime_scan_results())
        else:
            logger.error("Failed to log into SmartAPI.")
    else:
        logger.info("Starting in MOCK DATA mode.")
        asyncio.create_task(send_mock_scanner_updates())
    
    yield
    
    logger.info("Application shutting down.")

app = FastAPI(
    title="Indian Stock Scanner API",
    description="Real-time stock scanning and data API for the Indian market.",
    version="1.0.0",
    lifespan=lifespan
)

async def broadcast_realtime_scan_results():
    """Broadcasts top 5 bullish/bearish stocks and saves them to the database."""
    while True:
        await asyncio.sleep(5)
        if processing_engine.scan_results or processing_engine.index_data:
            all_results = []
            for token, result in list(processing_engine.scan_results.items()):
                result_with_token = result.copy()
                result_with_token['token'] = token
                all_results.append(result_with_token)
            
            bullish_stocks = [s for s in all_results if s.get("bias") == "Bullish" and s.get("score", 0) > 0]
            bearish_stocks = [s for s in all_results if s.get("bias") == "Bearish" and s.get("score", 0) > 0]
            bullish_stocks.sort(key=lambda x: x.get('score', 0), reverse=True)
            bearish_stocks.sort(key=lambda x: x.get('score', 0), reverse=True)
            
            top_bullish = bullish_stocks[:5]
            top_bearish = bearish_stocks[:5]

            for stock in top_bullish:
                database_service.save_signal(stock)
            for stock in top_bearish:
                database_service.save_signal(stock)

            update_message = {
                "type": "full_update",
                "payload": {
                    "bullish": top_bullish,
                    "bearish": top_bearish,
                    "indices": list(processing_engine.index_data.values())
                }
            }
            await manager.broadcast(json.dumps(update_message))
            logger.info(f"Broadcasted and saved Top {len(top_bullish)} Bullish and Top {len(top_bearish)} Bearish stocks.")

async def send_mock_scanner_updates():
    """Sends mock data to the frontend every 5 seconds."""
    mock_bullish = [{'symbol': 'RELIANCE-EQ', 'bias': 'Bullish'}, {'symbol': 'HDFCBANK-EQ', 'bias': 'Bullish'}]
    mock_bearish = [{'symbol': 'SBIN-EQ', 'bias': 'Bearish'}, {'symbol': 'ICICIBANK-EQ', 'bias': 'Bearish'}]
    mock_indices = [{"name": "NIFTY 50", "ltp": 23500.50, "change": 150.25, "percent_change": 0.64}, {"name": "BANK NIFTY", "ltp": 51000.75, "change": -200.40, "percent_change": -0.39}]
    
    while True:
        await asyncio.sleep(5)
        bullish_payload = [{"symbol": s['symbol'], "score": random.randint(80, 150), "price": round(random.uniform(1000, 3000), 2), "token": "mock_" + s['symbol']} for s in mock_bullish]
        bearish_payload = [{"symbol": s['symbol'], "score": random.randint(80, 150), "price": round(random.uniform(500, 1500), 2), "token": "mock_" + s['symbol']} for s in mock_bearish]
        update_message = {"type": "full_update", "payload": {"bullish": bullish_payload, "bearish": bearish_payload, "indices": mock_indices}}
        await manager.broadcast(json.dumps(update_message))
        logger.info("Broadcasted MOCK scanner update to frontend.")

@app.websocket("/ws/scanner-updates")
async def websocket_endpoint(websocket: WebSocket):
    """Handles the persistent WebSocket connection from the frontend."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/stock-details/{token}")
def get_stock_details(token: str):
    """Provides detailed signal breakdown for a single stock."""
    stock_info = settings.TOKEN_MAP.get(token)
    if not stock_info:
        return {"error": "Stock not found in watchlist"}

    df = processing_engine.data_store.get(token)
    orb = processing_engine.opening_ranges.get(token)
    
    if df is None or len(df) < 2 or orb is None:
        return {"error": "Not enough data available yet for analysis."}

    last_row = df.iloc[-1]
    prev_row = df.iloc[-2]

    details = {
        "symbol": stock_info.get("symbol"),
        "dailyBias": stock_info.get("bias"),
        "openingRangeHigh": orb.get('high'),
        "openingRangeLow": orb.get('low'),
        "lastPrice": last_row.get('close'),
        "intradayRsi": last_row.get('rsi', 0),
        "macdCrossover": (prev_row.get('macd', 0) < prev_row.get('macd_signal', 0) and 
                          last_row.get('macd', 0) > last_row.get('macd_signal', 0)),
        "bearishMacdCrossover": (prev_row.get('macd', 0) > prev_row.get('macd_signal', 0) and 
                                 last_row.get('macd', 0) < last_row.get('macd_signal', 0))
    }
    return details

@app.get("/intraday-history/{token}")
def get_intraday_history(token: str):
    """Provides 1-minute chart data for a single stock."""
    df = processing_engine.data_store.get(token)
    
    if df is None or df.empty:
        return {"error": "No intraday data available for this token."}

    df_reset = df.reset_index()
    df_reset['time'] = (df_reset['timestamp'].astype(int) / 10**9).astype(int)
    chart_data = df_reset[['time', 'open', 'high', 'low', 'close']].to_dict(orient='records')
    return chart_data

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

@app.get("/health")
def health_check():
    """A simple endpoint for Render's health checker to call."""
    return {"status": "ok"}

@app.get("/")
def read_root():
    """Root endpoint to confirm the server is running."""
    return {"message": "Welcome to the Indian Stock Scanner API!"}

