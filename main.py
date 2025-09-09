import asyncio
import json
import random
import uvicorn
import os
import pandas as pd
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from logzero import logger

# --- Assuming your service imports are in a 'services' folder ---
# If these paths are wrong, please adjust them to your project structure.
from services.smartapi_service import smartapi_service
from services.websocket_client import websocket_client
from services.processing_engine import processing_engine
from ws_connection.connection_manager import manager
from core.config import settings
from services.database_service import database_service

# --- Lifespan Manager (Refactored for Flexibility) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application starting up...")
    
    # --- SOLUTION: Use an environment variable to control the mode ---
    # This is more flexible than a hardcoded boolean.
    # Defaults to "MOCK" if the variable isn't set.
    run_mode = os.getenv("RUN_MODE", "MOCK").upper()

    if run_mode == "LIVE":
        logger.info("Starting in LIVE DATA mode.")
        # Your existing live data logic
        if smartapi_service.login():
            logger.info("Successfully logged into SmartAPI.")
            asyncio.create_task(processing_engine.start_processing_loop())
            websocket_client.connect()
            asyncio.create_task(broadcast_realtime_scan_results())
        else:
            logger.error("Failed to log into SmartAPI. The app may not function correctly.")
    else:
        logger.info("Starting in MOCK DATA mode.")
        # Your existing mock data logic
        asyncio.create_task(send_mock_scanner_updates())
    
    yield
    
    logger.info("Application shutting down.")

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Indian Stock Scanner API",
    description="Real-time stock scanning and data API for the Indian market.",
    version="1.0.0",
    lifespan=lifespan 
)

# --- Real-time Broadcaster ---
async def broadcast_realtime_scan_results():
    """
    This is the REAL broadcaster. It runs in the background, finds the top 5
    bullish and bearish stocks, saves them to the database, and then sends
    them to the frontend.
    """
    while True:
        await asyncio.sleep(5)
        if processing_engine.scan_results or processing_engine.index_data:
            all_results = []
            # Use .items() to safely iterate over the dictionary
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

# --- Mock Data Broadcaster ---
async def send_mock_scanner_updates():
    """
    Sends mock data to the frontend every 5 seconds to simulate live updates.
    """
    mock_bullish = [{'symbol': 'RELIANCE-EQ', 'bias': 'Bullish'}, {'symbol': 'HDFCBANK-EQ', 'bias': 'Bullish'}]
    mock_bearish = [{'symbol': 'SBIN-EQ', 'bias': 'Bearish'}, {'symbol': 'ICICIBANK-EQ', 'bias': 'Bearish'}]
    mock_indices = [
        {"name": "NIFTY 50", "ltp": 23500.50, "change": 150.25, "percent_change": 0.64},
        {"name": "BANK NIFTY", "ltp": 51000.75, "change": -200.40, "percent_change": -0.39}
    ]
    
    while True:
        await asyncio.sleep(5)
        
        bullish_payload = []
        for stock in mock_bullish:
            stock_with_token = stock.copy()
            stock_with_token.update({
                "score": random.randint(80, 150),
                "price": round(random.uniform(1000, 3000), 2),
                "token": "mock_token_" + stock['symbol']
            })
            bullish_payload.append(stock_with_token)

        bearish_payload = []
        for stock in mock_bearish:
            stock_with_token = stock.copy()
            stock_with_token.update({
                "score": random.randint(80, 150),
                "price": round(random.uniform(500, 1500), 2),
                "token": "mock_token_" + stock['symbol']
            })
            bearish_payload.append(stock_with_token)

        update_message = {
            "type": "full_update",
            "payload": {
                "bullish": bullish_payload,
                "bearish": bearish_payload,
                "indices": mock_indices
            }
        }
        await manager.broadcast(json.dumps(update_message))
        logger.info("Broadcasted MOCK scanner update to frontend.")

# --- WebSocket Endpoint ---
@app.websocket("/ws/scanner-updates")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# --- Other API Endpoints ---
@app.get("/stock-details/{token}")
def get_stock_details(token: str):
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
    df = processing_engine.data_store.get(token)
    
    if df is None or df.empty:
        return {"error": "No intraday data available for this token."}

    df_reset = df.reset_index()
    df_reset['time'] = (df_reset['timestamp'].astype(int) / 10**9).astype(int)
    chart_data = df_reset[['time', 'open', 'high', 'low', 'close']].to_dict(orient='records')
    return chart_data

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# --- Root Endpoint ---
@app.get("/")
def read_root():
    return {"message": "Welcome to the Indian Stock Scanner API!"}
