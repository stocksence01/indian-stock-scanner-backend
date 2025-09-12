import sys
import os
# Add the project root to the Python path to fix import errors
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from __future__ import annotations

# Standard library imports
import os
import asyncio
from contextlib import asynccontextmanager

# Third-party imports
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from logzero import logger
import logzero

# Local application imports
from services.smartapi_service import smartapi_service
from services.websocket_client import websocket_client
from services.processing_engine import processing_engine
from ws_connection.connection_manager import manager

# Load environment variables and configure logging
load_dotenv()
logzero.loglevel(logzero.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("lifespan function started")
    """Handles application startup and shutdown events."""
    logger.info("Application starting up...")

    run_mode = os.getenv("RUN_MODE", "LIVE").upper()

    if run_mode == "LIVE":
        # Start SmartAPI login and websocket client
        smartapi_service.login()
        await websocket_client.connect()
        asyncio.create_task(processing_engine.start_processing_loop())
        asyncio.create_task(broadcast_live_watchlist())
    else:
        logger.warning("RUN_MODE is not LIVE. No live data will be processed.")

    yield

    logger.info("Application shutting down.")

app = FastAPI(
    title="Indian Stock Scanner API",
    description="Live watchlist tracker for the Indian market.",
    version="2.0.0",
    lifespan=lifespan
)

async def broadcast_live_watchlist():
    print("broadcast_live_watchlist started")
    """Broadcasts the full, live-updating watchlist to the frontend."""
    while True:
        await asyncio.sleep(2)  # Send updates every 2 seconds
        # Always inject test signal, even if market is closed
        processing_engine.scan_results['TEST'] = {
            "symbol": "TEST-EQ",
            "score": 123,
            "price": 100.0,
            "bias": "Bullish"
        }
        print(f"[TEST] Forced test signal in scan_results: {processing_engine.scan_results['TEST']}")
        all_stocks = list(processing_engine.scan_results.values())
        indices = list(processing_engine.index_data.values())
        bullish_stocks = [s for s in all_stocks if s.get("bias") == "Bullish"]
        bearish_stocks = [s for s in all_stocks if s.get("bias") == "Bearish"]

        print(f"Broadcasting bullish: {bullish_stocks}, bearish: {bearish_stocks}")

        if bullish_stocks or bearish_stocks or indices:
            await manager.broadcast({
                "bullish": bullish_stocks,
                "bearish": bearish_stocks,
                "indices": indices
            })

@app.websocket("/ws/scanner-updates")
async def websocket_endpoint(websocket: WebSocket):
    """Handles the persistent WebSocket connection from the frontend."""
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(60)  # Keep the connection alive
    except WebSocketDisconnect:
        await manager.disconnect(websocket)

app.add_middleware(
    CORSMiddleware,
    # IMPORTANT: For production, you MUST list your Vercel frontend URL here.
    allow_origins=[
        "http://localhost:5173",  # For local development
        "https://stoksence.vercel.app"  # Your Vercel frontend URL
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/")
def read_root():
    return {"message": "Indian Stock Scanner API is running"}