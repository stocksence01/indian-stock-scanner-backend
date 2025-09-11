from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Dict

import pytz
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from logzero import logger

from services.smartapi_service import smartapi_service
from services.websocket_client import websocket_client
from services.processing_engine import processing_engine
from ws_connection.connection_manager import manager
from core.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles application startup and shutdown events."""
    logger.info("Application starting up...")
    
    run_mode = os.getenv("RUN_MODE", "LIVE").upper()

    if run_mode == "LIVE":
        logger.info("Starting in LIVE DATA mode.")
        if smartapi_service.login():
            logger.info("Successfully logged into SmartAPI.")
            asyncio.create_task(processing_engine.start_processing_loop())
            asyncio.create_task(broadcast_live_watchlist())
            websocket_client.connect()
        else:
            logger.error("Failed to log into SmartAPI.")
    else:
        # Mock mode is no longer needed for this simpler version
        logger.info("Starting in MOCK DATA mode (no data will be sent).")
    
    yield
    
    logger.info("Application shutting down.")

app = FastAPI(
    title="Indian Stock Scanner API",
    description="Live watchlist tracker for the Indian market.",
    version="2.0.0",
    lifespan=lifespan
)

async def broadcast_live_watchlist():
    """Broadcasts the full, live-updating watchlist to the frontend."""
    while True:
        await asyncio.sleep(2) # Send updates more frequently
        if processing_engine.live_stock_data or processing_engine.index_data:
            all_stocks = list(processing_engine.live_stock_data.values())
            
            bullish_stocks = [s for s in all_stocks if s.get("bias") == "Bullish"]
            bearish_stocks = [s for s in all_stocks if s.get("bias") == "Bearish"]

            update_message = {
                "type": "full_update",
                "payload": {
                    "bullish": bullish_stocks,
                    "bearish": bearish_stocks,
                    "indices": list(processing_engine.index_data.values())
                }
            }
            await manager.broadcast(json.dumps(update_message))
            logger.info(f"Broadcasted {len(bullish_stocks)} bullish and {len(bearish_stocks)} bearish stocks.")

@app.websocket("/ws/scanner-updates")
async def websocket_endpoint(websocket: WebSocket):
    """Handles the persistent WebSocket connection from the frontend."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

@app.get("/health")
def health_check():
    """A simple endpoint for Render's health checker."""
    return {"status": "ok"}

@app.get("/")
def read_root():
    """Root endpoint to confirm the server is running."""
    return {"message": "Welcome to the StockSence Live Watchlist API!"}

