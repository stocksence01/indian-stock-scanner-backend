# backend/websocket/connection_manager.py

from fastapi import WebSocket
from typing import List
from logzero import logger
import json
import asyncio

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New frontend connection: {websocket.client}. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"Frontend connection closed: {websocket.client}. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Broadcast a message to all active connections."""
        # Serialize message to JSON
        message_json = json.dumps(message)
        tasks = [
            connection.send_text(message_json)
            for connection in self.active_connections
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

# Create a single, reusable instance
manager = ConnectionManager()