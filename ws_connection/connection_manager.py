# backend/websocket/connection_manager.py

from fastapi import WebSocket
from typing import List
from logzero import logger
import json

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

    async def broadcast(self, message):
        # Serialize message to JSON if it's not a string
        if not isinstance(message, str):
            message = json.dumps({"type": "full_update", "payload": message})
        for connection in self.active_connections:
            await connection.send_text(message)

# Create a single, reusable instance
manager = ConnectionManager()