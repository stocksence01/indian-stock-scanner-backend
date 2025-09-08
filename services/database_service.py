# backend/services/database_service.py

import sqlite3
from logzero import logger
from datetime import datetime

DATABASE_FILE = "trading_signals.db"

class DatabaseService:
    def __init__(self):
        """
        Initializes the database service and creates the necessary table if it doesn't exist.
        """
        try:
            self.conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
            self.cursor = self.conn.cursor()
            self.create_table()
            logger.info(f"Successfully connected to database: {DATABASE_FILE}")
        except Exception as e:
            logger.exception(f"Error connecting to database: {e}")

    def create_table(self):
        """
        Creates the 'signals' table if it's not already present.
        """
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                bias TEXT NOT NULL,
                score INTEGER NOT NULL,
                price REAL NOT NULL
            )
        ''')
        self.conn.commit()
        logger.info("Table 'signals' is ready.")

    def save_signal(self, stock_data):
        """
        Saves a single trading signal to the database.
        """
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            symbol = stock_data.get('symbol')
            bias = stock_data.get('bias')
            score = stock_data.get('score')
            price = stock_data.get('price')

            if all([symbol, bias, score, price]):
                self.cursor.execute('''
                    INSERT INTO signals (timestamp, symbol, bias, score, price)
                    VALUES (?, ?, ?, ?, ?)
                ''', (timestamp, symbol, bias, score, price))
                self.conn.commit()
                logger.debug(f"Saved signal for {symbol} to database.")
        except Exception as e:
            logger.exception(f"Error saving signal for {symbol}: {e}")

# Create a single, reusable instance of the service
database_service = DatabaseService()