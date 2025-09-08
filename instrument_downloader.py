# backend/instrument_downloader.py

import requests
import json
import csv
from io import StringIO
from logzero import logger

# URL for the broker's master list of all instruments
INSTRUMENT_LIST_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
# --- NEW: URL for the official NIFTY 500 stock list from the NSE ---
NIFTY_500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"


def download_and_filter_instruments():
    """
    Downloads the master instrument list and filters it to include only stocks
    that are part of the NIFTY 500 index, ensuring a high-quality, liquid universe.
    """
    try:
        # --- Step 1: Download the NIFTY 500 list and create a set for fast lookups ---
        logger.info(f"Downloading NIFTY 500 list from NSE...")
        # NSE website requires a user-agent header to allow the download
        headers = {'User-Agent': 'Mozilla/5.0'}
        response_n500 = requests.get(NIFTY_500_URL, headers=headers, timeout=10)
        response_n500.raise_for_status()
        
        nifty_500_symbols = set()
        # Use StringIO to read the CSV data from memory
        csv_data = StringIO(response_n500.text)
        csv_reader = csv.reader(csv_data)
        next(csv_reader) # Skip the header row
        for row in csv_reader:
            symbol = row[2] # The symbol is in the 3rd column
            nifty_500_symbols.add(f"{symbol.strip()}-EQ")
        logger.info(f"Successfully loaded {len(nifty_500_symbols)} symbols from the NIFTY 500 index.")

        # --- Step 2: Download the broker's master instrument list ---
        logger.info(f"Downloading full instrument list from broker...")
        response_broker = requests.get(INSTRUMENT_LIST_URL, timeout=10)
        response_broker.raise_for_status()
        instrument_data = response_broker.json()
        logger.info(f"Successfully downloaded {len(instrument_data)} total instruments.")

        # --- Step 3: Filter the master list against the NIFTY 500 list ---
        filtered_stocks = {}
        for item in instrument_data:
            symbol = item.get('symbol', '')
            
            # The stock must be in our NIFTY 500 set to be included
            if symbol in nifty_500_symbols:
                token = item.get('token')
                if token and symbol:
                    filtered_stocks[token] = symbol

        if not filtered_stocks:
            logger.error("No stocks matched the NIFTY 500 filtering criteria.")
            return

        output_filename = 'scannable_stocks.json'
        with open(output_filename, 'w') as f:
            json.dump(filtered_stocks, f, indent=2)
        
        logger.info(f"\nSUCCESS! Saved {len(filtered_stocks)} NIFTY 500 stocks to {output_filename}.")

    except Exception as e:
        logger.exception(f"An error occurred during the process: {e}")

if __name__ == "__main__":
    download_and_filter_instruments()