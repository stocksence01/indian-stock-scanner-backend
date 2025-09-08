# backend/services/smartapi_service.py

import pyotp
from SmartApi import SmartConnect
from logzero import logger

# Import the settings we created
from core.config import settings

class SmartApiService:
    def __init__(self):
        """
        Initializes the SmartApiService with credentials from the settings file.
        """
        self.smart_api = SmartConnect(settings.API_KEY)
        self.session = None
        self.feed_token = None
        self.jwt_token = None
        logger.info("SmartAPI service initialized.")

    def login(self):
        """
        Performs the login flow to generate a session.
        Uses the TOTP secret to generate a valid Time-based One-Time Password.
        """
        try:
            totp = pyotp.TOTP(settings.TOTP_SECRET).now()
            logger.info("Generated TOTP for login.")

            data = self.smart_api.generateSession(
                settings.CLIENT_CODE,
                settings.CLIENT_PASSWORD,
                totp
            )

            if data.get('status') and data.get('data'):
                self.jwt_token = data['data']['jwtToken']
                self.feed_token = self.smart_api.getfeedToken()
                logger.info("SmartAPI login successful.")
                return True
            else:
                error_message = data.get('message', 'Unknown error')
                logger.error(f"SmartAPI login failed: {error_message}")
                return False
        except Exception as e:
            logger.exception(f"An exception occurred during SmartAPI login: {e}")
            return False

# Create a single, reusable instance of the service
smartapi_service = SmartApiService()