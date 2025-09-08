# backend/run_server.py
import waitress
from main import app

print("Starting server with waitress on http://0.0.0.0:8001")
waitress.serve(app, host='0.0.0.0', port=8001)