import os

BITSO_WS_URL = os.getenv("BITSO_WS_URL", "wss://ws.bitso.com")
BOOK_TO_SUBSCRIBE = os.getenv("BOOK_TO_SUBSCRIBE", "btc_mxn")
OUTPUT_BASE_PATH = os.getenv("OUTPUT_BASE_PATH", "./bid_ask_pread_output")
