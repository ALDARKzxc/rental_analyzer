"""Application configuration and constants."""
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DB_PATH = DATA_DIR / "rental_analyzer.db"

# API settings
API_HOST = "127.0.0.1"
API_PORT = 8765
API_BASE_URL = f"http://{API_HOST}:{API_PORT}"

# Parser settings
PARSER_TIMEOUT = 20_000          # ms (wait_until="commit" резолвится быстро)
PARSER_RETRY_COUNT = 3
PARSER_RETRY_DELAY = 2           # seconds (сокращено с 5)
PARSER_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Analytics thresholds
PRICE_HIGH_THRESHOLD = 0.15      # 15% above market → recommend lower
PRICE_LOW_THRESHOLD = -0.10      # 10% below market → recommend higher

# Supported sites
SUPPORTED_SITES = {
    "ostrovok.ru": "ostrovok",
    "avito.ru": "avito",
    "sutochno.ru": "sutochno",
    "booking.com": "booking",
    "airbnb.com": "airbnb",
}

# Colors for GUI
COLORS = {
    "primary": "#2563EB",
    "secondary": "#7C3AED",
    "success": "#16A34A",
    "warning": "#D97706",
    "danger": "#DC2626",
    "bg": "#F8FAFC",
    "card": "#FFFFFF",
    "text": "#1E293B",
    "text_muted": "#64748B",
    "border": "#E2E8F0",
}
