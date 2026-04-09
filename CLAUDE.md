# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Context7

Use Context7 MCP to fetch up-to-date documentation for any library used in this project whenever you need to check API details, method signatures, or behaviour. Do this automatically — without waiting for a reminder — whenever working with: PySide6, FastAPI, SQLAlchemy, Playwright, Pydantic, httpx, aiosqlite, matplotlib, pandas, loguru, tenacity, PyInstaller, or any other dependency from `requirements.txt`.

To use Context7, call the `resolve-library-id` tool first to find the library, then `get-library-docs` to fetch the relevant section.

## Commands

**Run the application:**
```bash
python main.py
```

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**Install Playwright browsers (required for parsers):**
```bash
python -m playwright install chromium
```

**Build executable (PyInstaller):**
```bash
pyinstaller build.spec --clean
```

## Architecture

This is a desktop GUI application for monitoring rental property prices across Russian booking sites. It follows a **three-layer architecture** running in a single process with two threads.

### Threading model
- **Main thread** — PySide6 Qt GUI
- **Background daemon thread** — asyncio event loop running the FastAPI backend

The backend loop is registered via `app/gui/api_client.py:register_backend_loop()`. The GUI communicates with the backend by submitting coroutines to that loop using `asyncio.run_coroutine_threadsafe()` — no HTTP calls are made internally. The `ApiClient` class wraps all backend calls with blocking `.result(timeout=60)`.

### Layer responsibilities

**`app/backend/`** — Data layer
- `database.py` — SQLAlchemy async ORM (`Property`, `PriceRecord` models), `PropertyRepository` and `PriceRepository` static-method classes, auto-migration via `_migrate()` on startup
- `api.py` — FastAPI app (registered but not served over HTTP in production; routes exist for potential external use). Parse tasks run as FastAPI `BackgroundTasks` with a concurrency semaphore of 5.

**`app/parser/`** — Web scraping layer
- `base_parser.py` — Abstract `BaseParser` with Playwright browser lifecycle, system proxy auto-detection (env vars + Windows registry), block/captcha detection, retry logic (3 attempts with exponential backoff), and price extraction from raw text
- `dispatcher.py` — Routes URLs to site-specific parsers by hostname; falls back to `GenericParser`
- Site parsers: `ostrovok_parser.py`, `avito_parser.py`, `sutochno_parser.py`, `booking_parser.py`, `airbnb_parser.py`, `generic_parser.py` — each implements `_fetch_once(url) -> dict`
- Parse URL format for dates: `{base_url}?dates=DD.MM.YYYY-DD.MM.YYYY&guests=2`

**`app/analytics/`** — Stats computation
- `engine.py` — `AnalyticsEngine.compute()` calculates current/avg/min/max price, trend (linear regression slope on last 5 records), and recommendation based on thresholds in `config.py` (`PRICE_HIGH_THRESHOLD=0.15`, `PRICE_LOW_THRESHOLD=-0.10`)

**`app/gui/`** — PySide6 UI
- `main_window.py` — `QMainWindow` with sidebar nav and `QStackedWidget` for three screens
- `screens/property_list.py` — Main list with category tabs, parse triggers, date picker
- `screens/add_property.py` — Form to add new property URL
- `screens/detail.py` — Price history chart and analytics for a single property
- `widgets/chart_widget.py` — matplotlib chart embedded in Qt
- `widgets/date_picker.py` — Custom date range picker widget
- `api_client.py` — `ApiClient` singleton bridging GUI thread to backend asyncio loop

**`app/utils/config.py`** — All constants: paths (`DATA_DIR`, `LOGS_DIR`, `DB_PATH`), `SUPPORTED_SITES` dict, parser timeouts, analytics thresholds, GUI colors.

### Data model
- `Property`: title, url (stored without query params), site, category (`CATEGORIES = ["Квартиры", "Апартаменты", "Дома", "Коттеджи"]`), `parse_dates` (string `"DD.MM.YYYY-DD.MM.YYYY"`), notes, is_active
- `PriceRecord`: price (float, nullable), currency (default RUB), status (`ok`/`error`/`blocked`/`captcha`/`not_found`), `parse_dates`, error_message, recorded_at

### Key design decisions
- URLs are stored stripped of query params (`url.split("?")[0]`). Date ranges are stored separately in `parse_dates` and injected at parse time.
- The FastAPI app object exists in `api.py` but is **not started with uvicorn** — the GUI calls backend functions directly through the shared asyncio loop.
- Database migrations are additive-only (ALTER TABLE ADD COLUMN) applied at every startup; failures are silently swallowed since columns likely already exist.
- PyInstaller packaging bundles Chromium from `%LOCALAPPDATA%\ms-playwright\chromium-*` — see `build.spec` and `hook-playwright.py`.
