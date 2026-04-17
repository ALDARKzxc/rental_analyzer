# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec для Rental Price Analyzer.
Собирает в ОДНУ папку с одним exe.
Запуск сборки: pyinstaller build.spec --clean
"""

import sys
import os
from pathlib import Path

# ── Находим где Playwright хранит браузеры ──────────────────────
# Обычно: C:\Users\USERNAME\AppData\Local\ms-playwright
ms_playwright_dir = Path.home() / "AppData" / "Local" / "ms-playwright"

# Ищем папку chromium-* внутри
chromium_dir = None
if ms_playwright_dir.exists():
    for item in ms_playwright_dir.iterdir():
        if item.is_dir() and item.name.startswith("chromium"):
            chromium_dir = item
            break

if not chromium_dir:
    print("=" * 60)
    print("ОШИБКА: Chromium не найден!")
    print("Запустите: python -m playwright install chromium")
    print("=" * 60)
    sys.exit(1)

print(f"Найден Chromium: {chromium_dir}")

# ── Находим пакет playwright ────────────────────────────────────
import playwright
playwright_pkg = Path(playwright.__file__).parent

block_cipher = None

a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        # Весь пакет playwright (драйверы, нода)
        (str(playwright_pkg), 'playwright'),
        # Браузер Chromium (самая тяжёлая часть — ~150 МБ)
        (str(chromium_dir), f'ms-playwright/{chromium_dir.name}'),
        # Пустые папки для данных
        ('data', 'data'),
        ('logs', 'logs'),
    ],
    hiddenimports=[
        # Uvicorn — все внутренние модули явно
        'uvicorn', 'uvicorn.main', 'uvicorn.config', 'uvicorn.server',
        'uvicorn.logging', 'uvicorn.loops', 'uvicorn.loops.auto',
        'uvicorn.loops.asyncio', 'uvicorn.protocols',
        'uvicorn.protocols.http', 'uvicorn.protocols.http.auto',
        'uvicorn.protocols.http.h11_impl', 'uvicorn.protocols.http.httptools_impl',
        'uvicorn.protocols.websockets', 'uvicorn.protocols.websockets.auto',
        'uvicorn.protocols.websockets.websockets_impl',
        'uvicorn.protocols.websockets.wsproto_impl',
        'uvicorn.lifespan', 'uvicorn.lifespan.on', 'uvicorn.lifespan.off',
        'uvicorn.middleware', 'uvicorn.middleware.proxy_headers',
        'uvicorn.middleware.wsgi',
        # h11 (HTTP-парсер uvicorn)
        'h11', 'h11._readers', 'h11._writers', 'h11._events',
        'h11._headers', 'h11._receivebuffer', 'h11._state',
        'h11._util', 'h11._connection',
        # Starlette
        'starlette', 'starlette.applications', 'starlette.routing',
        'starlette.middleware', 'starlette.middleware.cors',
        'starlette.requests', 'starlette.responses', 'starlette.background',
        'starlette.concurrency', 'starlette.datastructures',
        'starlette.exceptions', 'starlette.status', 'starlette.types',
        # FastAPI / Pydantic
        'fastapi', 'fastapi.routing', 'fastapi.middleware.cors',
        'pydantic', 'pydantic.deprecated.class_validators', 'pydantic_core',
        # Anyio
        'anyio', 'anyio.from_thread', 'anyio._backends._asyncio',
        'anyio.abc', 'anyio.streams',
        # SQLAlchemy
        'aiosqlite', 'sqlalchemy', 'sqlalchemy.dialects.sqlite',
        'sqlalchemy.dialects.sqlite.aiosqlite',
        'sqlalchemy.ext.asyncio', 'sqlalchemy.pool',
        'sqlalchemy.orm', 'sqlalchemy.sql',
        # httpx + HTTP/2
        'httpx', 'httpcore', 'httpcore._async', 'httpcore._sync',
        'h2', 'h2.connection', 'h2.config', 'h2.events', 'h2.exceptions',
        'h2.settings', 'h2.stream', 'h2.utilities', 'h2.errors',
        'hpack', 'hpack.hpack', 'hpack.table', 'hyperframe', 'hyperframe.frame',
        # Matplotlib
        'matplotlib', 'matplotlib.backends.backend_qtagg',
        'matplotlib.backends.backend_agg',
        'pyparsing', 'pyparsing.testing',
        # Наши модули
        'app', 'app.backend', 'app.backend.api', 'app.backend.database',
        'app.parser', 'app.parser.dispatcher', 'app.parser.base_parser',
        'app.parser.ostrovok_parser', 'app.parser.avito_parser',
        'app.parser.generic_parser', 'app.parser.sutochno_parser',
        'app.parser.booking_parser', 'app.parser.airbnb_parser',
        'app.analytics', 'app.analytics.engine',
        'app.gui', 'app.gui.main_window', 'app.gui.styles',
        'app.gui.api_client', 'app.gui.screens.property_list',
        'app.gui.screens.add_property', 'app.gui.screens.detail',
        'app.gui.widgets.chart_widget', 'app.gui.widgets.date_picker',
        'app.utils', 'app.utils.config',
        # Windows registry
        'winreg',
        # Stdlib нужные зависимостям
        'unittest', 'unittest.mock', 'email', 'email.mime',
        'email.mime.text', 'email.mime.multipart',
        'multipart', 'numpy', 'pandas',
        # Asyncio internals
        'asyncio', 'asyncio.events', 'asyncio.futures',
        'asyncio.tasks', 'asyncio.streams',
    ],
    hookspath=['.'],      # кастомный хук лежит рядом
    hooksconfig={},
    runtime_hooks=['runtime_hook.py'],
    excludes=['tkinter'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='RentalAnalyzer',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,     # ← False = без чёрного консольного окна
    icon=None,         # ← сюда можно добавить путь к .ico файлу
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=['vcruntime140.dll', 'python*.dll'],
    name='RentalAnalyzer',
)
