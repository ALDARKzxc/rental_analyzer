#!/usr/bin/env python3
"""Rental Price Analyzer — entry point."""
import sys
import os
import threading
import time

# PyInstaller: добавляем папку с exe в sys.path
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
    sys.path.insert(0, BASE_DIR)
    os.chdir(BASE_DIR)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, BASE_DIR)

# Отключаем прокси для localhost
for _var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
             "ALL_PROXY", "all_proxy"):
    os.environ.pop(_var, None)
os.environ["NO_PROXY"] = "127.0.0.1,localhost,::1"
os.environ["no_proxy"] = "127.0.0.1,localhost,::1"

from loguru import logger
from app.utils.config import DATA_DIR, LOGS_DIR
from app.utils.version import APP_VERSION

LOGS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

logger.add(
    LOGS_DIR / "app.log",
    rotation="10 MB",
    retention="30 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{line} | {message}"
)

# Флаг ошибки запуска бэкенда — виден из основного потока
_backend_error: str = ""


def start_api_server():
    """Запускаем asyncio loop бэкенда и регистрируем его для прямых вызовов."""
    global _backend_error
    import asyncio
    from app.backend.database import init_db
    from app.backend import api as backend_api  # noqa — регистрирует роуты
    from app.gui.api_client import register_backend_loop

    async def _main():
        await init_db()
        loop = asyncio.get_event_loop()
        register_backend_loop(loop)
        logger.info("Backend loop started and registered")
        while True:
            await asyncio.sleep(1)

    try:
        asyncio.run(_main())
    except Exception as e:
        _backend_error = str(e)
        logger.error(f"Backend thread crashed: {e}", exc_info=True)


def wait_for_api(timeout: float = 30.0) -> bool:
    """Ждём пока backend loop зарегистрируется."""
    global _backend_error
    from app.gui import api_client as _ac
    deadline = time.time() + timeout
    while time.time() < deadline:
        # Быстрый выход если бэкенд упал
        if _backend_error:
            return False
        with _ac._backend_loop_lock:
            loop = _ac._backend_loop
        if loop is not None and loop.is_running():
            return True
        time.sleep(0.2)
    return False


def main():
    """Main entry point."""
    from PySide6.QtWidgets import QApplication, QSplashScreen, QMessageBox
    from PySide6.QtCore import Qt
    from PySide6.QtGui import QPixmap, QColor
    from app.gui.main_window import MainWindow

    app = QApplication(sys.argv)
    app.setApplicationName("Rental Price Analyzer")
    app.setOrganizationName("RentalTools")
    app.setApplicationVersion(APP_VERSION.replace("V ", ""))

    # Splash screen
    pix = QPixmap(420, 180)
    pix.fill(QColor("#0C0A0B"))
    splash = QSplashScreen(pix)
    splash.showMessage(
        "RENTAL PRICE ANALYZER\n\nЗапуск сервера...",
        Qt.AlignmentFlag.AlignCenter,
        QColor("#9B2C2C"),
    )
    splash.show()
    app.processEvents()

    # Запускаем API в фоновом потоке
    api_thread = threading.Thread(target=start_api_server, daemon=True)
    api_thread.start()
    logger.info("API thread started")

    splash.showMessage(
        "RENTAL PRICE ANALYZER\n\nОжидание API...",
        Qt.AlignmentFlag.AlignCenter,
        QColor("#9B2C2C"),
    )
    app.processEvents()

    ready = wait_for_api(timeout=30.0)
    splash.close()

    if not ready:
        error_detail = f"\n\nПричина: {_backend_error}" if _backend_error else ""
        log_path = LOGS_DIR / "app.log"
        log_hint = f"\n\nЛог: {log_path}" if log_path.exists() else ""
        QMessageBox.critical(
            None,
            "Ошибка запуска",
            "API-сервер не ответил за 30 секунд.\n\n"
            "Попробуйте:\n"
            "1. Перезапустить приложение\n"
            "2. Временно отключить антивирус\n"
            "3. Проверить порт: netstat -ano | findstr 8765"
            + error_detail + log_hint,
        )
        sys.exit(1)

    logger.info("API ready, launching GUI")
    window = MainWindow()
    window.show()

    logger.info("Application started successfully")
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
