"""
Runtime hook — запускается самым первым внутри exe.
Настраивает пути для Playwright Chromium.
"""
import os
import sys
from pathlib import Path


if getattr(sys, 'frozen', False):
    # Папка где лежит exe (dist/RentalAnalyzer/)
    exe_dir = Path(sys.executable).parent
    # Внутренняя папка PyInstaller с распакованными файлами
    bundle_dir = Path(sys._MEIPASS)

    # Playwright ищет браузеры по PLAYWRIGHT_BROWSERS_PATH
    # Мы положили chromium в ms-playwright/chromium-XXXX/
    ms_pw = bundle_dir / "ms-playwright"
    if ms_pw.exists():
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(ms_pw)
    else:
        # Запасной путь — рядом с exe
        ms_pw2 = exe_dir / "ms-playwright"
        if ms_pw2.exists():
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(ms_pw2)

    # Playwright также проверяет эту переменную
    os.environ["PLAYWRIGHT_DRIVER_PATH"] = str(bundle_dir / "playwright")

    # Убираем прокси для всех соединений
    for v in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy",
              "https_proxy", "ALL_PROXY", "all_proxy"):
        os.environ.pop(v, None)
    os.environ["NO_PROXY"] = "127.0.0.1,localhost,::1"
    os.environ["no_proxy"] = "127.0.0.1,localhost,::1"
