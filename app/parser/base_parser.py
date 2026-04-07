"""
Base parser — proxy-aware, retry logic, block detection.
"""
from __future__ import annotations

import asyncio
import random
import re
import os
import sys
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from loguru import logger

from app.utils.config import (
    PARSER_TIMEOUT, PARSER_RETRY_COUNT,
    PARSER_RETRY_DELAY, PARSER_USER_AGENTS
)


class ParserError(Exception):
    pass

class BlockedError(ParserError):
    pass

class CaptchaError(ParserError):
    pass

class DataNotFoundError(ParserError):
    pass


def _detect_system_proxy() -> Optional[str]:
    """Читаем системный прокси из переменных окружения."""
    for key in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY"):
        val = os.environ.get(key)
        if val:
            return val
    # Windows registry proxy (через winreg)
    if sys.platform == "win32":
        try:
            import winreg
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
            ) as key:
                enabled, _ = winreg.QueryValueEx(key, "ProxyEnable")
                if enabled:
                    proxy_server, _ = winreg.QueryValueEx(key, "ProxyServer")
                    if proxy_server:
                        if not proxy_server.startswith("http"):
                            proxy_server = f"http://{proxy_server}"
                        logger.debug(f"BaseParser: detected Windows proxy: {proxy_server}")
                        return proxy_server
        except Exception:
            pass
    return None


class BaseParser(ABC):
    """
    Abstract base for all site parsers.
    Auto-detects and uses system proxy for Playwright.
    """

    def __init__(self):
        self._browser = None
        self._playwright = None
        self._proxy: Optional[str] = _detect_system_proxy()
        if self._proxy:
            logger.info(f"BaseParser: using system proxy {self._proxy}")
        else:
            logger.debug("BaseParser: no system proxy detected")

    def _random_ua(self) -> str:
        return random.choice(PARSER_USER_AGENTS)

    def _playwright_proxy_config(self) -> Optional[dict]:
        """Формируем конфиг прокси для Playwright."""
        if not self._proxy:
            return None
        proxy_url = self._proxy
        config = {"server": proxy_url}
        # Если прокси требует аутентификацию (http://user:pass@host:port)
        m = re.match(r'https?://([^:@]+):([^@]+)@', proxy_url)
        if m:
            config["username"] = m.group(1)
            config["password"] = m.group(2)
        return config

    async def _get_browser(self):
        """Lazy browser initialization с поддержкой прокси и exe-окружения."""
        if self._browser is None:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()

            launch_args = [
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1366,768",
            ]

            proxy_config = self._playwright_proxy_config()

            # В exe-режиме Playwright может не найти chromium автоматически.
            # Ищем executable_path явно.
            executable_path = self._find_chromium_executable()
            if executable_path:
                logger.info(f"BaseParser: using chromium at {executable_path}")

            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=launch_args,
                proxy=proxy_config,
                executable_path=executable_path,  # None = ищет сам
            )
            logger.debug(
                f"BaseParser: browser launched "
                f"proxy={'yes' if proxy_config else 'no'}"
            )
        return self._browser

    def _find_chromium_executable(self) -> Optional[str]:
        """Ищем chrome.exe внутри exe-пакета или рядом с exe."""
        import sys
        from pathlib import Path

        search_roots = []

        if getattr(sys, 'frozen', False):
            # Внутри PyInstaller
            search_roots.append(Path(sys._MEIPASS) / "ms-playwright")
            search_roots.append(Path(sys.executable).parent / "ms-playwright")

        # Системный путь Playwright
        pw_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
        if pw_path:
            search_roots.append(Path(pw_path))

        import platform
        exe_name = "chrome.exe" if platform.system() == "Windows" else "chrome"

        for root in search_roots:
            if not root.exists():
                continue
            # Ищем рекурсивно chrome.exe / chrome
            for found in root.rglob(exe_name):
                if found.is_file():
                    logger.debug(f"Found chromium: {found}")
                    return str(found)

        return None  # Playwright найдёт сам через PATH
        return self._browser

    async def _new_context(self):
        browser = await self._get_browser()
        context = await browser.new_context(
            user_agent=self._random_ua(),
            viewport={"width": 1366, "height": 768},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            extra_http_headers={
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            ignore_https_errors=True,   # прокси часто подменяют SSL
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
            window.chrome = { runtime: {} };
        """)
        return context

    async def _human_delay(self, min_s: float = 1.0, max_s: float = 3.0):
        await asyncio.sleep(random.uniform(min_s, max_s))

    def _detect_block(self, html: str) -> bool:
        """
        Улучшенная детекция блокировки.
        Слова 'cloudflare' и 'forbidden' могут быть в скриптах аналитики —
        проверяем контекст и несколько признаков одновременно.
        """
        if len(html) < 2000:
            # Слишком короткий HTML — почти наверняка блок
            block_words = ["captcha", "robot", "forbidden", "access denied",
                           "429", "cloudflare", "ddos-guard"]
            html_lower = html.lower()
            return any(w in html_lower for w in block_words)

        # Для нормальных страниц требуем совпадение нескольких признаков
        html_lower = html.lower()
        hard_blocks = [
            "access denied",
            "you have been blocked",
            "вы заблокированы",
            "слишком много запросов",
            "429 too many",
            "403 forbidden",
        ]
        if any(w in html_lower for w in hard_blocks):
            return True

        # Капча — однозначная блокировка
        captcha_signs = [
            'type="checkbox"' in html and "captcha" in html_lower,
            "recaptcha" in html_lower,
            "hcaptcha" in html_lower,
            "cf-challenge" in html_lower,
        ]
        if any(captcha_signs):
            return True

        return False

    def _extract_price_from_text(self, text: str) -> Optional[float]:
        text = re.sub(r"(\d)\s+(\d)", r"\1\2", text)
        matches = re.findall(r"\d[\d\s.,]*(?:\s*(?:₽|руб|rub|RUB|\$|€|USD|EUR))?", text)
        for m in matches:
            clean = re.sub(r"[^\d.,]", "", m).replace(",", ".")
            if clean:
                try:
                    val = float(clean.split(".")[0])
                    if 100 <= val <= 1_000_000:
                        return val
                except ValueError:
                    continue
        return None

    async def fetch(self, url: str) -> Dict[str, Any]:
        for attempt in range(1, PARSER_RETRY_COUNT + 1):
            try:
                result = await self._fetch_once(url)
                logger.debug(f"[{self.__class__.__name__}] attempt={attempt} price={result.get('price')} status={result.get('status')}")
                return result
            except CaptchaError as e:
                logger.warning(f"[{self.__class__.__name__}] Captcha attempt {attempt}: {e}")
                if attempt < PARSER_RETRY_COUNT:
                    await asyncio.sleep(PARSER_RETRY_DELAY * attempt)
                else:
                    return self._unavailable("captcha", str(e))
            except BlockedError as e:
                logger.warning(f"[{self.__class__.__name__}] Blocked attempt {attempt}: {e}")
                if attempt < PARSER_RETRY_COUNT:
                    await asyncio.sleep(PARSER_RETRY_DELAY * attempt * 2)
                else:
                    return self._unavailable("blocked", str(e))
            except DataNotFoundError as e:
                return self._unavailable("not_found", str(e))
            except Exception as e:
                logger.error(f"[{self.__class__.__name__}] Error attempt {attempt}: {e}")
                if attempt < PARSER_RETRY_COUNT:
                    await asyncio.sleep(PARSER_RETRY_DELAY)
                else:
                    return self._unavailable("error", str(e))
        return self._unavailable("error", "Max retries exceeded")

    def _unavailable(self, status: str, error: str) -> Dict[str, Any]:
        return {"price": None, "title": None, "external_id": None,
                "status": status, "error": error}

    @abstractmethod
    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        ...

    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._browser = None
        self._playwright = None
