"""
Base parser — proxy-aware, retry logic, block detection.
"""
from __future__ import annotations

import asyncio
import json
import random
import re
import os
import sys
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from urllib.parse import urljoin

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
    """Abstract base for all site parsers."""

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
        if not self._proxy:
            return None
        proxy_url = self._proxy
        config = {"server": proxy_url}
        m = re.match(r'https?://([^:@]+):([^@]+)@', proxy_url)
        if m:
            config["username"] = m.group(1)
            config["password"] = m.group(2)
        return config

    async def _get_browser(self):
        """Lazy browser initialization с поддержкой прокси. Перезапускает браузер если упал."""
        if self._browser is not None:
            try:
                if not self._browser.is_connected():
                    logger.warning(f"[{self.__class__.__name__}] Browser disconnected, relaunching")
                    self._browser = None
                    if self._playwright:
                        try:
                            await self._playwright.stop()
                        except Exception:
                            pass
                        self._playwright = None
            except Exception:
                self._browser = None
                self._playwright = None

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
            executable_path = self._find_chromium_executable()
            if executable_path:
                logger.info(f"BaseParser: using chromium at {executable_path}")

            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=launch_args,
                proxy=proxy_config,
                executable_path=executable_path,
            )
            logger.debug(
                f"BaseParser: browser launched "
                f"proxy={'yes' if proxy_config else 'no'}"
            )
        return self._browser

    def _find_chromium_executable(self) -> Optional[str]:
        """Ищем chrome.exe внутри exe-пакета или рядом с exe."""
        from pathlib import Path
        import platform

        search_roots = []
        if getattr(sys, 'frozen', False):
            search_roots.append(Path(sys._MEIPASS) / "ms-playwright")
            search_roots.append(Path(sys.executable).parent / "ms-playwright")

        pw_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
        if pw_path:
            search_roots.append(Path(pw_path))

        exe_name = "chrome.exe" if platform.system() == "Windows" else "chrome"

        for root in search_roots:
            if not root.exists():
                continue
            for found in root.rglob(exe_name):
                if found.is_file():
                    logger.debug(f"Found chromium: {found}")
                    return str(found)

        return None

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
            ignore_https_errors=True,
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
            window.chrome = { runtime: {} };
        """)
        return context

    async def _human_delay(self, min_s: float = 0.4, max_s: float = 1.0):
        await asyncio.sleep(random.uniform(min_s, max_s))

    def _detect_block(self, html: str) -> bool:
        if len(html) < 2000:
            block_words = ["captcha", "robot", "forbidden", "access denied",
                           "429", "cloudflare", "ddos-guard"]
            html_lower = html.lower()
            return any(w in html_lower for w in block_words)

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

        captcha_signs = [
            'type="checkbox"' in html and "captcha" in html_lower,
            "recaptcha" in html_lower,
            "hcaptcha" in html_lower,
            "cf-challenge" in html_lower,
        ]
        return any(captcha_signs)

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

    async def fetch_metadata(self, url: str) -> Dict[str, Any]:
        for attempt in range(1, PARSER_RETRY_COUNT + 1):
            try:
                metadata = await self._fetch_metadata_once(url)
                logger.debug(
                    f"[{self.__class__.__name__}] metadata attempt={attempt} "
                    f"title={bool(metadata.get('title'))} "
                    f"image={bool(metadata.get('image_url'))}"
                )
                return metadata
            except (BlockedError, CaptchaError, DataNotFoundError) as e:
                logger.warning(
                    f"[{self.__class__.__name__}] Metadata attempt {attempt}: {e}"
                )
                if attempt < PARSER_RETRY_COUNT:
                    await asyncio.sleep(PARSER_RETRY_DELAY * attempt)
            except Exception as e:
                logger.warning(
                    f"[{self.__class__.__name__}] Metadata error attempt {attempt}: {e}"
                )
                if attempt < PARSER_RETRY_COUNT:
                    await asyncio.sleep(PARSER_RETRY_DELAY)
        return {}

    async def _fetch_metadata_once(self, url: str) -> Dict[str, Any]:
        context = await self._new_context()
        page = await context.new_page()
        try:
            try:
                response = await page.goto(
                    url,
                    timeout=PARSER_TIMEOUT,
                    wait_until="domcontentloaded",
                )
                if response and response.status in (403, 429, 503):
                    raise BlockedError(f"HTTP {response.status}")
            except BlockedError:
                raise
            except Exception as exc:
                logger.debug(
                    f"[{self.__class__.__name__}] metadata nav warning: {exc}"
                )
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=8_000)
            except Exception:
                pass
            await self._human_delay(0.2, 0.5)

            try:
                html = await page.content()
            except Exception:
                html = ""
            if html and self._detect_block(html):
                raise BlockedError("Blocked")

            return await self._extract_listing_metadata(page, html, url)
        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass

    async def _extract_listing_metadata(
        self,
        page,
        html: str,
        url: str,
    ) -> Dict[str, Any]:
        body_text = await self._page_text(page)
        return {
            "title": await self._extract_listing_title(page, html),
            "image_url": await self._extract_listing_image(page, html, url),
            "address": await self._extract_listing_address(page, html, body_text),
            "guest_capacity": self._extract_guest_capacity(html, body_text),
        }

    async def _extract_listing_title(self, page, html: str) -> Optional[str]:
        title = await self._first_text(
            page,
            [
                "h1[itemprop='name']",
                "[data-testid*='title']",
                "[class*='title'] h1",
                "h1",
            ],
        )
        if title:
            return title[:300]

        for pattern in (
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']',
            r'<title>\s*([^<]{3,300})',
        ):
            match = re.search(pattern, html, re.IGNORECASE | re.S)
            if match:
                return " ".join(match.group(1).split())[:300]

        return None

    async def _extract_listing_image(
        self,
        page,
        html: str,
        url: str,
    ) -> Optional[str]:
        candidate = await self._first_attr(
            page,
            [
                "meta[property='og:image']",
                "meta[name='twitter:image']",
            ],
            "content",
        )
        if not candidate:
            candidate = await self._first_attr(page, ["img[src]"], "src")

        if not candidate:
            for pattern in (
                r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
                r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
            ):
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    candidate = match.group(1)
                    break

        if not candidate:
            for obj in self._jsonld_objects(html):
                image = self._jsonld_image(obj)
                if image:
                    candidate = image
                    break

        if candidate:
            return urljoin(url, candidate)
        return None

    async def _extract_listing_address(
        self,
        page,
        html: str,
        body_text: str,
    ) -> Optional[str]:
        address = await self._first_text(
            page,
            [
                "[itemprop='streetAddress']",
                "[data-testid*='address']",
                "[class*='address']",
                "[class*='location']",
            ],
        )
        if address:
            return address[:500]

        for obj in self._jsonld_objects(html):
            street = self._jsonld_address(obj)
            if street:
                return street[:500]

        match = re.search(
            r"(?:Адрес|Address)\s*[:\-]?\s*([^\n\r]{5,200})",
            body_text,
            re.IGNORECASE,
        )
        if match:
            return " ".join(match.group(1).split())[:500]

        return None

    async def _first_text(self, page, selectors: list[str]) -> Optional[str]:
        for selector in selectors:
            try:
                element = await page.query_selector(selector)
                if not element:
                    continue
                text = (await element.inner_text()).strip()
                if text:
                    return " ".join(text.split())
            except Exception:
                continue
        return None

    async def _first_attr(
        self,
        page,
        selectors: list[str],
        attr_name: str,
    ) -> Optional[str]:
        for selector in selectors:
            try:
                element = await page.query_selector(selector)
                if not element:
                    continue
                value = await element.get_attribute(attr_name)
                if value:
                    return value.strip()
            except Exception:
                continue
        return None

    async def _page_text(self, page) -> str:
        try:
            text = await page.evaluate(
                "() => document.body ? (document.body.innerText || document.body.textContent || '') : ''"
            )
        except Exception:
            return ""
        return " ".join(str(text).split())

    def _jsonld_objects(self, html: str) -> list[Any]:
        objects: list[Any] = []
        for match in re.finditer(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html,
            re.IGNORECASE | re.S,
        ):
            try:
                parsed = json.loads(match.group(1))
            except Exception:
                continue
            if isinstance(parsed, list):
                objects.extend(parsed)
            else:
                objects.append(parsed)
        return objects

    def _jsonld_image(self, obj: Any) -> Optional[str]:
        if isinstance(obj, dict):
            image = obj.get("image")
            if isinstance(image, str) and image:
                return image
            if isinstance(image, list):
                for item in image:
                    if isinstance(item, str) and item:
                        return item
                    if isinstance(item, dict):
                        url = item.get("url")
                        if isinstance(url, str) and url:
                            return url
            if isinstance(image, dict):
                url = image.get("url")
                if isinstance(url, str) and url:
                    return url
            for value in obj.values():
                found = self._jsonld_image(value)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = self._jsonld_image(item)
                if found:
                    return found
        return None

    def _jsonld_address(self, obj: Any) -> Optional[str]:
        if isinstance(obj, dict):
            address = obj.get("address")
            if isinstance(address, dict):
                street = address.get("streetAddress")
                if isinstance(street, str) and street:
                    return " ".join(street.split())
            for value in obj.values():
                found = self._jsonld_address(value)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = self._jsonld_address(item)
                if found:
                    return found
        return None

    def _extract_guest_capacity(self, html: str, body_text: str) -> Optional[int]:
        for obj in self._jsonld_objects(html):
            found = self._jsonld_guest_capacity(obj)
            if found:
                return found

        patterns = (
            r"до\s+(\d{1,2})\s+гост",
            r"(\d{1,2})\s+гост[ьяей]",
            r"for\s+(\d{1,2})\s+guest",
            r"sleeps\s+(\d{1,2})",
            r"capacity\s*[:\-]?\s*(\d{1,2})",
        )
        for pattern in patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if not match:
                continue
            try:
                value = int(match.group(1))
            except ValueError:
                continue
            if 1 <= value <= 30:
                return value

        return None

    def _jsonld_guest_capacity(self, obj: Any) -> Optional[int]:
        if isinstance(obj, dict):
            for key in (
                "occupancy",
                "maxOccupancy",
                "maximumAttendeeCapacity",
                "guestCapacity",
                "numberOfGuests",
                "guests",
                "guest_count",
                "person_capacity",
                "capacity",
                "max_guests",
            ):
                value = obj.get(key)
                if isinstance(value, (int, float)) and 1 <= int(value) <= 30:
                    return int(value)
                if isinstance(value, str):
                    match = re.search(r"\d{1,2}", value)
                    if match:
                        amount = int(match.group(0))
                        if 1 <= amount <= 30:
                            return amount
            for value in obj.values():
                found = self._jsonld_guest_capacity(value)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = self._jsonld_guest_capacity(item)
                if found:
                    return found
        return None

    async def fetch_amenities(self, url: str) -> Dict[str, Any]:
        """
        Возвращает словарь:
            {"amenities":  {"<group_name>": ["item1", ...], ...},
             "description": "...",
             "key_facts":  ["До 6 гостей", "55 кв.м", ...]}

        Используется ИСКЛЮЧИТЕЛЬНО разделом "Сравнение объектов" по явной
        команде пользователя. На парсинг цен не влияет.
        """
        for attempt in range(1, PARSER_RETRY_COUNT + 1):
            try:
                data = await self._fetch_amenities_once(url)
                return data or {"amenities": {}, "description": None, "key_facts": []}
            except (BlockedError, CaptchaError) as e:
                logger.warning(
                    f"[{self.__class__.__name__}] Amenities attempt {attempt}: {e}"
                )
                if attempt < PARSER_RETRY_COUNT:
                    await asyncio.sleep(PARSER_RETRY_DELAY * attempt)
            except Exception as e:
                logger.warning(
                    f"[{self.__class__.__name__}] Amenities error attempt {attempt}: {e}"
                )
                if attempt < PARSER_RETRY_COUNT:
                    await asyncio.sleep(PARSER_RETRY_DELAY)
        return {"amenities": {}, "description": None, "key_facts": []}

    async def _fetch_amenities_once(self, url: str) -> Dict[str, Any]:
        """
        По умолчанию — не реализовано (для парсеров других сайтов).
        Раздел "Сравнение" просто покажет пустые удобства.
        """
        return {"amenities": {}, "description": None, "key_facts": []}

    async def fetch(self, url: str) -> Dict[str, Any]:
        for attempt in range(1, PARSER_RETRY_COUNT + 1):
            try:
                result = await self._fetch_once(url)
                logger.debug(
                    f"[{self.__class__.__name__}] attempt={attempt} "
                    f"price={result.get('price')} status={result.get('status')}"
                )
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
                    await asyncio.sleep(PARSER_RETRY_DELAY * attempt)
                else:
                    return self._unavailable("blocked", str(e))
            except DataNotFoundError as e:
                return self._unavailable("not_found", str(e))
            except Exception as e:
                logger.error(f"[{self.__class__.__name__}] Error attempt {attempt}: {e}")
                # Сбрасываем браузер между попытками — следующая попытка стартует чисто
                try:
                    await self.close()
                except Exception:
                    pass
                if attempt < PARSER_RETRY_COUNT:
                    await asyncio.sleep(PARSER_RETRY_DELAY)
                else:
                    return self._unavailable("error", str(e)[:500])
        return self._unavailable("error", "Max retries exceeded")

    def _unavailable(self, status: str, error: str) -> Dict[str, Any]:
        return {"price": None, "title": None, "external_id": None,
                "status": status, "error": error}

    @abstractmethod
    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        ...

    async def close(self):
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
        self._browser = None
        self._playwright = None
