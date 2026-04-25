"""
Avito.ru parser — многоуровневая стратегия:

1. Avito Public Item API  (/web/1/item/{id})  — JSON, без браузера
2. httpx + HTML парсинг                       — JSON, без браузера
3. Playwright с расширенным stealth            — если API недоступно
"""
from __future__ import annotations

import re
import json
import asyncio
import random
from typing import Dict, Any, Optional

import httpx
from loguru import logger

from app.parser.base_parser import (
    BaseParser, BlockedError, CaptchaError, DataNotFoundError
)
from app.utils.config import PARSER_TIMEOUT, PARSER_USER_AGENTS

_ITEM_API = "https://www.avito.ru/web/1/item/{item_id}"


class AvitoParser(BaseParser):

    async def _fetch_metadata_once(self, url: str) -> Dict[str, Any]:
        vp = random.choice([
            {"width": 1366, "height": 768},
            {"width": 1440, "height": 900},
            {"width": 1920, "height": 1080},
        ])
        context = await self._stealth_context(vp)
        page = await context.new_page()
        try:
            try:
                await page.goto(
                    "https://www.avito.ru/",
                    timeout=12_000,
                    wait_until="domcontentloaded",
                )
                await self._human_delay(0.6, 1.2)
            except Exception:
                pass

            response = await page.goto(
                url,
                timeout=PARSER_TIMEOUT,
                wait_until="domcontentloaded",
            )
            if response and response.status in (403, 429, 503):
                raise BlockedError(f"HTTP {response.status}")

            await self._human_delay(0.8, 1.5)
            await self._mouse_wander(page)

            html = await page.content()
            if self._detect_block(html):
                if "captcha" in html.lower():
                    raise CaptchaError("Captcha detected")
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

    async def _extract_listing_metadata(self, page, html: str, url: str) -> Dict[str, Any]:
        metadata = await super()._extract_listing_metadata(page, html, url)

        title = await self._pw_title(page)
        if title and title != "Unknown":
            metadata["title"] = title

        address = await self._first_text(
            page,
            [
                "[data-marker='item-view/item-address']",
                "[data-marker='item-view/address']",
                "[itemprop='address']",
                "[class*='address']",
            ],
        )
        if address:
            metadata["address"] = address

        return metadata

    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        item_id = self._extract_avito_id(url)

        # Strategy 1: public REST API
        if item_id:
            result = await self._try_api(item_id)
            if result and result.get("price"):
                logger.info(f"AvitoParser: API OK id={item_id} price={result['price']}")
                return result

        # Strategy 2: httpx HTML
        result = await self._try_httpx_html(url)
        if result and result.get("price"):
            logger.info(f"AvitoParser: httpx-HTML OK price={result['price']}")
            return result

        # Strategy 3: Playwright stealth
        logger.info("AvitoParser: Playwright stealth fallback")
        return await self._try_playwright(url, item_id)

    # ── Strategy 1 ──────────────────────────────────────────────

    async def _try_api(self, item_id: str) -> Optional[Dict[str, Any]]:
        headers = {
            "User-Agent": random.choice(PARSER_USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Referer": "https://www.avito.ru/",
            "X-Requested-With": "XMLHttpRequest",
        }
        api_url = _ITEM_API.format(item_id=item_id)
        try:
            async with httpx.AsyncClient(timeout=12, follow_redirects=True, trust_env=False) as client:
                await asyncio.sleep(random.uniform(0.2, 0.6))
                resp = await client.get(api_url, headers=headers)

            if resp.status_code not in (200, 201):
                logger.debug(f"AvitoParser API: HTTP {resp.status_code}")
                return None

            data = resp.json()
            price = self._price_from_json(data)
            title = self._title_from_json(data)
            if price:
                return {"price": price, "title": title, "external_id": item_id,
                        "status": "ok", "error": None}
        except Exception as e:
            logger.debug(f"AvitoParser API failed: {e}")
        return None

    def _price_from_json(self, data: dict) -> Optional[float]:
        paths = [
            ["price", "value"],
            ["item", "price", "value"],
            ["priceDetailed", "value"],
            ["item", "priceDetailed", "value"],
            ["price"],
        ]
        for path in paths:
            node = data
            for key in path:
                node = node.get(key) if isinstance(node, dict) else None
            if node is not None:
                try:
                    val = float(str(node).replace("\xa0", "").replace(" ", ""))
                    if 100 <= val <= 10_000_000:
                        return val
                except (ValueError, TypeError):
                    pass
        return None

    def _title_from_json(self, data: dict) -> Optional[str]:
        for path in [["title"], ["item", "title"], ["name"]]:
            node = data
            for key in path:
                node = node.get(key) if isinstance(node, dict) else None
            if node and isinstance(node, str):
                return node[:300]
        return None

    # ── Strategy 2 ──────────────────────────────────────────────

    async def _try_httpx_html(self, url: str) -> Optional[Dict[str, Any]]:
        headers = {
            "User-Agent": random.choice(PARSER_USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
        }
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True, trust_env=False) as client:
                await asyncio.sleep(random.uniform(0.3, 0.8))
                resp = await client.get(url, headers=headers)

            if resp.status_code in (403, 429, 503):
                return None

            html = resp.text
            if self._detect_block(html):
                return None

            # Быстрый выход: объявление снято с публикации
            html_lower = html.lower()
            REMOVED = ["снято с публикации", "объявление удалено",
                       "объявление не найдено", "объявление недоступно",
                       "item is not available", "listing removed"]
            if any(p in html_lower for p in REMOVED):
                ext_id = self._extract_avito_id(url)
                return {"price": None, "title": None, "external_id": ext_id,
                        "status": "not_found",
                        "error": "Объявление снято с публикации"}

            ext_id = self._extract_avito_id(url)
            price = (
                self._price_from_inline_json(html)
                or self._extract_from_jsonld(html)
                or self._extract_price_from_text(html[:80_000])
            )
            title = self._title_from_html(html)
            return {
                "price": price, "title": title, "external_id": ext_id,
                "status": "ok" if price else "not_found",
                "error": None if price else "Цена не найдена в HTML",
            }
        except Exception as e:
            logger.debug(f"AvitoParser httpx-HTML failed: {e}")
            return None

    def _price_from_inline_json(self, html: str) -> Optional[float]:
        patterns = [
            r'"priceDetailed"\s*:\s*\{[^}]*"value"\s*:\s*(\d+)',
            r'"price"\s*:\s*\{[^}]*"value"\s*:\s*(\d+)',
            r'"price"[^}]{0,300}"value"\s*:\s*(\d+)',
        ]
        for p in patterns:
            m = re.search(p, html)
            if m:
                try:
                    val = float(m.group(1))
                    if 100 <= val <= 10_000_000:
                        return val
                except ValueError:
                    pass
        return None

    def _title_from_html(self, html: str) -> Optional[str]:
        for p in [r'"title"\s*:\s*"([^"]{5,200})"', r'<h1[^>]*>([^<]{5,200})<']:
            m = re.search(p, html)
            if m:
                return m.group(1).strip()[:300]
        return None

    # ── Strategy 3 ──────────────────────────────────────────────

    async def _try_playwright(self, url: str, item_id: Optional[str]) -> Dict[str, Any]:
        vp = random.choice([
            {"width": 1366, "height": 768},
            {"width": 1440, "height": 900},
            {"width": 1920, "height": 1080},
        ])
        context = await self._stealth_context(vp)
        page = await context.new_page()
        try:
            # Warm up: visit main page first
            try:
                await page.goto("https://www.avito.ru/", timeout=12_000,
                                wait_until="domcontentloaded")
                await self._human_delay(0.8, 1.5)
            except Exception:
                pass

            response = await page.goto(url, timeout=PARSER_TIMEOUT,
                                       wait_until="domcontentloaded")
            if response and response.status in (403, 429, 503):
                raise BlockedError(f"HTTP {response.status}")

            await self._human_delay(1.0, 2.0)
            await self._mouse_wander(page)

            html = await page.content()
            if self._detect_block(html):
                if "captcha" in html.lower():
                    raise CaptchaError("Captcha detected")
                raise BlockedError("Blocked")

            title = await self._pw_title(page)
            price = await self._pw_price(page, html)

            return {
                "price": price, "title": title, "external_id": item_id,
                "status": "ok" if price else "not_found",
                "error": None if price else "Цена не найдена",
            }
        finally:
            await page.close()
            await context.close()

    async def _stealth_context(self, vp: dict):
        browser = await self._get_browser()
        ctx = await browser.new_context(
            user_agent=random.choice(PARSER_USER_AGENTS),
            viewport=vp,
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            extra_http_headers={
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                "sec-ch-ua": '"Chromium";v="120", "Google Chrome";v="120"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            },
        )
        await ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU','ru','en'] });
            window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {}, app: {} };
        """)
        return ctx

    async def _mouse_wander(self, page):
        try:
            vp = page.viewport_size or {"width": 1366, "height": 768}
            for _ in range(random.randint(3, 5)):
                await page.mouse.move(
                    random.randint(100, vp["width"] - 100),
                    random.randint(100, vp["height"] - 100),
                )
                await asyncio.sleep(random.uniform(0.1, 0.4))
            await page.evaluate(
                f"window.scrollTo({{top:{random.randint(200,600)},behavior:'smooth'}})"
            )
            await asyncio.sleep(random.uniform(0.5, 1.5))
        except Exception:
            pass

    async def _pw_title(self, page) -> Optional[str]:
        for sel in ["h1[itemprop='name']", "[data-marker='item-view/title-info']", "h1"]:
            try:
                el = await page.query_selector(sel)
                if el:
                    t = (await el.inner_text()).strip()
                    if t:
                        return t[:300]
            except Exception:
                pass
        return "Unknown"

    async def _pw_price(self, page, html: str) -> Optional[float]:
        selectors = [
            "[data-marker='item-view/item-price'] [itemprop='price']",
            "[data-marker='item-view/item-price']",
            "span[itemprop='price']",
            "[class*='price-value']",
            "[class*='price_value']",
            "[class*='ItemPrice']",
        ]
        for sel in selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    c = await el.get_attribute("content")
                    if c:
                        try:
                            return float(c)
                        except ValueError:
                            pass
                    t = (await el.inner_text()).strip()
                    p = self._extract_price_from_text(t)
                    if p:
                        return p
            except Exception:
                pass
        return (
            self._extract_from_jsonld(html)
            or self._price_from_inline_json(html)
            or self._extract_price_from_text(html[:80_000])
        )

    # ── Helpers ─────────────────────────────────────────────────

    def _extract_avito_id(self, url: str) -> Optional[str]:
        m = re.search(r"_(\d{6,12})(?:[/?#]|$)", url)
        if m:
            return m.group(1)
        m2 = re.search(r"/(\d{6,12})(?:[/?#]|$)", url)
        return m2.group(1) if m2 else None

    def _extract_from_jsonld(self, html: str) -> Optional[float]:
        pat = re.compile(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S
        )
        for m in pat.finditer(html):
            try:
                data = json.loads(m.group(1))
                if isinstance(data, dict):
                    offers = data.get("offers", {})
                    if isinstance(offers, dict):
                        p = offers.get("price")
                        if p:
                            return float(p)
            except Exception:
                pass
        return None
