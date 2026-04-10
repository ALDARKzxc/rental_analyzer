"""Generic parser — tries heuristic extraction on any page."""
from __future__ import annotations
import re
from typing import Dict, Any, Optional
from loguru import logger
from app.parser.base_parser import BaseParser, BlockedError
from app.utils.config import PARSER_TIMEOUT

_NO_AVAIL_PATTERNS = [
    "нет свободных", "нет доступных", "недоступно",
    "объект недоступен", "нет предложений",
    "no availability", "not available", "sold out",
    "нет номеров", "комнат нет",
]


class GenericParser(BaseParser):

    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        context = await self._new_context()
        page = await context.new_page()
        try:
            try:
                response = await page.goto(url, timeout=PARSER_TIMEOUT, wait_until="commit")
                if response and response.status in (403, 429, 503):
                    raise BlockedError(f"HTTP {response.status}")
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=8_000)
                except Exception:
                    pass
            except BlockedError:
                raise
            except Exception as e:
                logger.warning(f"GenericParser: nav error ({e.__class__.__name__}) — trying extraction anyway")

            await self._human_delay(0.3, 0.7)

            try:
                html = await page.content()
            except Exception:
                html = ""

            if html and self._detect_block(html):
                raise BlockedError("Blocked")

            # Быстрая проверка недоступности
            if html:
                html_lower = html.lower()
                for pat in _NO_AVAIL_PATTERNS:
                    if pat in html_lower:
                        logger.debug(f"GenericParser: no availability pattern '{pat}'")
                        ext = re.search(r"/(\d{4,})", url)
                        return {
                            "price": None,
                            "title": None,
                            "external_id": ext.group(1) if ext else None,
                            "status": "not_found",
                            "error": "Нет доступных предложений на выбранные даты",
                        }

            title = None
            try:
                el = await page.query_selector("h1")
                if el:
                    title = (await el.inner_text()).strip()[:300]
                if not title:
                    title_el = await page.query_selector("title")
                    if title_el:
                        title = (await title_el.inner_text()).strip()[:300]
            except Exception:
                pass

            price = None
            for sel in ["meta[itemprop='price']", "span[itemprop='price']"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        v = await el.get_attribute("content") or await el.inner_text()
                        p = self._extract_price_from_text(v)
                        if p:
                            price = p
                            break
                except Exception:
                    pass

            if not price and html:
                price = self._extract_price_from_text(html[:40000])

            ext_id = re.search(r"/(\d{4,})", url)

            return {
                "price": price,
                "title": title or url[:100],
                "external_id": ext_id.group(1) if ext_id else None,
                "status": "ok" if price else "not_found",
                "error": None if price else "Could not extract price from generic page"
            }
        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass
            # Браузер НЕ закрываем — singleton dispatcher переиспользует его


class BookingParser(GenericParser):
    """Booking.com — uses generic heuristics."""
    pass


class AirbnbParser(GenericParser):
    """Airbnb — uses generic heuristics."""
    pass
