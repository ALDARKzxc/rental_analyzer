"""Generic parser — tries heuristic extraction on any page."""
from __future__ import annotations
import re
from typing import Dict, Any, Optional
from loguru import logger
from app.parser.base_parser import BaseParser, BlockedError
from app.utils.config import PARSER_TIMEOUT


class GenericParser(BaseParser):

    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        context = await self._new_context()
        page = await context.new_page()
        try:
            await self._human_delay(1, 2)
            response = await page.goto(url, timeout=PARSER_TIMEOUT, wait_until="domcontentloaded")

            if response and response.status in (403, 429, 503):
                raise BlockedError(f"HTTP {response.status}")

            await self._human_delay(1, 2)
            html = await page.content()

            if self._detect_block(html):
                raise BlockedError("Blocked")

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

            # Try meta price
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

            if not price:
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
            await page.close()
            await context.close()


class BookingParser(GenericParser):
    """Booking.com — uses generic heuristics."""
    pass


class AirbnbParser(GenericParser):
    """Airbnb — uses generic heuristics."""
    pass
