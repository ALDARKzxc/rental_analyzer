"""Sutochno.ru parser."""
from __future__ import annotations
import re
import json
from typing import Dict, Any, Optional
from loguru import logger
from app.parser.base_parser import BaseParser, BlockedError, CaptchaError
from app.utils.config import PARSER_TIMEOUT


class SutochnoParser(BaseParser):

    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        context = await self._new_context()
        page = await context.new_page()
        try:
            await self._human_delay(1, 2)
            response = await page.goto(url, timeout=PARSER_TIMEOUT, wait_until="domcontentloaded")

            if response and response.status in (403, 429, 503):
                raise BlockedError(f"HTTP {response.status}")

            await self._human_delay(2, 3)
            html = await page.content()

            if self._detect_block(html):
                raise BlockedError("Blocked")

            title = None
            try:
                h1 = await page.query_selector("h1")
                if h1:
                    title = (await h1.inner_text()).strip()[:300]
            except Exception:
                pass

            price = None
            price_selectors = [
                ".object-price__value",
                "[class*='price']",
                ".Price",
                "[data-testid*='price']",
            ]
            for sel in price_selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        text = (await el.inner_text()).strip()
                        p = self._extract_price_from_text(text)
                        if p:
                            price = p
                            break
                except Exception:
                    pass

            if price is None:
                price = self._extract_price_from_text(html[:30000])

            external_id = re.search(r"/(\d+)", url)
            ext_id = external_id.group(1) if external_id else None

            return {
                "price": price,
                "title": title or "Sutochno listing",
                "external_id": ext_id,
                "status": "ok" if price else "not_found",
                "error": None if price else "Price not found"
            }
        finally:
            await page.close()
            await context.close()
