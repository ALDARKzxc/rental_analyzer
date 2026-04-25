"""Sutochno.ru parser."""
from __future__ import annotations
import re
import json
from typing import Dict, Any, Optional
from loguru import logger
from app.parser.base_parser import BaseParser, BlockedError, CaptchaError, DataNotFoundError
from app.utils.config import PARSER_TIMEOUT

# Признаки недоступности объекта на выбранные даты
_NO_AVAIL_PATTERNS = [
    "нет свободных", "нет доступных", "недоступно",
    "объект недоступен", "занято на эти даты",
    "нет предложений", "не доступен для бронирования",
    "no availability", "not available", "sold out",
]


class SutochnoParser(BaseParser):

    async def _extract_listing_metadata(self, page, html: str, url: str) -> Dict[str, Any]:
        metadata = await super()._extract_listing_metadata(page, html, url)

        title = await self._first_text(page, ["h1"])
        if title:
            metadata["title"] = title

        address = await self._first_text(
            page,
            [
                "[class*='address']",
                "[data-testid*='address']",
                "[itemprop='streetAddress']",
            ],
        )
        if address:
            metadata["address"] = address

        image_url = await self._first_attr(
            page,
            [
                "meta[property='og:image']",
                "meta[name='twitter:image']",
            ],
            "content",
        ) or await self._first_attr(
            page,
            [
                "[class*='gallery'] img[src]",
                "[class*='slider'] img[src]",
                "img[src]",
            ],
            "src",
        )
        if image_url:
            metadata["image_url"] = image_url

        return metadata

    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        context = await self._new_context()
        page = await context.new_page()
        try:
            try:
                response = await page.goto(url, timeout=PARSER_TIMEOUT, wait_until="commit")
                if response and response.status in (403, 429, 503):
                    raise BlockedError(f"HTTP {response.status}")
                # Ждём DOM максимум 8 сек (было 15)
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=8_000)
                except Exception:
                    pass
            except BlockedError:
                raise
            except Exception as e:
                logger.warning(f"SutochnoParser: nav error ({e.__class__.__name__}) — trying extraction anyway")

            await self._human_delay(0.3, 0.7)

            try:
                html = await page.content()
            except Exception:
                html = ""

            if html and self._detect_block(html):
                raise BlockedError("Blocked")

            # Быстрая проверка: нет доступных дат → сразу not_found
            if html:
                html_lower = html.lower()
                for pat in _NO_AVAIL_PATTERNS:
                    if pat in html_lower:
                        logger.debug(f"SutochnoParser: no availability pattern '{pat}'")
                        ext = re.search(r"/(\d+)", url)
                        return {
                            "price": None,
                            "title": None,
                            "external_id": ext.group(1) if ext else None,
                            "status": "not_found",
                            "error": "Нет доступных предложений на выбранные даты",
                        }

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

            if price is None and html:
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
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass
            # Браузер НЕ закрываем — singleton dispatcher переиспользует его
