"""
Parser dispatcher — routes URL to the correct site parser.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse
from typing import Dict, Any, Optional

from loguru import logger

from app.utils.config import SUPPORTED_SITES


class ParserDispatcher:

    def detect_site(self, url: str) -> Optional[str]:
        host = urlparse(url).netloc.lower().lstrip("www.")
        for domain, name in SUPPORTED_SITES.items():
            if domain in host:
                return name
        return "generic"

    async def parse(self, url: str) -> Dict[str, Any]:
        """Dispatch to appropriate parser and return result dict."""
        site = self.detect_site(url)
        logger.info(f"Dispatching parse: site={site} url={url[:80]}")

        try:
            if site == "ostrovok":
                from app.parser.ostrovok_parser import OstrovokParser
                parser = OstrovokParser()
            elif site == "avito":
                from app.parser.avito_parser import AvitoParser
                parser = AvitoParser()
            elif site == "sutochno":
                from app.parser.sutochno_parser import SutochnoParser
                parser = SutochnoParser()
            elif site == "booking":
                from app.parser.booking_parser import BookingParser
                parser = BookingParser()
            elif site == "airbnb":
                from app.parser.airbnb_parser import AirbnbParser
                parser = AirbnbParser()
            else:
                from app.parser.generic_parser import GenericParser
                parser = GenericParser()

            return await parser.fetch(url)

        except Exception as e:
            logger.error(f"Dispatcher error for {url}: {e}")
            return {
                "price": None,
                "title": None,
                "external_id": None,
                "status": "error",
                "error": str(e)
            }
