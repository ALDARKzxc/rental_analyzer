"""
Parser dispatcher — routes URL to the correct site parser.
Singleton parser instances — Chromium browser is reused across parses
(no relaunch overhead per property).
"""
from __future__ import annotations

from urllib.parse import urlparse
from typing import Dict, Any, Optional

from loguru import logger

from app.utils.config import SUPPORTED_SITES

# Module-level singleton instances keyed by site name.
# The parser's _browser stays alive between calls; base_parser._get_browser()
# checks is_connected() and relaunches automatically if it crashed.
_PARSER_INSTANCES: dict[str, "object"] = {}


def _make_parser(site: str):
    if site == "ostrovok":
        from app.parser.ostrovok_parser import OstrovokParser
        return OstrovokParser()
    elif site == "avito":
        from app.parser.avito_parser import AvitoParser
        return AvitoParser()
    elif site == "sutochno":
        from app.parser.sutochno_parser import SutochnoParser
        return SutochnoParser()
    elif site == "booking":
        from app.parser.booking_parser import BookingParser
        return BookingParser()
    elif site == "airbnb":
        from app.parser.airbnb_parser import AirbnbParser
        return AirbnbParser()
    else:
        from app.parser.generic_parser import GenericParser
        return GenericParser()


async def close_all_parsers():
    """Graceful shutdown — close all browser instances."""
    for site, parser in list(_PARSER_INSTANCES.items()):
        try:
            await parser.close()
            logger.debug(f"Dispatcher: closed browser for site={site}")
        except Exception:
            pass
    _PARSER_INSTANCES.clear()


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

        # Reuse existing parser (keeps browser alive); create on first use.
        if site not in _PARSER_INSTANCES:
            _PARSER_INSTANCES[site] = _make_parser(site)
            logger.debug(f"Dispatcher: created new parser for site={site}")

        parser = _PARSER_INSTANCES[site]

        try:
            return await parser.fetch(url)
        except Exception as e:
            logger.error(f"Dispatcher error for {url}: {e}")
            # If parser crashed hard, drop the instance so next call gets a fresh one.
            _PARSER_INSTANCES.pop(site, None)
            return {
                "price": None,
                "title": None,
                "external_id": None,
                "status": "error",
                "error": str(e),
            }

    async def fetch_metadata(self, url: str) -> Dict[str, Any]:
        """Dispatch metadata extraction to the matching parser."""
        site = self.detect_site(url)
        logger.info(f"Dispatching metadata: site={site} url={url[:80]}")

        if site not in _PARSER_INSTANCES:
            _PARSER_INSTANCES[site] = _make_parser(site)
            logger.debug(f"Dispatcher: created new parser for site={site}")

        parser = _PARSER_INSTANCES[site]

        try:
            return await parser.fetch_metadata(url)
        except Exception as e:
            logger.warning(f"Dispatcher metadata error for {url}: {e}")
            return {}

    async def fetch_amenities(self, url: str) -> Dict[str, Any]:
        """Dispatch amenities extraction (раздел «Сравнение»)."""
        site = self.detect_site(url)
        logger.info(f"Dispatching amenities: site={site} url={url[:80]}")

        if site not in _PARSER_INSTANCES:
            _PARSER_INSTANCES[site] = _make_parser(site)
            logger.debug(f"Dispatcher: created new parser for site={site}")

        parser = _PARSER_INSTANCES[site]

        try:
            return await parser.fetch_amenities(url)
        except Exception as e:
            logger.warning(f"Dispatcher amenities error for {url}: {e}")
            return {"amenities": {}, "description": None, "key_facts": []}
