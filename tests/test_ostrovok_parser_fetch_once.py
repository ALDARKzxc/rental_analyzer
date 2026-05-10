import unittest

from app.parser.ostrovok_parser import OstrovokParser


class _FetchOnceParser(OstrovokParser):
    playwright_result = None
    httpx_result = None

    def __init__(self):
        super().__init__()
        self._proxy = None

    def _extract_hotel_id(self, url: str):
        return "hid"

    def _extract_slug(self, url: str):
        return None

    def _extract_dates(self, url: str):
        return None, None

    def _normalize_url(self, url: str, checkin, checkout):
        return url

    async def _playwright_strategy(self, url: str, hotel_id):
        return type(self).playwright_result

    async def _httpx_strategy(self, url: str, hotel_id):
        return type(self).httpx_result


class _ApiSoldOutThenPlaywrightParser(_FetchOnceParser):
    def _extract_slug(self, url: str):
        return "slug"

    def _extract_dates(self, url: str):
        return "2026-05-03", "2026-05-04"

    async def _api_search_direct(self, client, slug, checkin, checkout, nights=0):
        return {"status": "sold_out", "prices": [], "data": {"rates": None}}


class OstrovokFetchOnceTests(unittest.IsolatedAsyncioTestCase):
    async def test_preserves_no_offers_from_playwright(self):
        _FetchOnceParser.playwright_result = {
            "price": None,
            "title": None,
            "external_id": "hid",
            "status": "not_found",
            "error": "Нет доступных предложений на выбранные даты",
        }
        _FetchOnceParser.httpx_result = None
        parser = _FetchOnceParser()

        result = await parser._fetch_once("https://example.com")

        self.assertEqual(result["status"], "not_found")
        self.assertEqual(
            result["error"],
            "Нет доступных предложений на выбранные даты",
        )

    async def test_preserves_blocked_from_playwright(self):
        _FetchOnceParser.playwright_result = {
            "price": None,
            "title": None,
            "external_id": "hid",
            "status": "blocked",
            "error": "Access denied",
        }
        _FetchOnceParser.httpx_result = None
        parser = _FetchOnceParser()

        result = await parser._fetch_once("https://example.com")

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["error"], "Access denied")

    async def test_httpx_price_still_overrides_non_price_playwright(self):
        _FetchOnceParser.playwright_result = {
            "price": None,
            "title": None,
            "external_id": "hid",
            "status": "not_found",
            "error": "Нет доступных предложений на выбранные даты",
        }
        _FetchOnceParser.httpx_result = {
            "price": 12345,
            "title": "Hotel",
            "external_id": "hid",
            "status": "ok",
            "error": None,
        }
        parser = _FetchOnceParser()

        result = await parser._fetch_once("https://example.com")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["price"], 12345)

    async def test_api_sold_out_is_verified_by_playwright_before_final_not_found(self):
        _ApiSoldOutThenPlaywrightParser.playwright_result = {
            "price": 10954,
            "title": "Hotel",
            "external_id": "hid",
            "status": "ok",
            "error": None,
        }
        _ApiSoldOutThenPlaywrightParser.httpx_result = None
        parser = _ApiSoldOutThenPlaywrightParser()

        result = await parser._fetch_once("https://example.com")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["price"], 10954)


if __name__ == "__main__":
    unittest.main()
