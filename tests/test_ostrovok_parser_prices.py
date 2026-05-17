import unittest
from datetime import date

from app.backend import deep_analysis as da
from app.parser.ostrovok_parser import OstrovokParser


class OstrovokPriceExtractionTests(unittest.TestCase):
    def setUp(self):
        self.parser = OstrovokParser()
        self.parser._proxy = None

    def test_does_not_use_rate_from_different_exact_stay_length(self):
        data = {
            "rates": [
                {
                    "nights": 1,
                    "payment_options": {
                        "payment_types": [{"show_amount": "7000"}],
                    },
                },
            ],
        }

        self.assertEqual(self.parser._prices_from_xhr(data, nights=3), [])

    def test_uses_matching_exact_stay_length(self):
        data = {
            "rates": [
                {
                    "nights": "3",
                    "payment_options": {
                        "payment_types": [{"show_amount": "21000"}],
                    },
                },
            ],
        }

        self.assertEqual(self.parser._prices_from_xhr(data, nights=3), [21000.0])

    def test_keeps_fallback_when_rates_have_no_stay_length_metadata(self):
        data = {
            "rates": [
                {
                    "payment_options": {
                        "payment_types": [{"show_amount": "18000"}],
                    },
                },
            ],
        }

        self.assertEqual(self.parser._prices_from_xhr(data, nights=3), [18000.0])

    def test_min_stay_allows_long_enough_requested_stay(self):
        data = {
            "rates": [
                {
                    "min_stay": 2,
                    "payment_options": {
                        "payment_types": [{"show_amount": "22000"}],
                    },
                },
            ],
        }

        self.assertEqual(self.parser._prices_from_xhr(data, nights=3), [22000.0])

    def test_min_stay_rejects_too_short_requested_stay(self):
        data = {
            "rates": [
                {
                    "min_nights": 3,
                    "payment_options": {
                        "payment_types": [{"show_amount": "22000"}],
                    },
                },
            ],
        }

        self.assertEqual(self.parser._prices_from_xhr(data, nights=2), [])

    def test_prefers_total_stay_price_over_nightly_price(self):
        data = {
            "rates": [
                {
                    "nights": 3,
                    "night_price": "7000",
                    "payment_options": {
                        "payment_types": [
                            {"show_amount": "21000", "amount": "21000"},
                        ],
                    },
                },
            ],
        }

        self.assertEqual(self.parser._prices_from_xhr(data, nights=3), [21000.0])

    def test_uses_nightly_price_only_when_total_is_absent(self):
        data = {
            "rates": [
                {
                    "nights": 2,
                    "night_price": "6500",
                },
            ],
        }

        self.assertEqual(self.parser._prices_from_xhr(data, nights=2), [6500.0])

    def test_normalize_url_converts_dates_param_to_checkin_checkout(self):
        url = (
            "https://ostrovok.ru/hotel/russia/terskol/mid13437806/object/"
            "?dates=03.05.2026-04.05.2026&guests=2"
        )

        normalized = self.parser._normalize_url(
            url,
            "2026-05-03",
            "2026-05-04",
        )

        self.assertIn("checkin=2026-05-03", normalized)
        self.assertIn("checkout=2026-05-04", normalized)
        self.assertIn("guests=2", normalized)
        self.assertNotIn("dates=", normalized)

if __name__ == "__main__":
    unittest.main()
