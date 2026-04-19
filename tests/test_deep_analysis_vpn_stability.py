import asyncio
import unittest
from datetime import date, timedelta

from app.backend import deep_analysis as da


class _SingleFetchParser:
    result = {"status": "error", "error": "unset"}

    def __init__(self):
        self._proxy = None

    async def fetch(self, url: str):
        return dict(type(self).result)

    async def close(self):
        return None


class DeepAnalysisVpnStabilityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._old_cancel_event = da._cancel_event
        self._old_progress = da._state["progress"]
        self._old_pause = da._SLOW_LANE_PAUSE_S
        da._cancel_event = asyncio.Event()
        da._state["progress"] = 0
        da._SLOW_LANE_PAUSE_S = 0.0

    def tearDown(self):
        da._cancel_event = self._old_cancel_event
        da._state["progress"] = self._old_progress
        da._SLOW_LANE_PAUSE_S = self._old_pause

    def test_classify_terminal_result(self):
        cases = [
            (
                {"status": "ok", "price": 8123},
                da._ROW_PRICED,
                8123.0,
                "phase:priced",
            ),
            (
                {"status": "occupied", "error": "occupied"},
                da._ROW_SOLD_OUT,
                None,
                "phase:sold_out:occupied",
            ),
            (
                {"status": "not_found", "error": "Нет доступных предложений"},
                da._ROW_SOLD_OUT,
                None,
                "phase:sold_out:not_found",
            ),
            (
                {"status": "blocked", "error": "Access denied"},
                da._ROW_BLOCKED,
                None,
                "phase:blocked:Access denied",
            ),
            (
                {"status": "captcha", "error": "captcha"},
                da._ROW_CAPTCHA,
                None,
                "phase:captcha:captcha",
            ),
            (
                {"status": "error", "error": "net:ConnectTimeout"},
                da._ROW_NETWORK,
                None,
                "phase:network:net:ConnectTimeout",
            ),
            (
                {"status": "error", "error": "unexpected failure"},
                da._ROW_ERROR,
                None,
                "phase:error:error:unexpected failure",
            ),
        ]

        for result, expected_state, expected_price, expected_reason in cases:
            with self.subTest(result=result):
                state, price, reason = da._classify_terminal_result(
                    result,
                    phase="phase",
                )
                self.assertEqual(state, expected_state)
                self.assertEqual(price, expected_price)
                self.assertEqual(reason, expected_reason)

    def test_should_run_slow_lane_thresholds(self):
        self.assertFalse(da._should_run_slow_lane(0, 3))
        self.assertFalse(da._should_run_slow_lane(10, 0))
        self.assertTrue(da._should_run_slow_lane(20, 3))
        self.assertTrue(da._should_run_slow_lane(20, 3))
        self.assertTrue(da._should_run_slow_lane(19, 2))

    def test_select_api_rescue_indices_only_network_and_capped(self):
        original_cap = da._API_RESCUE_MAX_PAIRS
        da._API_RESCUE_MAX_PAIRS = 2
        try:
            reasons = [
                "api:fallback:net:ConnectTimeout",
                "api:fallback:schema:rates_without_prices",
                "api:fallback:http:503",
                "playwright:fallback:fallback:no_price",
                "api:fallback:proxy error",
            ]
            selected = da._select_api_rescue_indices(
                list(range(len(reasons))),
                reasons,
            )
        finally:
            da._API_RESCUE_MAX_PAIRS = original_cap

        self.assertEqual(selected, [0, 2])

    async def test_final_verify_preserves_blocked_reason(self):
        today = date(2026, 4, 19)
        date_pairs = [(today, today + timedelta(days=1))]
        out = [da._format_row("OBJ", *date_pairs[0], status=da._ROW_FALLBACK)]
        states = [da._ROW_FALLBACK]
        reasons = ["playwright:fallback:fallback:no_price"]

        _SingleFetchParser.result = {"status": "blocked", "error": "Access denied"}
        parser = _SingleFetchParser()

        verify = await da._final_verify_phase(
            title="OBJ",
            base_url="https://example.com/hotel",
            date_pairs=date_pairs,
            indices=[0],
            out=out,
            states=states,
            reasons=reasons,
            parser=parser,
        )

        self.assertEqual(states[0], da._ROW_BLOCKED)
        self.assertIn("[blocked]", out[0])
        self.assertTrue(reasons[0].startswith("final-verify:blocked"))
        self.assertEqual(verify["slow_lane_candidates"], [0])
        self.assertEqual(da._state["progress"], 1)

    async def test_slow_lane_can_recover_price(self):
        today = date(2026, 4, 19)
        date_pairs = [(today, today + timedelta(days=2))]
        out = [da._format_row("OBJ", *date_pairs[0], status=da._ROW_NETWORK)]
        states = [da._ROW_NETWORK]
        reasons = ["final-verify:network:net:ConnectTimeout"]

        _SingleFetchParser.result = {"status": "ok", "price": 15432}
        parser = _SingleFetchParser()

        await da._slow_lane_phase(
            title="OBJ",
            base_url="https://example.com/hotel",
            date_pairs=date_pairs,
            indices=[0],
            out=out,
            states=states,
            reasons=reasons,
            parser=parser,
        )

        self.assertEqual(states[0], da._ROW_PRICED)
        self.assertIn("15", out[0])
        self.assertTrue(reasons[0].startswith("slow-lane:priced"))

    async def test_slow_lane_does_not_downgrade_known_reason_to_generic_error(self):
        today = date(2026, 4, 19)
        date_pairs = [(today, today + timedelta(days=3))]
        out = [da._format_row("OBJ", *date_pairs[0], status=da._ROW_CAPTCHA)]
        states = [da._ROW_CAPTCHA]
        reasons = ["final-verify:captcha:captcha"]

        _SingleFetchParser.result = {"status": "error", "error": "unexpected failure"}
        parser = _SingleFetchParser()

        await da._slow_lane_phase(
            title="OBJ",
            base_url="https://example.com/hotel",
            date_pairs=date_pairs,
            indices=[0],
            out=out,
            states=states,
            reasons=reasons,
            parser=parser,
        )

        self.assertEqual(states[0], da._ROW_CAPTCHA)
        self.assertIn("[captcha]", out[0])
        self.assertEqual(reasons[0], "final-verify:captcha:captcha")

    def test_seal_incomplete_pairs_keeps_explicit_terminal_states(self):
        today = date(2026, 4, 19)
        date_pairs = [
            (today, today + timedelta(days=1)),
            (today, today + timedelta(days=2)),
            (today, today + timedelta(days=3)),
        ]
        out = [
            da._format_row("OBJ", *date_pairs[0], status=da._ROW_PENDING),
            da._format_row("OBJ", *date_pairs[1], status=da._ROW_FALLBACK),
            da._format_row("OBJ", *date_pairs[2], status=da._ROW_BLOCKED),
        ]
        states = [da._ROW_PENDING, da._ROW_FALLBACK, da._ROW_BLOCKED]
        reasons = [None, "playwright:fallback:fallback:no_price", "final-verify:blocked:Access denied"]

        sealed = da._seal_incomplete_pairs(
            out=out,
            states=states,
            reasons=reasons,
            title="OBJ",
            date_pairs=date_pairs,
            cancelled=False,
        )

        self.assertEqual(sealed, 2)
        self.assertEqual(states[0], da._ROW_ERROR)
        self.assertEqual(states[1], da._ROW_ERROR)
        self.assertEqual(states[2], da._ROW_BLOCKED)
        self.assertTrue(reasons[0].startswith("seal:error"))
        self.assertIn("playwright:fallback", reasons[1])


if __name__ == "__main__":
    unittest.main()
