import asyncio
import unittest
from datetime import date, timedelta

from app.backend import deep_analysis as da


class _ProxySoldOutApiParser:
    """API returns sold_out via proxy, then ok via no-proxy direct client."""

    def __init__(self):
        self._proxy = "http://proxy.local:8080"
        self.calls = 0

    async def _api_search_direct(self, client, slug, checkin, checkout, nights):
        self.calls += 1
        if self.calls == 1:
            return {"status": "sold_out", "prices": [], "data": {"rates": None}}
        return {"status": "ok", "prices": [12345.0], "data": {"rates": []}}


class DeepAnalysisVpnStabilityTests(unittest.IsolatedAsyncioTestCase):
    """
    Tests for the v6 API-only pipeline. The Playwright fallback that was
    fabricating "от X ₽" prices from the page DOM has been removed; the
    direct /hotel/search/v1/site/hp/search API is the sole source of truth.
    """

    def setUp(self):
        self._old_cancel_event = da._cancel_event
        self._old_progress = da._state["progress"]
        da._cancel_event = asyncio.Event()
        da._state["progress"] = 0

    def tearDown(self):
        da._cancel_event = self._old_cancel_event
        da._state["progress"] = self._old_progress

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

        # net:ConnectTimeout, http:503 are network errors per _NETWORK_ERROR_MARKERS;
        # capped at 2 so first two network candidates win.
        self.assertEqual(selected, [0, 2])

    async def test_api_batch_rechecks_proxy_sold_out_without_proxy(self):
        """Sold_out via proxy must trigger a no-proxy retry that recovers price."""
        today = date(2026, 5, 3)
        date_pairs = [(today, today + timedelta(days=1))]
        out = [da._format_row("OBJ", *date_pairs[0], status=da._ROW_PENDING)]
        states = [da._ROW_PENDING]
        reasons = [None]
        parser = _ProxySoldOutApiParser()

        result = await da._run_api_batch(
            title="OBJ",
            slug="object-slug",
            date_pairs=date_pairs,
            indices=[0],
            out=out,
            states=states,
            reasons=reasons,
            parser=parser,
            api_headers={},
            concurrency=1,
        )

        self.assertEqual(result["ok_count"], 1)
        self.assertEqual(result["sold_out_count"], 0)
        self.assertEqual(states[0], da._ROW_PRICED)
        self.assertIn("12", out[0])
        self.assertGreaterEqual(parser.calls, 2)

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
        reasons = [
            None,
            "api:fallback:net:ConnectTimeout",
            "api:fallback:http:503",
        ]

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
        # Explicit terminal state must be preserved
        self.assertEqual(states[2], da._ROW_BLOCKED)
        self.assertTrue(reasons[0].startswith("seal:error"))
        self.assertIn("api:fallback", reasons[1])

    # ── MinLOS post-processing ──────────────────────────────────────────────
    #
    # New behavior: api:sold_out is now trusted as a confirmed sold_out signal
    # (previous code required a playwright-confirmed reason, but Playwright
    # verification was only producing false 6 399 ₽ recoveries from the DOM
    # header — removing it means API sold_outs are the most reliable signal).

    def test_minlos_marker_uses_same_checkin_anchor(self):
        today = date(2026, 4, 19)
        date_pairs = [
            (today, today + timedelta(days=1)),
            (today, today + timedelta(days=2)),
            (today, today + timedelta(days=3)),
        ]
        states = [da._ROW_SOLD_OUT, da._ROW_SOLD_OUT, da._ROW_PRICED]
        out = [
            da._format_row("OBJ", *date_pairs[0], status=states[0]),
            da._format_row("OBJ", *date_pairs[1], status=states[1]),
            da._format_row("OBJ", *date_pairs[2], status=states[2], price=9000),
        ]

        detected = da._apply_minlos_marker(
            out=out,
            states=states,
            title="OBJ",
            date_pairs=date_pairs,
        )

        self.assertEqual(detected, 3)
        self.assertIn("[MinLOS]", out[0])
        self.assertIn("[MinLOS]", out[1])
        # MinLOS post-processing must not mutate pair_states
        self.assertEqual(states, [da._ROW_SOLD_OUT, da._ROW_SOLD_OUT, da._ROW_PRICED])

    def test_minlos_marker_does_not_mix_different_checkins(self):
        today = date(2026, 4, 19)
        other = today + timedelta(days=1)
        date_pairs = [
            (today, today + timedelta(days=1)),
            (today, today + timedelta(days=2)),
            (other, other + timedelta(days=3)),
        ]
        states = [da._ROW_SOLD_OUT, da._ROW_SOLD_OUT, da._ROW_PRICED]
        out = [
            da._format_row("OBJ", *date_pairs[0], status=states[0]),
            da._format_row("OBJ", *date_pairs[1], status=states[1]),
            da._format_row("OBJ", *date_pairs[2], status=states[2], price=9000),
        ]

        detected = da._apply_minlos_marker(
            out=out,
            states=states,
            title="OBJ",
            date_pairs=date_pairs,
        )

        self.assertIsNone(detected)
        self.assertIn("[sold_out]", out[0])
        self.assertIn("[sold_out]", out[1])

    def test_minlos_marker_requires_resolved_shorter_lengths(self):
        today = date(2026, 4, 19)
        date_pairs = [
            (today, today + timedelta(days=1)),
            (today, today + timedelta(days=2)),
            (today, today + timedelta(days=3)),
        ]
        states = [da._ROW_SOLD_OUT, da._ROW_NETWORK, da._ROW_PRICED]
        out = [
            da._format_row("OBJ", *date_pairs[0], status=states[0]),
            da._format_row("OBJ", *date_pairs[1], status=states[1]),
            da._format_row("OBJ", *date_pairs[2], status=states[2], price=9000),
        ]

        detected = da._apply_minlos_marker(
            out=out,
            states=states,
            title="OBJ",
            date_pairs=date_pairs,
        )

        self.assertIsNone(detected)
        self.assertIn("[sold_out]", out[0])

    def test_minlos_marker_now_trusts_api_sold_out(self):
        """
        Previously this returned None because api:sold_out wasn't considered
        "confirmed". After v6 (Playwright verification removed) the API is the
        authoritative source, so api:sold_out + priced anchor → MinLOS.
        """
        today = date(2026, 4, 19)
        date_pairs = [
            (today, today + timedelta(days=1)),
            (today, today + timedelta(days=2)),
            (today, today + timedelta(days=3)),
        ]
        states = [da._ROW_SOLD_OUT, da._ROW_SOLD_OUT, da._ROW_PRICED]
        reasons = ["api:sold_out", "api:sold_out", "api:priced"]
        out = [
            da._format_row("OBJ", *date_pairs[0], status=states[0]),
            da._format_row("OBJ", *date_pairs[1], status=states[1]),
            da._format_row("OBJ", *date_pairs[2], status=states[2], price=9000),
        ]

        detected = da._apply_minlos_marker(
            out=out,
            states=states,
            reasons=reasons,
            title="OBJ",
            date_pairs=date_pairs,
        )

        self.assertEqual(detected, 3)
        self.assertIn("[MinLOS]", out[0])
        self.assertIn("[MinLOS]", out[1])

    def test_minlos_marker_does_not_guess_when_all_rows_are_sold_out(self):
        today = date(2026, 4, 19)
        date_pairs = [
            (today, today + timedelta(days=1)),
            (today, today + timedelta(days=2)),
            (today, today + timedelta(days=3)),
        ]
        states = [da._ROW_SOLD_OUT, da._ROW_SOLD_OUT, da._ROW_SOLD_OUT]
        out = [
            da._format_row("OBJ", *pair, status=state)
            for pair, state in zip(date_pairs, states)
        ]

        detected = da._apply_minlos_marker(
            out=out,
            states=states,
            title="OBJ",
            date_pairs=date_pairs,
        )

        self.assertIsNone(detected)
        self.assertIn("[sold_out]", out[0])
        self.assertIn("[sold_out]", out[1])


if __name__ == "__main__":
    unittest.main()
