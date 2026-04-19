"""
Ostrovok.ru parser.

Стратегии (по приоритету):
1. Playwright + XHR-перехват /hotel/search → самая надёжная.
   page.goto с wait_until="commit" (резолвится сразу как сервер начал отвечать).
   XHR с ценами приходит через 5–20 сек пока страница грузится в фоне.
   Если page.goto таймаутит — проверяем, есть ли уже XHR-цены и возвращаем их.
2. httpx — резерв без браузера (пробует с прокси и без).
"""
from __future__ import annotations

import re
import json
import asyncio
import random
import uuid
from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, parse_qs, quote

import httpx
from loguru import logger

from app.parser.base_parser import BaseParser, BlockedError, CaptchaError
from app.utils.config import PARSER_USER_AGENTS

_PW_NAV_TIMEOUT = 20_000   # ms: "commit" резолвится быстро, это safety-net
_XHR_WAIT       = 12       # секунд ждём XHR (если нет — fallback на DOM/HTML)


class OstrovokParser(BaseParser):

    # ── Entry point ──────────────────────────────────────────────

    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        hotel_id          = self._extract_hotel_id(url)
        hotel_slug        = self._extract_slug(url)
        checkin, checkout = self._extract_dates(url)
        fetch_url         = self._normalize_url(url, checkin, checkout)
        logger.info(
            f"OstrovokParser: hotel_id={hotel_id} slug={hotel_slug} "
            f"url={fetch_url[:90]}"
        )

        # ── Прямой API /hotel/search/v1/site/hp/search ───────────
        # Быстрый путь без браузера. Авторитетно ловит rates=null
        # (max-stay превышен) и не тратит 12–22 сек на ожидание XHR.
        if hotel_slug and checkin and checkout:
            try:
                nights = (
                    date.fromisoformat(checkout) - date.fromisoformat(checkin)
                ).days
            except Exception:
                nights = 0
            if nights > 0:
                try:
                    async with httpx.AsyncClient(
                        timeout=httpx.Timeout(8.0, connect=4.0),
                        trust_env=False,
                        headers=self._headers(),
                        proxy=self._proxy if getattr(self, "_proxy", None) else None,
                    ) as client:
                        res = await self._api_search_direct(
                            client, hotel_slug, checkin, checkout, nights,
                        )
                    st = res.get("status")
                    if st == "ok":
                        price = min(res["prices"])
                        logger.info(f"OstrovokParser API direct: price={price}")
                        return {
                            "price":       price,
                            "title":       None,
                            "external_id": hotel_id,
                            "status":      "ok",
                            "error":       None,
                        }
                    if st == "sold_out":
                        logger.info("OstrovokParser API direct: sold_out/max_stay")
                        return {
                            "price":       None,
                            "title":       None,
                            "external_id": hotel_id,
                            "status":      "not_found",
                            "error":       "Нет доступных предложений на выбранные даты",
                        }
                    logger.debug(
                        f"OstrovokParser API direct: {res.get('error')} — "
                        "fallback Playwright"
                    )
                except Exception as e:
                    logger.debug(
                        f"OstrovokParser API direct exception: {e} — fallback Playwright"
                    )

        # Playwright — XHR перехват (fallback при ошибке API)
        result = None
        try:
            result = await self._playwright_strategy(fetch_url, hotel_id)
        except (BlockedError, CaptchaError):
            raise
        except Exception as e:
            logger.warning(f"OstrovokParser: playwright failed ({e.__class__.__name__}): {e}")

        if result and result.get("price"):
            return result

        # httpx — резерв (без браузера, пробует с прокси и без)
        result2 = await self._httpx_strategy(fetch_url, hotel_id)
        if result2 and result2.get("price"):
            return result2

        best = result or result2 or {}
        return {
            "price":       None,
            "title":       best.get("title"),
            "external_id": hotel_id,
            "status":      "not_found",
            "error":       "Цена не найдена. Добавьте даты (?dates=DD.MM.YYYY-DD.MM.YYYY).",
        }

    # ── Playwright strategy ──────────────────────────────────────

    async def _playwright_strategy(self, url: str, hotel_id: Optional[str]) -> Optional[Dict]:
        # Определяем кол-во запрошенных ночей для фильтрации rates в XHR
        _ci, _co = self._extract_dates(url)
        _nights = 0
        if _ci and _co:
            try:
                _nights = (date.fromisoformat(_co) - date.fromisoformat(_ci)).days
                if _nights < 0:
                    _nights = 0
            except Exception:
                pass

        context = await self._new_context()
        page    = await context.new_page()

        xhr_prices: List[float]  = []
        xhr_event                = asyncio.Event()  # fires on any definitive XHR answer
        xhr_no_avail             = False            # True = XHR came, rates empty = занято

        async def on_response(response):
            nonlocal xhr_no_avail
            try:
                if response.status != 200:
                    return
                rurl = response.url
                if not any(ep in rurl for ep in [
                    "/hotel/search/v1/site/hp/search",
                    "/hotel/search/v2/site/hp/rates",
                    "/hotel/search/v1/site/hp/rates",
                ]):
                    return
                if "json" not in response.headers.get("content-type", ""):
                    return
                data   = await response.json()
                prices = self._prices_from_xhr(data, _nights)
                if prices:
                    xhr_prices.extend(prices)
                    xhr_event.set()
                    logger.debug(f"OstrovokParser XHR prices={prices} from {rurl[:70]}")
                elif "rates" in data:
                    # XHR пришёл, поле rates есть, но предложений нет → объект занят/недоступен
                    xhr_no_avail = True
                    xhr_event.set()
                    logger.debug(f"OstrovokParser XHR: rates=[] → no availability")
            except Exception as e:
                logger.debug(f"OstrovokParser XHR handler error: {e}")

        page.on("response", on_response)

        nav_ok = False
        html   = ""

        try:
            # "commit" = резолвится как только сервер начал слать ответ (~1-2 сек).
            # Страница продолжает грузиться в фоне, JS файрит XHR — on_response работает.
            try:
                resp = await page.goto(url, timeout=_PW_NAV_TIMEOUT, wait_until="commit")
                if resp and resp.status in (403, 429, 503):
                    raise BlockedError(f"HTTP {resp.status}")
                nav_ok = True
            except BlockedError:
                raise
            except Exception as nav_err:
                # Даже если goto таймаутит/падает — XHR мог уже прийти.
                # Ждём ещё немного на случай если XHR в пути.
                if not xhr_event.is_set():
                    try:
                        await asyncio.wait_for(xhr_event.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass
                if xhr_prices:
                    logger.info(
                        f"OstrovokParser: nav error [{nav_err.__class__.__name__}] "
                        f"but {len(xhr_prices)} XHR prices already captured — returning OK"
                    )
                    # Возвращаем XHR цены, навигация не нужна
                    return {
                        "price":       min(xhr_prices),
                        "title":       None,
                        "external_id": hotel_id,
                        "status":      "ok",
                        "error":       None,
                    }
                raise  # XHR тоже нет — пробрасываем ошибку

            # Навигация прошла — ждём XHR (JS грузит данные после mount)
            if not xhr_event.is_set():
                try:
                    await asyncio.wait_for(xhr_event.wait(), timeout=_XHR_WAIT)
                except asyncio.TimeoutError:
                    logger.debug("OstrovokParser: XHR wait timeout — trying DOM/HTML fallback")

            # Быстрый выход: XHR пришёл, но rates пустые → объект занят/нет предложений
            if xhr_no_avail and not xhr_prices:
                return {
                    "price":       None,
                    "title":       None,
                    "external_id": hotel_id,
                    "status":      "not_found",
                    "error":       "Нет доступных предложений на выбранные даты",
                }

            # Лёгкий скролл для lazy-load (только если XHR не ответил)
            if not xhr_prices:
                try:
                    await page.evaluate("window.scrollBy(0, 600)")
                    await asyncio.sleep(0.8)
                except Exception:
                    pass

            try:
                html = await page.content()
            except Exception:
                html = ""

            if len(html) < 3000 and not xhr_prices:
                raise BlockedError("HTML слишком короткий")

            # ── Экстракция цены ──────────────────────────────────
            price = min(xhr_prices) if xhr_prices else None
            logger.debug(f"OstrovokParser XHR collected: {xhr_prices}")

            if not price and html:
                price = await self._dom_rub_price(page)
                logger.debug(f"OstrovokParser DOM price: {price}")

            if not price and html:
                price = self._next_data_price(html)
                logger.debug(f"OstrovokParser __NEXT_DATA__ price: {price}")

            if not price and html:
                price = self._strict_json_price(html)
                logger.debug(f"OstrovokParser strict-json price: {price}")

            title = None
            try:
                title = await self._dom_title(page)
                if not title and html:
                    title = self._html_title(html)
            except Exception:
                if html:
                    title = self._html_title(html)

            return {
                "price":       price,
                "title":       title,
                "external_id": hotel_id,
                "status":      "ok" if price else "not_found",
                "error":       None if price else "Цена не найдена",
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

    # ── httpx strategy ───────────────────────────────────────────

    async def _httpx_strategy(self, url: str, hotel_id: Optional[str]) -> Optional[Dict]:
        """Пробует запрос с прокси, потом без — работает при любом типе соединения."""
        proxies_to_try = [self._proxy, None] if self._proxy else [None]

        for proxy in proxies_to_try:
            try:
                kwargs: dict = {"timeout": 18, "follow_redirects": True, "trust_env": False}
                if proxy:
                    kwargs["proxy"] = proxy

                async with httpx.AsyncClient(**kwargs) as client:
                    await asyncio.sleep(random.uniform(0.2, 0.6))
                    resp = await client.get(url, headers=self._headers())

                if resp.status_code not in (200, 201):
                    logger.debug(f"OstrovokParser httpx (proxy={bool(proxy)}): HTTP {resp.status_code}")
                    continue

                html = resp.text
                if len(html) < 3000 or self._detect_block(html):
                    continue

                price = self._next_data_price(html) or self._strict_json_price(html)
                title = self._html_title(html)
                label = f"proxy={proxy}" if proxy else "direct"
                logger.debug(f"OstrovokParser httpx ({label}): price={price}")

                if price:
                    return {
                        "price":       price,
                        "title":       title,
                        "external_id": hotel_id,
                        "status":      "ok",
                        "error":       None,
                    }
                # Нет цены — пробуем без прокси
                continue

            except Exception as e:
                logger.debug(f"OstrovokParser httpx (proxy={bool(proxy)}) error: {e}")
                continue

        return None

    # ── Direct API (no browser) ──────────────────────────────────
    #
    # Ostrovok отдаёт цены через /hotel/search/v1/site/hp/search с
    # query-параметром ?body={JSON}. Эндпоинт отвечает JSON-ом даже без
    # cookies/CSRF/Cloudflare-токенов, поэтому его можно дергать чистым
    # httpx — на порядок быстрее браузерной навигации и сразу даёт
    # однозначный сигнал «rates=null → max-stay превышен».
    #
    # Используется:
    #   • в _fetch_once как первичная стратегия (до Playwright);
    #   • в deep_analysis._api_phase как массовый пул запросов.

    def _extract_slug(self, url: str) -> Optional[str]:
        """Slug из URL вида /hotel/russia/<region>/mid<id>/<slug>/ — это
        поле `hotel` в API-запросе (ota_hotel_id)."""
        m = re.search(r'/mid\d+/([^/?#]+)', url)
        return m.group(1) if m else None

    def _build_api_search_url(
        self, slug: str, checkin: str, checkout: str, adults: int = 2,
    ) -> str:
        """
        Прямой URL /hotel/search/v1/site/hp/search.
        Ostrovok ожидает browser-like body c arrival/departure_date и paxes.
        """
        body = {
            "arrival_date":   checkin,
            "departure_date": checkout,
            "hotel":          slug,
            "currency":       "RUB",
            "lang":           "ru",
            "paxes":          [{"adults": adults}],
            "search_uuid":    str(uuid.uuid4()),
        }
        body_json = json.dumps(body, separators=(",", ":"))
        return (
            "https://ostrovok.ru/hotel/search/v1/site/hp/search"
            f"?body={quote(body_json)}"
        )

    async def _api_search_direct(
        self,
        client: httpx.AsyncClient,
        slug: str,
        checkin: str,
        checkout: str,
        nights: int = 0,
    ) -> Dict[str, Any]:
        """
        Возвращает:
          {"status":"ok",       "prices":[...], "data": dict}
          {"status":"sold_out", "prices":[],    "data": dict}  # rates=null/[] (max-stay / непродажа)
          {"status":"error",    "error":<msg>}                 # network/5xx/HTML
        """
        url = self._build_api_search_url(slug, checkin, checkout)
        try:
            resp = await client.get(url)
        except Exception as e:
            return {"status": "error", "error": f"net:{e.__class__.__name__}"}
        if resp.status_code != 200:
            return {"status": "error", "error": f"http:{resp.status_code}"}
        ct = resp.headers.get("content-type", "")
        if "json" not in ct:
            return {"status": "error", "error": f"ct:{ct[:30]}"}
        try:
            data = resp.json()
        except Exception as e:
            return {"status": "error", "error": f"json:{e.__class__.__name__}"}
        prices = self._prices_from_xhr(data, nights) if isinstance(data, dict) else []
        if prices:
            return {"status": "ok", "prices": prices, "data": data}
        if not isinstance(data, dict):
            return {"status": "error", "error": "schema:not_dict"}

        if any(k in data for k in ("error", "errors", "message")):
            keys = ",".join(sorted(str(k) for k in list(data.keys())[:5]))
            return {"status": "error", "error": f"api:{keys or 'message'}"}

        if "rates" not in data:
            keys = ",".join(sorted(str(k) for k in list(data.keys())[:5]))
            return {"status": "error", "error": f"schema:no_rates:{keys or 'empty'}"}

        rates = data.get("rates")
        if rates is None:
            return {"status": "sold_out", "prices": [], "data": data}
        if isinstance(rates, list):
            if not rates:
                return {"status": "sold_out", "prices": [], "data": data}
            return {"status": "error", "error": "schema:rates_without_prices"}
        return {"status": "error", "error": f"schema:rates_type:{type(rates).__name__}"}

    # ── XHR price extraction ─────────────────────────────────────

    def _prices_from_xhr(self, data: dict, nights: int = 0) -> List[float]:
        """
        Парсим JSON-ответ /hotel/search/v1/site/hp/search.
        Структура: { rates: [ { payment_options: { payment_types: [ {show_amount: 9589} ] } } ] }

        nights > 0: фильтруем по точному совпадению с кол-вом запрошенных ночей,
        чтобы min() не выбирал 1-ночную ставку для многодневных запросов.
        Если точного совпадения нет (field отсутствует или нет matching rate) —
        fallback: берём все rates (старое поведение, без регрессий).
        """

        def _rate_night_count(r: dict) -> int:
            """Кол-во ночей у rate (0 = неизвестно)."""
            for f in ("nights", "min_nights", "nights_count",
                      "min_stay", "length_of_stay", "stay_nights"):
                v = r.get(f)
                if isinstance(v, (int, float)) and v > 0:
                    return int(v)
            return 0

        def _collect_rate_prices(rate: dict) -> List[float]:
            prices: List[float] = []
            po = rate.get("payment_options", {})
            if isinstance(po, dict):
                for pt in po.get("payment_types", []):
                    if not isinstance(pt, dict):
                        continue
                    for field in ("show_amount", "amount", "price"):
                        p = self._to_price(pt.get(field))
                        if p:
                            prices.append(p)
            for field in ("price", "amount", "show_amount",
                          "base_amount", "total_price", "night_price"):
                p = self._to_price(rate.get(field))
                if p:
                    prices.append(p)
            return prices

        found: List[float] = []
        try:
            rates = data.get("rates", [])
            if not isinstance(rates, list):
                rates = []
            valid_rates = [r for r in rates if isinstance(r, dict)]

            if nights > 0 and valid_rates:
                annotated = [(r, _rate_night_count(r)) for r in valid_rates]
                has_info  = any(n > 0 for _, n in annotated)

                if has_info:
                    exact = [r for r, n in annotated if n == nights]
                    if exact:
                        logger.debug(
                            f"_prices_from_xhr: nights={nights}, "
                            f"exact-match rates={len(exact)}/{len(valid_rates)}"
                        )
                        for rate in exact:
                            found.extend(_collect_rate_prices(rate))
                        return [p for p in found if 500 <= p <= 300_000]
                    else:
                        available = sorted({n for _, n in annotated if n > 0})
                        logger.debug(
                            f"_prices_from_xhr: nights={nights}, no exact match, "
                            f"available={available} — fallback to all rates"
                        )
                        # Нет точного совпадения → fallback ниже

            # Fallback: берём все rates (текущее поведение)
            for rate in valid_rates:
                found.extend(_collect_rate_prices(rate))

        except Exception as e:
            logger.debug(f"_prices_from_xhr error: {e}")

        return [p for p in found if 500 <= p <= 300_000]

    # ── HTML price extraction ─────────────────────────────────────

    def _next_data_price(self, html: str) -> Optional[float]:
        m = re.search(
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
            html, re.S
        )
        if not m:
            return None
        try:
            data = json.loads(m.group(1))
            return self._dig_price(data, depth=0, max_depth=14)
        except Exception:
            return None

    def _strict_json_price(self, html: str) -> Optional[float]:
        KEYS = [
            "show_amount", "min_price", "minPrice",
            "base_amount", "night_price", "from_price",
            "total_price", "per_night", "min_rate",
        ]
        candidates = []
        for key in KEYS:
            pat = rf'"{re.escape(key)}"\s*:\s*"?(\d{{4,6}})"?'
            for m in re.finditer(pat, html):
                p = self._to_price(m.group(1))
                if p:
                    candidates.append(p)

        text_patterns = [
            r'(\d[\d\xa0\u202f\s]{2,6})\s*[₽]\s*/\s*ноч',
            r'(\d[\d\xa0\u202f\s]{2,6})\s*[₽]\s*за\s*ноч',
            r'от\s+(\d[\d\xa0\u202f\s]{2,6})\s*[₽](?:\s*/\s*ноч|\s*за\s*ноч)',
        ]
        for pat in text_patterns:
            for m in re.finditer(pat, html, re.IGNORECASE):
                raw = re.sub(r'[\xa0\u202f\s]', '', m.group(1))
                p = self._to_price(raw)
                if p:
                    candidates.append(p)

        valid = [p for p in candidates if 500 <= p <= 300_000]
        return min(valid) if valid else None

    # ── DOM extraction ────────────────────────────────────────────

    async def _dom_rub_price(self, page) -> Optional[float]:
        try:
            texts = await page.evaluate("""
                () => {
                    const results = [];
                    const all = document.querySelectorAll('span, div, p, strong, b');
                    for (const el of all) {
                        const childEls = el.querySelectorAll('*');
                        if (childEls.length > 2) continue;
                        const text = (el.innerText || el.textContent || '').trim();
                        if (text.includes('₽') && text.length >= 4 && text.length <= 25) {
                            results.push(text);
                        }
                    }
                    return results.slice(0, 50);
                }
            """)
        except Exception as e:
            logger.debug(f"OstrovokParser DOM JS error: {e}")
            return None

        logger.debug(f"OstrovokParser DOM ₽ texts (first 10): {texts[:10]}")
        prices = [p for t in texts for p in [self._parse_rub_text(t)] if p]
        valid = [p for p in prices if 500 <= p <= 300_000]
        return min(valid) if valid else None

    def _parse_rub_text(self, text: str) -> Optional[float]:
        if not text or '₽' not in text:
            return None
        before_rub = text.split('₽')[0]
        digits_only = re.sub(r'[^\d]', '', before_rub)
        if not digits_only:
            return None
        try:
            val = float(digits_only)
            if 500 <= val <= 300_000:
                return val
        except (ValueError, TypeError):
            pass
        return None

    async def _dom_title(self, page) -> Optional[str]:
        for sel in ["h1[class*='hotel']", "h1[class*='Hotel']",
                    "h1[class*='title']", "h1[itemprop='name']",
                    "[class*='HotelTitle']", "h1"]:
            try:
                el = await page.query_selector(sel)
                if el:
                    t = (await el.inner_text()).strip()
                    if t and len(t) > 2:
                        return t[:300]
            except Exception:
                pass
        return None

    def _html_title(self, html: str) -> Optional[str]:
        for p in [r'"hotelName"\s*:\s*"([^"]{3,200})"',
                  r'"name"\s*:\s*"([^"]{3,200})"',
                  r'<h1[^>]*>\s*([^<]{3,200}?)\s*</h1>',
                  r'<title>\s*([^<–|]{3,100})']:
            m = re.search(p, html, re.S)
            if m:
                t = m.group(1).strip()
                t = re.sub(r'\s*[|–—]\s*[Oo]strovok.*', '', t).strip()
                if len(t) > 2:
                    return t[:300]
        return None

    # ── JSON price digger ─────────────────────────────────────────

    def _dig_price(self, obj, depth: int, max_depth: int = 12) -> Optional[float]:
        if depth > max_depth:
            return None
        PRICE_KEYS = frozenset({
            "show_amount", "min_price", "minprice", "base_amount",
            "night_price", "from_price", "per_night", "min_rate",
            "lowest_price", "best_price", "total_price",
        })
        if isinstance(obj, dict):
            for key, val in obj.items():
                if key.lower() in PRICE_KEYS:
                    p = self._to_price(val)
                    if p and 500 <= p <= 300_000:
                        return p
            for val in obj.values():
                r = self._dig_price(val, depth + 1, max_depth)
                if r:
                    return r
        elif isinstance(obj, list):
            for item in obj[:30]:
                r = self._dig_price(item, depth + 1, max_depth)
                if r:
                    return r
        return None

    # ── Helpers ──────────────────────────────────────────────────

    def _to_price(self, v) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v) if v > 0 else None
        if isinstance(v, str):
            clean = re.sub(r'[\xa0\u202f\s,]', '', v).replace(',', '.')
            try:
                return float(clean) or None
            except (ValueError, TypeError):
                pass
        return None

    def _extract_hotel_id(self, url: str) -> Optional[str]:
        m = re.search(r'/mid(\d+)/', url)
        return m.group(1) if m else None

    def _extract_dates(self, url: str):
        qs = parse_qs(urlparse(url).query)
        ci = qs.get("checkin", [None])[0]
        co = qs.get("checkout", [None])[0]
        if ci and co:
            return ci, co
        ds = qs.get("dates", [None])[0]
        if ds:
            m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})', ds)
            if m:
                d1, m1, y1, d2, m2, y2 = m.groups()
                return f"{y1}-{m1}-{d1}", f"{y2}-{m2}-{d2}"
        return None, None

    def _normalize_url(self, url: str, ci: Optional[str], co: Optional[str]) -> str:
        if "checkin=" in url or "dates=" in url:
            return url
        if not ci:
            ci = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        if not co:
            co = (datetime.strptime(ci, "%Y-%m-%d") + timedelta(days=2)).strftime("%Y-%m-%d")
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}checkin={ci}&checkout={co}&guests=2"

    def _headers(self) -> dict:
        return {
            "User-Agent":      random.choice(PARSER_USER_AGENTS),
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer":         "https://www.google.com/",
        }
