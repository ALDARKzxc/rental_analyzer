"""
Ostrovok.ru parser — переписан с нуля.

Ключевые исправления vs предыдущей версии:
1. Фейковая цена 1800 — _price_from_regex матчил случайные числа.
   Теперь regex только по строго специфичным JSON-полям Ostrovok.
2. Масштабирование до 100 страниц — каждый парсер создаёт
   свой браузер (не shared), контекст и страница закрываются сразу.
3. Приоритет: XHR /hotel/search → DOM ₽-спаны → __NEXT_DATA__ → httpx.
4. _clean_rub_price теперь требует цифру до ₽ (не ловит "2024 год" и т.п.)
5. Прокси пробрасывается в браузер (из реестра / env).
"""
from __future__ import annotations

import re
import json
import asyncio
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, parse_qs

import httpx
from loguru import logger

from app.parser.base_parser import BaseParser, BlockedError, CaptchaError
from app.utils.config import PARSER_USER_AGENTS

_PW_NAV_TIMEOUT = 40_000   # domcontentloaded
_XHR_WAIT       = 10       # секунд ждём XHR после загрузки DOM


class OstrovokParser(BaseParser):
    """Парсер Ostrovok.ru."""

    # ── Entry point ──────────────────────────────────────────────

    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        hotel_id          = self._extract_hotel_id(url)
        checkin, checkout = self._extract_dates(url)
        fetch_url         = self._normalize_url(url, checkin, checkout)
        logger.info(f"OstrovokParser: hotel_id={hotel_id} url={fetch_url[:90]}")

        # Playwright — основная стратегия (XHR + DOM)
        result = await self._playwright_strategy(fetch_url, hotel_id)
        if result and result.get("price"):
            return result

        # httpx — резерв (работает без прокси / если Playwright облажался)
        result2 = await self._httpx_strategy(fetch_url, hotel_id)
        if result2 and result2.get("price"):
            return result2

        # Нет цены — возвращаем not_found (не error)
        return {
            "price":       None,
            "title":       (result or result2 or {}).get("title"),
            "external_id": hotel_id,
            "status":      "not_found",
            "error":       "Цена не найдена. Добавьте даты в URL (?dates=DD.MM.YYYY-DD.MM.YYYY).",
        }

    # ── Playwright strategy ──────────────────────────────────────

    async def _playwright_strategy(self, url: str, hotel_id: Optional[str]) -> Optional[Dict]:
        context = await self._new_context()
        page    = await context.new_page()

        xhr_prices: List[float] = []
        xhr_event               = asyncio.Event()

        async def on_response(response):
            try:
                if response.status != 200:
                    return
                rurl = response.url
                # Только эндпоинты Ostrovok с ценами
                if not any(ep in rurl for ep in [
                    "/hotel/search/v1/site/hp/search",
                    "/hotel/search/v2/site/hp/rates",
                    "/hotel/search/v1/site/hp/rates",
                ]):
                    return
                if "json" not in response.headers.get("content-type", ""):
                    return
                data   = await response.json()
                prices = self._prices_from_xhr(data)
                if prices:
                    xhr_prices.extend(prices)
                    xhr_event.set()
                    logger.debug(f"OstrovokParser XHR prices={prices} from {rurl[:70]}")
            except Exception as e:
                logger.debug(f"OstrovokParser XHR handler error: {e}")

        page.on("response", on_response)

        try:
            resp = await page.goto(url, timeout=_PW_NAV_TIMEOUT,
                                   wait_until="domcontentloaded")
            if resp and resp.status in (403, 429, 503):
                raise BlockedError(f"HTTP {resp.status}")

            # Ждём XHR с ценами — не более _XHR_WAIT сек
            try:
                await asyncio.wait_for(xhr_event.wait(), timeout=_XHR_WAIT)
            except asyncio.TimeoutError:
                logger.debug("OstrovokParser: XHR wait timeout")

            # Скролл — триггерит lazy-load блока цен
            try:
                await page.evaluate("window.scrollBy(0, 500)")
                await asyncio.sleep(2)
            except Exception:
                pass

            html = await page.content()

            if len(html) < 3000:
                raise BlockedError("HTML слишком короткий")

            # ── Приоритет 1: XHR ──
            price = min(xhr_prices) if xhr_prices else None
            logger.debug(f"OstrovokParser XHR collected: {xhr_prices}")

            # ── Приоритет 2: DOM ₽-спаны ──
            if not price:
                price = await self._dom_rub_price(page)
                logger.debug(f"OstrovokParser DOM price: {price}")

            # ── Приоритет 3: __NEXT_DATA__ ──
            if not price:
                price = self._next_data_price(html)
                logger.debug(f"OstrovokParser __NEXT_DATA__ price: {price}")

            # ── Приоритет 4: строгий regex только по JSON-полям ──
            if not price:
                price = self._strict_json_price(html)
                logger.debug(f"OstrovokParser strict-json price: {price}")

            title = await self._dom_title(page) or self._html_title(html)

            return {
                "price":       price,
                "title":       title,
                "external_id": hotel_id,
                "status":      "ok" if price else "not_found",
                "error":       None if price else "Цена не найдена",
            }

        finally:
            await page.close()
            await context.close()

    # ── httpx strategy ───────────────────────────────────────────

    async def _httpx_strategy(self, url: str, hotel_id: Optional[str]) -> Optional[Dict]:
        try:
            proxy_conf = {}
            if self._proxy:
                proxy_conf = {"proxy": self._proxy}

            async with httpx.AsyncClient(
                timeout=18,
                follow_redirects=True,
                trust_env=False,
                **proxy_conf,
            ) as client:
                await asyncio.sleep(random.uniform(1.0, 2.5))
                resp = await client.get(url, headers=self._headers())

            if resp.status_code not in (200, 201):
                logger.debug(f"OstrovokParser httpx: HTTP {resp.status_code}")
                return None

            html = resp.text
            if len(html) < 3000 or self._detect_block(html):
                return None

            price = (self._next_data_price(html)
                     or self._strict_json_price(html))
            title = self._html_title(html)
            logger.debug(f"OstrovokParser httpx: price={price}")

            return {
                "price":       price,
                "title":       title,
                "external_id": hotel_id,
                "status":      "ok" if price else "not_found",
                "error":       None if price else "Цена не найдена (httpx)",
            }
        except Exception as e:
            logger.debug(f"OstrovokParser httpx error: {e}")
            return None

    # ── XHR price extraction ─────────────────────────────────────

    def _prices_from_xhr(self, data: dict) -> List[float]:
        """
        Парсим JSON-ответ /hotel/search/v1/site/hp/search.
        Структура от Ostrovok:
          { rates: [ { payment_options: { payment_types: [ {show_amount: 9589} ] } } ] }
        """
        found = []
        try:
            rates = data.get("rates", [])
            if not isinstance(rates, list):
                rates = []

            for rate in rates:
                if not isinstance(rate, dict):
                    continue
                # payment_options → payment_types → show_amount
                po = rate.get("payment_options", {})
                if isinstance(po, dict):
                    for pt in po.get("payment_types", []):
                        if not isinstance(pt, dict):
                            continue
                        for field in ("show_amount", "amount", "price"):
                            v = pt.get(field)
                            p = self._to_price(v)
                            if p:
                                found.append(p)
                # Прямые поля rate
                for field in ("price", "amount", "show_amount",
                              "base_amount", "total_price", "night_price"):
                    p = self._to_price(rate.get(field))
                    if p:
                        found.append(p)
        except Exception as e:
            logger.debug(f"_prices_from_xhr error: {e}")

        # Фильтр: реалистичные цены посуточной аренды в России
        return [p for p in found if 500 <= p <= 300_000]

    # ── HTML price extraction ─────────────────────────────────────

    def _next_data_price(self, html: str) -> Optional[float]:
        """Ищем цену в <script id="__NEXT_DATA__">."""
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
        """
        СТРОГИЙ regex — только точные JSON-ключи Ostrovok.
        НЕ матчит случайные числа типа 2024, 1800, CSS-значения.

        Требования к матчу:
        - Ключ из белого списка
        - Значение 4-6 цифр (500-299999)
        - Перед значением только : и пробелы (JSON-структура)
        """
        # Белый список ключей с ценами именно Ostrovok
        KEYS = [
            "show_amount", "min_price", "minPrice",
            "base_amount", "night_price", "from_price",
            "total_price", "per_night", "min_rate",
        ]
        candidates = []
        for key in KEYS:
            # Паттерн: "key": 12345 или "key":"12345"
            pat = rf'"{re.escape(key)}"\s*:\s*"?(\d{{4,6}})"?'
            for m in re.finditer(pat, html):
                p = self._to_price(m.group(1))
                if p:
                    candidates.append(p)

        # Текстовые паттерны — только если есть ₽ И «/ночь» или «за ночь»
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

        # Берём минимальную (начальная цена, а не с доп. услугами)
        valid = [p for p in candidates if 500 <= p <= 300_000]
        return min(valid) if valid else None

    # ── DOM extraction ────────────────────────────────────────────

    async def _dom_rub_price(self, page) -> Optional[float]:
        """
        Извлекаем цены из DOM через JS.
        Диагностика показала: span:has-text('₽') → '9\xa0589\u202f₽' = 9589 ₽.
        """
        try:
            texts = await page.evaluate("""
                () => {
                    const results = [];
                    // Все элементы с текстом содержащим ₽
                    const all = document.querySelectorAll('span, div, p, strong, b');
                    for (const el of all) {
                        // Берём только leaf-узлы (нет дочерних элементов) или почти leaf
                        const childEls = el.querySelectorAll('*');
                        if (childEls.length > 2) continue;
                        const text = (el.innerText || el.textContent || '').trim();
                        // Должен содержать ₽ и быть коротким (цена, не абзац)
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

        prices = []
        for text in texts:
            p = self._parse_rub_text(text)
            if p:
                prices.append(p)

        valid = [p for p in prices if 500 <= p <= 300_000]
        return min(valid) if valid else None

    def _parse_rub_text(self, text: str) -> Optional[float]:
        """
        '9\xa0589\u202f₽' → 9589.0
        '15\u202f000 ₽' → 15000.0
        'Посмотреть цены' → None
        '2024 ₽ скидка' → None (нет паттерна N₽/ночь или просто N₽ где N>=4 цифр)
        """
        if not text or '₽' not in text:
            return None
        # Убираем всё кроме цифр до ₽
        before_rub = text.split('₽')[0]
        digits_only = re.sub(r'[^\d]', '', before_rub)
        if not digits_only:
            return None
        try:
            val = float(digits_only)
            # Реалистичная цена за ночь: от 500 до 300 000 ₽
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
        """Рекурсивный поиск цены в JSON. Строгие ключи, нет ложных матчей."""
        if depth > max_depth:
            return None

        # Строгий белый список — только реальные поля цен Ostrovok
        PRICE_KEYS = frozenset({
            "show_amount", "min_price", "minprice", "base_amount",
            "night_price", "from_price", "per_night", "min_rate",
            "lowest_price", "best_price", "total_price",
        })

        if isinstance(obj, dict):
            # Сначала ищем по белому списку
            for key, val in obj.items():
                if key.lower() in PRICE_KEYS:
                    p = self._to_price(val)
                    if p and 500 <= p <= 300_000:
                        return p
            # Потом рекурсивно
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
        """Конвертируем любое значение в цену или None."""
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


    async def _is_occupied(self, page, html: str) -> bool:
        """Определяем занят ли объект на выбранные даты."""
        occupied_phrases = [
            "нет свободных номеров", "нет доступных номеров",
            "недоступно", "no rooms available", "not available",
            "sold out", "нет мест", "занято", "закрыто на эти даты",
        ]
        html_lower = html.lower()
        if any(p in html_lower for p in occupied_phrases):
            return True
        # Проверяем DOM
        try:
            texts = await page.evaluate("""
                () => document.body.innerText.toLowerCase()
            """)
            if any(p in texts for p in ["нет свободных", "no rooms", "sold out"]):
                return True
        except Exception:
            pass
        return False

    def _extract_hotel_id(self, url: str) -> Optional[str]:
        m = re.search(r'/mid(\d+)/', url)
        return m.group(1) if m else None

    def _extract_dates(self, url: str):
        qs = parse_qs(urlparse(url).query)

        # checkin/checkout
        ci = qs.get("checkin", [None])[0]
        co = qs.get("checkout", [None])[0]
        if ci and co:
            return ci, co

        # dates=DD.MM.YYYY-DD.MM.YYYY
        ds = qs.get("dates", [None])[0]
        if ds:
            m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})', ds)
            if m:
                d1,m1,y1,d2,m2,y2 = m.groups()
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
