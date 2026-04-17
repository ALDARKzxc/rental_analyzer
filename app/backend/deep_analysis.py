"""
Deep Analysis Engine v3 — ультра-быстрый парсинг 435 пар для объекта.

Архитектура (вдохновлено rdrr: «no headless browser для повторных запросов»):

  Фаза A — Bootstrap (1 page.goto):
    Открываем страницу одного пэйра с помощью Playwright. Захватываем XHR-шаблон
    (URL+method+headers+post_data) запроса к /hotel/search/…/rates + cookies
    контекста. Цена первого пэйра тоже тут добывается.

  Фаза B — Replay (HTTP без браузера):
    Для остальных пэйров подставляем даты в URL+тело шаблона и выполняем прямой
    HTTP-вызов через context.request.fetch(). Никакой навигации, никакого JS,
    никакой отрисовки → ~200-800 мс на пэйр. До 30 параллельных запросов.

  Фаза C — Fallback pass:
    Пэйры, которые не дали цены на Фазе B (ошибка сети, устаревшая сессия,
    транзиент), перепробуем через полноценный page.goto в пуле из 4 страниц.
    Этот пасс ловит то, что Replay пропустил.

Исправлен баг пропуска цен:
  Раньше _consume_json при XHR c пустым rates сразу резолвил future в None,
  и последующий XHR /rates с реальными ценами игнорировался. Теперь:
  – цены аккумулируются со всех совпавших XHR в окне таймаута;
  – None ставится только если пришёл ответ с authoritative /rates endpoint-а
    с пустыми rates ИЛИ истёк общий таймаут;
  – всегда даём небольшой grace-период (~0.4 с) после первого прайса, чтобы
    параллельный XHR с более дешёвой ставкой успел доехать.

Оптимизации сети:
  – route-level блокировка image/font/media/stylesheet + трекеры (GA, GTM,
    doubleclick, Яндекс.Метрика, facebook, hotjar, criteo, adriver, sentry).
  – общий браузер с OstrovokParser dispatcher-синглтоном.
  – 1 browser.new_context() на объект (не N), внутри — N страниц для goto-пула
    и до 30 асинхронных HTTP-replay поверх context.request.
"""
from __future__ import annotations

import asyncio
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger


# ── Directory helpers ────────────────────────────────────────────────────────

def get_results_dir() -> Path:
    """Папка 'результаты анализа' рядом с exe (или рядом с main.py в dev-режиме)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent / "результаты анализа"
    from app.utils.config import BASE_DIR
    return BASE_DIR / "результаты анализа"


def _make_filename(results_dir: Path) -> Path:
    today = date.today()
    base = f"Глубокий Анализ {today.strftime('%d.%m.%Y')}"
    p = results_dir / f"{base}.txt"
    if not p.exists():
        return p
    n = 2
    while True:
        p = results_dir / f"{base} ({n}).txt"
        if not p.exists():
            return p
        n += 1


# ── Date helpers ─────────────────────────────────────────────────────────────

def generate_date_pairs(start: date, window: int = 30) -> List[Tuple[date, date]]:
    """Все пары (checkin, checkout) в окне [start, start+window). C(30,2) = 435."""
    days = [start + timedelta(days=i) for i in range(window)]
    return [
        (days[i], days[j])
        for i in range(len(days) - 1)
        for j in range(i + 1, len(days))
    ]


def _fmt_short(d: date) -> str:
    return d.strftime("%d.%m.%y")


# ── Tuning ───────────────────────────────────────────────────────────────────

_NAV_TIMEOUT_MS      = 12_000
_REPLAY_TIMEOUT_MS   = 9_000
_XHR_TIMEOUT_S       = 10.0       # Фаза A — ждём прайсы после goto
_XHR_GRACE_S         = 0.4        # grace после первого прайса — ловим параллельные XHR
_REPLAY_CONCURRENCY  = 30         # одновременных HTTP replay на объект
_GOTO_POOL_SIZE      = 4          # страниц в пуле для Фазы C (fallback)
_GOTO_FALLBACK_XHR_S = 12.0       # Фаза C — больше терпения к медленным ответам
_BOOTSTRAP_RETRIES   = 2          # сколько раз пробуем захватить шаблон

# ресурсы, которые можно безопасно блокировать (не влияют на XHR с ценами)
_BLOCK_RESOURCE_TYPES = {"image", "font", "media", "stylesheet"}

_BLOCK_URL_SUBSTRINGS = (
    "google-analytics", "googletagmanager", "doubleclick",
    "mc.yandex.ru", "yandex.ru/metrika", "metrika.yandex",
    "facebook.com", "fbevents", "hotjar",
    "criteo", "adriver", "adfox",
    "sentry.io", "bugsnag",
)

_OSTROVOK_XHR_PATHS = (
    "/hotel/search/v2/site/hp/rates",
    "/hotel/search/v1/site/hp/rates",
    "/hotel/search/v1/site/hp/search",
)
# "authoritative" — XHR, чьи пустые rates означают реальную недоступность.
_OSTROVOK_AUTHORITATIVE_PATHS = (
    "/hotel/search/v2/site/hp/rates",
    "/hotel/search/v1/site/hp/rates",
)


# ── Global state ─────────────────────────────────────────────────────────────

_state: Dict[str, Any] = {
    "running":   False,
    "cancelled": False,
    "progress":  0,
    "total":     0,
    "file_path": "",
    "elapsed":   0,
    "start_ts":  0.0,
}
_cancel_event:  Optional[asyncio.Event] = None
_analysis_task: Optional[asyncio.Task]  = None


def get_state() -> Dict[str, Any]:
    d = dict(_state)
    if d["running"] and d["start_ts"]:
        d["elapsed"] = int(time.time() - d["start_ts"])
    return d


def request_cancel() -> None:
    _state["cancelled"] = True
    if _cancel_event:
        _cancel_event.set()


# ── Public API ───────────────────────────────────────────────────────────────

async def start_task(prop_ids: List[int]) -> None:
    global _analysis_task
    if _state["running"]:
        logger.warning("DeepAnalysis: already running, ignoring start request")
        return
    loop = asyncio.get_event_loop()
    _analysis_task = loop.create_task(_run(prop_ids))
    logger.info(f"DeepAnalysis: task created for {len(prop_ids)} properties")


# ── Internal execution ───────────────────────────────────────────────────────

async def _run(prop_ids: List[int]) -> None:
    global _cancel_event
    _cancel_event = asyncio.Event()
    _state.update({
        "running":   True,
        "cancelled": False,
        "progress":  0,
        "total":     0,
        "file_path": "",
        "start_ts":  time.time(),
        "elapsed":   0,
    })
    logger.info("DeepAnalysis: started")

    try:
        await _do_analysis(prop_ids)
    except asyncio.CancelledError:
        _state["cancelled"] = True
        logger.info("DeepAnalysis: CancelledError")
    except Exception as e:
        logger.error(f"DeepAnalysis: unexpected error: {e}", exc_info=True)
    finally:
        _state["running"] = False
        _state["elapsed"] = int(time.time() - _state["start_ts"])
        logger.info(
            f"DeepAnalysis: finished. progress={_state['progress']}/{_state['total']} "
            f"elapsed={_state['elapsed']}s cancelled={_state['cancelled']}"
        )


async def _do_analysis(prop_ids: List[int]) -> None:
    from app.backend.database import PropertyRepository
    from app.parser.dispatcher import _PARSER_INSTANCES, _make_parser
    from app.parser.ostrovok_parser import OstrovokParser
    from app.utils.config import PARSER_USER_AGENTS

    # Объекты
    props = []
    for pid in prop_ids:
        prop = await PropertyRepository.get_by_id(pid)
        if prop and prop.is_active:
            props.append(prop)
    if not props:
        logger.warning("DeepAnalysis: no active properties found")
        return

    today      = date.today()
    date_pairs = generate_date_pairs(today, window=30)
    n_pairs    = len(date_pairs)
    total      = len(props) * n_pairs
    _state["total"] = total
    logger.info(f"DeepAnalysis: {len(props)} props × {n_pairs} pairs = {total} requests")

    results_dir = get_results_dir()
    results_dir.mkdir(parents=True, exist_ok=True)
    file_path = _make_filename(results_dir)
    _state["file_path"] = str(file_path)
    logger.info(f"DeepAnalysis: output → {file_path}")

    # Reuse dispatcher's OstrovokParser singleton (shared browser)
    if "ostrovok" not in _PARSER_INSTANCES:
        _PARSER_INSTANCES["ostrovok"] = _make_parser("ostrovok")
    parser: OstrovokParser = _PARSER_INSTANCES["ostrovok"]  # type: ignore[assignment]

    browser      = await parser._get_browser()
    proxy_config = parser._playwright_proxy_config()

    all_lines: List[str] = []

    for prop in props:
        if _cancel_event.is_set():
            break

        title    = prop.title
        base_url = prop.url.split("?")[0]

        # Pre-fill всех 435 слотов «—» — гарантирует корректный вывод при отмене/ошибке
        pair_results: List[str] = [
            f"{title}; {_fmt_short(ci)}-{_fmt_short(co)}; —"
            for ci, co in date_pairs
        ]

        t0 = time.time()
        await _analyze_property(
            browser      = browser,
            proxy_config = proxy_config,
            user_agent   = PARSER_USER_AGENTS[0],
            title        = title,
            base_url     = base_url,
            date_pairs   = date_pairs,
            out          = pair_results,
            price_parser = parser._prices_from_xhr,
        )
        dt = time.time() - t0
        rps = (n_pairs / dt) if dt > 0 else 0.0
        logger.info(
            f"DeepAnalysis: «{title[:40]}» done in {dt:.1f}s ({rps:.2f} pairs/s)"
        )

        for line in pair_results:
            all_lines.append(line)
        all_lines.append("")

        _write_file(file_path, all_lines)

    _write_file(file_path, all_lines)


# ── Per-property pipeline ────────────────────────────────────────────────────

async def _analyze_property(
    *,
    browser,
    proxy_config,
    user_agent: str,
    title: str,
    base_url: str,
    date_pairs: List[Tuple[date, date]],
    out: List[str],
    price_parser,
) -> None:
    """
    Три фазы: Bootstrap (1 goto), Replay (HTTP flood), Fallback (goto для невзятых).
    """
    pool = _OstrovokContextPool(
        browser=browser,
        user_agent=user_agent,
        price_parser=price_parser,
    )
    try:
        await pool.start()

        # ── Фаза A: Bootstrap — первый пэйр + захват XHR-шаблона ────
        bootstrap_idx = 0
        bootstrap_ci, bootstrap_co = date_pairs[bootstrap_idx]

        bootstrap_price: Optional[float] = None
        for attempt in range(_BOOTSTRAP_RETRIES):
            if _cancel_event.is_set():
                return
            bootstrap_price = await pool.bootstrap(base_url, bootstrap_ci, bootstrap_co)
            if pool.template is not None:
                break
            logger.warning(f"DeepAnalysis bootstrap attempt {attempt + 1} captured no template")

        _apply_price(out, bootstrap_idx, title, bootstrap_ci, bootstrap_co, bootstrap_price)

        if pool.template is None:
            logger.warning(
                f"DeepAnalysis: «{title[:40]}» — XHR template not captured, "
                f"falling back to full goto for all pairs"
            )

        # ── Фаза B: Replay через context.request.fetch() ────────────
        pending_indices: List[int] = []

        if pool.template is not None:
            sem = asyncio.Semaphore(_REPLAY_CONCURRENCY)

            async def do_replay(idx: int) -> None:
                if _cancel_event.is_set():
                    return
                if idx == bootstrap_idx:
                    return
                ci, co = date_pairs[idx]
                nights = (co - ci).days
                async with sem:
                    if _cancel_event.is_set():
                        return
                    price = await pool.replay(ci, co, nights)
                if price is None:
                    pending_indices.append(idx)
                else:
                    _apply_price(out, idx, title, ci, co, price)

            await asyncio.gather(
                *(do_replay(i) for i in range(len(date_pairs))),
                return_exceptions=True,
            )
        else:
            # Шаблон не получен — весь объект идёт через goto
            pending_indices = [i for i in range(len(date_pairs)) if i != bootstrap_idx]

        logger.info(
            f"DeepAnalysis: «{title[:40]}» Phase B done, "
            f"{len(pending_indices)}/{len(date_pairs)} unresolved → Phase C"
        )

        # ── Фаза C: Fallback — для непотвержденных пэйров через goto ─
        if pending_indices and not _cancel_event.is_set():
            await pool.open_goto_pool()
            goto_sem = asyncio.Semaphore(_GOTO_POOL_SIZE)

            async def do_goto(idx: int) -> None:
                if _cancel_event.is_set():
                    return
                ci, co = date_pairs[idx]
                nights = (co - ci).days
                async with goto_sem:
                    if _cancel_event.is_set():
                        return
                    price = await pool.goto_fetch(base_url, ci, co, nights)
                _apply_price(out, idx, title, ci, co, price)

            await asyncio.gather(
                *(do_goto(i) for i in pending_indices),
                return_exceptions=True,
            )
    finally:
        await pool.close()


def _apply_price(
    out: List[str],
    idx: int,
    title: str,
    ci: date,
    co: date,
    price: Optional[float],
) -> None:
    label = f"{_fmt_short(ci)}-{_fmt_short(co)}"
    if price and price > 0:
        price_str = f"{price:,.0f} ₽".replace(",", "\u202f")
    else:
        price_str = "—"
    out[idx] = f"{title}; {label}; {price_str}"
    _state["progress"] += 1


# ── Context pool: 1 browser context per property ─────────────────────────────

class _OstrovokContextPool:
    """
    Один browser.new_context() на объект + инструменты:
      • bootstrap(): захват XHR-шаблона через goto
      • replay():    прямой HTTP-запрос через context.request.fetch()
      • goto_fetch(): fallback через goto в пуле страниц
    """

    def __init__(self, *, browser, user_agent: str, price_parser):
        self._browser      = browser
        self._user_agent   = user_agent
        self._price_parser = price_parser

        self._context = None

        # XHR-шаблон — захватывается в bootstrap, используется в replay
        self.template: Optional[Dict[str, Any]] = None
        self.template_ci: Optional[date] = None
        self.template_co: Optional[date] = None

        # Пул страниц для Фазы C (fallback через goto)
        self._goto_pages: List[Any] = []
        self._goto_workers: List[_GotoPageWorker] = []

    # ── lifecycle ─────────────────────────────────────────────────

    async def start(self) -> None:
        self._context = await self._browser.new_context(
            user_agent=self._user_agent,
            viewport={"width": 1366, "height": 768},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            ignore_https_errors=True,
            extra_http_headers={
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        await self._context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            "Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});"
            "window.chrome={runtime:{}};"
        )
        await self._context.route("**/*", self._route_filter)

    async def close(self) -> None:
        for w in self._goto_workers:
            try:
                await w.close()
            except Exception:
                pass
        self._goto_workers.clear()
        self._goto_pages.clear()
        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass
        self._context = None

    async def _route_filter(self, route) -> None:
        try:
            req = route.request
            rt  = req.resource_type
            url = req.url
            if rt in _BLOCK_RESOURCE_TYPES:
                await route.abort()
                return
            if any(s in url for s in _BLOCK_URL_SUBSTRINGS):
                await route.abort()
                return
            await route.continue_()
        except Exception:
            try:
                await route.continue_()
            except Exception:
                pass

    # ── Phase A: bootstrap via page.goto + XHR capture ────────────

    async def bootstrap(self, base_url: str, ci: date, co: date) -> Optional[float]:
        """
        Открывает страницу первого пэйра. Захватывает:
          • XHR-запрос rates/search — шаблон URL/method/headers/body
          • cookies контекста (через обычный goto)
          • цену пэйра (если пришла в XHR-ответе)
        """
        if self._context is None:
            return None

        page = await self._context.new_page()
        captured_tpl: Optional[Dict[str, Any]] = None
        collected_prices: List[float] = []
        price_event    = asyncio.Event()
        sold_out_event = asyncio.Event()
        nights = max((co - ci).days, 0)

        def on_request(req) -> None:
            nonlocal captured_tpl
            try:
                url = req.url
                if not any(ep in url for ep in _OSTROVOK_XHR_PATHS):
                    return
                # Приоритет — rates endpoint; если уже захватили rates, не перекрываем search-ом
                if captured_tpl is not None:
                    existing_url = captured_tpl["url"]
                    is_existing_rates = any(
                        p in existing_url for p in _OSTROVOK_AUTHORITATIVE_PATHS
                    )
                    is_new_rates = any(
                        p in url for p in _OSTROVOK_AUTHORITATIVE_PATHS
                    )
                    if is_existing_rates and not is_new_rates:
                        return
                headers = dict(req.headers)
                # Удаляем псевдо-заголовки HTTP/2 и managed-by-fetch заголовки,
                # которые APIRequestContext либо не принимает, либо проставит сам.
                for h in ("host", "connection", "content-length",
                          ":authority", ":method", ":path", ":scheme"):
                    headers.pop(h, None)
                captured_tpl = {
                    "url":       url,
                    "method":    req.method,
                    "headers":   headers,
                    "post_data": req.post_data,
                }
            except Exception:
                pass

        async def on_response_async(resp) -> None:
            try:
                if resp.status != 200:
                    return
                rurl = resp.url
                if not any(ep in rurl for ep in _OSTROVOK_XHR_PATHS):
                    return
                if "json" not in resp.headers.get("content-type", ""):
                    return
                data = await resp.json()
            except Exception:
                return

            try:
                prices = self._price_parser(data, nights)
            except Exception:
                prices = []

            if prices:
                collected_prices.extend(prices)
                price_event.set()
                return

            # Authoritative sold-out signal — только от /rates endpoint-а
            if any(p in rurl for p in _OSTROVOK_AUTHORITATIVE_PATHS):
                if isinstance(data, dict) and isinstance(data.get("rates"), list):
                    sold_out_event.set()

        def on_response(resp) -> None:
            asyncio.create_task(on_response_async(resp))

        page.on("request",  on_request)
        page.on("response", on_response)

        try:
            url = _build_page_url(base_url, ci, co)
            try:
                await page.goto(url, wait_until="commit", timeout=_NAV_TIMEOUT_MS)
            except Exception:
                pass

            # Ждём первый сигнал: либо цена, либо authoritative sold-out
            try:
                await asyncio.wait_for(
                    _any_event([price_event, sold_out_event]),
                    timeout=_XHR_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                pass

            # Grace — даём параллельным XHR прийти (могут принести более низкую цену)
            if collected_prices:
                await asyncio.sleep(_XHR_GRACE_S)

            if captured_tpl is not None:
                self.template    = captured_tpl
                self.template_ci = ci
                self.template_co = co

            return min(collected_prices) if collected_prices else None
        finally:
            try:
                page.remove_listener("request",  on_request)
            except Exception:
                pass
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

    # ── Phase B: replay captured XHR with swapped dates ───────────

    async def replay(self, ci: date, co: date, nights: int) -> Optional[float]:
        """Прямой HTTP-вызов XHR через context.request.fetch() — без goto, без JS."""
        tpl = self.template
        if tpl is None or self._context is None:
            return None
        if self.template_ci is None or self.template_co is None:
            return None

        new_url  = _swap_dates(tpl["url"],       self.template_ci, self.template_co, ci, co)
        new_body = _swap_dates(tpl["post_data"], self.template_ci, self.template_co, ci, co)

        # Дата не найдена ни в URL, ни в теле → replay невозможен
        url_changed  = (new_url != tpl["url"])
        body_changed = (tpl["post_data"] is None) or (new_body != tpl["post_data"])
        if not url_changed and not body_changed:
            return None

        try:
            resp = await self._context.request.fetch(
                new_url,
                method=tpl["method"],
                headers=tpl["headers"],
                data=new_body,
                timeout=_REPLAY_TIMEOUT_MS,
                fail_on_status_code=False,
            )
        except Exception as e:
            logger.debug(f"DeepAnalysis replay network error: {e}")
            return None

        if resp.status != 200:
            return None
        try:
            data = await resp.json()
        except Exception:
            return None

        try:
            prices = self._price_parser(data, nights)
        except Exception:
            prices = []

        return min(prices) if prices else None

    # ── Phase C: fallback via page.goto (pool of pages) ───────────

    async def open_goto_pool(self) -> None:
        """Создаёт пул страниц для fallback (ленивая инициализация)."""
        if self._goto_workers or self._context is None:
            return
        for _ in range(_GOTO_POOL_SIZE):
            page = await self._context.new_page()
            w = _GotoPageWorker(page, self._price_parser)
            self._goto_workers.append(w)

    async def goto_fetch(
        self,
        base_url: str,
        ci: date,
        co: date,
        nights: int,
    ) -> Optional[float]:
        """Возвращает страницу из пула и делает goto — семафор вызывается снаружи."""
        if not self._goto_workers:
            return None
        # Round-robin выбор свободной страницы (семафор снаружи гарантирует,
        # что одновременно работает не больше _GOTO_POOL_SIZE запросов)
        worker = self._goto_workers[id(asyncio.current_task()) % len(self._goto_workers)]
        url = _build_page_url(base_url, ci, co)
        return await worker.fetch(url, nights)


# ── Dedicated page-worker for Phase C (goto fallback) ────────────────────────

class _GotoPageWorker:
    """
    Одна Playwright-страница, обрабатывает вызовы fetch() последовательно
    (семафор на уровне pool гарантирует сериализацию на страницу).

    Race-guard через seq-счётчик: устаревшие ответы от предыдущего goto
    не резолвят future текущего вызова.
    """

    def __init__(self, page, price_parser):
        self._page         = page
        self._price_parser = price_parser

        self._seq          = 0
        self._cur_seq      = 0
        self._cur_nights   = 0
        self._cur_prices: List[float]   = []
        self._price_event: Optional[asyncio.Event] = None
        self._sold_out_event: Optional[asyncio.Event] = None
        self._lock         = asyncio.Lock()

        page.on("response", self._on_response)

    async def close(self) -> None:
        try:
            await self._page.close()
        except Exception:
            pass

    def _on_response(self, response) -> None:
        seq = self._cur_seq
        if self._price_event is None:
            return
        try:
            if response.status != 200:
                return
            rurl = response.url
            if not any(ep in rurl for ep in _OSTROVOK_XHR_PATHS):
                return
            if "json" not in response.headers.get("content-type", ""):
                return
        except Exception:
            return
        asyncio.create_task(self._consume(response, seq, rurl))

    async def _consume(self, response, seq: int, rurl: str) -> None:
        try:
            data = await response.json()
        except Exception:
            return
        if seq != self._cur_seq:
            return

        try:
            prices = self._price_parser(data, self._cur_nights)
        except Exception:
            prices = []

        if seq != self._cur_seq:
            return

        if prices:
            self._cur_prices.extend(prices)
            if self._price_event and not self._price_event.is_set():
                self._price_event.set()
            return

        # Sold-out сигнал — только от authoritative /rates
        if any(p in rurl for p in _OSTROVOK_AUTHORITATIVE_PATHS):
            if isinstance(data, dict) and isinstance(data.get("rates"), list):
                if self._sold_out_event and not self._sold_out_event.is_set():
                    self._sold_out_event.set()

    async def fetch(self, url: str, nights: int) -> Optional[float]:
        async with self._lock:
            self._seq        += 1
            self._cur_seq     = self._seq
            self._cur_nights  = nights
            self._cur_prices  = []
            self._price_event    = asyncio.Event()
            self._sold_out_event = asyncio.Event()

            try:
                try:
                    await self._page.goto(url, wait_until="commit", timeout=_NAV_TIMEOUT_MS)
                except Exception:
                    # XHR всё равно мог улететь — ждём чуть подольше
                    pass

                try:
                    await asyncio.wait_for(
                        _any_event([self._price_event, self._sold_out_event]),
                        timeout=_GOTO_FALLBACK_XHR_S,
                    )
                except asyncio.TimeoutError:
                    pass

                # Grace — ловим параллельный более дешёвый XHR
                if self._cur_prices:
                    await asyncio.sleep(_XHR_GRACE_S)

                return min(self._cur_prices) if self._cur_prices else None
            finally:
                # Инвалидируем seq, чтобы запоздавшие отклики не лезли в следующий fetch
                self._cur_seq += 1000
                self._price_event    = None
                self._sold_out_event = None


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _any_event(events: List[asyncio.Event]) -> None:
    """Возвращается при срабатывании любого из переданных event-ов."""
    tasks = [asyncio.create_task(e.wait()) for e in events]
    try:
        _done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
    except Exception:
        for t in tasks:
            t.cancel()
        raise


def _build_page_url(base_url: str, ci: date, co: date) -> str:
    return (
        f"{base_url}"
        f"?dates={ci.strftime('%d.%m.%Y')}-{co.strftime('%d.%m.%Y')}"
        f"&guests=2"
    )


def _swap_dates(
    text: Optional[str],
    old_ci: date,
    old_co: date,
    new_ci: date,
    new_co: date,
) -> Optional[str]:
    """
    Меняет старые даты на новые в произвольном тексте (URL или тело запроса).
    Поддерживает форматы: ISO (YYYY-MM-DD), RU (DD.MM.YYYY), slash (DD/MM/YYYY).
    """
    if text is None:
        return None

    formats = (
        "%Y-%m-%d",     # 2026-04-17
        "%d.%m.%Y",     # 17.04.2026
        "%d/%m/%Y",     # 17/04/2026
        "%Y/%m/%d",     # 2026/04/17
    )

    for fmt in formats:
        old_a = old_ci.strftime(fmt)
        new_a = new_ci.strftime(fmt)
        old_b = old_co.strftime(fmt)
        new_b = new_co.strftime(fmt)
        if old_a in text or old_b in text:
            # Заменяем в одном проходе, используя маркеры, чтобы не было коллизий
            # (если new_ci совпадает со старым old_co и т.п.)
            marker_a = f"\x01CI{fmt}\x01"
            marker_b = f"\x01CO{fmt}\x01"
            text = text.replace(old_a, marker_a).replace(old_b, marker_b)
            text = text.replace(marker_a, new_a).replace(marker_b, new_b)
    return text


# ── File writer ──────────────────────────────────────────────────────────────

def _write_file(path: Path, lines: List[str]) -> None:
    try:
        content = "\n".join(lines).rstrip("\n")
        if content:
            path.write_text(content + "\n", encoding="utf-8")
    except Exception as e:
        logger.error(f"DeepAnalysis _write_file: {e}")
