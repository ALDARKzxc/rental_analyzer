"""
Deep Analysis Engine v4 — ультра-быстрый парсинг 435 пар для объекта.

Архитектура (вдохновлено rdrr: «no headless browser для повторных запросов»):

  Фаза A — Bootstrap (1 page.goto):
    Открываем страницу одного пэйра с помощью Playwright. Захватываем XHR-шаблон
    (URL+method+headers+post_data) запроса к /hotel/search/…/rates + cookies
    контекста. Цена первого пэйра тоже тут добывается.

  Фаза B — Replay через httpx (HTTP/2 без браузера):
    Cookies контекста переливаются в httpx.AsyncClient(http2=True) → реальный
    OS-level параллелизм с мультиплексированием. Без IPC в Playwright-драйвер
    → 3-5× быстрее, чем context.request.fetch(). До 50 параллельных запросов.
    Если httpx упал сетевой ошибкой N раз подряд — автофоллбэк на Playwright.

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

import httpx
from loguru import logger

try:
    import h2  # noqa: F401
    _HAS_HTTP2 = True
except ImportError:
    _HAS_HTTP2 = False


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

_NAV_TIMEOUT_MS       = 12_000
_REPLAY_TIMEOUT_MS    = 6_000      # быстрее отказываемся от зависших replay (было 9000)
_XHR_TIMEOUT_S        = 10.0       # Фаза A — ждём прайсы после goto
_XHR_GRACE_S          = 0.4        # grace после первого прайса — ловим параллельные XHR
# /rates — единственный *authoritative* endpoint. Если мы захватили только
# /search, Phase B replay возвращает пустые rates для большинства дат
# (/search — routing-endpoint, фильтрует по региону, без master_id хотеля).
# Поэтому даём /rates длинное окно — без него 80%+ пар уходят в медленную Phase C.
_XHR_RATES_EXTRA_GRACE_S = 10.0    # было 3.0 — увеличено, чтобы дождаться lazy /rates

# Concurrency Phase B — httpx primary path (HTTP/2, реальный параллелизм):
_REPLAY_CONCURRENCY_AUTH       = 50   # /rates (авторитетный) через httpx
_REPLAY_CONCURRENCY_SOFT       = 20   # /search (хрупкий) через httpx
_REPLAY_RETRY_CONCURRENCY_AUTH = 16   # retry на /rates
_REPLAY_RETRY_CONCURRENCY_SOFT = 8    # retry на /search
_REPLAY_RETRY_TIMEOUT_MS = 9_000      # retry даём больше времени — сервер мог быть перегружен

# Playwright APIRequestContext — fallback, если httpx dead:
_REPLAY_CONCURRENCY_AUTH_PW    = 30   # /rates через Playwright (bottleneck: IPC драйвера)
_REPLAY_CONCURRENCY_SOFT_PW    = 12   # /search через Playwright

# httpx connection pool (общий на объект):
_HTTPX_POOL_MAX_CONNECTIONS    = 80
_HTTPX_POOL_MAX_KEEPALIVE      = 40
_HTTPX_DEAD_THRESHOLD          = 5    # после N подряд сетевых ошибок пометить httpx как dead

_GOTO_POOL_SIZE       = 6          # страниц в пуле для Фазы C (было 4 → 6: быстрее fallback)
_GOTO_FALLBACK_XHR_S  = 12.0       # Фаза C — больше терпения к медленным ответам
_BOOTSTRAP_RETRIES    = 2          # сколько раз пробуем захватить шаблон
_PROPERTY_CONCURRENCY = 3          # одновременно обрабатываемых объектов (было 2 → 3)

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

    # Параллельная обработка объектов.
    # Каждый объект получает свой _OstrovokContextPool → browser.new_context().
    # Порядок строк в файле всегда сохраняется по индексу объекта (результаты
    # кладутся в results_per_prop[i], запись ведётся в порядке input).
    results_per_prop: Dict[int, List[str]] = {}
    write_lock = asyncio.Lock()
    prop_sem   = asyncio.Semaphore(_PROPERTY_CONCURRENCY)

    async def _flush() -> None:
        """Пишет файл: все готовые объекты в порядке input; незаконченные пропускаются."""
        lines: List[str] = []
        for i in range(len(props)):
            res = results_per_prop.get(i)
            if res is not None:
                lines.extend(res)
                lines.append("")
        _write_file(file_path, lines)

    async def process_property(idx: int, prop) -> None:
        if _cancel_event.is_set():
            return
        title    = prop.title
        base_url = prop.url.split("?")[0]

        # Pre-fill всех 435 слотов «—» — гарантирует корректный вывод при отмене/ошибке
        pair_results: List[str] = [
            f"{title}; {_fmt_short(ci)}-{_fmt_short(co)}; —"
            for ci, co in date_pairs
        ]

        t0 = time.time()
        try:
            async with prop_sem:
                if _cancel_event.is_set():
                    return
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
        except Exception as e:
            logger.error(
                f"DeepAnalysis property «{title[:40]}» failed: {e}", exc_info=True
            )
        finally:
            dt = time.time() - t0
            rps = (n_pairs / dt) if dt > 0 else 0.0
            logger.info(
                f"DeepAnalysis: «{title[:40]}» done in {dt:.1f}s ({rps:.2f} pairs/s)"
            )
            async with write_lock:
                results_per_prop[idx] = pair_results
                await _flush()

    await asyncio.gather(
        *(process_property(i, p) for i, p in enumerate(props)),
        return_exceptions=True,
    )

    # Финальная перезапись — на всякий случай гарантируем, что все секции на диске
    async with write_lock:
        await _flush()


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
                is_auth = any(
                    p in pool.template["url"] for p in _OSTROVOK_AUTHORITATIVE_PATHS
                )
                if is_auth:
                    break  # идеальный случай — /rates
                if attempt < _BOOTSTRAP_RETRIES - 1:
                    # Захватили только /search — пробуем ещё раз ради /rates
                    logger.info(
                        f"DeepAnalysis: «{title[:40]}» bootstrap attempt {attempt + 1} "
                        f"captured non-authoritative template (/search), retrying for /rates"
                    )
                    pool.template    = None
                    pool.template_ci = None
                    pool.template_co = None
                    continue
                # последняя попытка — принимаем /search как есть
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
        phase_b_ok        = 0
        phase_b_sold_out  = 0
        template_is_auth  = False

        if pool.template is not None:
            # Адаптивная concurrency: /rates держит 30 потоков, /search — только ~12.
            # Если захватили неавторитетный template, снижаем concurrency,
            # иначе сервер выбрасывает Timeout/ECONNRESET и 70%+ пар падает в Phase C.
            template_is_auth = any(
                p in pool.template["url"] for p in _OSTROVOK_AUTHORITATIVE_PATHS
            )
            replay_conc = (
                _REPLAY_CONCURRENCY_AUTH if template_is_auth
                else _REPLAY_CONCURRENCY_SOFT
            )
            logger.info(
                f"DeepAnalysis: «{title[:40]}» template={'auth /rates' if template_is_auth else 'soft /search'}, "
                f"Phase B concurrency={replay_conc}"
            )
            sem = asyncio.Semaphore(replay_conc)

            async def do_replay(idx: int) -> None:
                nonlocal phase_b_ok, phase_b_sold_out
                if _cancel_event.is_set():
                    return
                if idx == bootstrap_idx:
                    return
                ci, co = date_pairs[idx]
                nights = (co - ci).days
                async with sem:
                    if _cancel_event.is_set():
                        return
                    price, outcome = await pool.replay(ci, co, nights)
                if outcome == "ok":
                    _apply_price(out, idx, title, ci, co, price)
                    phase_b_ok += 1
                elif outcome == "sold_out":
                    # Авторитетный «нет цены» — пишем «—» сразу, Phase C не нужен
                    _apply_price(out, idx, title, ci, co, None)
                    phase_b_sold_out += 1
                else:
                    pending_indices.append(idx)

            await asyncio.gather(
                *(do_replay(i) for i in range(len(date_pairs))),
                return_exceptions=True,
            )
        else:
            # Шаблон не получен — весь объект идёт через goto
            pending_indices = [i for i in range(len(date_pairs)) if i != bootstrap_idx]

        logger.info(
            f"DeepAnalysis: «{title[:40]}» Phase B done: "
            f"ok={phase_b_ok}, sold_out={phase_b_sold_out}, "
            f"unresolved={len(pending_indices)}/{len(date_pairs)} → Phase B2, "
            f"fail_stats={pool._fail_stats}"
        )

        # ── Фаза B2: Retry для транзиентных сбоев Phase B ────────────
        # Многие "unresolved" — это не sold_out, а Timeout/ECONNRESET от перегрузки.
        # Повторяем их с пониженной concurrency и увеличенным таймаутом.
        # Это ключ к стабильной скорости между разными объектами: без retry
        # транзиенты гонят 70%+ пар в медленную Phase C (~5 сек/пара).
        if pending_indices and pool.template is not None and not _cancel_event.is_set():
            retry_indices = pending_indices
            pending_indices = []
            retry_conc = (
                _REPLAY_RETRY_CONCURRENCY_AUTH if template_is_auth
                else _REPLAY_RETRY_CONCURRENCY_SOFT
            )
            retry_sem = asyncio.Semaphore(retry_conc)
            b2_ok = 0
            b2_sold_out = 0

            async def do_replay_retry(idx: int) -> None:
                nonlocal b2_ok, b2_sold_out
                if _cancel_event.is_set():
                    return
                ci, co = date_pairs[idx]
                nights = (co - ci).days
                async with retry_sem:
                    if _cancel_event.is_set():
                        return
                    price, outcome = await pool.replay(
                        ci, co, nights, timeout_ms=_REPLAY_RETRY_TIMEOUT_MS
                    )
                if outcome == "ok":
                    _apply_price(out, idx, title, ci, co, price)
                    b2_ok += 1
                elif outcome == "sold_out":
                    _apply_price(out, idx, title, ci, co, None)
                    b2_sold_out += 1
                else:
                    pending_indices.append(idx)

            await asyncio.gather(
                *(do_replay_retry(i) for i in retry_indices),
                return_exceptions=True,
            )
            phase_b_ok       += b2_ok
            phase_b_sold_out += b2_sold_out
            logger.info(
                f"DeepAnalysis: «{title[:40]}» Phase B2 done: "
                f"recovered_ok={b2_ok}, recovered_sold_out={b2_sold_out}, "
                f"still_unresolved={len(pending_indices)}/{len(retry_indices)} → Phase C"
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

        # httpx primary path (HTTP/2): переливаем cookies из Playwright-контекста
        # и стреляем напрямую по XHR-endpoint-у. Миновав IPC Playwright-драйвера,
        # получаем реальный параллелизм и 3-5× ускорение на replay.
        self._httpx_client: Optional[httpx.AsyncClient] = None
        self._httpx_dead = False                 # после N сетевых ошибок flip → Playwright fallback
        self._httpx_net_fail_streak = 0          # счётчик подряд идущих сетевых ошибок
        # Диагностика: лог первых N fail-ов с реальным ответом сервера
        self._fail_log_budget = 3
        self._fail_stats = {"net": 0, "status": 0, "parse": 0, "empty_auth": 0, "empty_soft": 0}

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

        # Общий httpx-клиент на объект. HTTP/2 если h2 установлен, иначе HTTP/1.1.
        # Keep-alive → экономим TCP/TLS handshake между 400+ replay-запросами.
        limits = httpx.Limits(
            max_connections=_HTTPX_POOL_MAX_CONNECTIONS,
            max_keepalive_connections=_HTTPX_POOL_MAX_KEEPALIVE,
        )
        default_timeout = httpx.Timeout(
            connect=5.0,
            read=_REPLAY_TIMEOUT_MS / 1000.0,
            write=5.0,
            pool=10.0,
        )
        try:
            self._httpx_client = httpx.AsyncClient(
                http2=_HAS_HTTP2,
                limits=limits,
                timeout=default_timeout,
                follow_redirects=True,
                verify=True,
            )
        except Exception as e:
            logger.warning(f"DeepAnalysis: httpx client init failed ({e}); fallback HTTP/1.1")
            self._httpx_client = httpx.AsyncClient(
                http2=False,
                limits=limits,
                timeout=default_timeout,
                follow_redirects=True,
                verify=True,
            )

    async def close(self) -> None:
        for w in self._goto_workers:
            try:
                await w.close()
            except Exception:
                pass
        self._goto_workers.clear()
        self._goto_pages.clear()
        if self._httpx_client is not None:
            try:
                await self._httpx_client.aclose()
            except Exception:
                pass
            self._httpx_client = None
        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass
        self._context = None

    async def _sync_cookies_from_context(self) -> None:
        """Переливает cookies Playwright-контекста в httpx-клиент. Вызывается
        после успешного bootstrap, когда контекст уже получил session cookies.
        """
        if self._context is None or self._httpx_client is None:
            return
        try:
            pw_cookies = await self._context.cookies()
        except Exception as e:
            logger.debug(f"DeepAnalysis: failed to read context cookies: {e}")
            return
        # Обнуляем jar, чтобы не держать stale cookies между объектами
        try:
            self._httpx_client.cookies.clear()
        except Exception:
            pass
        for c in pw_cookies:
            try:
                self._httpx_client.cookies.set(
                    name=c.get("name", ""),
                    value=c.get("value", ""),
                    domain=c.get("domain", ""),
                    path=c.get("path", "/"),
                )
            except Exception:
                continue

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
        rates_event    = asyncio.Event()   # срабатывает при захвате /rates-шаблона
        # Диагностика: собираем все увиденные XHR-пути
        seen_xhr_paths: List[str] = []
        nights = max((co - ci).days, 0)
        # Трекинг orphan-тасков — без этого Python 3.14 ругается
        # "Task was destroyed but it is pending!" при закрытии страницы
        pending_response_tasks: List[asyncio.Task] = []

        def on_request(req) -> None:
            nonlocal captured_tpl
            try:
                url = req.url
                if not any(ep in url for ep in _OSTROVOK_XHR_PATHS):
                    return
                # Записываем путь — для логирования после bootstrap
                for ep in _OSTROVOK_XHR_PATHS:
                    if ep in url:
                        seen_xhr_paths.append(ep)
                        break
                is_new_rates = any(
                    p in url for p in _OSTROVOK_AUTHORITATIVE_PATHS
                )
                # Приоритет — rates endpoint; если уже захватили rates, не перекрываем search-ом
                if captured_tpl is not None:
                    existing_url = captured_tpl["url"]
                    is_existing_rates = any(
                        p in existing_url for p in _OSTROVOK_AUTHORITATIVE_PATHS
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
                if is_new_rates and not rates_event.is_set():
                    rates_event.set()
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
            t = asyncio.create_task(on_response_async(resp))
            pending_response_tasks.append(t)
            t.add_done_callback(
                lambda done: pending_response_tasks.remove(done)
                if done in pending_response_tasks else None
            )

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

            # Если захватили только /search — форсируем появление /rates:
            #   1) networkidle (вдруг /rates lazy-loaded)
            #   2) scroll to bottom — некоторые блоки цен инициируют /rates
            #      только когда блок видим в viewport
            #   3) полноценное ожидание rates_event до _XHR_RATES_EXTRA_GRACE_S
            if captured_tpl is not None and not rates_event.is_set():
                try:
                    await page.wait_for_load_state(
                        "networkidle", timeout=3_000
                    )
                except Exception:
                    pass
                if not rates_event.is_set():
                    try:
                        await page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight);"
                        )
                    except Exception:
                        pass
                    try:
                        await asyncio.wait_for(
                            rates_event.wait(),
                            timeout=_XHR_RATES_EXTRA_GRACE_S,
                        )
                    except asyncio.TimeoutError:
                        pass

            if captured_tpl is not None:
                self.template    = captured_tpl
                self.template_ci = ci
                self.template_co = co
                # Теперь в контексте есть session cookies — переливаем в httpx.
                await self._sync_cookies_from_context()

            # Диагностика — какие XHR увидели и что в итоге захватили
            tpl_path = "none"
            if captured_tpl is not None:
                for ep in _OSTROVOK_XHR_PATHS:
                    if ep in captured_tpl["url"]:
                        tpl_path = ep
                        break
            # Компактный счётчик встретившихся путей
            path_counts: Dict[str, int] = {}
            for p in seen_xhr_paths:
                path_counts[p] = path_counts.get(p, 0) + 1
            logger.info(
                f"DeepAnalysis bootstrap: template={tpl_path}, "
                f"prices_seen={len(collected_prices)}, xhr_hits={path_counts}"
            )

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
            # Дожидаемся orphan-тасков от on_response, иначе при
            # закрытии страницы они попадают в GC как pending
            if pending_response_tasks:
                snapshot = list(pending_response_tasks)
                for t in snapshot:
                    if not t.done():
                        t.cancel()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*snapshot, return_exceptions=True),
                        timeout=1.0,
                    )
                except Exception:
                    pass
            try:
                await page.close()
            except Exception:
                pass

    # ── Phase B: replay captured XHR with swapped dates ───────────

    async def replay(
        self, ci: date, co: date, nights: int,
        timeout_ms: int = _REPLAY_TIMEOUT_MS,
    ) -> Tuple[Optional[float], str]:
        """
        Главный путь Phase B. Primary — httpx (HTTP/2, настоящий параллелизм,
        без IPC в Playwright-драйвер). При подряд идущих сетевых ошибках
        помечает pool как dead и переключается на Playwright APIRequestContext.

        Возвращает (price, outcome):
          • ("ok",       price)  — цена получена
          • ("sold_out", None)   — 200 c пустыми rates на авторитетном endpoint
                                   → пара реально продана, Phase C не нужен
          • ("fail",     None)   — сетевая ошибка / non-200 / parse fail /
                                   пустой ответ на неавторитетном endpoint
                                   → пара уходит в Phase C на повторную проверку
        """
        if not self._httpx_dead and self._httpx_client is not None:
            result, network_error = await self._replay_via_httpx(ci, co, nights, timeout_ms)
            if network_error:
                self._httpx_net_fail_streak += 1
                if self._httpx_net_fail_streak >= _HTTPX_DEAD_THRESHOLD:
                    logger.warning(
                        f"DeepAnalysis: httpx marked DEAD after {self._httpx_net_fail_streak} "
                        f"consecutive network errors → fallback to Playwright fetch"
                    )
                    self._httpx_dead = True
                # Per-request fallback — даём шанс Playwright подобрать эту пару
                return await self._replay_via_playwright(ci, co, nights, timeout_ms)
            # Успех (в т.ч. sold_out / fail от сервера, не сетевой) — сбрасываем streak
            self._httpx_net_fail_streak = 0
            return result
        return await self._replay_via_playwright(ci, co, nights, timeout_ms)

    async def _replay_via_httpx(
        self, ci: date, co: date, nights: int, timeout_ms: int,
    ) -> Tuple[Tuple[Optional[float], str], bool]:
        """Возвращает ((price, outcome), network_error_flag).
        network_error_flag=True — httpx поймал connect/read/pool ошибку
        (→ триггерит streak-счётчик для маркировки dead).
        """
        tpl = self.template
        if tpl is None or self._httpx_client is None:
            return ((None, "fail"), False)
        if self.template_ci is None or self.template_co is None:
            return ((None, "fail"), False)

        new_url  = _swap_dates(tpl["url"],       self.template_ci, self.template_co, ci, co)
        new_body = _swap_dates(tpl["post_data"], self.template_ci, self.template_co, ci, co)

        url_changed  = (new_url != tpl["url"])
        body_changed = (tpl["post_data"] is None) or (new_body != tpl["post_data"])
        if not url_changed and not body_changed:
            return ((None, "fail"), False)

        content = new_body.encode("utf-8") if isinstance(new_body, str) else new_body
        req_timeout = httpx.Timeout(
            connect=5.0,
            read=timeout_ms / 1000.0,
            write=5.0,
            pool=10.0,
        )
        try:
            resp = await self._httpx_client.request(
                method=tpl["method"],
                url=new_url,
                headers=tpl["headers"],
                content=content,
                timeout=req_timeout,
            )
        except httpx.TimeoutException as e:
            self._fail_stats["net"] += 1
            logger.debug(f"DeepAnalysis httpx timeout: {e}")
            return ((None, "fail"), True)
        except httpx.NetworkError as e:
            self._fail_stats["net"] += 1
            logger.debug(f"DeepAnalysis httpx network error: {e}")
            return ((None, "fail"), True)
        except httpx.ProtocolError as e:
            self._fail_stats["net"] += 1
            logger.debug(f"DeepAnalysis httpx protocol error: {e}")
            return ((None, "fail"), True)
        except Exception as e:
            self._fail_stats["net"] += 1
            logger.debug(f"DeepAnalysis httpx unexpected: {e}")
            return ((None, "fail"), True)

        if resp.status_code != 200:
            self._fail_stats["status"] += 1
            if self._fail_log_budget > 0:
                self._fail_log_budget -= 1
                body_snip = resp.text[:200] if resp.text else ""
                logger.info(
                    f"DeepAnalysis httpx fail: status={resp.status_code} "
                    f"body[:200]={body_snip!r}"
                )
            return ((None, "fail"), False)
        try:
            data = resp.json()
        except Exception:
            self._fail_stats["parse"] += 1
            if self._fail_log_budget > 0:
                self._fail_log_budget -= 1
                logger.info(
                    f"DeepAnalysis httpx fail: json parse error, "
                    f"body[:200]={resp.text[:200]!r}"
                )
            return ((None, "fail"), False)

        try:
            prices = self._price_parser(data, nights)
        except Exception:
            prices = []

        if prices:
            return ((min(prices), "ok"), False)

        is_auth = any(p in new_url for p in _OSTROVOK_AUTHORITATIVE_PATHS)
        if is_auth and isinstance(data, dict) and isinstance(data.get("rates"), list):
            return ((None, "sold_out"), False)

        # Пустой ответ — на /search это типично (routing endpoint фильтрует);
        # на /rates без rates-поля — странно. Логируем первый такой кейс.
        if is_auth:
            self._fail_stats["empty_auth"] += 1
        else:
            self._fail_stats["empty_soft"] += 1
        if self._fail_log_budget > 0:
            self._fail_log_budget -= 1
            keys_snip = list(data.keys())[:10] if isinstance(data, dict) else type(data).__name__
            logger.info(
                f"DeepAnalysis httpx empty: auth={is_auth}, "
                f"data_keys={keys_snip}, body[:200]={str(data)[:200]!r}"
            )
        return ((None, "fail"), False)

    async def _replay_via_playwright(
        self, ci: date, co: date, nights: int, timeout_ms: int,
    ) -> Tuple[Optional[float], str]:
        """Fallback: прямой HTTP-вызов через Playwright APIRequestContext.
        Медленнее httpx (IPC в Node.js-драйвер), но использует тот же state
        что и захваченный XHR-шаблон — гарантированно работает, когда
        httpx не смог (например, тонкая валидация cookies/TLS на сервере).
        """
        tpl = self.template
        if tpl is None or self._context is None:
            return (None, "fail")
        if self.template_ci is None or self.template_co is None:
            return (None, "fail")

        new_url  = _swap_dates(tpl["url"],       self.template_ci, self.template_co, ci, co)
        new_body = _swap_dates(tpl["post_data"], self.template_ci, self.template_co, ci, co)

        url_changed  = (new_url != tpl["url"])
        body_changed = (tpl["post_data"] is None) or (new_body != tpl["post_data"])
        if not url_changed and not body_changed:
            return (None, "fail")

        try:
            resp = await self._context.request.fetch(
                new_url,
                method=tpl["method"],
                headers=tpl["headers"],
                data=new_body,
                timeout=timeout_ms,
                fail_on_status_code=False,
            )
        except Exception as e:
            logger.debug(f"DeepAnalysis playwright fetch error: {e}")
            return (None, "fail")

        if resp.status != 200:
            return (None, "fail")
        try:
            data = await resp.json()
        except Exception:
            return (None, "fail")

        try:
            prices = self._price_parser(data, nights)
        except Exception:
            prices = []

        if prices:
            return (min(prices), "ok")

        is_auth = any(p in new_url for p in _OSTROVOK_AUTHORITATIVE_PATHS)
        if is_auth and isinstance(data, dict) and isinstance(data.get("rates"), list):
            return (None, "sold_out")

        return (None, "fail")

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
        self._pending_consume_tasks: List[asyncio.Task] = []

        page.on("response", self._on_response)

    async def close(self) -> None:
        # Cleanup orphan consume-tasks перед закрытием страницы —
        # иначе Python 3.14 ругается "Task was destroyed but it is pending!"
        snapshot = list(self._pending_consume_tasks)
        for t in snapshot:
            if not t.done():
                t.cancel()
        if snapshot:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*snapshot, return_exceptions=True),
                    timeout=1.0,
                )
            except Exception:
                pass
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
        t = asyncio.create_task(self._consume(response, seq, rurl))
        self._pending_consume_tasks.append(t)
        t.add_done_callback(
            lambda done: self._pending_consume_tasks.remove(done)
            if done in self._pending_consume_tasks else None
        )

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
        # Дожидаемся завершения отменённых тасков — иначе при GC
        # Python 3.14 выдаёт "Task was destroyed but it is pending!"
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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
