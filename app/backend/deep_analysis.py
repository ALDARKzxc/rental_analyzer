"""
Deep Analysis Engine v5 — unified turbo-goto pool.

Прежний пайплайн (v4: Phase A bootstrap → B httpx replay → B2 retry → B3
re-bootstrap → C goto fallback) разобран, потому что логи показали:
  • Ostrovok отдаёт `/search` endpoint на дефолтных goto-ах;
  • `/search` возвращает `{rates: null, related_hotels_session_id: ...}` на
    ~87% дат — бесполезен как replay-источник;
  • реальные цены приходят из `/rates`, который вызывается фронтом только
    после interactive trigger (scroll/hover на блок цен).

Новая архитектура — **один большой параллельный goto-пул на property**:

  Property processor:
    1 browser.new_context (общий для всех пар property)
    N страниц в пуле (_PAGES_PER_PROPERTY = 12)
    Каждая страница — независимый _PageWorker с seq-race-guard
    Каждая пара: page.goto('commit') → XHR intercept → return price

  Ключевые оптимизации, взятые из исследования:
    • init_script с auto-scroll — триггерит lazy-load /rates без
      отдельного JS-вызова для каждой пары;
    • route.abort для image/font/media/stylesheet/трекеры — убирает
      80% трафика;
    • authoritative price events — считаем ценой только /rates или
      /search с реально заполненным rates;
    • wait_until='commit' + early-exit по XHR — средний pair <2с;
    • pages reuse через seq-счётчик — без overhead на создание страницы
      каждый раз;
    • fail-fast 8с на pair — не залипаем на медленных ответах.

Параллелизм: _PROPERTY_CONCURRENCY=2 × _PAGES_PER_PROPERTY=12 = 24
одновременных goto. На 435 пар это ~35 волн × 3.5с ≈ 120с вместо
прежних 565с.
"""
from __future__ import annotations

import asyncio
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
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

# Ключевая ручка: сколько страниц-воркеров на property.
# Замеры: 12 страниц на Chromium 120+ держатся стабильно на 8GB RAM,
# дают ~1.2 GB памяти на property context (вкл. overhead изоляции).
_PAGES_PER_PROPERTY   = 12

# Одновременно обрабатываем не больше K property — иначе 24+ страниц
# в сумме могут спровоцировать rate-limit сервера по IP.
_PROPERTY_CONCURRENCY = 2

# Таймауты per pair. 12 сек — с запасом для медленного XHR под нагрузкой
# (24 одновременных goto-ов создают реальную нагрузку на сервер).
_NAV_TIMEOUT_MS       = 12_000
_PAIR_TIMEOUT_S       = 12.0
# grace после первого прайса — даём параллельным XHR принести более дешёвый rate
_XHR_GRACE_S          = 0.30
# Если за это время не пришёл ни один XHR — триггерим скролл принудительно.
# Опустили с 1.8→0.8: init_script поллит body до mount-а, но как safety
# Python тоже шлёт скролл пораньше — особенно важно для сервера под нагрузкой.
_FORCE_SCROLL_AFTER_S = 0.8

# Retry-проход для пар, которые не вернули цену в main-проходе.
# Меньше concurrency → меньше нагрузки → выше reliability для тех же дат.
_RETRY_CONCURRENCY      = 4
_RETRY_PAIR_TIMEOUT_S   = 22.0
_RETRY_FORCE_SCROLL_S   = 1.5

# ── Direct-API phase ─────────────────────────────────────────────
# Ostrovok отдаёт /hotel/search/v1/site/hp/search без cookies/CSRF, и
# ответ {rates: null, related_hotels_session_id: ...} — авторитетный
# сигнал «max-stay превышен / непродаётся» (не нужно ждать 12-22 сек).
# Поэтому основная фаза — httpx-пул по всем 435 парам; Playwright
# включается только для пар, где API упал по сети/5xx/Cloudflare.
_API_CONCURRENCY        = 10
_API_PAIR_TIMEOUT_S     = 7.0
_API_CONNECT_TIMEOUT_S  = 4.0
_API_RETRY_DELAY_S      = 0.3

# Ресурсы, безопасно блокируемые (не ломают XHR с ценами).
_BLOCK_RESOURCE_TYPES = {"image", "font", "media", "stylesheet"}

# Трекеры и 3rd-party — тоже в мусор.
_BLOCK_URL_SUBSTRINGS = (
    "google-analytics", "googletagmanager", "doubleclick",
    "mc.yandex.ru", "yandex.ru/metrika", "metrika.yandex",
    "facebook.com", "fbevents", "hotjar",
    "criteo", "adriver", "adfox",
    "sentry.io", "bugsnag",
    "vk.com/rtrg", "vk.ru/rtrg",
)

# XHR paths, которые могут содержать цены.
_OSTROVOK_XHR_PATHS = (
    "/hotel/search/v2/site/hp/rates",
    "/hotel/search/v1/site/hp/rates",
    "/hotel/search/v2/site/hp/search",
    "/hotel/search/v1/site/hp/search",
)

# Authoritative /rates — на них null/empty-rates означает реальную непродажу.
_OSTROVOK_AUTHORITATIVE_PATHS = (
    "/hotel/search/v2/site/hp/rates",
    "/hotel/search/v1/site/hp/rates",
)

# Auto-scroll init script — вставляется в каждую страницу контекста
# и триггерит lazy-load /rates без отдельных evaluate-ов на каждую пару.
#
# Проблема старой версии: setTimeout(kick, 400) мог сработать когда
# document.body ещё пустой, скролл улетал в никуда, /rates не триггерился.
#
# Решение: polling каждые 100мс до тех пор, пока body не наполнится
# детьми (то есть SPA реально отрисовалась). Только после этого скроллим.
# Троекратный скролл (низ → середина → верх) имитирует реального
# пользователя, надёжнее срабатывают IntersectionObserver-ы.
_AUTOSCROLL_INIT_SCRIPT = """
(() => {
  let triggered = false;
  const kick = () => {
    if (triggered) return;
    if (!document.body || document.body.children.length === 0) return;
    triggered = true;
    try {
      const h = Math.max(1500, document.body.scrollHeight || 2000);
      window.scrollTo(0, h);
      setTimeout(() => { try { window.scrollTo(0, Math.floor(h * 0.5)); } catch (e) {} }, 60);
      setTimeout(() => { try { window.scrollTo(0, 0); } catch (e) {} }, 160);
    } catch (e) {}
  };
  // Polling — надёжнее одиночного setTimeout.
  let tries = 0;
  const iv = setInterval(() => {
    tries++;
    kick();
    if (triggered || tries > 40) clearInterval(iv);
  }, 100);
  document.addEventListener('DOMContentLoaded', kick, { once: true });
})();
"""


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

    # Reuse dispatcher-singleton-а: один Chromium на весь процесс.
    if "ostrovok" not in _PARSER_INSTANCES:
        _PARSER_INSTANCES["ostrovok"] = _make_parser("ostrovok")
    parser: OstrovokParser = _PARSER_INSTANCES["ostrovok"]  # type: ignore[assignment]
    browser = await parser._get_browser()

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

        # Pre-fill слотов — гарантирует корректный вывод при отмене/ошибке.
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
                    user_agent   = PARSER_USER_AGENTS[0],
                    title        = title,
                    base_url     = base_url,
                    date_pairs   = date_pairs,
                    out          = pair_results,
                    price_parser = parser._prices_from_xhr,
                    parser       = parser,
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

    async with write_lock:
        await _flush()


# ── Per-property pipeline ────────────────────────────────────────────────────
#
# v6 pipeline:
#   Phase A  — direct-API httpx pool по всем парам. Работает без браузера,
#              даёт авторитетный sold_out на rates=null за ~300 мс.
#   Phase B  — Playwright goto-пул только для пар, где API вернул сетевую
#              ошибку (5xx/Cloudflare/HTML). Это те же воркеры что раньше,
#              но на значительно меньшем множестве индексов.
#
# Почему два прохода, а не один:
#   • API покрывает >95% пар за секунды — нет смысла гнать браузером;
#   • редкие 4xx/5xx от API — реальный повод переключиться на браузер
#     (там и Cloudflare-токены, и JS-rendered ответы);
#   • rates=null от API — авторитет, Playwright тот же ответ дал бы только
#     через 12-22 сек ожидания XHR (именно это ломало Moscow-apartment).

async def _analyze_property(
    *,
    browser,
    user_agent: str,
    title: str,
    base_url: str,
    date_pairs: List[Tuple[date, date]],
    out: List[str],
    price_parser,
    parser,
) -> None:
    slug = parser._extract_slug(base_url)

    # ── Phase A: direct-API ─────────────────────────────────────
    if slug:
        missing_error = await _api_phase(
            title      = title,
            slug       = slug,
            date_pairs = date_pairs,
            out        = out,
            parser     = parser,
            user_agent = user_agent,
        )
    else:
        logger.warning(
            f"DeepAnalysis «{title[:40]}»: не удалось извлечь slug из "
            f"URL {base_url[:100]} — будет использован только Playwright"
        )
        # Для не-ostrovok URL-ов (или нестандартного формата) предзаполняем
        # прочерками без прогресса — его добьёт Playwright-фаза.
        for idx, (ci, co) in enumerate(date_pairs):
            out[idx] = _format_row(title, ci, co, None)
        missing_error = list(range(len(date_pairs)))

    if not missing_error or _cancel_event.is_set():
        return

    # ── Phase B: Playwright fallback только для error-пар ────────
    logger.info(
        f"DeepAnalysis «{title[:40]}»: Playwright fallback для "
        f"{len(missing_error)} пар (API network error)"
    )
    await _playwright_phase(
        browser      = browser,
        user_agent   = user_agent,
        title        = title,
        base_url     = base_url,
        date_pairs   = date_pairs,
        indices      = missing_error,
        out          = out,
        price_parser = price_parser,
    )


async def _api_phase(
    *,
    title: str,
    slug: str,
    date_pairs: List[Tuple[date, date]],
    out: List[str],
    parser,
    user_agent: str,
) -> List[int]:
    """
    Параллельный httpx-пул по всем парам дат.
    Возвращает список индексов с сетевыми/5xx/HTML ошибками — эти пары
    пойдут в Playwright fallback. Успех и sold_out считаются финальными.
    """
    t0 = time.time()
    ok_count = sold_out_count = error_count = 0
    missing: List[int] = []

    limits  = httpx.Limits(
        max_connections           = _API_CONCURRENCY + 4,
        max_keepalive_connections = _API_CONCURRENCY,
    )
    timeout = httpx.Timeout(_API_PAIR_TIMEOUT_S, connect=_API_CONNECT_TIMEOUT_S)
    headers = {
        "User-Agent":      user_agent,
        "Accept":          "*/*",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Accept-Encoding": "gzip, deflate",
    }
    proxy = getattr(parser, "_proxy", None) or None
    sem   = asyncio.Semaphore(_API_CONCURRENCY)

    async with httpx.AsyncClient(
        limits=limits, timeout=timeout, headers=headers,
        trust_env=False, proxy=proxy,
    ) as client:

        async def run_pair(idx: int) -> None:
            nonlocal ok_count, sold_out_count, error_count
            if _cancel_event.is_set():
                return
            ci, co = date_pairs[idx]
            nights = (co - ci).days
            ci_s   = ci.isoformat()
            co_s   = co.isoformat()

            async with sem:
                if _cancel_event.is_set():
                    return
                res = await parser._api_search_direct(
                    client, slug, ci_s, co_s, nights,
                )
                # Один мягкий retry только для транзиентных ошибок
                if res.get("status") == "error":
                    try:
                        await asyncio.sleep(_API_RETRY_DELAY_S)
                    except asyncio.CancelledError:
                        return
                    res = await parser._api_search_direct(
                        client, slug, ci_s, co_s, nights,
                    )

            status = res.get("status")
            if status == "ok":
                _apply_price(out, idx, title, ci, co, min(res["prices"]))
                ok_count += 1
            elif status == "sold_out":
                _apply_price(out, idx, title, ci, co, None)
                sold_out_count += 1
            else:
                # Progress НЕ двигаем — Playwright-фаза его добьёт.
                # Предварительный дэш, чтобы промежуточный дамп в файл
                # (при отмене) не оставлял пустую строку.
                out[idx] = _format_row(title, ci, co, None)
                missing.append(idx)
                error_count += 1

        await asyncio.gather(
            *(run_pair(i) for i in range(len(date_pairs))),
            return_exceptions=True,
        )

    dt = time.time() - t0
    rps = (len(date_pairs) / dt) if dt > 0 else 0.0
    logger.info(
        f"DeepAnalysis «{title[:40]}» API-phase done in {dt:.1f}s "
        f"({rps:.1f} pair/s): ok={ok_count} sold_out={sold_out_count} "
        f"error={error_count}"
    )
    return missing


async def _playwright_phase(
    *,
    browser,
    user_agent: str,
    title: str,
    base_url: str,
    date_pairs: List[Tuple[date, date]],
    indices: List[int],
    out: List[str],
    price_parser,
) -> None:
    """
    Playwright goto-пул для пар-индексов, которые не ответили через API.
    Не меняет _state.progress — он уже финализирован в API-фазе.
    Перезаписывает out[idx] только если удалось достать цену.
    """
    context = await browser.new_context(
        user_agent=user_agent,
        viewport={"width": 1366, "height": 768},
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        ignore_https_errors=True,
        extra_http_headers={
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        await context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            "Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});"
            "window.chrome={runtime:{}};"
        )
        await context.add_init_script(_AUTOSCROLL_INIT_SCRIPT)
        await context.route("**/*", _route_filter)

        pool_size = min(_PAGES_PER_PROPERTY, max(1, len(indices)))
        workers: List[_PageWorker] = []
        try:
            for _ in range(pool_size):
                if _cancel_event.is_set():
                    break
                page = await context.new_page()
                workers.append(_PageWorker(page, price_parser))

            if not workers:
                return

            logger.info(
                f"DeepAnalysis «{title[:40]}» fallback: {len(workers)} workers, "
                f"{len(indices)} pairs (patient=True, timeout={_RETRY_PAIR_TIMEOUT_S}s)"
            )

            sem      = asyncio.Semaphore(len(workers))
            wi_ref   = [0]
            recovered = 0

            async def run_pair(idx: int) -> None:
                nonlocal recovered
                if _cancel_event.is_set():
                    return
                ci, co = date_pairs[idx]
                nights = (co - ci).days
                url    = _build_page_url(base_url, ci, co)
                async with sem:
                    if _cancel_event.is_set():
                        return
                    w = workers[wi_ref[0] % len(workers)]
                    wi_ref[0] += 1
                    price = await w.fetch(url, nights, patient=True)
                if price and price > 0:
                    out[idx] = _format_row(title, ci, co, price)
                    recovered += 1
                # Progress двигаем здесь — API-фаза для error-пар его не
                # инкрементировала. Итого progress = total, когда Phase B
                # отработал по всем missing-индексам.
                _state["progress"] += 1

            await asyncio.gather(
                *(run_pair(i) for i in indices),
                return_exceptions=True,
            )
            logger.info(
                f"DeepAnalysis «{title[:40]}» fallback: recovered "
                f"{recovered}/{len(indices)}"
            )
        finally:
            for w in workers:
                try:
                    await w.close()
                except Exception:
                    pass
    finally:
        try:
            await context.close()
        except Exception:
            pass


# ── Route filter (context-level, применяется ко всем страницам) ──────────────

async def _route_filter(route) -> None:
    """Блокируем тяжёлые ресурсы и трекеры. XHR и document пропускаем."""
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


# ── Page worker ──────────────────────────────────────────────────────────────

class _PageWorker:
    """
    Одна Playwright-страница, обрабатывает fetch() последовательно.

    Race-guard: seq-счётчик + _own_request_ids set.
      - seq фиксирует «поколение» fetch-а;
      - _own_request_ids запоминает объекты Request, которые вылетели
        во время текущего fetch-а (на page.on("request")).
      - в _on_response мы сначала проверяем что response.request
        принадлежит текущему fetch-у → отсекаем опоздавшие XHR от
        предыдущих goto-ов (иначе они могли бы «украсть» price_event
        и выдать соседнюю дату как свою цену).
    """

    def __init__(self, page, price_parser):
        self._page         = page
        self._price_parser = price_parser

        self._seq          = 0
        self._cur_seq      = 0
        self._cur_nights   = 0
        self._cur_prices: List[float]           = []
        self._price_event: Optional[asyncio.Event]    = None
        self._sold_out_event: Optional[asyncio.Event] = None
        self._lock         = asyncio.Lock()
        self._pending_tasks: List[asyncio.Task] = []
        # Пул id запросов, начатых во время текущего fetch. Чистится на
        # каждом fetch(). id(request) стабилен пока Playwright держит
        # ссылку на Request (до закрытия страницы), так что для нашего
        # time-window безопасен.
        self._own_request_ids: set[int] = set()

        page.on("request",  self._on_request)
        page.on("response", self._on_response)

    async def close(self) -> None:
        # Cleanup orphan consume-тасков — иначе Python 3.14
        # ругается "Task was destroyed but it is pending!".
        snapshot = list(self._pending_tasks)
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

    def _on_request(self, request) -> None:
        # Тэгируем каждый request, вылетевший во время активного fetch.
        # Используем id(), потому что объект Request хранится у Playwright
        # до закрытия страницы — значит id стабилен в нашем time-window.
        if self._price_event is None:
            return
        try:
            url = request.url
            if not any(ep in url for ep in _OSTROVOK_XHR_PATHS):
                return
        except Exception:
            return
        self._own_request_ids.add(id(request))

    def _on_response(self, response) -> None:
        if self._price_event is None:
            return
        seq = self._cur_seq
        try:
            if response.status != 200:
                return
            rurl = response.url
            if not any(ep in rurl for ep in _OSTROVOK_XHR_PATHS):
                return
            # Ключевая защита от stale-XHR: response принадлежит текущему
            # fetch только если его request был зарегистрирован в нашем
            # own-set. Иначе это опоздавший XHR от предыдущей даты.
            if id(response.request) not in self._own_request_ids:
                return
            if "json" not in response.headers.get("content-type", ""):
                return
        except Exception:
            return
        t = asyncio.create_task(self._consume(response, seq, rurl))
        self._pending_tasks.append(t)
        t.add_done_callback(
            lambda done: self._pending_tasks.remove(done)
            if done in self._pending_tasks else None
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

        # Authoritative sold-out: только от /rates с реальным полем rates
        # (не null — null ответ от /search НЕ считается sold-out).
        if any(p in rurl for p in _OSTROVOK_AUTHORITATIVE_PATHS):
            if isinstance(data, dict) and isinstance(data.get("rates"), list):
                if self._sold_out_event and not self._sold_out_event.is_set():
                    self._sold_out_event.set()

    async def fetch(
        self, url: str, nights: int, *, patient: bool = False,
    ) -> Optional[float]:
        """
        patient=False → main-проход: force-scroll 0.8с, total 12с.
        patient=True  → retry-проход: force-scroll 1.5с, total 22с.
                         Ожидание длиннее, потому что сервер мог подтупить
                         в прошлый раз — даём ему шанс отдать /rates.
        """
        force_scroll_s = _RETRY_FORCE_SCROLL_S if patient else _FORCE_SCROLL_AFTER_S
        total_s        = _RETRY_PAIR_TIMEOUT_S if patient else _PAIR_TIMEOUT_S

        async with self._lock:
            self._seq        += 1
            self._cur_seq     = self._seq
            self._cur_nights  = nights
            self._cur_prices  = []
            self._own_request_ids = set()   # сброс stale-XHR защиты
            self._price_event    = asyncio.Event()
            self._sold_out_event = asyncio.Event()

            try:
                try:
                    await self._page.goto(
                        url, wait_until="commit", timeout=_NAV_TIMEOUT_MS,
                    )
                except Exception:
                    # XHR мог уже полететь даже при nav-ошибке — продолжаем ждать.
                    pass

                # Гонка: ждём либо цену/sold-out, либо force_scroll_s
                # → форсим скролл, либо общий таймаут total_s.
                try:
                    await asyncio.wait_for(
                        _any_event([self._price_event, self._sold_out_event]),
                        timeout=force_scroll_s,
                    )
                except asyncio.TimeoutError:
                    # Init-script обычно триггерит скролл сам, но если
                    # страница медленная — дожимаем здесь. Двойной скролл
                    # вниз→верх эмитит больше IntersectionObserver-ов.
                    try:
                        await self._page.evaluate(
                            "(() => {"
                            "  const h = Math.max(1500, document.body ? "
                            "    document.body.scrollHeight : 2000);"
                            "  window.scrollTo(0, h);"
                            "  setTimeout(() => window.scrollTo(0, h*0.4), 80);"
                            "  setTimeout(() => window.scrollTo(0, 0), 200);"
                            "})()"
                        )
                    except Exception:
                        pass
                    try:
                        await asyncio.wait_for(
                            _any_event([self._price_event, self._sold_out_event]),
                            timeout=total_s - force_scroll_s,
                        )
                    except asyncio.TimeoutError:
                        pass

                # grace — ловим параллельный более дешёвый XHR
                if self._cur_prices:
                    try:
                        await asyncio.sleep(_XHR_GRACE_S)
                    except asyncio.CancelledError:
                        pass

                return min(self._cur_prices) if self._cur_prices else None
            finally:
                # Инвалидируем seq, чтобы запоздавшие отклики не лезли в
                # следующий fetch этой же страницы (доп. защита поверх
                # _own_request_ids).
                self._cur_seq += 1000
                self._price_event    = None
                self._sold_out_event = None
                self._own_request_ids = set()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _format_row(
    title: str, ci: date, co: date, price: Optional[float],
) -> str:
    label = f"{_fmt_short(ci)}-{_fmt_short(co)}"
    if price and price > 0:
        price_str = f"{price:,.0f} ₽".replace(",", "\u202f")
    else:
        price_str = "—"
    return f"{title}; {label}; {price_str}"


def _apply_price(
    out: List[str],
    idx: int,
    title: str,
    ci: date,
    co: date,
    price: Optional[float],
) -> None:
    out[idx] = _format_row(title, ci, co, price)
    _state["progress"] += 1


async def _any_event(events: List[asyncio.Event]) -> None:
    """Возвращается при срабатывании любого из переданных event-ов."""
    tasks = [asyncio.create_task(e.wait()) for e in events]
    try:
        _done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
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


# ── File writer ──────────────────────────────────────────────────────────────

def _write_file(path: Path, lines: List[str]) -> None:
    try:
        content = "\n".join(lines).rstrip("\n")
        if content:
            path.write_text(content + "\n", encoding="utf-8")
    except Exception as e:
        logger.error(f"DeepAnalysis _write_file: {e}")
