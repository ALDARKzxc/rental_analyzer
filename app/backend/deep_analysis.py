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
from collections import Counter
from contextlib import AsyncExitStack
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from loguru import logger

try:
    import h2  # noqa: F401
except ImportError:
    _HTTP2_AVAILABLE = False
else:
    _HTTP2_AVAILABLE = True


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

# Одновременно обрабатываем только 1 property.
# После перехода на API-first это самый безопасный режим: он убирает
# межобъектное самозадушение через один IP/прокси и при этом сохраняет
# быстрый per-property API пул.
_PROPERTY_CONCURRENCY = 1

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
_API_PAIR_TIMEOUT_S     = 5.0
_API_CONNECT_TIMEOUT_S  = 4.0
_API_RETRY_DELAY_S      = 0.3
_API_BATCH_SIZE         = 60
_API_DEGRADED_CONCURRENCY = 4
_API_DEGRADE_NET_ERRORS   = 4
_API_ABORT_NET_ERRORS     = 12
_API_RESCUE_CONCURRENCY = 2
_API_RESCUE_PAIR_TIMEOUT_S = 7.0
_API_RESCUE_DELAYS_S    = (0.0, 0.6, 1.4, 2.4)
_API_RESCUE_MAX_PAIRS   = 12
_FINAL_VERIFY_GRACE_S   = 0.35
_SLOW_LANE_TRIGGER_COUNT = 3
_SLOW_LANE_TRIGGER_RATIO = 0.10
_SLOW_LANE_PAUSE_S       = 2.0

_ROW_PENDING   = "pending"
_ROW_FALLBACK  = "fallback"
_ROW_PRICED    = "priced"
_ROW_SOLD_OUT  = "sold_out"
_ROW_BLOCKED   = "blocked"
_ROW_CAPTCHA   = "captcha"
_ROW_NETWORK   = "network"
_ROW_ERROR     = "error"
_ROW_CANCELLED = "cancelled"
_FINAL_PROGRESS_STATES = {
    _ROW_PRICED,
    _ROW_SOLD_OUT,
    _ROW_BLOCKED,
    _ROW_CAPTCHA,
    _ROW_NETWORK,
    _ROW_ERROR,
}
_UNRESOLVED_STATES     = {_ROW_PENDING, _ROW_FALLBACK}
_SLOW_LANE_RETRY_STATES = {_ROW_BLOCKED, _ROW_CAPTCHA, _ROW_NETWORK}

_NO_OFFERS_MARKERS = (
    "нет доступных предложений",
    "нет предложений",
    "no available offers",
    "no offers available",
)
_CAPTCHA_MARKERS = (
    "captcha",
    "recaptcha",
    "hcaptcha",
)
_BLOCKED_MARKERS = (
    "access denied",
    "forbidden",
    "blocked",
    "you have been blocked",
    "cf-challenge",
    "cloudflare",
)
_NETWORK_ERROR_MARKERS = (
    "net:",
    "http:",
    "ct:",
    "json:",
    "timeout",
    "timed out",
    "connect",
    "connection",
    "proxy",
    "ssl",
    "tls",
    "socket",
    "dns",
    "network",
    "tunnel",
    "remoteprotocolerror",
    "readtimeout",
    "connecttimeout",
    "proxyerror",
    "econn",
    "eof",
    "err_",
    "ns_error",
)

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


def _compose_reason(phase: str, category: str, detail: Optional[str] = None) -> str:
    if detail:
        clean = " ".join(str(detail).split())[:160]
        return f"{phase}:{category}:{clean}"
    return f"{phase}:{category}"


def _reason_group(reason: Optional[str]) -> Optional[str]:
    if not reason:
        return None
    parts = reason.split(":", 2)
    return ":".join(parts[:2])


def _set_pair_reason(reasons: Optional[List[Optional[str]]], idx: int, reason: Optional[str]) -> None:
    if reasons is not None and reason:
        reasons[idx] = reason


def _has_any_marker(text: str, markers: Tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def _is_no_offers_error(error: str) -> bool:
    return _has_any_marker((error or "").lower(), _NO_OFFERS_MARKERS)


def _is_network_error(error: str) -> bool:
    return _has_any_marker((error or "").lower(), _NETWORK_ERROR_MARKERS)


def _is_blocked_error(status: str, error: str) -> bool:
    if status == "blocked":
        return True
    return _has_any_marker((error or "").lower(), _BLOCKED_MARKERS)


def _is_captcha_error(status: str, error: str) -> bool:
    if status == "captcha":
        return True
    return _has_any_marker((error or "").lower(), _CAPTCHA_MARKERS)


def _classify_terminal_result(
    result: Dict[str, Any],
    *,
    phase: str,
) -> Tuple[str, Optional[float], str]:
    status = str(result.get("status") or "")
    price = result.get("price")
    error = str(result.get("error") or "")

    if status == "ok" and isinstance(price, (int, float)) and price > 0:
        return _ROW_PRICED, float(price), _compose_reason(phase, "priced")

    if status == "occupied" or (status == "not_found" and _is_no_offers_error(error)):
        return _ROW_SOLD_OUT, None, _compose_reason(phase, "sold_out", status or None)

    if _is_captcha_error(status, error):
        return _ROW_CAPTCHA, None, _compose_reason(phase, "captcha", error or status)

    if _is_blocked_error(status, error):
        return _ROW_BLOCKED, None, _compose_reason(phase, "blocked", error or status)

    if status in {"error", "not_found"} and _is_network_error(error):
        return _ROW_NETWORK, None, _compose_reason(phase, "network", error or status)

    detail = f"{status}:{error}" if error else (status or "unknown")
    return _ROW_ERROR, None, _compose_reason(phase, "error", detail)


def _should_run_slow_lane(unresolved_count: int, degraded_count: int) -> bool:
    if unresolved_count <= 0 or degraded_count <= 0:
        return False
    return (
        degraded_count >= _SLOW_LANE_TRIGGER_COUNT
        or (degraded_count / unresolved_count) > _SLOW_LANE_TRIGGER_RATIO
    )


def _summarize_terminal_states(states: List[str]) -> Dict[str, int]:
    counts = Counter(states)
    ordered = (
        _ROW_PRICED,
        _ROW_SOLD_OUT,
        _ROW_BLOCKED,
        _ROW_CAPTCHA,
        _ROW_NETWORK,
        _ROW_ERROR,
        _ROW_CANCELLED,
    )
    return {key: counts[key] for key in ordered if counts.get(key)}


def _should_try_api_rescue(reason: Optional[str]) -> bool:
    if not reason:
        return False
    parts = reason.split(":", 2)
    if len(parts) < 3:
        return False
    phase, category, detail = parts
    return phase == "api" and category == "fallback" and _is_network_error(detail)


def _select_api_rescue_indices(
    indices: List[int],
    reasons: List[Optional[str]],
) -> List[int]:
    selected: List[int] = []
    for idx in indices:
        if len(selected) >= _API_RESCUE_MAX_PAIRS:
            break
        if _should_try_api_rescue(reasons[idx]):
            selected.append(idx)
    return selected


def _apply_terminal_result(
    *,
    result: Dict[str, Any],
    phase: str,
    out: List[str],
    states: List[str],
    reasons: Optional[List[Optional[str]]],
    idx: int,
    title: str,
    ci: date,
    co: date,
    count_progress: bool,
) -> Tuple[str, Optional[float], str]:
    status, price, reason = _classify_terminal_result(result, phase=phase)
    _set_pair_status(
        out,
        states,
        idx,
        title,
        ci,
        co,
        status=status,
        price=price,
        count_progress=count_progress,
        reasons=reasons,
        reason=reason,
    )
    return status, price, reason


def _api_client_kwargs(
    *,
    parser,
    api_headers: Dict[str, str],
    concurrency: int,
    pair_timeout_s: float,
) -> Dict[str, Any]:
    return {
        "limits": httpx.Limits(
            max_connections=concurrency + 4,
            max_keepalive_connections=concurrency,
        ),
        "timeout": httpx.Timeout(pair_timeout_s, connect=_API_CONNECT_TIMEOUT_S),
        "headers": dict(api_headers),
        "trust_env": False,
        "proxy": getattr(parser, "_proxy", None) or None,
        "http2": _HTTP2_AVAILABLE,
    }


def _should_try_direct_api_without_proxy(parser, error: str) -> bool:
    """
    Безопасный trigger для второй попытки без системного прокси.

    Используем его только на явно сетевых/прокси-подобных ошибках. Нормальные
    API-ответы и схемные ошибки не трогаем, чтобы не тратить лишнее время и не
    менять корректное поведение на быстрых машинах.
    """
    if not (getattr(parser, "_proxy", None) or None):
        return False
    return error.startswith(("net:", "http:", "ct:", "json:"))


def get_state() -> Dict[str, Any]:
    d = dict(_state)
    if d["running"] and d["start_ts"]:
        d["elapsed"] = int(time.time() - d["start_ts"])
    return d


def request_cancel() -> None:
    if not _state["cancelled"]:
        logger.info("DeepAnalysis: cancel requested")
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
    api_headers = parser._headers()
    api_headers["User-Agent"] = PARSER_USER_AGENTS[0]

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
        title    = prop.title
        base_url = prop.url.split("?")[0]
        pair_states: List[str] = [_ROW_PENDING for _ in date_pairs]
        pair_reasons: List[Optional[str]] = [None for _ in date_pairs]

        pair_results: List[str] = [
            _format_row(title, ci, co, status=_ROW_PENDING)
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
                    states       = pair_states,
                    reasons      = pair_reasons,
                    price_parser = parser._prices_from_xhr,
                    parser       = parser,
                    api_headers  = api_headers,
                )
        except Exception as e:
            logger.error(
                f"DeepAnalysis property «{title[:40]}» failed: {e}", exc_info=True
            )
        finally:
            sealed = _seal_incomplete_pairs(
                out=pair_results,
                states=pair_states,
                reasons=pair_reasons,
                title=title,
                date_pairs=date_pairs,
                cancelled=_cancel_event.is_set(),
            )
            if sealed:
                label = "cancelled" if _cancel_event.is_set() else "error"
                logger.info(
                    f"DeepAnalysis: «{title[:40]}» finalized {sealed} "
                    f"unfinished pairs as {label}"
                )
            dt = time.time() - t0
            rps = (n_pairs / dt) if dt > 0 else 0.0
            logger.info(
                f"DeepAnalysis: «{title[:40]}» done in {dt:.1f}s ({rps:.2f} pairs/s)"
            )
            logger.info(
                f"DeepAnalysis: В«{title[:40]}В» terminal states "
                f"{_summarize_terminal_states(pair_states)}, "
                f"reasons_top={dict(Counter(filter(None, (_reason_group(r) for r in pair_reasons))).most_common(8))}"
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
    states: List[str],
    reasons: List[Optional[str]],
    price_parser,
    parser,
    api_headers: Dict[str, str],
) -> None:
    slug = parser._extract_slug(base_url)

    # ── Phase A: direct-API ─────────────────────────────────────
    if slug:
        missing_error = await _api_phase(
            title      = title,
            slug       = slug,
            date_pairs = date_pairs,
            out        = out,
            states     = states,
            reasons    = reasons,
            parser     = parser,
            api_headers= api_headers,
        )
    else:
        logger.warning(
            f"DeepAnalysis «{title[:40]}»: не удалось извлечь slug из "
            f"URL {base_url[:100]} — будет использован только Playwright"
        )
        missing_error = list(range(len(date_pairs)))

    if not missing_error or _cancel_event.is_set():
        return

    rescue_candidates = [
        idx for idx in missing_error
        if _should_try_api_rescue(reasons[idx])
    ]
    rescue_indices = _select_api_rescue_indices(missing_error, reasons)
    if rescue_indices:
        rescued_remaining = await _api_rescue_phase(
            title       = title,
            slug        = slug,
            date_pairs  = date_pairs,
            indices     = rescue_indices,
            out         = out,
            states      = states,
            reasons     = reasons,
            parser      = parser,
            api_headers = api_headers,
        )
        rescued_set = set(rescue_indices)
        missing_error = [
            idx for idx in missing_error
            if idx not in rescued_set
        ] + rescued_remaining
        skipped = len(rescue_candidates) - len(rescue_indices)
        if skipped > 0:
            logger.info(
                f'DeepAnalysis "{title[:40]}" skipped API-rescue for '
                f"{skipped} degraded pairs to avoid long stalls"
            )
    elif missing_error:
        logger.info(
            f'DeepAnalysis "{title[:40]}" skipping API-rescue: '
            "remaining errors are non-network or capped"
        )

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
        states       = states,
        reasons      = reasons,
        price_parser = price_parser,
    )

    if _cancel_event.is_set():
        return

    fallback_missing = [
        idx for idx in missing_error
        if states[idx] == _ROW_FALLBACK
    ]
    if not fallback_missing:
        return

    verify = await _final_verify_phase(
        title      = title,
        base_url   = base_url,
        date_pairs = date_pairs,
        indices    = fallback_missing,
        out        = out,
        states     = states,
        reasons    = reasons,
        parser     = parser,
    )

    degraded_candidates = verify["slow_lane_candidates"]
    if (
        degraded_candidates
        and _should_run_slow_lane(len(fallback_missing), len(degraded_candidates))
        and not _cancel_event.is_set()
    ):
        await _slow_lane_phase(
            title      = title,
            base_url   = base_url,
            date_pairs = date_pairs,
            indices    = degraded_candidates,
            out        = out,
            states     = states,
            reasons    = reasons,
            parser     = parser,
        )


async def _api_phase(
    *,
    title: str,
    slug: str,
    date_pairs: List[Tuple[date, date]],
    out: List[str],
    states: List[str],
    reasons: List[Optional[str]],
    parser,
    api_headers: Dict[str, str],
) -> List[int]:
    """
    Параллельный httpx-пул по всем парам дат.
    Возвращает список индексов с сетевыми/5xx/HTML ошибками — эти пары
    пойдут в Playwright fallback. Успех и sold_out считаются финальными.
    """
    t0 = time.time()
    ok_count = sold_out_count = error_count = deferred_count = 0
    missing: List[int] = []
    error_reasons: Counter[str] = Counter()
    slow_over_5s = 0
    concurrency = _API_CONCURRENCY
    all_indices = list(range(len(date_pairs)))

    for offset in range(0, len(all_indices), _API_BATCH_SIZE):
        if _cancel_event.is_set():
            break
        batch_indices = all_indices[offset:offset + _API_BATCH_SIZE]
        batch = await _run_api_batch(
            title       = title,
            slug        = slug,
            date_pairs  = date_pairs,
            indices     = batch_indices,
            out         = out,
            states      = states,
            reasons     = reasons,
            parser      = parser,
            api_headers = api_headers,
            concurrency = concurrency,
        )
        ok_count       += batch["ok_count"]
        sold_out_count += batch["sold_out_count"]
        error_count    += batch["error_count"]
        slow_over_5s   += batch["slow_over_5s"]
        error_reasons.update(batch["error_reasons"])

        missing.extend(batch["missing"])

        batch_net_errors = sum(
            count
            for reason, count in batch["error_reasons"].items()
            if str(reason).startswith("net:")
        )
        if (
            batch_net_errors >= _API_DEGRADE_NET_ERRORS
            and concurrency > _API_DEGRADED_CONCURRENCY
        ):
            concurrency = _API_DEGRADED_CONCURRENCY
            logger.warning(
                f"DeepAnalysis «{title[:40]}»: API degraded after batch "
                f"{offset // _API_BATCH_SIZE + 1}; net_errors={batch_net_errors}, "
                f"switching concurrency to {concurrency}"
            )

        if batch_net_errors >= _API_ABORT_NET_ERRORS:
            rest = all_indices[offset + _API_BATCH_SIZE:]
            if rest:
                deferred_count += len(rest)
                missing.extend(rest)
                logger.warning(
                    f"DeepAnalysis «{title[:40]}»: API circuit breaker tripped; "
                    f"deferring {len(rest)} remaining pairs to Playwright fallback"
                )
            break

    dt = time.time() - t0
    rps = (len(date_pairs) / dt) if dt > 0 else 0.0
    logger.info(
        f"DeepAnalysis «{title[:40]}» API-phase done in {dt:.1f}s "
        f"({rps:.1f} pair/s): ok={ok_count} sold_out={sold_out_count} "
        f"error={error_count}, deferred={deferred_count}, slow_gt_5s={slow_over_5s}, "
        f"errors_top={dict(error_reasons.most_common(5))}"
    )
    return missing


async def _api_rescue_phase(
    *,
    title: str,
    slug: str,
    date_pairs: List[Tuple[date, date]],
    indices: List[int],
    out: List[str],
    states: List[str],
    reasons: List[Optional[str]],
    parser,
    api_headers: Dict[str, str],
) -> List[int]:
    """
    Второй API-проход по редким error-парам.

    Здесь важнее корректность, чем raw-throughput: используем свежий httpx-клиент,
    малую concurrency и более длинный backoff, чтобы выбить транзиентные
    ConnectTimeout/ConnectError до тяжёлого Playwright fallback.
    """
    if not indices:
        return []

    t0 = time.time()
    ok_count = sold_out_count = error_count = 0
    remaining: List[int] = []
    error_reasons: Counter[str] = Counter()
    sem = asyncio.Semaphore(_API_RESCUE_CONCURRENCY)

    rescue_kwargs = _api_client_kwargs(
        parser=parser,
        api_headers=api_headers,
        concurrency=_API_RESCUE_CONCURRENCY,
        pair_timeout_s=_API_RESCUE_PAIR_TIMEOUT_S,
    )

    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(**rescue_kwargs))
        direct_client = None
        if getattr(parser, "_proxy", None):
            direct_kwargs = dict(rescue_kwargs)
            direct_kwargs["proxy"] = None
            direct_client = await stack.enter_async_context(
                httpx.AsyncClient(**direct_kwargs)
            )

        async def run_pair(idx: int) -> None:
            nonlocal ok_count, sold_out_count, error_count
            if _cancel_event.is_set():
                return
            ci, co = date_pairs[idx]
            nights = (co - ci).days
            ci_s   = ci.isoformat()
            co_s   = co.isoformat()
            res: Dict[str, Any] = {"status": "error", "error": "rescue:not_run"}

            async with sem:
                if _cancel_event.is_set():
                    return
                for delay_s in _API_RESCUE_DELAYS_S:
                    if delay_s > 0:
                        try:
                            await asyncio.sleep(delay_s)
                        except asyncio.CancelledError:
                            return
                    if _cancel_event.is_set():
                        return
                    res = await parser._api_search_direct(
                        client, slug, ci_s, co_s, nights,
                    )
                    if (
                        res.get("status") == "error"
                        and direct_client is not None
                        and _should_try_direct_api_without_proxy(
                            parser, str(res.get("error", ""))
                        )
                    ):
                        direct_res = await parser._api_search_direct(
                            direct_client, slug, ci_s, co_s, nights,
                        )
                        if direct_res.get("status") != "error":
                            res = direct_res
                    if res.get("status") != "error":
                        break

            status = res.get("status")
            if status == "ok":
                _set_pair_status(
                    out, states, idx, title, ci, co,
                    status=_ROW_PRICED,
                    price=min(res["prices"]),
                    count_progress=True,
                    reasons=reasons,
                    reason=_compose_reason("api-rescue", "priced"),
                )
                ok_count += 1
            elif status == "sold_out":
                _set_pair_status(
                    out, states, idx, title, ci, co,
                    status=_ROW_SOLD_OUT,
                    count_progress=True,
                    reasons=reasons,
                    reason=_compose_reason("api-rescue", "sold_out"),
                )
                sold_out_count += 1
            else:
                _set_pair_reason(
                    reasons,
                    idx,
                    _compose_reason(
                        "api-rescue",
                        "error",
                        str(res.get("error", "unknown")),
                    ),
                )
                remaining.append(idx)
                error_count += 1
                error_reasons[res.get("error", "unknown")] += 1

        await asyncio.gather(
            *(run_pair(i) for i in indices),
            return_exceptions=True,
        )

    dt = time.time() - t0
    logger.info(
        f"DeepAnalysis «{title[:40]}» API-rescue done in {dt:.1f}s: "
        f"recovered_ok={ok_count} recovered_sold_out={sold_out_count} "
        f"still_error={error_count}, errors_top={dict(error_reasons.most_common(5))}"
    )
    return remaining


async def _run_api_batch(
    *,
    title: str,
    slug: str,
    date_pairs: List[Tuple[date, date]],
    indices: List[int],
    out: List[str],
    states: List[str],
    reasons: List[Optional[str]],
    parser,
    api_headers: Dict[str, str],
    concurrency: int,
) -> Dict[str, Any]:
    ok_count = sold_out_count = error_count = 0
    missing: List[int] = []
    error_reasons: Counter[str] = Counter()
    slow_over_5s = 0
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(
        **_api_client_kwargs(
            parser=parser,
            api_headers=api_headers,
            concurrency=concurrency,
            pair_timeout_s=_API_PAIR_TIMEOUT_S,
        )
    ) as client:
        async def run_pair(idx: int) -> None:
            nonlocal ok_count, sold_out_count, error_count, slow_over_5s
            if _cancel_event.is_set():
                return
            ci, co = date_pairs[idx]
            nights = (co - ci).days
            ci_s   = ci.isoformat()
            co_s   = co.isoformat()

            async with sem:
                if _cancel_event.is_set():
                    return
                pair_t0 = time.time()
                res = await parser._api_search_direct(
                    client, slug, ci_s, co_s, nights,
                )
                if res.get("status") == "error":
                    try:
                        await asyncio.sleep(_API_RETRY_DELAY_S)
                    except asyncio.CancelledError:
                        return
                    if _cancel_event.is_set():
                        return
                    res = await parser._api_search_direct(
                        client, slug, ci_s, co_s, nights,
                    )
                if (time.time() - pair_t0) > 5.0:
                    slow_over_5s += 1

            status = res.get("status")
            if status == "ok":
                _set_pair_status(
                    out, states, idx, title, ci, co,
                    status=_ROW_PRICED,
                    price=min(res["prices"]),
                    count_progress=True,
                    reasons=reasons,
                    reason=_compose_reason("api", "priced"),
                )
                ok_count += 1
            elif status == "sold_out":
                _set_pair_status(
                    out, states, idx, title, ci, co,
                    status=_ROW_SOLD_OUT,
                    count_progress=True,
                    reasons=reasons,
                    reason=_compose_reason("api", "sold_out"),
                )
                sold_out_count += 1
            else:
                _set_pair_status(
                    out,
                    states,
                    idx,
                    title,
                    ci,
                    co,
                    status=_ROW_FALLBACK,
                    reasons=reasons,
                    reason=_compose_reason(
                        "api",
                        "fallback",
                        str(res.get("error", "unknown")),
                    ),
                )
                missing.append(idx)
                error_count += 1
                error_reasons[res.get("error", "unknown")] += 1

        await asyncio.gather(
            *(run_pair(i) for i in indices),
            return_exceptions=True,
        )

    return {
        "ok_count": ok_count,
        "sold_out_count": sold_out_count,
        "error_count": error_count,
        "missing": missing,
        "error_reasons": error_reasons,
        "slow_over_5s": slow_over_5s,
    }


async def _playwright_phase(
    *,
    browser,
    user_agent: str,
    title: str,
    base_url: str,
    date_pairs: List[Tuple[date, date]],
    indices: List[int],
    out: List[str],
    states: List[str],
    reasons: List[Optional[str]],
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
            sold_out_recovered = 0
            error_reasons: Counter[str] = Counter()

            async def run_pair(idx: int) -> None:
                nonlocal recovered, sold_out_recovered
                if _cancel_event.is_set():
                    return
                ci, co = date_pairs[idx]
                nights = (co - ci).days
                url    = _build_page_url(base_url, ci, co)
                try:
                    async with sem:
                        if _cancel_event.is_set():
                            return
                        w = workers[wi_ref[0] % len(workers)]
                        wi_ref[0] += 1
                        result = await w.fetch(url, nights, patient=True)
                except Exception as e:
                    error_reasons[f"worker:{e.__class__.__name__}"] += 1
                    return
                status = result.get("status")
                price = result.get("price")
                if status == "ok" and price and price > 0:
                    _set_pair_status(
                        out, states, idx, title, ci, co,
                        status=_ROW_PRICED,
                        price=price,
                        count_progress=True,
                        reasons=reasons,
                        reason=_compose_reason("playwright", "priced"),
                    )
                    recovered += 1
                elif status == "sold_out":
                    _set_pair_status(
                        out, states, idx, title, ci, co,
                        status=_ROW_SOLD_OUT,
                        count_progress=True,
                        reasons=reasons,
                        reason=_compose_reason("playwright", "sold_out"),
                    )
                    sold_out_recovered += 1
                elif not _cancel_event.is_set():
                    detail = str(result.get("error", "fallback:no_price"))
                    _set_pair_reason(
                        reasons,
                        idx,
                        _compose_reason("playwright", "fallback", detail),
                    )
                    error_reasons[detail] += 1

            await asyncio.gather(
                *(run_pair(i) for i in indices),
                return_exceptions=True,
            )
            logger.info(
                f"DeepAnalysis «{title[:40]}» fallback: recovered "
                f"priced={recovered}, sold_out={sold_out_recovered}, "
                f"still_unresolved={sum(1 for i in indices if states[i] == _ROW_FALLBACK)}, "
                f"errors_top={dict(error_reasons.most_common(5))}"
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

async def _final_verify_phase(
    *,
    title: str,
    base_url: str,
    date_pairs: List[Tuple[date, date]],
    indices: List[int],
    out: List[str],
    states: List[str],
    reasons: List[Optional[str]],
    parser,
) -> Dict[str, Any]:
    """
    Последний страховочный проход только для пар, которые пережили и API, и
    браузерный fallback без подтверждённой цены.

    Здесь используем parser.fetch() целиком: direct API -> Playwright -> httpx
    fallback с ретраями. Это дороже обычного fallback-пула, но применяется к
    единичным аномалиям и снимает ложные [error], когда цена существует, но
    не успела прийти в массовом параллельном режиме.
    """
    if not indices:
        return {"slow_lane_candidates": []}

    t0 = time.time()
    ok_count = sold_out_count = error_count = 0
    blocked_count = captcha_count = network_count = 0
    error_reasons: Counter[str] = Counter()
    slow_lane_candidates: List[int] = []

    for idx in indices:
        if _cancel_event.is_set():
            return {"slow_lane_candidates": slow_lane_candidates}
        if states[idx] != _ROW_FALLBACK:
            continue

        ci, co = date_pairs[idx]
        url = _build_page_url(base_url, ci, co)
        try:
            result = await parser.fetch(url)
        except Exception as e:
            result = {"status": "error", "error": f"verify:{e.__class__.__name__}"}

        terminal_state, _price, reason = _apply_terminal_result(
            result=result,
            phase="final-verify",
            out=out,
            states=states,
            reasons=reasons,
            idx=idx,
            title=title,
            ci=ci,
            co=co,
            count_progress=True,
        )

        if terminal_state == _ROW_PRICED:
            ok_count += 1
            continue

        if terminal_state == _ROW_SOLD_OUT:
            sold_out_count += 1
            continue

        if terminal_state == _ROW_BLOCKED:
            blocked_count += 1
            slow_lane_candidates.append(idx)
            error_reasons[reason] += 1
            try:
                await asyncio.sleep(_FINAL_VERIFY_GRACE_S)
            except asyncio.CancelledError:
                return {"slow_lane_candidates": slow_lane_candidates}
            continue

        if terminal_state == _ROW_CAPTCHA:
            captcha_count += 1
            slow_lane_candidates.append(idx)
            error_reasons[reason] += 1
            try:
                await asyncio.sleep(_FINAL_VERIFY_GRACE_S)
            except asyncio.CancelledError:
                return {"slow_lane_candidates": slow_lane_candidates}
            continue

        if terminal_state == _ROW_NETWORK:
            network_count += 1
            slow_lane_candidates.append(idx)
            error_reasons[reason] += 1
            try:
                await asyncio.sleep(_FINAL_VERIFY_GRACE_S)
            except asyncio.CancelledError:
                return {"slow_lane_candidates": slow_lane_candidates}
            continue

        error_count += 1
        error_reasons[reason] += 1
        try:
            await asyncio.sleep(_FINAL_VERIFY_GRACE_S)
        except asyncio.CancelledError:
            return {"slow_lane_candidates": slow_lane_candidates}

    dt = time.time() - t0
    logger.info(
        f'DeepAnalysis "{title[:40]}" final-verify done in {dt:.1f}s: '
        f"recovered_ok={ok_count} recovered_sold_out={sold_out_count} "
        f"blocked={blocked_count} captcha={captcha_count} network={network_count} "
        f"still_error={error_count}, errors_top={dict(error_reasons.most_common(5))}"
    )
    return {"slow_lane_candidates": slow_lane_candidates}


async def _slow_lane_phase(
    *,
    title: str,
    base_url: str,
    date_pairs: List[Tuple[date, date]],
    indices: List[int],
    out: List[str],
    states: List[str],
    reasons: List[Optional[str]],
    parser,
) -> None:
    if not indices:
        return

    try:
        await asyncio.sleep(_SLOW_LANE_PAUSE_S)
    except asyncio.CancelledError:
        return

    recovered_ok = recovered_sold_out = 0
    blocked_count = captcha_count = network_count = kept_terminal = 0
    error_reasons: Counter[str] = Counter()
    t0 = time.time()

    for idx in indices:
        if _cancel_event.is_set():
            return
        if states[idx] not in _SLOW_LANE_RETRY_STATES:
            continue

        ci, co = date_pairs[idx]
        url = _build_page_url(base_url, ci, co)
        previous_state = states[idx]
        previous_reason = reasons[idx]
        fresh_parser = parser.__class__()
        try:
            fresh_parser._proxy = getattr(parser, "_proxy", None)
            result = await fresh_parser.fetch(url)
        except Exception as e:
            result = {"status": "error", "error": f"slow-lane:{e.__class__.__name__}"}
        finally:
            try:
                await fresh_parser.close()
            except Exception:
                pass

        terminal_state, price, reason = _classify_terminal_result(
            result,
            phase="slow-lane",
        )

        if terminal_state in {_ROW_PRICED, _ROW_SOLD_OUT}:
            _set_pair_status(
                out,
                states,
                idx,
                title,
                ci,
                co,
                status=terminal_state,
                price=price,
                count_progress=True,
                reasons=reasons,
                reason=reason,
            )
            if terminal_state == _ROW_PRICED:
                recovered_ok += 1
            else:
                recovered_sold_out += 1
            continue

        if terminal_state in _SLOW_LANE_RETRY_STATES:
            _set_pair_status(
                out,
                states,
                idx,
                title,
                ci,
                co,
                status=terminal_state,
                count_progress=True,
                reasons=reasons,
                reason=reason,
            )
            if terminal_state == _ROW_BLOCKED:
                blocked_count += 1
            elif terminal_state == _ROW_CAPTCHA:
                captcha_count += 1
            else:
                network_count += 1
            error_reasons[reason] += 1
            continue

        _set_pair_status(
            out,
            states,
            idx,
            title,
            ci,
            co,
            status=previous_state,
            count_progress=True,
            reasons=reasons,
            reason=previous_reason or reason,
        )
        kept_terminal += 1
        error_reasons[reason] += 1

    dt = time.time() - t0
    logger.info(
        f'DeepAnalysis "{title[:40]}" slow-lane done in {dt:.1f}s: '
        f"recovered_ok={recovered_ok} recovered_sold_out={recovered_sold_out} "
        f"blocked={blocked_count} captcha={captcha_count} network={network_count} "
        f"kept_terminal={kept_terminal}, errors_top={dict(error_reasons.most_common(5))}"
    )


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
        self._saw_authoritative_sold_out = False
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
            if isinstance(data, dict) and data.get("rates") == []:
                self._saw_authoritative_sold_out = True
                if self._sold_out_event and not self._sold_out_event.is_set():
                    self._sold_out_event.set()

    async def fetch(
        self, url: str, nights: int, *, patient: bool = False,
    ) -> Dict[str, Any]:
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
            self._saw_authoritative_sold_out = False
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

                if self._cur_prices:
                    return {"status": "ok", "price": min(self._cur_prices)}

                if self._saw_authoritative_sold_out:
                    try:
                        await asyncio.sleep(_XHR_GRACE_S)
                    except asyncio.CancelledError:
                        pass
                    if self._cur_prices:
                        return {"status": "ok", "price": min(self._cur_prices)}
                    return {"status": "sold_out", "price": None}

                return {"status": "error", "price": None, "error": "fallback:no_price"}
            finally:
                # Инвалидируем seq, чтобы запоздавшие отклики не лезли в
                # следующий fetch этой же страницы (доп. защита поверх
                # _own_request_ids).
                self._cur_seq += 1000
                self._price_event    = None
                self._sold_out_event = None
                self._saw_authoritative_sold_out = False
                self._own_request_ids = set()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _format_row(
    title: str,
    ci: date,
    co: date,
    *,
    status: str,
    price: Optional[float] = None,
) -> str:
    label = f"{_fmt_short(ci)}-{_fmt_short(co)}"
    if status == _ROW_PRICED and price and price > 0:
        price_str = f"{price:,.0f} ₽".replace(",", "\u202f")
    elif status == _ROW_SOLD_OUT:
        price_str = "[sold_out]"
    elif status == _ROW_BLOCKED:
        price_str = "[blocked]"
    elif status == _ROW_CAPTCHA:
        price_str = "[captcha]"
    elif status == _ROW_NETWORK:
        price_str = "[network]"
    elif status == _ROW_PENDING:
        price_str = "[pending]"
    elif status == _ROW_FALLBACK:
        price_str = "[fallback]"
    elif status == _ROW_CANCELLED:
        price_str = "[cancelled]"
    elif status == _ROW_ERROR:
        price_str = "[error]"
    else:
        price_str = "[unknown]"
    return f"{title}; {label}; {price_str}"


def _set_pair_status(
    out: List[str],
    states: List[str],
    idx: int,
    title: str,
    ci: date,
    co: date,
    *,
    status: str,
    price: Optional[float] = None,
    count_progress: bool = False,
    reasons: Optional[List[Optional[str]]] = None,
    reason: Optional[str] = None,
) -> None:
    prev = states[idx]
    out[idx] = _format_row(title, ci, co, status=status, price=price)
    states[idx] = status
    _set_pair_reason(reasons, idx, reason)
    if count_progress and prev not in _FINAL_PROGRESS_STATES:
        _state["progress"] += 1


def _seal_incomplete_pairs(
    *,
    out: List[str],
    states: List[str],
    reasons: List[Optional[str]],
    title: str,
    date_pairs: List[Tuple[date, date]],
    cancelled: bool,
) -> int:
    sealed = 0
    final_state = _ROW_CANCELLED if cancelled else _ROW_ERROR
    count_progress = not cancelled
    for idx, state in enumerate(states):
        if state not in _UNRESOLVED_STATES:
            continue
        ci, co = date_pairs[idx]
        _set_pair_status(
            out,
            states,
            idx,
            title,
            ci,
            co,
            status=final_state,
            count_progress=count_progress,
            reasons=reasons,
            reason=(
                _compose_reason("seal", "cancelled", reasons[idx])
                if cancelled
                else _compose_reason("seal", "error", reasons[idx])
            ),
        )
        sealed += 1
    return sealed


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
