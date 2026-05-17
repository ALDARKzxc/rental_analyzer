"""
Deep Analysis Engine v6 — direct-API only.

Прежняя архитектура (Playwright goto-пул + DOM fallback) была удалена,
потому что страницы Ostrovok игнорируют checkin/checkout в URL и не
триггерят /search или /rates без интерактивной работы пользователя с
календарём. DOM-фолбэк подбирал маркетинговый заголовок "от X ₽" из
шапки страницы — так в файлы попадали выдуманные цены 6 399 / 6 836 /
6 962 ₽ для каждого Property (~28 ложных recoveries в каждом прогоне).

Текущий пайплайн:
  Phase A  — direct /hotel/search/v1/site/hp/search для всех 435 пар
             параллельно (httpx, concurrency=10, batch=60). При sold_out
             повторяется без системного прокси — это уже встроено.
  Phase B  — API-rescue для пар с сетевыми ошибками (httpx, concurrency=2,
             до 4 попыток с растущей задержкой). Только сетевой error.
  Post-pr  — MinLOS пост-обработка: для check-in, где есть priced ≥ K ночей
             и sold_out для всех 1..K-1, короткие длительности
             переписываются [sold_out] → [MinLOS].

Точность: единственный источник истины — JSON-ответ /search. Если он
вернул rates с прайсом — это и есть цена бронирования. Если rates=null
— непродаётся (true sold_out, MinLOS, max-stay или cutoff времени;
MinLOS отделяется пост-обработкой). Сетевые сбои честно остаются [error]
вместо инвентов цен.

Скорость: ~30-60 с на property вместо прежних 150-450 с. На 3 properties
полный прогон укладывается в ~1.5-3 минуты против прежних 7-15.
"""
from __future__ import annotations

import asyncio
import re
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


def _make_filename(results_dir: Path, suffix: str = ".xlsx") -> Path:
    today = date.today()
    base = f"Глубокий Анализ {today.strftime('%d.%m.%Y')}"
    p = results_dir / f"{base}{suffix}"
    if not p.exists():
        return p
    n = 2
    while True:
        p = results_dir / f"{base} ({n}){suffix}"
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

# Одновременно обрабатываем только одно property — снижает межобъектное
# самозадушение через один IP/прокси и стабилизирует Ostrovok rate-limit.
_PROPERTY_CONCURRENCY = 1

# ── Direct-API phase ─────────────────────────────────────────────────────────
# Ostrovok отдаёт /hotel/search/v1/site/hp/search без cookies/CSRF.
# Ответ {rates: null, related_hotels_session_id: ...} — авторитетный сигнал
# "недоступно на эти даты" (true sold-out, MinLOS, max-stay, cutoff времени).
# Это единственный надёжный источник истины: попытка догнать прайс через
# Playwright заканчивалась чтением "от X ₽" заголовка с витрины, что давало
# фальшивые цены. Сетевые сбои API лечатся одним повторным проходом без
# системного прокси, а оставшиеся ошибки честно фиксируются как [error].
_API_CONCURRENCY          = 10
_API_PAIR_TIMEOUT_S       = 5.0
_API_CONNECT_TIMEOUT_S    = 4.0
_API_RETRY_DELAY_S        = 0.3
_API_BATCH_SIZE           = 60
_API_DEGRADED_CONCURRENCY = 4
_API_DEGRADE_NET_ERRORS   = 4
_API_ABORT_NET_ERRORS     = 12
_API_RESCUE_CONCURRENCY    = 2
_API_RESCUE_PAIR_TIMEOUT_S = 7.0
_API_RESCUE_DELAYS_S       = (0.0, 0.6, 1.4, 2.4)
_API_RESCUE_MAX_PAIRS      = 12

_ROW_PENDING   = "pending"
_ROW_FALLBACK  = "fallback"
_ROW_PRICED    = "priced"
_ROW_SOLD_OUT  = "sold_out"
_ROW_BLOCKED   = "blocked"
_ROW_CAPTCHA   = "captcha"
_ROW_NETWORK   = "network"
_ROW_ERROR     = "error"
_ROW_CANCELLED = "cancelled"
# Используется ТОЛЬКО при пост-обработке выходных строк (см. _apply_minlos_marker).
# Никогда не записывается в pair_states и не участвует в retry/progress-логике.
_ROW_MIN_LOS   = "min_los"
_FINAL_PROGRESS_STATES = {
    _ROW_PRICED,
    _ROW_SOLD_OUT,
    _ROW_BLOCKED,
    _ROW_CAPTCHA,
    _ROW_NETWORK,
    _ROW_ERROR,
}
_UNRESOLVED_STATES     = {_ROW_PENDING, _ROW_FALLBACK}

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


def _is_network_error(error: str) -> bool:
    return _has_any_marker((error or "").lower(), _NETWORK_ERROR_MARKERS)


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


def _is_api_sold_out_reason(reason: Optional[str]) -> bool:
    return bool(
        reason
        and (
            reason.startswith("api:sold_out")
            or reason.startswith("api-rescue:sold_out")
        )
    )


def _is_confirmed_sold_out_reason(
    reason: Optional[str],
    *,
    require_confirmation: bool,
) -> bool:
    """
    Now that the pipeline trusts the direct API as the authoritative source
    (Playwright DOM fallback was generating false 6 399 ₽ recoveries from
    the "от X ₽" marketing header), every ":sold_out" reason — including
    "api:sold_out" — is treated as confirmed for MinLOS post-processing.
    """
    if not require_confirmation:
        return True
    if not reason:
        return False
    return ":sold_out" in reason


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
    from app.backend.deep_analysis_export import (
        build_property_export_result,
        write_deep_analysis_xlsx,
    )
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

    # Reuse dispatcher singleton parser; deep analysis is API-only so no
    # Chromium browser is launched here.
    if "ostrovok" not in _PARSER_INSTANCES:
        _PARSER_INSTANCES["ostrovok"] = _make_parser("ostrovok")
    parser: OstrovokParser = _PARSER_INSTANCES["ostrovok"]  # type: ignore[assignment]
    api_headers = parser._headers()
    api_headers["User-Agent"] = PARSER_USER_AGENTS[0]

    export_results_per_prop: Dict[int, Any] = {}
    legacy_rows_per_prop: Dict[int, List[str]] = {}
    write_lock = asyncio.Lock()
    prop_sem   = asyncio.Semaphore(_PROPERTY_CONCURRENCY)

    def _legacy_lines() -> List[str]:
        """Пишет файл: все готовые объекты в порядке input; незаконченные пропускаются."""
        lines: List[str] = []
        for i in range(len(props)):
            res = legacy_rows_per_prop.get(i)
            if res is not None:
                lines.extend(res)
                lines.append("")
        return lines

    def _write_final_output() -> None:
        """Writes one final XLSX workbook; falls back to legacy text on export errors."""
        ordered = [
            export_results_per_prop[i]
            for i in range(len(props))
            if i in export_results_per_prop
        ]
        try:
            write_deep_analysis_xlsx(file_path, ordered, date_pairs)
            return
        except Exception as e:
            txt_path = _make_filename(results_dir, suffix=".txt")
            _state["file_path"] = str(txt_path)
            logger.error(
                f"DeepAnalysis: XLSX export failed, writing legacy txt to {txt_path}: {e}",
                exc_info=True,
            )
            _write_file(txt_path, _legacy_lines())

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
                    title        = title,
                    base_url     = base_url,
                    date_pairs   = date_pairs,
                    out          = pair_results,
                    states       = pair_states,
                    reasons      = pair_reasons,
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
            # MinLOS-маркировка: пост-обработка строк, не затрагивает парсинг
            # и pair_states. При любой ошибке — оставляет [sold_out] как было.
            _apply_minlos_marker(
                out=pair_results,
                states=pair_states,
                reasons=pair_reasons,
                title=title,
                date_pairs=date_pairs,
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
                legacy_rows_per_prop[idx] = pair_results
                export_results_per_prop[idx] = build_property_export_result(
                    prop=prop,
                    date_pairs=date_pairs,
                    rows=pair_results,
                    states=pair_states,
                    reasons=pair_reasons,
                )

    await asyncio.gather(
        *(process_property(i, p) for i, p in enumerate(props)),
        return_exceptions=True,
    )

    async with write_lock:
        _write_final_output()


# ── Per-property pipeline ────────────────────────────────────────────────────
#
# v6 pipeline (API-only):
#   Phase A  — direct-API httpx pool по всем парам. Авторитетный sold_out
#              на rates=null за ~300 мс; цены — из заполненного rates[].
#   Phase B  — API-rescue для пар с сетевыми ошибками (httpx no-proxy,
#              concurrency=2, до 4 попыток). Только net:* классификация.
#
# Playwright goto-пул удалён: страницы Ostrovok игнорируют URL-даты, /rates
# не триггерится без клика по календарю, а DOM содержит лишь "от X ₽" из
# шапки витрины — это давало фальшивые "восстановленные" цены.

async def _analyze_property(
    *,
    title: str,
    base_url: str,
    date_pairs: List[Tuple[date, date]],
    out: List[str],
    states: List[str],
    reasons: List[Optional[str]],
    parser,
    api_headers: Dict[str, str],
) -> None:
    """
    API-only pipeline. Direct /hotel/search/v1/site/hp/search is the only
    authoritative source for Ostrovok prices and availability.

    The previous architecture had a Playwright "verification" fallback that
    opened the hotel page and scraped any RUB amount from the DOM when XHR
    didn't fire — but Ostrovok's detail page never triggers /search or /rates
    when the URL already has checkin/checkout params (the UI shows "Укажите
    даты"). The DOM only contains the marketing "от X ₽" header price, so
    every "playwright:priced" recovery was that bogus advertised value.
    Removing the fallback eliminates that source of fabricated prices and
    cuts runtime by ~3x.

    Real network errors are retried via a small API-rescue pool with no-proxy
    direct connection. Pairs that survive both attempts are honestly marked
    as [error] / [network] rather than papered over with a fake price.
    """
    slug = parser._extract_slug(base_url)

    if not slug:
        logger.warning(
            f"DeepAnalysis «{title[:40]}»: не удалось извлечь slug из "
            f"URL {base_url[:100]} — Ostrovok API недоступен, пары будут "
            "помечены как [error]"
        )
        return

    # ── Phase A: direct API for all pairs ───────────────────────
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

    if _cancel_event.is_set():
        return

    # ── Phase B: API-rescue for network errors only (no Playwright) ─────
    rescue_indices = _select_api_rescue_indices(missing_error, reasons)
    if rescue_indices:
        skipped = sum(
            1 for idx in missing_error if _should_try_api_rescue(reasons[idx])
        ) - len(rescue_indices)
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
        if skipped > 0:
            logger.info(
                f'DeepAnalysis "{title[:40]}" skipped API-rescue for '
                f"{skipped} degraded pairs to avoid long stalls"
            )
        if rescued_remaining:
            logger.info(
                f'DeepAnalysis "{title[:40]}" {len(rescued_remaining)} pairs '
                "remain in error state after API rescue"
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

                if res.get("status") == "sold_out" and direct_client is not None:
                    direct_res = await parser._api_search_direct(
                        direct_client, slug, ci_s, co_s, nights,
                    )
                    if direct_res.get("status") != "sold_out":
                        res = direct_res

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

    client_kwargs = _api_client_kwargs(
        parser=parser,
        api_headers=api_headers,
        concurrency=concurrency,
        pair_timeout_s=_API_PAIR_TIMEOUT_S,
    )

    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(**client_kwargs))
        direct_client = None
        if getattr(parser, "_proxy", None):
            direct_kwargs = dict(client_kwargs)
            direct_kwargs["proxy"] = None
            direct_client = await stack.enter_async_context(
                httpx.AsyncClient(**direct_kwargs)
            )

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
                if res.get("status") == "sold_out" and direct_client is not None:
                    direct_res = await parser._api_search_direct(
                        direct_client, slug, ci_s, co_s, nights,
                    )
                    if direct_res.get("status") != "sold_out":
                        res = direct_res
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
    elif status == _ROW_MIN_LOS:
        price_str = "[MinLOS]"
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


def _apply_minlos_marker(
    *,
    out: List[str],
    states: List[str],
    reasons: Optional[List[Optional[str]]] = None,
    title: str,
    date_pairs: List[Tuple[date, date]],
) -> Optional[int]:
    """
    Пост-обработка: определяет MinLOS объекта по уже распарсенным результатам
    и переписывает [sold_out] → [MinLOS] для коротких длительностей.

    Запускается ПОСЛЕ полной финализации pair_states. Никаких сетевых запросов,
    парсинг не затрагивается. На внутренние состояния не влияет — переписывает
    только выходные строки `out`.

    Стратегия (защита от ложных срабатываний):
      1. Группируем результаты по check-in, а внутри него по nights.
      2. Для каждого check-in ищем первый priced на K ночей.
      3. Помечаем MinLOS только локально для этого check-in, если все
         длительности 1..K-1 существуют и являются sold_out.
      4. Если хотя бы одна короткая длительность не resolved или не sold_out,
         ничего не помечаем для этого check-in.

    Возвращает определённый MinLOS (для логирования) или None.
    Любая ошибка → возврат None без изменений `out`.
    """
    try:
        if len(states) != len(date_pairs) or len(out) != len(date_pairs):
            return None

        # Conservative local proof only: same check-in has sold_out for 1..K-1
        # nights and a real price at K nights. We intentionally do not infer
        # MinLOS for all-sold-out check-ins from other dates; API sold_out can be
        # stale or proxy-dependent, and weak inference was the source of false
        # MinLOS rows.
        by_checkin: Dict[date, Dict[int, int]] = {}
        for idx, (ci, co) in enumerate(date_pairs):
            n = (co - ci).days
            if n <= 0:
                continue
            by_checkin.setdefault(ci, {})[n] = idx

        candidate_counts: Counter[int] = Counter()
        mark_indices = set()

        for ci, nights_to_idx in by_checkin.items():
            priced_nights = sorted(
                n for n, idx in nights_to_idx.items()
                if states[idx] == _ROW_PRICED
            )
            if priced_nights:
                k_min_priced = priced_nights[0]
                if k_min_priced > 1:
                    shorter_indices: List[int] = []
                    valid_anchor = True
                    for k_prime in range(1, k_min_priced):
                        idx = nights_to_idx.get(k_prime)
                        if (
                            idx is None
                            or states[idx] != _ROW_SOLD_OUT
                            or not _is_confirmed_sold_out_reason(
                                reasons[idx] if reasons and idx < len(reasons) else None,
                                require_confirmation=reasons is not None,
                            )
                        ):
                            valid_anchor = False
                            break
                        shorter_indices.append(idx)

                    if valid_anchor:
                        candidate_counts[3 if k_min_priced >= 3 else k_min_priced] += 1
                        mark_indices.update(shorter_indices)

        dominant_minlos: Optional[int] = None
        if candidate_counts:
            most_common = candidate_counts.most_common()
            dominant_minlos = most_common[0][0]

        if not mark_indices:
            return None

        replaced = 0
        for idx in sorted(mark_indices):
            if states[idx] == _ROW_SOLD_OUT:
                ci, co = date_pairs[idx]
                out[idx] = _format_row(title, ci, co, status=_ROW_MIN_LOS)
                replaced += 1

        detected_minlos = dominant_minlos or (
            candidate_counts.most_common(1)[0][0] if candidate_counts else None
        )
        if replaced:
            confidence = "high" if len(candidate_counts) == 1 else "mixed"
            logger.info(
                f"DeepAnalysis: «{title[:40]}» MinLOS={detected_minlos} "
                f"confidence={confidence} candidates={dict(candidate_counts)} "
                f"(переписано {replaced} строк sold_out → MinLOS)"
            )
        return detected_minlos
    except Exception as e:
        logger.warning(
            f"DeepAnalysis: _apply_minlos_marker ошибка для «{title[:40]}»: {e}"
        )
        return None


# ── File writer ──────────────────────────────────────────────────────────────

def _write_file(path: Path, lines: List[str]) -> None:
    try:
        content = "\n".join(lines).rstrip("\n")
        if content:
            path.write_text(content + "\n", encoding="utf-8")
    except Exception as e:
        logger.error(f"DeepAnalysis _write_file: {e}")
