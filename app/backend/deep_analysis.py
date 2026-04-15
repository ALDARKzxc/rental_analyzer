"""
Deep Analysis Engine — парсит все датовые пары (435 пар = 30 дней окно) для каждого объекта.

Алгоритм:
  Для каждого объекта: checkin_day ∈ [0..28], checkout_day ∈ [checkin+1..29]
  Итого: 29+28+...+1 = 435 пар на объект.

Все пары объекта выполняются параллельно (с ограничением по семафору).
Объекты обрабатываются последовательно.
"""
from __future__ import annotations

import asyncio
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import sys

from loguru import logger


# ── Directory helpers ────────────────────────────────────────────────────────

def get_results_dir() -> Path:
    """Папка 'результаты анализа' рядом с exe (или рядом с main.py в dev-режиме)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent / "результаты анализа"
    # Dev режим: BASE_DIR — корень проекта
    from app.utils.config import BASE_DIR
    return BASE_DIR / "результаты анализа"


def _make_filename(results_dir: Path) -> Path:
    """Возвращает уникальный путь к файлу с нумерацией (2), (3)..."""
    today = date.today()
    date_str = today.strftime("%d.%m.%Y")
    base = f"Глубокий Анализ {date_str}"

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

def generate_date_pairs(start: date, window: int = 30) -> List[tuple]:
    """
    Генерируем все пары (checkin, checkout) в окне [start, start+window).
    Всего: window*(window-1)/2 = 435 для window=30.
    Порядок: (0,1), (0,2),...,(0,29), (1,2), (1,3),...,(28,29).
    """
    days = [start + timedelta(days=i) for i in range(window)]
    return [
        (days[i], days[j])
        for i in range(len(days) - 1)
        for j in range(i + 1, len(days))
    ]


def _fmt_short(d: date) -> str:
    """DD.MM.YY — формат для строк в файле."""
    return d.strftime("%d.%m.%y")


# ── Global state (читается из GUI-потока без блокировок — GIL защищает dict) ─

_state: Dict[str, Any] = {
    "running":   False,
    "cancelled": False,
    "progress":  0,
    "total":     0,
    "file_path": "",
    "elapsed":   0,
    "start_ts":  0.0,
}
_cancel_event: Optional[asyncio.Event] = None
_analysis_task: Optional[asyncio.Task] = None


def get_state() -> Dict[str, Any]:
    """Потокобезопасное чтение состояния (вызывается из Qt-потока)."""
    d = dict(_state)
    if d["running"] and d["start_ts"]:
        d["elapsed"] = int(time.time() - d["start_ts"])
    return d


def request_cancel() -> None:
    """Должна вызываться из asyncio-потока (через loop.call_soon_threadsafe)."""
    _state["cancelled"] = True
    if _cancel_event:
        _cancel_event.set()


# ── Public API ───────────────────────────────────────────────────────────────

async def start_task(prop_ids: List[int]) -> None:
    """
    Запускает анализ как фоновую asyncio Task.
    Возвращается немедленно — задача работает в фоне.
    """
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
        logger.info("DeepAnalysis: task was CancelledError")
    except Exception as e:
        logger.error(f"DeepAnalysis: unexpected error: {e}", exc_info=True)
    finally:
        _state["running"]  = False
        _state["elapsed"]  = int(time.time() - _state["start_ts"])
        logger.info(
            f"DeepAnalysis: finished. progress={_state['progress']}/{_state['total']} "
            f"elapsed={_state['elapsed']}s cancelled={_state['cancelled']}"
        )


async def _do_analysis(prop_ids: List[int]) -> None:
    from app.backend.database import PropertyRepository
    from app.parser.dispatcher import ParserDispatcher

    dispatcher = ParserDispatcher()

    # ── Загрузка объектов ────────────────────────────────────────
    props = []
    for pid in prop_ids:
        prop = await PropertyRepository.get_by_id(pid)
        if prop and prop.is_active:
            props.append(prop)

    if not props:
        logger.warning("DeepAnalysis: no active properties found")
        return

    # ── Генерация дат ─────────────────────────────────────────────
    today      = date.today()
    date_pairs = generate_date_pairs(today, window=30)   # 435 пар
    n_pairs    = len(date_pairs)
    total      = len(props) * n_pairs
    _state["total"] = total
    logger.info(f"DeepAnalysis: {len(props)} properties × {n_pairs} pairs = {total} requests")

    # ── Файл вывода ───────────────────────────────────────────────
    results_dir = get_results_dir()
    results_dir.mkdir(parents=True, exist_ok=True)
    file_path = _make_filename(results_dir)
    _state["file_path"] = str(file_path)
    logger.info(f"DeepAnalysis: output → {file_path}")

    # ── Семафор: не более 5 одновременных Playwright-запросов ────
    sem = asyncio.Semaphore(5)

    all_lines: List[str] = []

    for prop in props:
        if _cancel_event.is_set():
            break

        prop_title = prop.title
        base_url   = prop.url.split("?")[0]

        # Результаты для этого объекта, в порядке пар
        pair_results: List[str] = [""] * n_pairs

        async def fetch_one(
            idx: int,
            ci: date,
            co: date,
            _title: str  = prop_title,
            _base:  str  = base_url,
            _res:   list = pair_results,
        ) -> None:
            label = f"{_fmt_short(ci)}-{_fmt_short(co)}"

            if _cancel_event.is_set():
                _res[idx] = f"{_title}; {label}; —"
                _state["progress"] += 1
                return

            async with sem:
                if _cancel_event.is_set():
                    _res[idx] = f"{_title}; {label}; —"
                    _state["progress"] += 1
                    return

                # Используем dates=DD.MM.YYYY-DD.MM.YYYY&guests=2 —
                # именно этот формат читает JS Ostrovok и запускает XHR с ценами.
                # Формат checkin=YYYY-MM-DD игнорируется Ostrovok JS →
                # XHR не стреляет → DOM-fallback → статичная "от X ₽" без дат.
                url = (
                    f"{_base}"
                    f"?dates={ci.strftime('%d.%m.%Y')}-{co.strftime('%d.%m.%Y')}"
                    f"&guests=2"
                )

                try:
                    result    = await dispatcher.parse(url)
                    price     = result.get("price")
                    status    = result.get("status", "ok")

                    if price is not None and status == "ok":
                        # Формат: 5 000 ₽ (с узким пробелом)
                        price_str = f"{price:,.0f} ₽".replace(",", "\u202f")
                    else:
                        price_str = "—"
                except Exception as e:
                    logger.debug(f"DeepAnalysis pair {label}: {e}")
                    price_str = "—"

                _res[idx]           = f"{_title}; {label}; {price_str}"
                _state["progress"] += 1

        # Создаём все задачи сразу — семафор регулирует параллелизм
        tasks = [
            asyncio.create_task(fetch_one(i, ci, co))
            for i, (ci, co) in enumerate(date_pairs)
        ]

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            # Отменяем ещё не запущенные задачи
            for t in tasks:
                if not t.done():
                    t.cancel()
            _cancel_event.set()
            # Дожидаемся отмены
            await asyncio.gather(*tasks, return_exceptions=True)
            break

        # Добавляем строки объекта (в порядке пар)
        for line in pair_results:
            if line:
                all_lines.append(line)
        all_lines.append("")  # пустая строка между объектами

        # Записываем файл после каждого объекта (сохраняем промежуточный результат)
        _write_file(file_path, all_lines)

    # Финальная запись (на случай если цикл прервался до записи)
    _write_file(file_path, all_lines)


def _write_file(path: Path, lines: List[str]) -> None:
    """Записывает строки в файл. Тихо логирует ошибки."""
    try:
        content = "\n".join(lines).rstrip("\n")
        if content:
            path.write_text(content + "\n", encoding="utf-8")
    except Exception as e:
        logger.error(f"DeepAnalysis _write_file: {e}")
