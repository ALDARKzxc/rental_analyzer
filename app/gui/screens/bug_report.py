"""
Диалог «Лог работы программы» — отладочная информация для пользователя.

UX-принципы:
  • Понятный язык: технические префиксы (`DeepAnalysis:`, `OstrovokParser:`)
    остаются для копирования в bug-report, но уровень переведён на русский.
  • Цветовая разметка: ошибки — красным, предупреждения — оранжевым.
  • Сводка наверху: счётчики ошибок/предупреждений за последние сутки.
  • Не подвисает на больших файлах: читаются только последние ~3 МБ.
  • Не блокирует UI: чтение и парсинг — синхронные, но с лимитом записей.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from PySide6.QtCore import Qt
from PySide6.QtGui import QGuiApplication, QTextCursor
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTextBrowser, QCheckBox, QFrame, QMessageBox,
)

from app.utils.config import LOGS_DIR

_LOG_LINE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+\|\s+"
    r"(\w+)\s+\|\s+([^|]+?)\s+\|\s+(.*)$"
)

# Сколько хвоста файла грузим (3 МБ ≈ ~10000 строк формата loguru).
_TAIL_BYTES = 3 * 1024 * 1024
# Сколько записей максимум показываем (свежее — наверху).
_MAX_ENTRIES = 1000

_LEVEL_COLOR = {
    "ERROR":    "#e54b4b",   # красный — акцент
    "CRITICAL": "#e54b4b",
    "WARNING":  "#ffa987",   # оранжевый
    "INFO":     "#d0c8c4",   # светлый нейтральный
    "DEBUG":    "#7a6a64",   # тусклый серый
    "TRACE":    "#7a6a64",
}

_LEVEL_LABEL = {
    "ERROR":    "ОШИБКА",
    "CRITICAL": "КРИТИЧНО",
    "WARNING":  "ВНИМАНИЕ",
    "INFO":     "ИНФО",
    "DEBUG":    "ОТЛАДКА",
    "TRACE":    "ТРАССА",
}


class _LogEntry:
    __slots__ = ("dt", "level", "module", "message")

    def __init__(self, dt: datetime, level: str, module: str, message: str):
        self.dt      = dt
        self.level   = level
        self.module  = module
        self.message = message

    def append_continuation(self, line: str) -> None:
        # Многострочные tracebacks приклеиваются к последнему сообщению
        self.message = self.message + "\n" + line.rstrip()


def _read_tail(path: Path, max_bytes: int) -> str:
    """Читаем последние max_bytes файла. На больших логах не загружаем всё."""
    try:
        size = path.stat().st_size
    except OSError:
        return ""
    offset = max(0, size - max_bytes)
    try:
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read()
    except OSError:
        return ""
    text = data.decode("utf-8", errors="replace")
    if offset > 0:
        # Отрезаем первую (вероятно неполную) строку
        nl = text.find("\n")
        if nl >= 0:
            text = text[nl + 1:]
    return text


def _parse_log(text: str) -> List[_LogEntry]:
    entries: List[_LogEntry] = []
    for raw in text.splitlines():
        m = _LOG_LINE_RE.match(raw)
        if m:
            d, t, level, module, message = m.groups()
            try:
                dt = datetime.strptime(f"{d} {t}", "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            entries.append(_LogEntry(
                dt=dt, level=level.upper(),
                module=module.strip(), message=message,
            ))
        else:
            # Продолжение многострочной записи (traceback) — приклеиваем
            if entries and raw.strip():
                entries[-1].append_continuation(raw)
    return entries


def _humanize_summary(entries: List[_LogEntry]) -> Tuple[int, int, int]:
    """Возвращает (errors_24h, warnings_24h, total)."""
    cutoff = datetime.now() - timedelta(hours=24)
    errors = warnings = 0
    for e in entries:
        if e.dt < cutoff:
            continue
        if e.level in ("ERROR", "CRITICAL"):
            errors += 1
        elif e.level == "WARNING":
            warnings += 1
    return errors, warnings, len(entries)


def _format_entry_html(e: _LogEntry, show_module: bool) -> str:
    color = _LEVEL_COLOR.get(e.level, "#d0c8c4")
    label = _LEVEL_LABEL.get(e.level, e.level)
    ts = e.dt.strftime("%d.%m %H:%M:%S")
    msg_html = (
        e.message
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br>&nbsp;&nbsp;&nbsp;&nbsp;")
    )
    module_html = ""
    if show_module:
        module_html = (
            f' <span style="color:#7a6a64;font-size:10px;">'
            f'[{e.module}]</span>'
        )
    return (
        f'<div style="margin:2px 0;color:{color};font-family:Consolas,monospace;'
        f'font-size:12px;">'
        f'<span style="color:#7a6a64;">[{ts}]</span> '
        f'<b>{label}</b>{module_html} · {msg_html}'
        f'</div>'
    )


class BugReportDialog(QDialog):
    """Диалог просмотра логов программы."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Лог работы программы")
        self.setMinimumSize(820, 560)
        self.resize(960, 620)
        self.setStyleSheet(
            "QDialog { background:#1e1e24; }"
            "QLabel { color:#f7ebe8; background:transparent; }"
        )
        self._entries: List[_LogEntry] = []
        self._setup_ui()
        self._reload()

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(20, 18, 20, 18); root.setSpacing(10)

        # Заголовок
        title = QLabel("ЛОГ РАБОТЫ ПРОГРАММЫ")
        title.setStyleSheet(
            "color:#ffa987;font-size:13px;font-weight:700;letter-spacing:2px;"
        )
        root.addWidget(title)

        hint = QLabel(
            "Здесь видно, что недавно делала программа. "
            "Ошибки выделены красным, предупреждения — оранжевым. "
            "Эти данные помогут разобраться, если что-то работает не так."
        )
        hint.setStyleSheet("color:#b0a09a;font-size:12px;")
        hint.setWordWrap(True)
        root.addWidget(hint)

        # Сводка
        self.summary_lbl = QLabel("Загрузка…")
        self.summary_lbl.setStyleSheet(
            "color:#f7ebe8;font-size:13px;background:#252530;"
            "border:1px solid #5a5554;border-radius:8px;padding:10px 14px;"
        )
        root.addWidget(self.summary_lbl)

        # Фильтры
        filt = QFrame()
        filt.setStyleSheet("background:transparent;")
        fl = QHBoxLayout(filt); fl.setContentsMargins(0, 0, 0, 0); fl.setSpacing(14)
        fl.addWidget(QLabel("Показывать:"))

        self.cb_error = self._mk_filter("Ошибки", True, "#e54b4b")
        self.cb_warn  = self._mk_filter("Предупреждения", True, "#ffa987")
        self.cb_info  = self._mk_filter("Инфо", False, "#d0c8c4")
        self.cb_debug = self._mk_filter("Отладка", False, "#7a6a64")
        for cb in (self.cb_error, self.cb_warn, self.cb_info, self.cb_debug):
            fl.addWidget(cb)
        fl.addStretch()
        root.addWidget(filt)

        # Тело лога
        self.body = QTextBrowser()
        self.body.setOpenExternalLinks(False)
        self.body.setStyleSheet(
            "QTextBrowser { background:#252530; color:#f7ebe8;"
            " border:1px solid #5a5554; border-radius:8px; padding:8px; }"
        )
        root.addWidget(self.body, stretch=1)

        # Кнопки
        btns = QHBoxLayout(); btns.setSpacing(8)
        btn_copy = self._mk_btn("📋  Скопировать всё")
        btn_copy.clicked.connect(self._copy_to_clipboard)
        btns.addWidget(btn_copy)

        btn_open = self._mk_btn("📂  Открыть папку с логами")
        btn_open.clicked.connect(self._open_logs_folder)
        btns.addWidget(btn_open)

        btn_reload = self._mk_btn("⟳  Обновить")
        btn_reload.clicked.connect(self._reload)
        btns.addWidget(btn_reload)

        btns.addStretch()

        btn_close = self._mk_btn("Закрыть", primary=True)
        btn_close.clicked.connect(self.accept)
        btns.addWidget(btn_close)
        root.addLayout(btns)

    def _mk_filter(self, text: str, checked: bool, color: str) -> QCheckBox:
        cb = QCheckBox(text); cb.setChecked(checked)
        cb.setStyleSheet(
            "QCheckBox { color:" + color + "; font-size:12px; background:transparent;"
            " padding:2px 0; spacing:6px; }"
            "QCheckBox::indicator { width:14px; height:14px; border-radius:3px;"
            " border:1px solid #5a5554; background:#2a2a32; }"
            "QCheckBox::indicator:checked { background:#e54b4b; border-color:#e54b4b; }"
        )
        cb.toggled.connect(self._render)
        return cb

    def _mk_btn(self, text: str, primary: bool = False) -> QPushButton:
        btn = QPushButton(text); btn.setFixedHeight(32)
        if primary:
            btn.setStyleSheet(
                "QPushButton { background:#e54b4b; color:#f7ebe8; border:none;"
                " border-radius:6px; padding:6px 18px; font-size:12px; font-weight:600; }"
                "QPushButton:hover { background:#c03030; }"
            )
        else:
            btn.setStyleSheet(
                "QPushButton { background:#3a3938; color:#f7ebe8;"
                " border:1px solid #5a5554; border-radius:6px;"
                " padding:6px 14px; font-size:12px; }"
                "QPushButton:hover { border-color:#ffa987; background:#444140; }"
            )
        return btn

    # ── Data ────────────────────────────────────────────────────

    def _reload(self) -> None:
        log_path = LOGS_DIR / "app.log"
        if not log_path.exists():
            self._entries = []
            self.summary_lbl.setText("Лог-файл ещё не создан.")
            self.body.setHtml(
                "<div style='color:#b0a09a;padding:20px;'>"
                "Лог пуст — программа только запущена. Поработайте с приложением и нажмите «Обновить»."
                "</div>"
            )
            return

        try:
            text = _read_tail(log_path, _TAIL_BYTES)
        except Exception as e:
            self._entries = []
            self.summary_lbl.setText("Не удалось прочитать лог.")
            self.body.setHtml(
                f"<div style='color:#e54b4b;padding:20px;'>Ошибка чтения: {e}</div>"
            )
            return

        self._entries = _parse_log(text)
        self._render()

    def _render(self) -> None:
        # Сводка по последним суткам
        errors_24h, warnings_24h, total = _humanize_summary(self._entries)
        if errors_24h or warnings_24h:
            summary = (
                f"За последние сутки: "
                f"<b style='color:#e54b4b;'>{errors_24h} ошиб.</b> · "
                f"<b style='color:#ffa987;'>{warnings_24h} предупр.</b> · "
                f"всего записей в логе: {total}"
            )
        else:
            summary = (
                f"<b style='color:#4ade80;'>За последние сутки ошибок не было.</b> "
                f"Всего записей в логе: {total}"
            )
        self.summary_lbl.setText(summary)

        # Фильтр уровней
        levels: set[str] = set()
        if self.cb_error.isChecked(): levels |= {"ERROR", "CRITICAL"}
        if self.cb_warn.isChecked():  levels.add("WARNING")
        if self.cb_info.isChecked():  levels.add("INFO")
        if self.cb_debug.isChecked(): levels |= {"DEBUG", "TRACE"}

        # Свежее сверху
        filtered = [e for e in self._entries if e.level in levels]
        filtered.reverse()
        if len(filtered) > _MAX_ENTRIES:
            filtered = filtered[:_MAX_ENTRIES]

        if not filtered:
            self.body.setHtml(
                "<div style='color:#b0a09a;padding:20px;'>"
                "По выбранным фильтрам записей нет. Включите больше уровней выше."
                "</div>"
            )
            return

        # Технический префикс модуля показываем только в DEBUG-режиме —
        # для обычных пользователей он шум.
        show_module = self.cb_debug.isChecked()
        html_parts = [_format_entry_html(e, show_module) for e in filtered]
        if len(self._entries) > 0 and len(filtered) >= _MAX_ENTRIES:
            html_parts.insert(
                0,
                f"<div style='color:#b0a09a;font-size:11px;padding:6px 0;'>"
                f"Показаны последние {_MAX_ENTRIES} записей из {len(self._entries)}. "
                f"Полный лог — кнопка «Скопировать всё» или папка."
                f"</div>",
            )
        self.body.setHtml("".join(html_parts))
        # Прокрутка наверх (там самое свежее)
        self.body.moveCursor(QTextCursor.MoveOperation.Start)

    # ── Actions ─────────────────────────────────────────────────

    def _copy_to_clipboard(self) -> None:
        if not self._entries:
            QMessageBox.information(self, "Лог пуст", "Нечего копировать.")
            return
        lines = []
        for e in self._entries:
            ts = e.dt.strftime("%Y-%m-%d %H:%M:%S")
            lines.append(f"{ts} | {e.level} | {e.module} | {e.message}")
        QGuiApplication.clipboard().setText("\n".join(lines))
        QMessageBox.information(
            self, "Скопировано",
            f"В буфер обмена: {len(lines)} записей.\n"
            f"Можно вставить в письмо разработчику."
        )

    def _open_logs_folder(self) -> None:
        path = Path(LOGS_DIR)
        try:
            path.mkdir(parents=True, exist_ok=True)
            if sys.platform == "win32":
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as e:
            QMessageBox.warning(
                self, "Не удалось открыть папку",
                f"Папка с логами:\n{path}\n\nОшибка: {e}"
            )
