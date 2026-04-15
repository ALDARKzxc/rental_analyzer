"""
Screen 1 — Property list v2.
Фильтр по категории + кнопка выбора даты с анимированным календарём.
Кнопка «Глубокий анализ» — запуск полного анализа 435 датовых пар.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timedelta
from functools import partial
from typing import Dict, List, Optional

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QScrollArea, QFrame, QMessageBox,
    QMenu, QGraphicsOpacityEffect, QLineEdit, QTextEdit, QDialog
)
from PySide6.QtCore import (
    Qt, Signal, QThread, QObject, QTimer,
    QDate, QPropertyAnimation, QEasingCurve, QParallelAnimationGroup, QPoint
)
from PySide6.QtGui import QAction, QTextCursor
from loguru import logger
from app.backend.database import CATEGORIES


class _PencilLabel(QLabel):
    """Кликабельный эмодзи без кнопки."""
    clicked = Signal()
    def __init__(self, parent=None):
        super().__init__("✏️", parent)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setToolTip("Редактировать название и заметки")
        self.setStyleSheet("background:transparent; border:none; padding:0; margin:0;")
    def mousePressEvent(self, e):
        if e.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(e)


# ── Workers ──────────────────────────────────────────────────────

class LoadWorker(QObject):
    finished = Signal(list, int)
    error    = Signal(str)
    def __init__(self, api, seq, category=None):
        super().__init__(); self.api = api; self.seq = seq; self.category = category
    def run(self):
        try:    self.finished.emit(self.api.get_properties(self.category), self.seq)
        except Exception as e: self.error.emit(str(e))


class ParseWorker(QObject):
    finished = Signal(int)
    error    = Signal(int, str)
    def __init__(self, api, prop_id):
        super().__init__(); self.api = api; self.prop_id = prop_id; self._stop = False
    def stop(self): self._stop = True
    def run(self):
        try:
            self.api.trigger_parse(self.prop_id)
            for _ in range(150):
                if self._stop: break
                time.sleep(2)
                try:
                    s = self.api.get_parse_status(self.prop_id).get("status","idle")
                    if s == "done" or s.startswith("error") or s == "idle": break
                except Exception: break
            self.finished.emit(self.prop_id)
        except Exception as e:
            self.error.emit(self.prop_id, str(e))


class CardRefreshWorker(QObject):
    finished = Signal(int, dict)
    def __init__(self, api, prop_id, category=None):
        super().__init__(); self.api = api; self.prop_id = prop_id; self.category = category
    def run(self):
        try:
            props = self.api.get_properties(self.category)
            prop  = next((p for p in props if p["id"] == self.prop_id), None)
            if prop: self.finished.emit(self.prop_id, prop)
        except Exception as e:
            logger.error(f"CardRefreshWorker: {e}")


class EditSaveWorker(QObject):
    finished = Signal(int)
    error    = Signal(int, str)
    def __init__(self, api, prop_id, title, notes):
        super().__init__()
        self.api = api; self.prop_id = prop_id
        self.title = title; self.notes = notes or None
    def run(self):
        try:
            # title_locked=True — пользователь вручную задал название,
            # парсер не должен его перезаписывать
            self.api.update_property(
                self.prop_id,
                title=self.title,
                notes=self.notes,
                title_locked=True,
            )
            self.finished.emit(self.prop_id)
        except Exception as e:
            self.error.emit(self.prop_id, str(e))


# ── Date picker popup ─────────────────────────────────────────────

class DatePickerPopup(QWidget):
    """Всплывающий календарь для выбора диапазона дат."""
    dates_selected = Signal(str)   # "DD.MM.YYYY-DD.MM.YYYY"

    def __init__(self, parent_widget: QWidget):
        super().__init__(parent_widget.window(), Qt.WindowType.Popup)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self._date_from: Optional[QDate] = None
        self._date_to:   Optional[QDate] = None
        self._setup_ui()

    def _setup_ui(self):
        from PySide6.QtWidgets import QCalendarWidget, QVBoxLayout, QHBoxLayout
        outer = QVBoxLayout(self); outer.setContentsMargins(0, 0, 0, 0)

        inner = QWidget(self); inner.setObjectName("calendarPopup")
        inner.setStyleSheet("""
            #calendarPopup { background:#2a2a32; border:1px solid #ffa987;
                             border-radius:14px; padding:8px; }
        """)
        il = QVBoxLayout(inner); il.setContentsMargins(12, 12, 12, 12); il.setSpacing(10)

        self._info = QLabel("Выберите дату заезда")
        self._info.setStyleSheet("color:#ffa987;font-weight:600;background:transparent;")
        self._info.setAlignment(Qt.AlignmentFlag.AlignCenter); il.addWidget(self._info)

        self._cal = QCalendarWidget()
        self._cal.setGridVisible(False)
        self._cal.setVerticalHeaderFormat(QCalendarWidget.VerticalHeaderFormat.NoVerticalHeader)
        self._cal.setFixedSize(310, 260)
        self._cal.clicked.connect(self._on_clicked)
        il.addWidget(self._cal)

        btn_row = QHBoxLayout()
        btn_clear = QPushButton("Сбросить")
        btn_clear.setObjectName("ghostBtn"); btn_clear.clicked.connect(self._reset)
        btn_row.addWidget(btn_clear); btn_row.addStretch()
        il.addLayout(btn_row)
        outer.addWidget(inner)
        self.setFixedSize(334, 340)

        self._opacity = QGraphicsOpacityEffect(self)
        self._opacity.setOpacity(0.0)
        self.setGraphicsEffect(self._opacity)

    def _on_clicked(self, date: QDate):
        if self._date_from is None:
            self._date_from = date
            self._info.setText(f"Заезд: {date.toString('dd.MM.yyyy')} → выберите выезд")
        elif self._date_to is None or date <= self._date_from:
            if date <= self._date_from:
                self._date_from = date
                self._info.setText(f"Заезд: {date.toString('dd.MM.yyyy')} → выберите выезд")
                return
            self._date_to = date
            d1 = self._date_from.toString("dd.MM.yyyy")
            d2 = self._date_to.toString("dd.MM.yyyy")
            result = f"{d1}-{d2}"
            self._info.setText(f"✓ {d1} → {d2}")
            QTimer.singleShot(600, lambda: self._emit_and_hide(result))

    def _emit_and_hide(self, result: str):
        self.dates_selected.emit(result)
        self._hide_anim()

    def _reset(self):
        self._date_from = None; self._date_to = None
        self._info.setText("Выберите дату заезда")

    def show_at(self, anchor: QPoint):
        self._reset()
        start_pos  = QPoint(anchor.x(), anchor.y() + 30)
        target_pos = QPoint(anchor.x(), anchor.y() + 8)
        self.move(start_pos); self.show()

        self._pa = QPropertyAnimation(self, b"pos")
        self._pa.setStartValue(start_pos); self._pa.setEndValue(target_pos)
        self._pa.setDuration(200); self._pa.setEasingCurve(QEasingCurve.Type.OutCubic)

        self._fa = QPropertyAnimation(self._opacity, b"opacity")
        self._fa.setStartValue(0.0); self._fa.setEndValue(1.0)
        self._fa.setDuration(200); self._fa.setEasingCurve(QEasingCurve.Type.OutCubic)

        self._g = QParallelAnimationGroup()
        self._g.addAnimation(self._pa); self._g.addAnimation(self._fa); self._g.start()

    def _hide_anim(self):
        pos = self.pos()
        self._ha = QPropertyAnimation(self._opacity, b"opacity")
        self._ha.setStartValue(1.0); self._ha.setEndValue(0.0)
        self._ha.setDuration(150); self._ha.setEasingCurve(QEasingCurve.Type.InCubic)
        self._ha.finished.connect(self.hide); self._ha.start()


# ── Card ──────────────────────────────────────────────────────────

class PropertyCard(QFrame):
    parse_requested  = Signal(int)
    delete_requested = Signal(int)
    edit_requested   = Signal(int, str, str)   # prop_id, title, notes

    _MAX_NOTES = 200


    def __init__(self, prop: Dict):
        super().__init__()
        self.prop_id      = prop["id"]
        self._parse_dates = prop.get("parse_dates") or ""
        self._parsing     = False
        self._dot_cnt     = 0
        self._timer       = QTimer(self)
        self._timer.timeout.connect(self._tick)
        self.setObjectName("card"); self.setMinimumHeight(116)
        self._build(prop)

    def _build(self, prop: Dict):
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0); root.setSpacing(0)

        # ── Основная строка ───────────────────────────────────
        cw = QWidget(); cw.setStyleSheet("background:transparent;")
        lay = QHBoxLayout(cw)
        lay.setContentsMargins(18, 14, 18, 14); lay.setSpacing(14)

        # Левая полоса
        bar = QFrame(); bar.setFixedWidth(3); bar.setFixedHeight(56)
        bar.setStyleSheet("background:#ffa987;border-radius:2px;")
        lay.addWidget(bar, 0, Qt.AlignmentFlag.AlignVCenter)

        # Информация
        info = QVBoxLayout(); info.setSpacing(4)

        # Заголовок + карандаш
        title_row = QHBoxLayout(); title_row.setSpacing(4); title_row.setContentsMargins(0,0,0,0)
        t = QLabel(prop.get("title","Без названия")[:60]); t.setObjectName("cardTitle")
        title_row.addWidget(t)

        self._pencil = _PencilLabel()
        self._pencil.clicked.connect(self._toggle_edit)
        title_row.addWidget(self._pencil)
        title_row.addStretch()

        title_w = QWidget(); title_w.setLayout(title_row)
        title_w.setStyleSheet("background:transparent;")
        info.addWidget(title_w)

        # Категория + сайт + даты
        cat    = prop.get("category", "")
        site   = (prop.get("site") or "").capitalize()
        pdates = prop.get("parse_dates") or ""
        row2   = QHBoxLayout(); row2.setSpacing(8)
        if cat:    row2.addWidget(self._qlbl(cat, "categoryBadge"))
        if site:   row2.addWidget(self._qlbl(f"🌐 {site}", "cardSub"))
        if pdates: row2.addWidget(self._qlbl(f"📅 {pdates}", "hintLabel"))
        row2.addStretch()
        row2_w = QWidget(); row2_w.setLayout(row2)
        row2_w.setStyleSheet("background:transparent;")
        info.addWidget(row2_w)

        url_lbl = QLabel(prop.get("url","")[:70]); url_lbl.setObjectName("hintLabel")
        info.addWidget(url_lbl)

        notes = (prop.get("notes") or "").strip()
        if notes:
            preview = notes[:90] + ("…" if len(notes) > 90 else "")
            nl = QLabel(f"📝 {preview}"); nl.setObjectName("hintLabel"); nl.setWordWrap(True)
            info.addWidget(nl)

        lay.addLayout(info, stretch=1)

        # Цена
        pc = QVBoxLayout(); pc.setAlignment(Qt.AlignmentFlag.AlignCenter)
        pc.setSpacing(4); pc.setContentsMargins(10,0,10,0)
        p = prop.get("latest_price"); st = prop.get("latest_status") or ""
        if st == "occupied":
            pl = QLabel("Занято"); pl.setObjectName("priceOccupied")
        elif p is not None:
            pl = QLabel(f"{p:,.0f} ₽".replace(",","\u202f")); pl.setObjectName("priceLabel")
        else:
            pl = QLabel("нет данных"); pl.setObjectName("priceUnavailable")
        pl.setAlignment(Qt.AlignmentFlag.AlignCenter); pc.addWidget(pl)
        bt, bo = self._badge(st)
        if bt:
            b = QLabel(bt); b.setObjectName(bo)
            b.setAlignment(Qt.AlignmentFlag.AlignCenter); pc.addWidget(b)
        lay.addLayout(pc)

        # Кнопки
        ac = QVBoxLayout(); ac.setSpacing(6)
        ac.setAlignment(Qt.AlignmentFlag.AlignVCenter); ac.setContentsMargins(6,0,0,0)

        self.btn_parse = QPushButton("Обновить")
        self.btn_parse.setFixedHeight(34); self.btn_parse.setMinimumWidth(110)
        self.btn_parse.setStyleSheet("""
            QPushButton { background:#ffa987; color:#1e1e24; border:none; border-radius:8px;
                          font-weight:700; font-size:13px; padding:0 12px; }
            QPushButton:hover   { background:#ffb99a; }
            QPushButton:pressed { background:#e08060; }
            QPushButton:disabled{ background:#5a5554; color:#888; }
        """)
        self.btn_parse.clicked.connect(partial(self.parse_requested.emit, self.prop_id))
        ac.addWidget(self.btn_parse)

        bd = QPushButton("Удалить")
        bd.setFixedHeight(34); bd.setMinimumWidth(110)
        bd.setStyleSheet("""
            QPushButton { background:#2a1010; color:#e54b4b; border:1.5px solid #e54b4b;
                          border-radius:8px; font-weight:600; font-size:13px; padding:0 12px; }
            QPushButton:hover { background:#3a1515; }
        """)
        bd.clicked.connect(partial(self.delete_requested.emit, self.prop_id))
        ac.addWidget(bd)
        lay.addLayout(ac)

        root.addWidget(cw)

        # ── Панель редактирования (скрыта по умолчанию) ───────
        self._edit_panel = self._build_edit_panel(prop)
        self._edit_panel.setVisible(False)
        root.addWidget(self._edit_panel)

    def _build_edit_panel(self, prop: Dict) -> QFrame:
        panel = QFrame()
        panel.setStyleSheet("""
            QFrame {
                background:#1a1a22;
                border-top:1px solid #3a3938;
                border-bottom-left-radius:12px;
                border-bottom-right-radius:12px;
            }
        """)
        pl = QVBoxLayout(panel)
        pl.setContentsMargins(22, 14, 22, 16); pl.setSpacing(8)

        hdr = QLabel("✎  РЕДАКТИРОВАНИЕ")
        hdr.setStyleSheet(
            "color:#ffa987;font-size:10px;font-weight:700;"
            "background:transparent;letter-spacing:1.5px;"
        )
        pl.addWidget(hdr)

        pl.addWidget(self._field_lbl("НАЗВАНИЕ"))
        self._inp_edit_title = QLineEdit(prop.get("title") or "")
        self._inp_edit_title.setFixedHeight(38)
        self._inp_edit_title.setStyleSheet("""
            QLineEdit { background:#2a2a32; border:1.5px solid #5a5554; border-radius:8px;
                        padding:6px 12px; font-size:13px; color:#f7ebe8; }
            QLineEdit:focus { border-color:#ffa987; }
        """)
        pl.addWidget(self._inp_edit_title)

        pl.addWidget(self._field_lbl("ЗАМЕТКИ"))
        self._inp_edit_notes = QTextEdit()
        self._inp_edit_notes.setPlaceholderText("Необязательные заметки…")
        self._inp_edit_notes.setPlainText(prop.get("notes") or "")
        self._inp_edit_notes.setFixedHeight(72)
        self._inp_edit_notes.setStyleSheet("""
            QTextEdit { background:#2a2a32; border:1.5px solid #5a5554; border-radius:8px;
                        padding:4px 12px; font-size:13px; color:#f7ebe8; }
            QTextEdit:focus { border-color:#ffa987; }
        """)
        pl.addWidget(self._inp_edit_notes)

        cur_len = len(prop.get("notes") or "")
        color = "#e54b4b" if cur_len >= self._MAX_NOTES else "#6a5a54"
        self._edit_counter = QLabel(f"{cur_len}/{self._MAX_NOTES}")
        self._edit_counter.setAlignment(Qt.AlignmentFlag.AlignRight)
        self._edit_counter.setStyleSheet(f"color:{color};background:transparent;font-size:11px;")
        pl.addWidget(self._edit_counter)
        self._inp_edit_notes.textChanged.connect(self._on_edit_notes_changed)

        self._edit_error = QLabel("")
        self._edit_error.setStyleSheet("color:#e54b4b;background:transparent;font-size:11px;")
        pl.addWidget(self._edit_error)

        btn_row = QHBoxLayout(); btn_row.addStretch()

        btn_cancel = QPushButton("Отмена")
        btn_cancel.setFixedHeight(32); btn_cancel.setMinimumWidth(90)
        btn_cancel.setStyleSheet("""
            QPushButton { background:#2a2a32; color:#ffa987; border:1.5px solid #ffa987;
                          border-radius:8px; font-weight:600; font-size:12px; padding:0 10px; }
            QPushButton:hover { background:#3a3938; }
        """)
        btn_cancel.clicked.connect(self._cancel_edit)
        btn_row.addWidget(btn_cancel)

        self._btn_edit_save = QPushButton("✓  Сохранить")
        self._btn_edit_save.setFixedHeight(32); self._btn_edit_save.setMinimumWidth(120)
        self._btn_edit_save.setStyleSheet("""
            QPushButton { background:#ffa987; color:#1e1e24; border:none; border-radius:8px;
                          font-weight:700; font-size:12px; padding:0 12px; }
            QPushButton:hover   { background:#ffb99a; }
            QPushButton:pressed { background:#e08060; }
            QPushButton:disabled{ background:#5a5554; color:#888; }
        """)
        self._btn_edit_save.clicked.connect(self._save_edit)
        btn_row.addWidget(self._btn_edit_save)
        pl.addLayout(btn_row)

        return panel

    # ── helpers ───────────────────────────────────────────────
    @staticmethod
    def _qlbl(text: str, obj: str) -> QLabel:
        l = QLabel(text); l.setObjectName(obj); return l

    @staticmethod
    def _field_lbl(text: str) -> QLabel:
        l = QLabel(text)
        l.setStyleSheet(
            "color:#9a8a84;font-size:10px;font-weight:600;"
            "background:transparent;letter-spacing:1px;"
        )
        return l

    # ── toggle / cancel ───────────────────────────────────────
    def _toggle_edit(self):
        visible = not self._edit_panel.isVisible()
        self._edit_panel.setVisible(visible)

    def _cancel_edit(self):
        self._edit_panel.setVisible(False)

    # ── notes counter ─────────────────────────────────────────
    def _on_edit_notes_changed(self):
        text = self._inp_edit_notes.toPlainText()
        if len(text) > self._MAX_NOTES:
            self._inp_edit_notes.blockSignals(True)
            self._inp_edit_notes.setPlainText(text[:self._MAX_NOTES])
            self._inp_edit_notes.moveCursor(QTextCursor.MoveOperation.End)
            self._inp_edit_notes.blockSignals(False)
            text = self._inp_edit_notes.toPlainText()
        count = len(text)
        color = "#e54b4b" if count >= self._MAX_NOTES else "#6a5a54"
        self._edit_counter.setText(f"{count}/{self._MAX_NOTES}")
        self._edit_counter.setStyleSheet(f"color:{color};background:transparent;font-size:11px;")

    # ── save ──────────────────────────────────────────────────
    def _save_edit(self):
        title = self._inp_edit_title.text().strip()
        if not title:
            self._edit_error.setText("Название не может быть пустым")
            return
        self._edit_error.setText("")
        self._btn_edit_save.setEnabled(False)
        self._btn_edit_save.setText("Сохранение…")
        notes = self._inp_edit_notes.toPlainText().strip()
        self.edit_requested.emit(self.prop_id, title, notes)

    def reset_edit_btn(self):
        """Вызывается экраном при ошибке сохранения."""
        self._btn_edit_save.setEnabled(True)
        self._btn_edit_save.setText("✓  Сохранить")

    # ── parsing animation ─────────────────────────────────────
    def set_parsing(self, on: bool):
        if on == self._parsing: return
        self._parsing = on
        if on:
            self._dot_cnt = 0; self.btn_parse.setEnabled(False)
            self.btn_parse.setText("Парсинг..."); self._timer.start(380)
        else:
            self._timer.stop(); self.btn_parse.setEnabled(True)
            self.btn_parse.setText("Обновить")

    def _tick(self):
        self._dot_cnt = (self._dot_cnt+1)%4
        self.btn_parse.setText(f"Парсинг{'.'*self._dot_cnt:<3}")

    @staticmethod
    def _badge(st):
        if st == "ok":                                  return "● OK",         "badgeOk"
        if st == "occupied":                            return "● Занято",     "badgeOccupied"
        if st in ("error","blocked"):                   return "● Ошибка",     "badgeError"
        if st in ("unavailable","captcha","not_found"): return "● Недоступно", "badgeUnavailable"
        return "", ""


# ── Deep Analysis: Worker ─────────────────────────────────────────

class DeepAnalysisWorker(QObject):
    """Запускает глубокий анализ и каждые 0.5с опрашивает его состояние."""
    progress = Signal(int, int)       # (current, total)
    finished = Signal(str, int, int)  # (file_path, progress_count, elapsed_secs)
    error    = Signal(str)

    def __init__(self, api, prop_ids: list):
        super().__init__()
        self.api      = api
        self.prop_ids = prop_ids
        self._stop    = False

    def stop(self):
        self._stop = True

    def run(self):
        # Запускаем анализ в asyncio-потоке (возвращается мгновенно)
        try:
            self.api.start_deep_analysis(self.prop_ids)
        except Exception as e:
            self.error.emit(f"Не удалось запустить анализ: {e}")
            return

        # Опрашиваем состояние каждые 0.5 секунды
        while not self._stop:
            time.sleep(0.5)
            try:
                state = self.api.get_deep_analysis_state()
                if state.get("running", False):
                    self.progress.emit(state.get("progress", 0), state.get("total", 0))
                else:
                    self.finished.emit(
                        state.get("file_path", ""),
                        state.get("progress", 0),
                        state.get("elapsed", 0),
                    )
                    return
            except Exception as e:
                logger.error(f"DeepAnalysisWorker poll: {e}")


# ── Deep Analysis: Button ─────────────────────────────────────────

class DeepAnalysisButton(QPushButton):
    """
    Кнопка с тремя визуальными состояниями:
      • idle    — «Глубокий анализ» (обычная)
      • running — анимированные «Анализ...»
      • running + hover — «Информация»
    """
    status_requested = Signal()  # пользователь навёл и кликнул во время анализа

    def __init__(self, parent=None):
        super().__init__("  ◉  Глубокий анализ  ", parent)
        self.setObjectName("deepAnalysisBtn")
        self.setFixedHeight(38)

        self._running  = False
        self._hovered  = False
        self._dot_cnt  = 0

        self._anim = QTimer(self)
        self._anim.timeout.connect(self._tick)

    # ── Public ──────────────────────────────────────────────────
    def set_running(self, on: bool):
        self._running = on
        self._hovered = False
        if on:
            self._dot_cnt = 0
            self.setText("  ◉  Анализ.   ")
            self._anim.start(400)
        else:
            self._anim.stop()
            self.setText("  ◉  Глубокий анализ  ")

    @property
    def is_running(self) -> bool:
        return self._running

    # ── Animation ────────────────────────────────────────────────
    def _tick(self):
        if not self._hovered:
            self._dot_cnt = (self._dot_cnt + 1) % 4
            dots = "." * self._dot_cnt
            self.setText(f"  ◉  Анализ{dots:<3}")

    # ── Events ───────────────────────────────────────────────────
    def enterEvent(self, event):
        super().enterEvent(event)
        if self._running:
            self._hovered = True
            self.setText("  ●  Информация  ")

    def leaveEvent(self, event):
        super().leaveEvent(event)
        if self._running:
            self._hovered = False
            self._tick()  # восстанавливаем текущий кадр анимации

    def mousePressEvent(self, event):
        if self._running and self._hovered and event.button() == Qt.MouseButton.LeftButton:
            self.status_requested.emit()
        elif not self._running:
            super().mousePressEvent(event)

    def mouseReleaseEvent(self, event):
        if not self._running:
            super().mouseReleaseEvent(event)


# ── Deep Analysis: Status Widget ──────────────────────────────────

class DeepAnalysisStatusWidget(QWidget):
    """
    Всплывающий не-модальный виджет со статусом глубокого анализа.
    Содержит: прогресс, таймер, кнопку «Отмена».
    """
    cancel_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent, Qt.WindowType.Window | Qt.WindowType.FramelessWindowHint)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, False)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)

        self._elapsed = 0
        self._ticker  = QTimer(self)
        self._ticker.timeout.connect(self._tick_timer)

        self._setup_ui()

    def _setup_ui(self):
        self.setFixedWidth(360)
        self.setStyleSheet("""
            DeepAnalysisStatusWidget {
                background: #22222a;
                border: 1.5px solid #ffa987;
                border-radius: 14px;
            }
        """)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        card = QWidget()
        card.setStyleSheet("""
            QWidget {
                background: #22222a;
                border-radius: 14px;
            }
        """)
        cl = QVBoxLayout(card)
        cl.setContentsMargins(22, 18, 22, 18)
        cl.setSpacing(12)

        # Заголовок + крестик
        hdr_row = QHBoxLayout()
        title = QLabel("◉  ГЛУБОКИЙ АНАЛИЗ")
        title.setStyleSheet(
            "color:#ffa987;font-size:11px;font-weight:700;"
            "background:transparent;letter-spacing:1.5px;"
        )
        hdr_row.addWidget(title)
        hdr_row.addStretch()

        btn_close = QPushButton("✕")
        btn_close.setFixedSize(22, 22)
        btn_close.setStyleSheet("""
            QPushButton {
                background:transparent; color:#b0a09a; border:none;
                font-size:14px; font-weight:700;
                min-width:22px; min-height:22px; padding:0;
            }
            QPushButton:hover { color:#e54b4b; }
        """)
        btn_close.clicked.connect(self.hide)
        hdr_row.addWidget(btn_close)
        cl.addLayout(hdr_row)

        # Разделитель
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet("background:#3a3938;max-height:1px;")
        cl.addWidget(sep)

        # Прогресс
        self._lbl_progress = QLabel("Проанализировано: 0 / —")
        self._lbl_progress.setStyleSheet(
            "color:#f7ebe8;font-size:14px;font-weight:600;background:transparent;"
        )
        cl.addWidget(self._lbl_progress)

        # Таймер
        self._lbl_timer = QLabel("Время: 00:00")
        self._lbl_timer.setStyleSheet(
            "color:#b0a09a;font-size:13px;background:transparent;"
        )
        cl.addWidget(self._lbl_timer)

        # Кнопка «Отмена»
        sep2 = QFrame()
        sep2.setFrameShape(QFrame.Shape.HLine)
        sep2.setStyleSheet("background:#3a3938;max-height:1px;")
        cl.addWidget(sep2)

        self._btn_cancel = QPushButton("Остановить анализ")
        self._btn_cancel.setStyleSheet("""
            QPushButton {
                background: #2a1010; color: #e54b4b;
                border: 1.5px solid #e54b4b; border-radius: 8px;
                font-weight: 600; font-size: 13px;
                min-height: 34px; padding: 0 14px;
            }
            QPushButton:hover { background: #3a1515; }
            QPushButton:pressed { background: #4a1a1a; }
            QPushButton:disabled { background: #1a1010; color: #7a3030; border-color: #5a2020; }
        """)
        self._btn_cancel.clicked.connect(self.cancel_requested.emit)
        cl.addWidget(self._btn_cancel)

        outer.addWidget(card)

    # ── Public ──────────────────────────────────────────────────
    def start_timer(self):
        self._elapsed = 0
        self._ticker.start(1000)

    def stop_timer(self):
        self._ticker.stop()

    def update_progress(self, current: int, total: int):
        total_str = str(total) if total > 0 else "—"
        self._lbl_progress.setText(f"Проанализировано: {current} / {total_str}")

    def show_near_button(self, btn: QPushButton):
        """Показывает виджет рядом с кнопкой."""
        pos   = btn.mapToGlobal(QPoint(0, btn.height() + 6))
        screen = btn.screen()
        if screen:
            sr    = screen.availableGeometry()
            x     = min(pos.x(), sr.right()  - self.width()  - 10)
            y     = min(pos.y(), sr.bottom() - self.height() - 10)
            self.move(x, y)
        else:
            self.move(pos)
        self.show()
        self.raise_()

    def set_cancelling(self):
        """Блокирует кнопку отмены и меняет текст — сигнал что запрос принят."""
        self._btn_cancel.setEnabled(False)
        self._btn_cancel.setText("Останавливается…")

    # ── Timer tick ───────────────────────────────────────────────
    def _tick_timer(self):
        self._elapsed += 1
        h = self._elapsed // 3600
        m = (self._elapsed % 3600) // 60
        s = self._elapsed % 60
        if h:
            self._lbl_timer.setText(f"Время: {h}:{m:02d}:{s:02d}")
        else:
            self._lbl_timer.setText(f"Время: {m:02d}:{s:02d}")


# ── Screen ────────────────────────────────────────────────────────

class PropertyListScreen(QWidget):
    open_add = Signal()

    def __init__(self, api):
        super().__init__()
        self.api = api
        self._current_category: Optional[str] = None  # None = "Все"
        self._cards:         Dict[int, PropertyCard] = {}
        self._parse_threads: Dict[int, tuple]        = {}
        self._dead_threads:  List[tuple]             = []
        self._load_seq   = 0
        self._loading    = False
        self._date_popup:    Optional[DatePickerPopup]          = None
        self._status_widget: Optional[DeepAnalysisStatusWidget] = None
        self._deep_worker:   Optional[DeepAnalysisWorker]       = None
        self._deep_thread:   Optional[QThread]                  = None
        self._setup_ui()

    # ── UI ──────────────────────────────────────────────────────

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(32,26,32,26); root.setSpacing(0)

        # ── Верхняя панель ──
        hdr = QHBoxLayout(); hdr.setSpacing(10)

        col = QVBoxLayout(); col.setSpacing(3)
        pt = QLabel("МОИ ОБЪЕКТЫ"); pt.setObjectName("pageTitle")
        self.sub_lbl = QLabel("ЗАГРУЗКА..."); self.sub_lbl.setObjectName("sectionTitle")
        col.addWidget(pt); col.addWidget(self.sub_lbl)
        hdr.addLayout(col); hdr.addStretch()

        # Фильтр по категории
        self.btn_filter = QPushButton("  ▾  Все категории  ")
        self.btn_filter.setObjectName("filterBtn"); self.btn_filter.setFixedHeight(38)
        self.btn_filter.clicked.connect(self._show_filter_menu); hdr.addWidget(self.btn_filter)

        # Выбор даты
        self.btn_date = QPushButton("  📅  Выбрать дату  ")
        self.btn_date.setObjectName("dateBtn"); self.btn_date.setFixedHeight(38)
        self.btn_date.clicked.connect(self._show_date_picker); hdr.addWidget(self.btn_date)

        # Обновить все
        self.btn_all = QPushButton("  ↻  Обновить все  ")
        self.btn_all.setObjectName("primaryBtn"); self.btn_all.setFixedHeight(38)
        self.btn_all.clicked.connect(self._parse_all); hdr.addWidget(self.btn_all)

        # Глубокий анализ
        self._deep_btn = DeepAnalysisButton()
        self._deep_btn.clicked.connect(self._start_deep_analysis)
        self._deep_btn.status_requested.connect(self._show_analysis_status)
        hdr.addWidget(self._deep_btn)

        # Добавить
        btn_add = QPushButton("  ＋  Добавить  ")
        btn_add.setObjectName("secondaryBtn"); btn_add.setFixedHeight(38)
        btn_add.clicked.connect(self.open_add.emit); hdr.addWidget(btn_add)

        root.addLayout(hdr)

        div = QFrame(); div.setFrameShape(QFrame.Shape.HLine)
        div.setStyleSheet("background:#5a5554;max-height:1px;margin:18px 0 14px 0;")
        root.addWidget(div)

        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        self._container = QWidget(); self._container.setStyleSheet("background:transparent;")
        self._cl = QVBoxLayout(self._container)
        self._cl.setContentsMargins(0,0,6,0); self._cl.setSpacing(10)
        self._cl.addStretch()

        scroll.setWidget(self._container); root.addWidget(scroll, stretch=1)

    # ── Filter ──────────────────────────────────────────────────

    def _show_filter_menu(self):
        menu = QMenu(self)
        menu.setStyleSheet("""
            QMenu { background:#2a2a32; border:1px solid #ffa987; border-radius:8px; padding:4px; color:#f7ebe8; }
            QMenu::item { padding:8px 20px; border-radius:6px; }
            QMenu::item:selected { background:#e54b4b; color:#f7ebe8; }
        """)
        items = ["Все"] + CATEGORIES
        for item in items:
            act = QAction(item, self)
            act.triggered.connect(partial(self._set_filter, item))
            menu.addAction(act)
        btn_pos = self.btn_filter.mapToGlobal(self.btn_filter.rect().bottomLeft())
        menu.exec(btn_pos)

    def _set_filter(self, category: str):
        self._current_category = None if category == "Все" else category
        label = f"  ▾  {category}  " if category != "Все" else "  ▾  Все категории  "
        self.btn_filter.setText(label)
        self.btn_filter.setChecked(category != "Все")
        self.refresh()

    # ── Date picker ─────────────────────────────────────────────

    def _show_date_picker(self):
        if self._date_popup is None:
            self._date_popup = DatePickerPopup(self)
            self._date_popup.dates_selected.connect(self._on_dates_selected)
        anchor = self.btn_date.mapToGlobal(
            QPoint(0, self.btn_date.height())
        )
        self._date_popup.show_at(anchor)

    def _on_dates_selected(self, dates_str: str):
        """Применяем даты ко всей текущей категории или всем объектам."""
        cat = self._current_category
        try:
            if cat:
                result = self.api.set_category_dates(cat, dates_str)
                count  = result.get("updated", 0)
                msg = f"Дата {dates_str} применена к {count} объектам категории «{cat}»"
            else:
                # Все категории — применяем по очереди
                count = 0
                for c in CATEGORIES:
                    r = self.api.set_category_dates(c, dates_str)
                    count += r.get("updated", 0)
                msg = f"Дата {dates_str} применена ко всем {count} объектам"

            self.btn_date.setText(f"  📅  {dates_str}  ")
            QMessageBox.information(self, "Дата обновлена", msg)
            self.refresh()
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", str(e))

    # ── Load ────────────────────────────────────────────────────

    def refresh(self):
        self._load_seq += 1
        if self._loading: return
        self._start_load()

    def _start_load(self):
        self._loading = True
        seq = self._load_seq
        t = QThread()
        w = LoadWorker(self.api, seq, self._current_category)
        w.moveToThread(t)
        t.started.connect(w.run)
        w.finished.connect(self._on_loaded)
        w.error.connect(self._on_load_error)
        w.finished.connect(t.quit)
        t.finished.connect(self._after_load)
        self._dead_threads.append((t, w)); t.start()

    def _after_load(self):
        self._loading = False
        if self._load_seq > 1:
            self._load_seq = 1; self._start_load()
        else:
            self._load_seq = 0

    def _on_load_error(self, e: str):
        self._loading = False
        self.sub_lbl.setText(f"ОШИБКА: {e}")

    def _on_loaded(self, props: list, seq: int):
        parsing_now = set(self._parse_threads.keys())
        while self._cl.count() > 1:
            item = self._cl.takeAt(0)
            if item.widget(): item.widget().deleteLater()
        self._cards.clear()

        cat_label = f" [{self._current_category}]" if self._current_category else ""
        n = len(props)
        self.sub_lbl.setText(
            f"НЕТ ОБЪЕКТОВ{cat_label}" if n == 0 else
            f"1 ОБЪЕКТ{cat_label}" if n == 1 else
            f"{n} ОБЪЕКТА{cat_label}" if n <= 4 else
            f"{n} ОБЪЕКТОВ{cat_label}"
        )

        if not props:
            lbl = QLabel("Нет объектов. Нажмите «Добавить» для начала.")
            lbl.setObjectName("cardSub"); lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl.setStyleSheet("padding:50px;color:#6a5a54;"); self._cl.insertWidget(0, lbl)
            return

        for prop in props:
            card = PropertyCard(prop)
            card.parse_requested.connect(self._parse_one)
            card.delete_requested.connect(self._delete)
            card.edit_requested.connect(self._run_edit_save)
            self._cl.insertWidget(self._cl.count()-1, card)
            self._cards[prop["id"]] = card
            if prop["id"] in parsing_now: card.set_parsing(True)

    # ── Parse ────────────────────────────────────────────────────

    def _parse_one(self, prop_id: int):
        if prop_id in self._parse_threads: return
        card = self._cards.get(prop_id)
        if card and not card._parse_dates:
            QMessageBox.warning(
                self, "Дата не выбрана",
                "Для этого объекта не указан период дат.\n\n"
                "Выберите даты с помощью кнопки «📅 Выбрать дату» и повторите."
            )
            return
        if card: card.set_parsing(True)

        t = QThread(); w = ParseWorker(self.api, prop_id)
        w.moveToThread(t); t.started.connect(w.run)
        w.finished.connect(partial(self._on_parse_done, prop_id))
        w.error.connect(partial(self._on_parse_error, prop_id))
        w.finished.connect(t.quit)
        t.finished.connect(partial(self._cleanup_parse, prop_id))
        self._parse_threads[prop_id] = (t, w); t.start()

    def _on_parse_done(self, prop_id: int, *_):
        if c := self._cards.get(prop_id): c.set_parsing(False)
        self._async_reload_card(prop_id)

    def _on_parse_error(self, prop_id: int, error: str, *_):
        if c := self._cards.get(prop_id): c.set_parsing(False)
        self._async_reload_card(prop_id)
        logger.error(f"Parse error prop={prop_id}: {error}")

    def _cleanup_parse(self, prop_id: int):
        pair = self._parse_threads.pop(prop_id, None)
        if pair: self._dead_threads.append(pair)

    def _async_reload_card(self, prop_id: int):
        t = QThread()
        w = CardRefreshWorker(self.api, prop_id, self._current_category)
        w.moveToThread(t); t.started.connect(w.run)
        w.finished.connect(self._on_card_refreshed)
        w.finished.connect(t.quit)
        self._dead_threads.append((t, w)); t.start()

    def _on_card_refreshed(self, prop_id: int, prop: dict):
        old = self._cards.get(prop_id)
        if not old: return
        idx = self._cl.indexOf(old)
        if idx < 0: return
        self._cl.takeAt(idx); old.deleteLater()

        new_card = PropertyCard(prop)
        new_card.parse_requested.connect(self._parse_one)
        new_card.delete_requested.connect(self._delete)
        new_card.edit_requested.connect(self._run_edit_save)
        self._cl.insertWidget(min(idx, self._cl.count()), new_card)
        self._cards[prop_id] = new_card

    def _parse_all(self):
        ids = list(self._cards.keys())
        if not ids: return

        ids_ready   = [pid for pid in ids if self._cards[pid]._parse_dates]
        ids_skipped = len(ids) - len(ids_ready)

        if not ids_ready:
            QMessageBox.warning(
                self, "Дата не выбрана",
                "Ни у одного объекта не указан период дат.\n\n"
                "Выберите даты с помощью кнопки «📅 Выбрать дату»."
            )
            return

        self.btn_all.setEnabled(False); self.btn_all.setText("  ↻  Запуск...  ")
        for i, pid in enumerate(ids_ready):
            QTimer.singleShot(i * 800, partial(self._parse_one, pid))
        QTimer.singleShot(max(len(ids_ready)*800+500, 3000),
                          lambda: [self.btn_all.setEnabled(True),
                                   self.btn_all.setText("  ↻  Обновить все  ")])

        if ids_skipped:
            QMessageBox.information(
                self, "Часть объектов пропущена",
                f"{ids_skipped} объект(ов) пропущено — для них не указана дата.\n\n"
                "Выберите даты и нажмите «Обновить все» ещё раз."
            )

    # ── Edit save ────────────────────────────────────────────────

    def _run_edit_save(self, prop_id: int, title: str, notes: str):
        t = QThread()
        w = EditSaveWorker(self.api, prop_id, title, notes)
        w.moveToThread(t); t.started.connect(w.run)
        w.finished.connect(partial(self._on_edit_done, prop_id))
        w.error.connect(partial(self._on_edit_error, prop_id))
        w.finished.connect(t.quit)
        self._dead_threads.append((t, w)); t.start()

    def _on_edit_done(self, prop_id: int, *_):
        self._async_reload_card(prop_id)

    def _on_edit_error(self, prop_id: int, error: str, *_):
        if card := self._cards.get(prop_id):
            card.reset_edit_btn()
        QMessageBox.critical(self, "Ошибка сохранения", error)

    # ── Delete ───────────────────────────────────────────────────

    def _delete(self, prop_id: int):
        if prop_id in self._parse_threads:
            QMessageBox.information(self, "Подождите", "Дождитесь окончания обновления."); return
        if QMessageBox.question(
            self, "Удалить объект?", "История цен будет удалена.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        ) == QMessageBox.StandardButton.Yes:
            try:
                self.api.delete_property(prop_id); self.refresh()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    # ── Deep Analysis ─────────────────────────────────────────────

    def _start_deep_analysis(self):
        """Запускает глубокий анализ всех объектов."""
        if self._deep_btn.is_running:
            return

        try:
            props = self.api.get_properties()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось получить список объектов: {e}")
            return

        if not props:
            QMessageBox.information(
                self, "Нет объектов",
                "Добавьте хотя бы один объект перед запуском глубокого анализа."
            )
            return

        prop_ids = [p["id"] for p in props]
        n_props  = len(prop_ids)

        # Создаём статус-виджет заранее, чтобы сразу показать при нажатии
        self._status_widget = DeepAnalysisStatusWidget(self.window())
        self._status_widget.cancel_requested.connect(self._cancel_deep_analysis)
        self._status_widget.update_progress(0, n_props * 435)
        self._status_widget.show_near_button(self._deep_btn)
        self._status_widget.start_timer()

        self._deep_btn.set_running(True)

        t = QThread()
        w = DeepAnalysisWorker(self.api, prop_ids)
        w.moveToThread(t)
        t.started.connect(w.run)
        w.progress.connect(self._on_analysis_progress)
        w.finished.connect(self._on_analysis_finished)
        w.error.connect(self._on_analysis_error)
        w.finished.connect(t.quit)
        t.finished.connect(lambda: self._dead_threads.append((t, w)))

        self._deep_worker = w
        self._deep_thread = t
        t.start()

    def _show_analysis_status(self):
        """Показывает/поднимает статус-виджет при клике на кнопку во время анализа."""
        if self._status_widget is None:
            self._status_widget = DeepAnalysisStatusWidget(self.window())
            self._status_widget.cancel_requested.connect(self._cancel_deep_analysis)
        self._status_widget.show_near_button(self._deep_btn)

    def _on_analysis_progress(self, current: int, total: int):
        if self._status_widget:
            self._status_widget.update_progress(current, total)

    def _cancel_deep_analysis(self):
        """Запрашивает отмену — уже собранные данные будут записаны в файл.
        Воркер продолжает опрашивать состояние и штатно завершится когда asyncio-анализ остановится."""
        try:
            self.api.cancel_deep_analysis()
        except Exception as e:
            logger.error(f"cancel_deep_analysis: {e}")
        # НЕ останавливаем воркер: он должен дождаться running=False и эмитить finished,
        # иначе кнопка навсегда застрянет в состоянии «анализ».
        if self._status_widget:
            self._status_widget.set_cancelling()

    def _on_analysis_error(self, msg: str):
        self._deep_btn.set_running(False)
        if self._status_widget:
            self._status_widget.stop_timer()
            self._status_widget.hide()
        QMessageBox.critical(self, "Ошибка анализа", msg)

    def _on_analysis_finished(self, file_path: str, count: int, elapsed: int):
        """Вызывается когда анализ завершён (штатно или по отмене)."""
        self._deep_btn.set_running(False)

        if self._status_widget:
            self._status_widget.stop_timer()
            self._status_widget.hide()

        # Форматируем время
        h = elapsed // 3600
        m = (elapsed % 3600) // 60
        s = elapsed % 60
        if h:
            time_str = f"{h}:{m:02d}:{s:02d}"
        else:
            time_str = f"{m:02d}:{s:02d}"

        if file_path:
            msg = (
                f"Проанализировано: {count} запросов\n"
                f"Общее время анализа: {time_str}\n\n"
                f"Созданный файл находится по адресу:\n{file_path}"
            )
            title = "Анализ завершён"
        else:
            msg = (
                f"Анализ остановлен.\n"
                f"Проанализировано: {count} запросов\n"
                f"Время: {time_str}"
            )
            title = "Анализ остановлен"

        QMessageBox.information(self, title, msg)

        self._deep_worker = None
        self._deep_thread = None
