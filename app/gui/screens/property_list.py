"""
Screen 1 — Property list v2.
Фильтр по категории + кнопка выбора даты с анимированным календарём.
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
    QMenu, QGraphicsOpacityEffect
)
from PySide6.QtCore import (
    Qt, Signal, QThread, QObject, QTimer,
    QDate, QPropertyAnimation, QEasingCurve, QParallelAnimationGroup, QPoint
)
from PySide6.QtGui import QAction
from loguru import logger
from app.backend.database import CATEGORIES


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

    def __init__(self, prop: Dict):
        super().__init__()
        self.prop_id  = prop["id"]
        self._parsing = False
        self._dot_cnt = 0
        self._timer   = QTimer(self)
        self._timer.timeout.connect(self._tick)
        self.setObjectName("card"); self.setMinimumHeight(116)
        self._build(prop)

    def _build(self, prop: Dict):
        lay = QHBoxLayout(self)
        lay.setContentsMargins(18, 14, 18, 14); lay.setSpacing(14)

        # Левая полоса
        bar = QFrame(); bar.setFixedWidth(3); bar.setFixedHeight(56)
        bar.setStyleSheet("background:#ffa987;border-radius:2px;")
        lay.addWidget(bar, 0, Qt.AlignmentFlag.AlignVCenter)

        # Информация
        info = QVBoxLayout(); info.setSpacing(4)
        t = QLabel(prop.get("title","Без названия")[:60]); t.setObjectName("cardTitle")
        info.addWidget(t)

        # Категория + сайт
        cat   = prop.get("category","")
        site  = (prop.get("site") or "").capitalize()
        pdates = prop.get("parse_dates") or ""

        row2 = QHBoxLayout(); row2.setSpacing(8)
        if cat:
            cat_lbl = QLabel(cat); cat_lbl.setObjectName("categoryBadge"); row2.addWidget(cat_lbl)
        if site:
            s = QLabel(f"🌐 {site}"); s.setObjectName("cardSub"); row2.addWidget(s)
        if pdates:
            dl = QLabel(f"📅 {pdates}"); dl.setObjectName("hintLabel"); row2.addWidget(dl)
        row2.addStretch()
        row2_w = QWidget(); row2_w.setLayout(row2); row2_w.setStyleSheet("background:transparent;")
        info.addWidget(row2_w)

        url_lbl = QLabel(prop.get("url","")[:70]); url_lbl.setObjectName("hintLabel")
        info.addWidget(url_lbl)
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

        self.btn_parse = QPushButton("  Обновить  "); self.btn_parse.setObjectName("primaryBtn")
        self.btn_parse.setMinimumWidth(108); self.btn_parse.setFixedHeight(32)
        self.btn_parse.clicked.connect(partial(self.parse_requested.emit, self.prop_id))
        ac.addWidget(self.btn_parse)

        bd = QPushButton("  Удалить  "); bd.setObjectName("dangerBtn")
        bd.setMinimumWidth(108); bd.setFixedHeight(32)
        bd.clicked.connect(partial(self.delete_requested.emit, self.prop_id)); ac.addWidget(bd)
        lay.addLayout(ac)

    def set_parsing(self, on: bool):
        if on == self._parsing: return
        self._parsing = on
        if on:
            self._dot_cnt = 0; self.btn_parse.setEnabled(False)
            self.btn_parse.setText("  Парсинг   "); self._timer.start(380)
        else:
            self._timer.stop(); self.btn_parse.setEnabled(True)
            self.btn_parse.setText("  Обновить  ")

    def _tick(self):
        self._dot_cnt = (self._dot_cnt+1)%4
        self.btn_parse.setText(f"  Парсинг{'.'*self._dot_cnt:<3}")

    @staticmethod
    def _badge(st):
        if st == "ok":                                  return "● OK",         "badgeOk"
        if st == "occupied":                            return "● Занято",     "badgeOccupied"
        if st in ("error","blocked"):                   return "● Ошибка",     "badgeError"
        if st in ("unavailable","captcha","not_found"): return "● Недоступно", "badgeUnavailable"
        return "", ""


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
        self._date_popup: Optional[DatePickerPopup] = None
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
            self._cl.insertWidget(self._cl.count()-1, card)
            self._cards[prop["id"]] = card
            if prop["id"] in parsing_now: card.set_parsing(True)

    # ── Parse ────────────────────────────────────────────────────

    def _parse_one(self, prop_id: int):
        if prop_id in self._parse_threads: return
        if card := self._cards.get(prop_id): card.set_parsing(True)

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
        self._cl.insertWidget(min(idx, self._cl.count()), new_card)
        self._cards[prop_id] = new_card

    def _parse_all(self):
        ids = list(self._cards.keys())
        if not ids: return
        self.btn_all.setEnabled(False); self.btn_all.setText("  ↻  Запуск...  ")
        for i, pid in enumerate(ids):
            QTimer.singleShot(i * 800, partial(self._parse_one, pid))
        QTimer.singleShot(max(len(ids)*800+500, 3000),
                          lambda: [self.btn_all.setEnabled(True),
                                   self.btn_all.setText("  ↻  Обновить все  ")])

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
