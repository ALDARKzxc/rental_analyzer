"""Screen 3 — Detail + analytics. Crimson Abyss theme."""
from __future__ import annotations

from typing import List, Dict, Optional
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QFrame, QTableWidget, QTableWidgetItem,
    QSplitter, QScrollArea, QHeaderView, QMessageBox, QSizePolicy
)
from PySide6.QtCore import Qt, Signal, QThread, QObject
from PySide6.QtGui import QFont, QColor
from loguru import logger

from app.gui.widgets.chart_widget import PriceChartWidget


class DetailWorker(QObject):
    finished = Signal(dict, list, dict)
    error = Signal(str)
    def __init__(self, api, prop_id):
        super().__init__()
        self.api = api; self.prop_id = prop_id
    def run(self):
        try:
            props = self.api.get_properties()
            prop = next((p for p in props if p["id"] == self.prop_id), {})
            prices = self.api.get_prices(self.prop_id)
            analytics = self.api.get_analytics(self.prop_id)
            self.finished.emit(prop, prices, analytics)
        except Exception as e:
            self.error.emit(str(e))


class ParseWorker(QObject):
    finished = Signal()
    error = Signal(str)
    def __init__(self, api, prop_id):
        super().__init__()
        self.api = api; self.prop_id = prop_id
    def run(self):
        import time
        try:
            self.api.trigger_parse(self.prop_id)
            for _ in range(90):
                time.sleep(2)
                s = self.api.get_parse_status(self.prop_id).get("status","idle")
                if s in ("done","idle") or s.startswith("error"):
                    break
            self.finished.emit()
        except Exception as e:
            self.error.emit(str(e))


class StatCard(QFrame):
    def __init__(self, label: str, value: str, color: str = "#F0E6D2", sub: str = ""):
        super().__init__()
        self.setObjectName("analyticsCard")
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.setFixedHeight(88)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(4)

        l_lbl = QLabel(label.upper())
        l_lbl.setObjectName("analyticsLabel")
        layout.addWidget(l_lbl)

        v_lbl = QLabel(value)
        v_lbl.setObjectName("analyticsValue")
        v_lbl.setStyleSheet(f"color: {color}; font-size: 18px; font-weight: 700; background: transparent;")
        layout.addWidget(v_lbl)

        if sub:
            s_lbl = QLabel(sub)
            s_lbl.setObjectName("analyticsLabel")
            layout.addWidget(s_lbl)


class DetailScreen(QWidget):
    go_back = Signal()

    def __init__(self, api):
        super().__init__()
        self.api = api
        self.prop_id: Optional[int] = None
        self._setup_ui()

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(36, 32, 36, 32)
        root.setSpacing(16)

        # ── Header ──
        hdr = QHBoxLayout()
        back = QPushButton("← НАЗАД")
        back.setObjectName("ghostBtn")
        back.clicked.connect(self.go_back.emit)
        hdr.addWidget(back)

        self.lbl_title = QLabel("ЗАГРУЗКА...")
        self.lbl_title.setObjectName("pageTitle")
        hdr.addWidget(self.lbl_title)
        hdr.addStretch()

        self.btn_parse = QPushButton("↻  ОБНОВИТЬ ЦЕНУ")
        self.btn_parse.setObjectName("primaryBtn")
        self.btn_parse.setFixedHeight(38)
        self.btn_parse.clicked.connect(self._trigger_parse)
        hdr.addWidget(self.btn_parse)
        root.addLayout(hdr)

        self.lbl_url = QLabel()
        self.lbl_url.setObjectName("hintLabel")
        self.lbl_url.setOpenExternalLinks(True)
        root.addWidget(self.lbl_url)

        div = QFrame(); div.setFrameShape(QFrame.Shape.HLine)
        div.setStyleSheet("background:#2A1A1A; max-height:1px;")
        root.addWidget(div)

        # ── Stat cards ──
        self.stats_row = QHBoxLayout()
        self.stats_row.setSpacing(10)
        root.addLayout(self.stats_row)

        # ── Recommendation ──
        self.rec_frame = QFrame()
        self.rec_frame.setObjectName("recBox")
        rec_l = QHBoxLayout(self.rec_frame)
        rec_l.setContentsMargins(16, 12, 16, 12)
        rec_icon = QLabel("◆")
        rec_icon.setStyleSheet("color:#9B2C2C; font-size:16px; background:transparent; padding-right:8px;")
        rec_l.addWidget(rec_icon)
        self.rec_lbl = QLabel("Загрузка...")
        self.rec_lbl.setObjectName("cardSub")
        self.rec_lbl.setWordWrap(True)
        rec_l.addWidget(self.rec_lbl, stretch=1)
        root.addWidget(self.rec_frame)

        # ── Splitter: chart + table ──
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.setStyleSheet("QSplitter::handle { background: #2A1A1A; height:1px; }")

        self.chart = PriceChartWidget()
        splitter.addWidget(self.chart)

        tbl_container = QWidget()
        tbl_container.setStyleSheet("background: transparent;")
        tbl_l = QVBoxLayout(tbl_container)
        tbl_l.setContentsMargins(0, 8, 0, 0)
        tbl_l.setSpacing(8)

        tbl_hdr = QLabel("ИСТОРИЯ ЦЕН")
        tbl_hdr.setObjectName("sectionTitle")
        tbl_l.addWidget(tbl_hdr)

        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["ДАТА", "ЦЕНА", "СТАТУС", "ПРИМЕЧАНИЕ"])
        hv = self.table.horizontalHeader()
        hv.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hv.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hv.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        hv.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        tbl_l.addWidget(self.table)

        splitter.addWidget(tbl_container)
        splitter.setSizes([320, 220])
        root.addWidget(splitter, stretch=1)

    def load(self, prop_id: int):
        self.prop_id = prop_id
        self.lbl_title.setText("ЗАГРУЗКА...")
        self._clear_stats()
        self._thread = QThread()
        self._worker = DetailWorker(self.api, prop_id)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.finished.connect(self._on_loaded)
        self._worker.error.connect(self._on_error)
        self._worker.finished.connect(self._thread.quit)
        self._thread.start()

    def _on_loaded(self, prop, prices, analytics):
        self.lbl_title.setText((prop.get("title") or "Объект")[:60].upper())
        url = prop.get("url", "")
        self.lbl_url.setText(f'<a href="{url}" style="color:#4A1C1C;">{url[:80]}</a>')
        self._render_stats(analytics)
        self.rec_lbl.setText(analytics.get("recommendation", "Нет данных"))
        self._render_table(prices)
        self.chart.plot(prices)

    def _on_error(self, e: str):
        self.lbl_title.setText("ОШИБКА")
        self.rec_lbl.setText(e)

    def _clear_stats(self):
        while self.stats_row.count():
            item = self.stats_row.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

    def _render_stats(self, a: dict):
        self._clear_stats()

        def fp(v):
            return f"{v:,.0f} ₽".replace(",", "\u202f") if v is not None else "—"

        def fpct(v):
            if v is None: return "—"
            return f"+{v:.1f}%" if v >= 0 else f"{v:.1f}%"

        TREND = {
            "up": ("↑ РОСТ", "#4ADE80"),
            "down": ("↓ СНИЖЕНИЕ", "#9B2C2C"),
            "stable": ("→ СТАБИЛЬНО", "#D97706"),
            "insufficient_data": ("— ДАННЫХ МАЛО", "#7A6A5C"),
        }
        pct = a.get("price_change_pct")
        pct_col = "#4ADE80" if pct and pct >= 0 else "#9B2C2C"
        trend_text, trend_col = TREND.get(a.get("trend","insufficient_data"), ("—","#7A6A5C"))

        for label, value, color, sub in [
            ("Текущая цена",  fp(a.get("current_price")),  "#9B2C2C",  "актуальное значение"),
            ("Средняя цена",  fp(a.get("avg_price")),      "#F0E6D2",  "за всё время"),
            ("Мин / Макс",    f"{fp(a.get('min_price'))} · {fp(a.get('max_price'))}", "#7A6A5C", "диапазон"),
            ("Изменение",     fpct(pct),                   pct_col,    "vs первая запись"),
            ("Тренд",         trend_text,                  trend_col,  f"{a.get('records_count',0)} записей"),
        ]:
            self.stats_row.addWidget(StatCard(label, value, color, sub))

    def _render_table(self, prices: list):
        self.table.setRowCount(0)
        STATUS = {
            "ok": "● OK", "not_found": "⚠ НЕ НАЙДЕНО",
            "error": "✕ ОШИБКА", "blocked": "✕ БЛОК",
            "captcha": "⚠ КАПЧА", "unavailable": "⚠ НЕДОСТУПНО",
        }
        STATUS_COLORS = {
            "ok": QColor("#0D2B1F"),
            "not_found": QColor("#1E1508"),
            "error": QColor("#2A0D0D"),
            "blocked": QColor("#2A0D0D"),
            "captcha": QColor("#1E1508"),
            "unavailable": QColor("#1E1508"),
        }
        for i, rec in enumerate(prices):
            self.table.insertRow(i)
            dt = (rec.get("recorded_at","")).replace("T"," ").split(".")[0]
            self.table.setItem(i, 0, QTableWidgetItem(dt))

            p = rec.get("price")
            p_item = QTableWidgetItem(f"{p:,.0f} ₽".replace(",","\u202f") if p else "—")
            p_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            if p:
                p_item.setForeground(QColor("#9B2C2C"))
                p_item.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
            self.table.setItem(i, 1, p_item)

            st = rec.get("status","ok")
            st_item = QTableWidgetItem(STATUS.get(st, st))
            st_item.setBackground(STATUS_COLORS.get(st, QColor("#181114")))
            self.table.setItem(i, 2, st_item)

            err = rec.get("error_message") or ""
            err_item = QTableWidgetItem(err[:100])
            err_item.setForeground(QColor("#7A6A5C"))
            err_item.setToolTip(err)
            self.table.setItem(i, 3, err_item)

    def _trigger_parse(self):
        if not self.prop_id: return
        self.btn_parse.setEnabled(False)
        self.btn_parse.setText("↻  ОБНОВЛЕНИЕ...")
        self._pt = QThread(); self._pw = ParseWorker(self.api, self.prop_id)
        self._pw.moveToThread(self._pt)
        self._pt.started.connect(self._pw.run)
        self._pw.finished.connect(self._parse_done)
        self._pw.error.connect(self._parse_err)
        self._pw.finished.connect(self._pt.quit)
        self._pt.start()

    def _parse_done(self):
        self.btn_parse.setEnabled(True)
        self.btn_parse.setText("↻  ОБНОВИТЬ ЦЕНУ")
        self.load(self.prop_id)

    def _parse_err(self, e: str):
        self.btn_parse.setEnabled(True)
        self.btn_parse.setText("↻  ОБНОВИТЬ ЦЕНУ")
        QMessageBox.warning(self, "Ошибка парсинга",
            f"Не удалось получить цену:\n{e}\n\nПоследнее значение сохранено.")
        self.load(self.prop_id)
