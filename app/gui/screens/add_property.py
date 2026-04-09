"""Screen 2 — Add property v2: категория вместо дат."""
from __future__ import annotations

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QLineEdit, QTextEdit, QComboBox,
    QFrame, QMessageBox, QScrollArea
)
from PySide6.QtCore import Qt, Signal, QThread, QObject, QTimer
from loguru import logger
from app.backend.database import CATEGORIES


class SaveWorker(QObject):
    finished = Signal(dict)
    error    = Signal(str)

    def __init__(self, api, data, prop_id=None):
        super().__init__()
        self.api = api; self.data = data; self.prop_id = prop_id

    def run(self):
        try:
            if self.prop_id:
                r = self.api.update_property(self.prop_id, **self.data)
            else:
                r = self.api.create_property(**self.data)
            if not isinstance(r, dict):
                r = {"id": 0}
            self.finished.emit(r)
        except Exception as e:
            logger.error(f"SaveWorker: {e}")
            self.error.emit(str(e))


class AddPropertyScreen(QWidget):
    saved     = Signal()
    cancelled = Signal()
    go_back   = Signal()

    def __init__(self, api):
        super().__init__()
        self.api = api; self.prop_id = None
        self._setup_ui()

    def _setup_ui(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0); outer.setSpacing(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        content = QWidget(); content.setStyleSheet("background:transparent;")
        lay = QVBoxLayout(content)
        lay.setContentsMargins(36, 28, 36, 28); lay.setSpacing(20)

        # Header
        hdr = QHBoxLayout()
        back = QPushButton("← Назад"); back.setObjectName("ghostBtn")
        back.clicked.connect(self.go_back.emit); hdr.addWidget(back)
        hdr.addStretch(); lay.addLayout(hdr)

        self.page_title = QLabel("НОВЫЙ ОБЪЕКТ")
        self.page_title.setObjectName("pageTitle"); lay.addWidget(self.page_title)

        sep = QFrame(); sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet("background:#5a5554;max-height:1px;"); lay.addWidget(sep)

        # Form card
        card = QFrame(); card.setObjectName("card")
        cl = QVBoxLayout(card); cl.setContentsMargins(24, 20, 24, 20); cl.setSpacing(14)

        cl.addWidget(self._sec("ОСНОВНАЯ ИНФОРМАЦИЯ"))

        cl.addWidget(self._lbl("НАЗВАНИЕ ОБЪЕКТА"))
        self.inp_title = QLineEdit()
        self.inp_title.setPlaceholderText("Например: Апартаменты Центр 2к")
        self.inp_title.setFixedHeight(40); cl.addWidget(self.inp_title)

        cl.addWidget(self._lbl("ССЫЛКА НА ОБЪЯВЛЕНИЕ"))
        self.inp_url = QLineEdit()
        self.inp_url.setPlaceholderText("https://ostrovok.ru/hotel/...")
        self.inp_url.setFixedHeight(40); cl.addWidget(self.inp_url)

        hint = QLabel("Вставьте чистую ссылку без дат — даты задаются отдельно на главном экране")
        hint.setObjectName("hintLabel"); hint.setWordWrap(True); cl.addWidget(hint)

        cl.addWidget(self._div())
        cl.addWidget(self._sec("КАТЕГОРИЯ"))

        cl.addWidget(self._lbl("ВЫБРАТЬ КАТЕГОРИЮ"))
        self.combo_cat = QComboBox()
        for c in CATEGORIES:
            self.combo_cat.addItem(c)
        self.combo_cat.setFixedHeight(40); cl.addWidget(self.combo_cat)

        cl.addWidget(self._div())
        cl.addWidget(self._lbl("ЗАМЕТКИ"))
        self.inp_notes = QTextEdit()
        self.inp_notes.setPlaceholderText("Необязательные заметки...")
        self.inp_notes.setFixedHeight(80); cl.addWidget(self.inp_notes)

        lay.addWidget(card)
        lay.addStretch()

        scroll.setWidget(content); outer.addWidget(scroll, stretch=1)

        # Buttons — outside scroll so they are always visible
        btn_bar = QWidget()
        btn_bar.setStyleSheet("background:#1e1e24; border-top: 1px solid #5a5554;")
        btn_row = QHBoxLayout(btn_bar)
        btn_row.setContentsMargins(36, 12, 36, 12); btn_row.setSpacing(10)
        btn_row.addStretch()

        self.btn_cancel = QPushButton("  Отмена  ")
        self.btn_cancel.setObjectName("secondaryBtn"); self.btn_cancel.setFixedHeight(40)
        self.btn_cancel.clicked.connect(self.cancelled.emit); btn_row.addWidget(self.btn_cancel)

        self.btn_save = QPushButton("  ◈  Сохранить  ")
        self.btn_save.setObjectName("primaryBtn"); self.btn_save.setFixedHeight(40)
        self.btn_save.clicked.connect(self._save); btn_row.addWidget(self.btn_save)

        outer.addWidget(btn_bar)

    def _lbl(self, t):
        l = QLabel(t); l.setObjectName("formLabel"); return l
    def _sec(self, t):
        l = QLabel(t); l.setObjectName("sectionTitle"); return l
    def _div(self):
        f = QFrame(); f.setFrameShape(QFrame.Shape.HLine)
        f.setStyleSheet("background:#5a5554;max-height:1px;"); return f

    def reset(self, prop=None):
        self.prop_id = None
        self.inp_title.clear(); self.inp_url.clear(); self.inp_notes.clear()
        self.combo_cat.setCurrentIndex(0)
        self.btn_save.setEnabled(True); self.btn_save.setText("  ◈  Сохранить  ")
        self.page_title.setText("НОВЫЙ ОБЪЕКТ")
        if prop:
            self.prop_id = prop.get("id")
            self.inp_title.setText(prop.get("title",""))
            self.inp_url.setText(prop.get("url",""))
            self.inp_notes.setText(prop.get("notes") or "")
            cat = prop.get("category","Квартиры")
            idx = self.combo_cat.findText(cat)
            if idx >= 0: self.combo_cat.setCurrentIndex(idx)
            self.page_title.setText("РЕДАКТИРОВАТЬ ОБЪЕКТ")

    def _save(self):
        title = self.inp_title.text().strip()
        url   = self.inp_url.text().strip()
        if not title:
            QMessageBox.warning(self, "Ошибка", "Введите название"); return
        if not url.startswith("http"):
            QMessageBox.warning(self, "Ошибка", "Введите корректную ссылку"); return

        self.btn_save.setEnabled(False); self.btn_save.setText("  Сохранение...  ")
        data = {
            "title": title, "url": url,
            "category": self.combo_cat.currentText(),
            "notes": self.inp_notes.toPlainText().strip() or None,
        }
        self._thread = QThread()
        self._worker = SaveWorker(self.api, data, self.prop_id)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.finished.connect(self._on_saved)
        self._worker.error.connect(self._on_error)
        self._worker.finished.connect(self._thread.quit)
        self._thread.start()

    def _on_saved(self, _):
        self.reset()
        self.btn_save.setText("  ✓  Сохранено!  ")
        self.btn_save.setStyleSheet("""
            QPushButton { background:#1a4731; color:#4ade80; border:none;
            border-radius:9px; padding:0 18px; font-weight:600; }""")
        QTimer.singleShot(1200, self._after_save)

    def _after_save(self):
        self.btn_save.setStyleSheet("")
        self.btn_save.setText("  ◈  Сохранить  ")
        self.saved.emit()

    def _on_error(self, e):
        self.btn_save.setEnabled(True); self.btn_save.setText("  ◈  Сохранить  ")
        msg = "Объект с таким URL уже добавлен" if "already exists" in e else e
        QMessageBox.critical(self, "Ошибка сохранения", msg)
