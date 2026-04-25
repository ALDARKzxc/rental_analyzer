"""Screen 2 — Add property: с тумблером ручного ввода названия."""
from __future__ import annotations

from urllib.parse import urlparse

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QLineEdit, QTextEdit, QComboBox,
    QFrame, QMessageBox, QScrollArea
)
from PySide6.QtCore import Qt, Signal, QThread, QObject, QTimer
from PySide6.QtGui import QPainter, QColor, QPainterPath
from loguru import logger
from app.backend.database import CATEGORIES


# ── Кастомный переключатель-тумблер ──────────────────────────────────────────
class ToggleSwitch(QWidget):
    toggled = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._checked = False
        self.setFixedSize(46, 26)
        self.setCursor(Qt.CursorShape.PointingHandCursor)

    def isChecked(self) -> bool:
        return self._checked

    def setChecked(self, val: bool):
        if self._checked != val:
            self._checked = val
            self.update()

    def mousePressEvent(self, event):
        self._checked = not self._checked
        self.toggled.emit(self._checked)
        self.update()

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        w, h = self.width(), self.height()
        r = h / 2

        # Track
        track = QColor("#ffa987") if self._checked else QColor("#3a3938")
        p.setBrush(track)
        p.setPen(Qt.PenStyle.NoPen)
        path = QPainterPath()
        path.addRoundedRect(0, 0, w, h, r, r)
        p.drawPath(path)

        # Border glow when on
        if self._checked:
            pen_color = QColor("#ffa987")
            pen_color.setAlpha(80)
        else:
            pen_color = QColor("#5a5554")
        from PySide6.QtGui import QPen
        p.setPen(QPen(pen_color, 1))
        p.setBrush(Qt.BrushStyle.NoBrush)
        p.drawPath(path)

        # Knob
        margin = 3
        d = h - 2 * margin
        cx = (w - margin - d) if self._checked else margin
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(QColor("#f7ebe8"))
        p.drawEllipse(cx, margin, d, d)

        p.end()


# ── Фоновый воркер сохранения ─────────────────────────────────────────────────
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

    # ── стили кнопок (inline, не зависят от каскада) ──────────────
    _MAX_NOTES = 200

    _BTN_PRIMARY = """
        QPushButton { background:#ffa987; color:#1e1e24; border:none;
                      border-radius:9px; font-weight:700; font-size:13px; padding:0 20px; }
        QPushButton:hover   { background:#ffb99a; }
        QPushButton:pressed { background:#e08060; }
        QPushButton:disabled{ background:#5a5554; color:#888; }
    """
    _BTN_SECONDARY = """
        QPushButton { background:#444140; color:#ffa987; border:1.5px solid #ffa987;
                      border-radius:9px; font-weight:600; font-size:13px; padding:0 20px; }
        QPushButton:hover { background:#3a3938; color:#f7ebe8; }
    """
    _INP_NORMAL = """
        QLineEdit { background:#2a2a32; border:1.5px solid #5a5554; border-radius:9px;
                    padding:9px 13px; font-size:13px; color:#f7ebe8; }
        QLineEdit:focus { border-color:#ffa987; }
    """
    _INP_DISABLED = """
        QLineEdit { background:#1e1e24; border:1.5px solid #3a3938; border-radius:9px;
                    padding:9px 13px; font-size:13px; color:#5a5554; }
    """

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

        # ── Поле названия ──────────────────────────────────────────
        cl.addWidget(self._lbl("НАЗВАНИЕ ОБЪЕКТА"))

        self.inp_title = QLineEdit()
        self.inp_title.setPlaceholderText("Будет получено при сохранении")
        self.inp_title.setFixedHeight(40)
        self.inp_title.setEnabled(False)
        self.inp_title.setStyleSheet(self._INP_DISABLED)
        cl.addWidget(self.inp_title)

        # ── Тумблер под полем ввода ────────────────────────────────
        toggle_row = QHBoxLayout()
        toggle_row.setSpacing(10)
        toggle_row.setContentsMargins(0, 2, 0, 0)

        self.toggle_title = ToggleSwitch()
        self.toggle_title.toggled.connect(self._on_title_toggle)
        toggle_row.addWidget(self.toggle_title)

        toggle_hint = QLabel("Ввести название вручную")
        toggle_hint.setObjectName("hintLabel")
        toggle_hint.setWordWrap(True)
        toggle_row.addWidget(toggle_hint, 1)
        cl.addLayout(toggle_row)

        # ── URL ────────────────────────────────────────────────────
        cl.addWidget(self._lbl("ССЫЛКА НА ОБЪЯВЛЕНИЕ"))
        self.inp_url = QLineEdit()
        self.inp_url.setPlaceholderText("https://ostrovok.ru/hotel/...")
        self.inp_url.setFixedHeight(40)
        self.inp_url.setStyleSheet(self._INP_NORMAL)
        cl.addWidget(self.inp_url)

        hint = QLabel("Вставьте чистую ссылку без дат — даты задаются отдельно на главном экране")
        hint.setObjectName("hintLabel"); hint.setWordWrap(True); cl.addWidget(hint)

        # ── Тумблер «Свой объект» (независимая отметка) ────────────
        own_row = QHBoxLayout()
        own_row.setSpacing(10)
        own_row.setContentsMargins(0, 10, 0, 0)

        self.toggle_own = ToggleSwitch()
        own_row.addWidget(self.toggle_own)

        own_hint = QLabel("Свой объект (✅ и зелёная рамка в списке)")
        own_hint.setObjectName("hintLabel")
        own_hint.setWordWrap(True)
        own_row.addWidget(own_hint, 1)
        cl.addLayout(own_row)

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
        self._notes_counter = QLabel(f"0/{self._MAX_NOTES}")
        self._notes_counter.setAlignment(Qt.AlignmentFlag.AlignRight)
        self._notes_counter.setStyleSheet("color:#6a5a54;background:transparent;font-size:11px;")
        cl.addWidget(self._notes_counter)
        self.inp_notes.textChanged.connect(self._on_notes_changed)

        lay.addWidget(card)
        lay.addStretch()

        scroll.setWidget(content); outer.addWidget(scroll, stretch=1)

        # ── Кнопки — вне скролла, всегда видны ───────────────────
        btn_bar = QFrame()
        btn_bar.setAutoFillBackground(True)
        btn_bar.setStyleSheet("QFrame { background:#252530; border-top:1px solid #5a5554; }")
        btn_row = QHBoxLayout(btn_bar)
        btn_row.setContentsMargins(36, 14, 36, 14); btn_row.setSpacing(12)
        btn_row.addStretch()

        self.btn_cancel = QPushButton("Отмена")
        self.btn_cancel.setFixedHeight(42); self.btn_cancel.setMinimumWidth(120)
        self.btn_cancel.setStyleSheet(self._BTN_SECONDARY)
        self.btn_cancel.clicked.connect(self.cancelled.emit); btn_row.addWidget(self.btn_cancel)

        self.btn_save = QPushButton("◈  Сохранить")
        self.btn_save.setFixedHeight(42); self.btn_save.setMinimumWidth(150)
        self.btn_save.setStyleSheet(self._BTN_PRIMARY)
        self.btn_save.clicked.connect(self._save); btn_row.addWidget(self.btn_save)

        outer.addWidget(btn_bar)

    # ── helpers ───────────────────────────────────────────────────
    def _lbl(self, t):
        l = QLabel(t); l.setObjectName("formLabel"); return l
    def _sec(self, t):
        l = QLabel(t); l.setObjectName("sectionTitle"); return l
    def _div(self):
        f = QFrame(); f.setFrameShape(QFrame.Shape.HLine)
        f.setStyleSheet("background:#5a5554;max-height:1px;"); return f

    # ── notes counter ─────────────────────────────────────────────
    def _on_notes_changed(self):
        text = self.inp_notes.toPlainText()
        if len(text) > self._MAX_NOTES:
            cur = self.inp_notes.textCursor()
            self.inp_notes.blockSignals(True)
            self.inp_notes.setPlainText(text[:self._MAX_NOTES])
            self.inp_notes.moveCursor(cur.MoveOperation.End)
            self.inp_notes.blockSignals(False)
            text = self.inp_notes.toPlainText()
        count = len(text)
        color = "#e54b4b" if count >= self._MAX_NOTES else "#6a5a54"
        self._notes_counter.setText(f"{count}/{self._MAX_NOTES}")
        self._notes_counter.setStyleSheet(
            f"color:{color};background:transparent;font-size:11px;"
        )

    # ── toggle ────────────────────────────────────────────────────
    def _on_title_toggle(self, checked: bool):
        self.inp_title.setEnabled(checked)
        if checked:
            self.inp_title.setStyleSheet(self._INP_NORMAL)
            self.inp_title.setPlaceholderText("Введите название объекта")
            self.inp_title.setFocus()
        else:
            self.inp_title.setStyleSheet(self._INP_DISABLED)
            self.inp_title.clear()
            self.inp_title.setPlaceholderText("Будет получено при сохранении")

    # ── reset / load ──────────────────────────────────────────────
    def reset(self, prop=None):
        self.prop_id = None
        self.inp_url.clear(); self.inp_notes.clear()
        self.combo_cat.setCurrentIndex(0)
        self.btn_save.setEnabled(True)
        self.btn_save.setStyleSheet(self._BTN_PRIMARY)
        self.btn_save.setText("◈  Сохранить")
        self.page_title.setText("НОВЫЙ ОБЪЕКТ")
        self._notes_counter.setText(f"0/{self._MAX_NOTES}")
        self._notes_counter.setStyleSheet("color:#6a5a54;background:transparent;font-size:11px;")

        # Сбрасываем тумблер без сигнала
        self.toggle_title.toggled.disconnect(self._on_title_toggle)
        self.toggle_title.setChecked(False)
        self.toggle_title.toggled.connect(self._on_title_toggle)
        self._on_title_toggle(False)

        self.toggle_own.setChecked(False)

        if prop:
            self.prop_id = prop.get("id")
            self.inp_url.setText(prop.get("url", ""))
            self.inp_notes.setText(prop.get("notes") or "")
            cat = prop.get("category", "Квартиры")
            idx = self.combo_cat.findText(cat)
            if idx >= 0: self.combo_cat.setCurrentIndex(idx)
            self.page_title.setText("РЕДАКТИРОВАТЬ ОБЪЕКТ")

            locked = bool(prop.get("title_locked", False))
            self.toggle_title.toggled.disconnect(self._on_title_toggle)
            self.toggle_title.setChecked(locked)
            self.toggle_title.toggled.connect(self._on_title_toggle)
            self._on_title_toggle(locked)
            if locked:
                self.inp_title.setText(prop.get("title", ""))

            self.toggle_own.setChecked(bool(prop.get("is_own", False)))

    # ── save ──────────────────────────────────────────────────────
    def _save(self):
        url = self.inp_url.text().strip()
        if not url.startswith("http"):
            QMessageBox.warning(self, "Ошибка", "Введите корректную ссылку"); return

        title_locked = self.toggle_title.isChecked()
        if title_locked:
            title = self.inp_title.text().strip()
            if not title:
                QMessageBox.warning(self, "Ошибка", "Введите название объекта"); return
        else:
            try:
                host = urlparse(url).netloc.replace("www.", "")
                title = host if host else "Объект"
            except Exception:
                title = "Объект"

        self.btn_save.setEnabled(False); self.btn_save.setText("Сохранение и загрузка...")
        data = {
            "title": title, "url": url,
            "category": self.combo_cat.currentText(),
            "notes": self.inp_notes.toPlainText().strip() or None,
            "title_locked": title_locked,
            "is_own": self.toggle_own.isChecked(),
        }
        # Сохраняем старый поток, чтобы GC не убил его раньше времени
        self._dead_threads = getattr(self, "_dead_threads", [])
        if hasattr(self, "_thread") and self._thread is not None:
            self._dead_threads.append((self._thread, getattr(self, "_worker", None)))

        self._thread = QThread()
        self._worker = SaveWorker(self.api, data, self.prop_id)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.finished.connect(self._on_saved)
        self._worker.error.connect(self._on_error)
        self._worker.finished.connect(self._thread.quit)
        self._worker.error.connect(self._thread.quit)   # поток завершается и при ошибке
        self._thread.finished.connect(lambda: self._gc_threads())
        self._thread.start()

    def _on_saved(self, _):
        self.reset()
        self.btn_save.setText("✓  Сохранено!")
        self.btn_save.setStyleSheet("""
            QPushButton { background:#1a4731; color:#4ade80; border:none;
                          border-radius:9px; font-weight:700; font-size:13px; padding:0 20px; }
        """)
        QTimer.singleShot(1200, self._after_save)

    def _after_save(self):
        self.btn_save.setStyleSheet(self._BTN_PRIMARY)
        self.btn_save.setText("◈  Сохранить")
        self.saved.emit()

    def _on_error(self, e):
        try:
            self.btn_save.setEnabled(True)
            self.btn_save.setText("◈  Сохранить")
            if "already exists" in e:
                msg = "Объект с таким URL уже есть в списке."
            else:
                msg = str(e)
            QMessageBox.critical(self, "Ошибка сохранения", msg)
        except Exception:
            pass

    def _gc_threads(self):
        """Удаляем уже завершённые треды из списка."""
        self._dead_threads = [
            (t, w) for t, w in getattr(self, "_dead_threads", [])
            if t.isRunning()
        ]
