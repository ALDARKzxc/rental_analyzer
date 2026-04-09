"""Main window v2 — новая палитра."""
from __future__ import annotations

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QStackedWidget, QLabel, QPushButton, QFrame
)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont, QColor, QPalette

from app.gui.styles import STYLESHEET
from app.gui.screens.property_list import PropertyListScreen
from app.gui.screens.add_property import AddPropertyScreen
from app.gui.api_client import ApiClient


class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Rental Price Analyzer")
        self.setMinimumSize(1200, 700); self.resize(1440, 840)

        palette = QPalette()
        palette.setColor(QPalette.ColorRole.Window,       QColor("#1e1e24"))
        palette.setColor(QPalette.ColorRole.WindowText,   QColor("#f7ebe8"))
        palette.setColor(QPalette.ColorRole.Base,         QColor("#2a2a32"))
        palette.setColor(QPalette.ColorRole.AlternateBase,QColor("#22222a"))
        palette.setColor(QPalette.ColorRole.Text,         QColor("#f7ebe8"))
        palette.setColor(QPalette.ColorRole.Button,       QColor("#444140"))
        palette.setColor(QPalette.ColorRole.ButtonText,   QColor("#f7ebe8"))
        palette.setColor(QPalette.ColorRole.Highlight,    QColor("#ffa987"))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#1e1e24"))
        self.setPalette(palette)

        self.api = ApiClient()
        self._setup_ui()
        self.setStyleSheet(STYLESHEET)

    def _setup_ui(self):
        central = QWidget(); central.setObjectName("centralWidget")
        self.setCentralWidget(central)
        root = QHBoxLayout(central); root.setContentsMargins(0,0,0,0); root.setSpacing(0)

        root.addWidget(self._build_sidebar())

        self.stack = QStackedWidget(); root.addWidget(self.stack, stretch=1)

        self.screen_list = PropertyListScreen(self.api)
        self.screen_add  = AddPropertyScreen(self.api)

        self.stack.addWidget(self.screen_list)
        self.stack.addWidget(self.screen_add)

        self.screen_list.open_add.connect(self._show_add)
        self.screen_add.saved.connect(self._on_saved)
        self.screen_add.cancelled.connect(self._show_list)
        self.screen_add.go_back.connect(self._show_list)
        self._show_list()

    def _build_sidebar(self) -> QWidget:
        sb = QFrame(); sb.setObjectName("sidebar"); sb.setFixedWidth(210)
        lay = QVBoxLayout(sb); lay.setContentsMargins(0,0,0,0); lay.setSpacing(0)

        logo = QWidget(); logo.setObjectName("logoArea")
        ll = QVBoxLayout(logo); ll.setContentsMargins(18,20,18,18); ll.setSpacing(3)
        icon = QLabel("◈"); icon.setObjectName("logoIcon")
        icon.setFont(QFont("Segoe UI Symbol", 24))
        icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title = QLabel("RENTAL"); title.setObjectName("logoTitle")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        sub = QLabel("PRICE ANALYZER"); sub.setObjectName("logoSubtitle")
        sub.setAlignment(Qt.AlignmentFlag.AlignCenter)
        ll.addWidget(icon); ll.addWidget(title); ll.addWidget(sub)
        lay.addWidget(logo)

        nav = QWidget(); nl = QVBoxLayout(nav)
        nl.setContentsMargins(10,14,10,14); nl.setSpacing(4)
        self.btn_list = self._nav("  ОБЪЕКТЫ", "📋", 0)
        self.btn_add  = self._nav("  ДОБАВИТЬ", "＋", 1)
        nl.addWidget(self.btn_list); nl.addWidget(self.btn_add); nl.addStretch()
        lay.addWidget(nav, stretch=1)

        sep = QFrame(); sep.setObjectName("sidebarDivider"); sep.setFixedHeight(1)
        lay.addWidget(sep)

        self.status_lbl = QLabel("● ГОТОВ")
        self.status_lbl.setObjectName("sidebarStatus"); lay.addWidget(self.status_lbl)

        self._blink_state = True
        self._blink_timer = QTimer()
        self._blink_timer.timeout.connect(self._blink)
        self._blink_timer.start(2000)
        return sb

    def _nav(self, label, icon, idx):
        btn = QPushButton(f"{icon}{label}"); btn.setObjectName("navButton")
        btn.setCheckable(True); btn.setFixedHeight(40)
        btn.clicked.connect(lambda: self._nav_go(idx, btn))
        return btn

    def _nav_go(self, idx, btn):
        self.stack.setCurrentIndex(idx)
        for b in [self.btn_list, self.btn_add]: b.setChecked(b is btn)

    def _show_list(self):
        self.stack.setCurrentIndex(0)
        self.btn_list.setChecked(True); self.btn_add.setChecked(False)
        self.screen_list.refresh()

    def _show_add(self):
        self.screen_add.reset(); self.stack.setCurrentIndex(1)
        self.btn_list.setChecked(False); self.btn_add.setChecked(True)

    def _on_saved(self): self._show_list()

    def _blink(self):
        self._blink_state = not self._blink_state
        self.status_lbl.setText("● ГОТОВ" if self._blink_state else "○ ГОТОВ")
