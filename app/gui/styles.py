"""Новая цветовая палитра по ТЗ."""

STYLESHEET = """
* {
    font-family: 'Segoe UI', 'Inter', 'Ubuntu', sans-serif;
    font-size: 13px;
    color: #f7ebe8;
    background-color: transparent;
    outline: none;
    border: none;
}

QMainWindow, QDialog { background-color: #1e1e24; }

QWidget { background-color: transparent; color: #f7ebe8; }

QWidget#centralWidget, QStackedWidget,
QScrollArea, QScrollArea > QWidget, QScrollArea > QWidget > QWidget {
    background-color: #1e1e24;
}

/* ── Sidebar ── */
#sidebar {
    background-color: #444140;
    border-right: 1px solid #e54b4b;
    min-width: 210px; max-width: 210px;
}
#logoArea {
    background: #3a3938;
    border-bottom: 1px solid #e54b4b;
    padding: 20px;
}
#logoIcon   { color: #ffa987; font-size: 28px; background: transparent; }
#logoTitle  { color: #f7ebe8; font-size: 13px; font-weight: 700; letter-spacing: 2px; background: transparent; }
#logoSubtitle { color: #b0a09a; font-size: 9px; letter-spacing: 2px; background: transparent; }

#navButton {
    background: transparent;
    color: #b0a09a;
    border: 1px solid transparent;
    border-radius: 8px;
    padding: 10px 14px;
    text-align: left;
    font-size: 13px;
    min-height: 38px;
}
#navButton:hover   { background: #3a3938; color: #f7ebe8; border-color: #ffa987; }
#navButton:checked {
    background: qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 #e54b4b, stop:1 #c03030);
    color: #f7ebe8; border-color: #e54b4b;
}

#sidebarStatus { color: #ffa987; font-size: 11px; background: transparent; padding: 10px 14px; }
#sidebarDivider { background: #5a5554; max-height: 1px; }

/* ── Cards (правая часть) ── */
#card {
    background-color: #1e1e24;
    border: 1px solid #5a5554;
    border-radius: 14px;
}
#card:hover { border-color: #ffa987; background: #22222a; }

#cardTitle    { font-size: 14px; font-weight: 600; color: #f7ebe8; background: transparent; }
#cardSub      { font-size: 12px; color: #b0a09a; background: transparent; }
#categoryBadge {
    font-size: 10px; font-weight: 700;
    color: #1e1e24; background: #ffa987;
    border-radius: 10px; padding: 2px 8px;
}
#priceLabel   { font-size: 20px; font-weight: 700; color: #ffa987; background: transparent; }
#priceOccupied { font-size: 13px; font-weight: 600; color: #e54b4b; background: transparent; }
#priceUnavailable { font-size: 12px; color: #b0a09a; font-style: italic; background: transparent; }

#analyticsCard { background: #1e1e24; border: 1px solid #5a5554; border-radius: 12px; }
#analyticsValue { font-size: 17px; font-weight: 700; background: transparent; }
#analyticsLabel { font-size: 11px; color: #b0a09a; background: transparent; }

#recBox {
    background: #22222a;
    border: 1px solid #5a5554;
    border-left: 3px solid #ffa987;
    border-radius: 10px;
}

/* ── Кнопки ── */
QPushButton {
    border-radius: 9px;
    padding: 0 16px;
    min-height: 34px;
    min-width: 90px;
    font-size: 13px;
    font-weight: 600;
}

QPushButton#primaryBtn {
    background: qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 #ffa987, stop:1 #e08060);
    color: #1e1e24; border: none; min-width: 110px; padding: 0 18px;
}
QPushButton#primaryBtn:hover   { background: qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 #ffb99a, stop:1 #ffa987); }
QPushButton#primaryBtn:pressed { background: #e08060; }
QPushButton#primaryBtn:disabled{ background: #5a5554; color: #888; }

QPushButton#secondaryBtn {
    background: #444140; color: #ffa987;
    border: 1.5px solid #ffa987; min-width: 110px; padding: 0 18px;
}
QPushButton#secondaryBtn:hover   { background: #3a3938; color: #f7ebe8; }
QPushButton#secondaryBtn:pressed { background: #333; }

QPushButton#dangerBtn {
    background: transparent; color: #b0a09a;
    border: 1px solid #5a5554; min-width: 90px;
}
QPushButton#dangerBtn:hover { border-color: #e54b4b; color: #e54b4b; background: #2a1a1a; }

QPushButton#filterBtn {
    background: #444140; color: #f7ebe8;
    border: 1.5px solid #5a5554;
    border-radius: 9px; min-width: 110px; padding: 0 14px;
}
QPushButton#filterBtn:hover   { border-color: #ffa987; color: #ffa987; }
QPushButton#filterBtn:checked { background: #e54b4b; border-color: #e54b4b; color: #f7ebe8; }

QPushButton#ghostBtn {
    background: transparent; color: #b0a09a;
    border: none; border-radius: 7px; min-width: 70px; padding: 0 12px;
}
QPushButton#ghostBtn:hover { background: #3a3938; color: #f7ebe8; }

QPushButton#dateBtn {
    background: #444140; color: #f7ebe8;
    border: 1.5px solid #ffa987;
    border-radius: 9px; min-width: 150px; padding: 0 14px;
    text-align: left;
}
QPushButton#dateBtn:hover { background: #3a3938; }

/* ── Inputs ── */
QLineEdit, QTextEdit {
    background: #2a2a32; border: 1.5px solid #5a5554;
    border-radius: 9px; padding: 9px 13px;
    font-size: 13px; color: #f7ebe8;
    selection-background-color: #e54b4b;
}
QLineEdit:focus, QTextEdit:focus { border-color: #ffa987; }
QLineEdit:disabled { background: #1e1e24; border-color: #3a3938; color: #5a5554; }

QComboBox {
    background: #2a2a32; border: 1.5px solid #5a5554;
    border-radius: 9px; padding: 8px 13px;
    color: #f7ebe8; font-size: 13px; min-height: 34px;
}
QComboBox:focus { border-color: #ffa987; }
QComboBox::drop-down { border: none; width: 28px; }
QComboBox::down-arrow {
    image: none; width: 0; height: 0;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 6px solid #ffa987;
    margin-right: 10px;
}
QComboBox QAbstractItemView {
    background: #2a2a32; border: 1px solid #ffa987;
    color: #f7ebe8; selection-background-color: #e54b4b;
    border-radius: 0 0 9px 9px;
}

/* ── Labels ── */
#pageTitle    { font-size: 21px; font-weight: 700; color: #f7ebe8; background: transparent; }
#sectionTitle { font-size: 11px; font-weight: 700; color: #ffa987; letter-spacing: 2px; background: transparent; }
#formLabel    { font-size: 11px; font-weight: 600; color: #b0a09a; letter-spacing: 1px; background: transparent; }
#hintLabel    { font-size: 11px; color: #6a5a54; background: transparent; }

/* ── Badges ── */
#badgeOk          { background: #0d2b1f; color: #4ade80; border-radius: 20px; padding: 3px 10px; font-size: 11px; font-weight: 600; }
#badgeError       { background: #2a1010; color: #e54b4b; border-radius: 20px; padding: 3px 10px; font-size: 11px; font-weight: 600; }
#badgeUnavailable { background: #2a2010; color: #d97706; border-radius: 20px; padding: 3px 10px; font-size: 11px; font-weight: 600; }
#badgeOccupied    { background: #3a1020; color: #e54b4b; border-radius: 20px; padding: 3px 10px; font-size: 11px; font-weight: 700; }

/* ── Table ── */
QTableWidget {
    background: #1e1e24; border: 1px solid #5a5554;
    border-radius: 10px; gridline-color: #2a2a32;
    selection-background-color: #3a3938;
    alternate-background-color: #22222a;
}
QTableWidget::item { padding: 8px; color: #f7ebe8; border: none; }
QTableWidget::item:selected { background: #3a3938; }
QHeaderView { background: transparent; }
QHeaderView::section {
    background: #1e1e24; border: none;
    border-bottom: 1px solid #e54b4b;
    padding: 9px; font-weight: 700; color: #b0a09a;
    font-size: 10px; letter-spacing: 1px;
}

/* ── Scrollbar ── */
QScrollBar:vertical   { width: 6px; background: transparent; }
QScrollBar::handle:vertical { background: #5a5554; border-radius: 3px; min-height: 20px; }
QScrollBar::handle:vertical:hover { background: #ffa987; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QScrollBar:horizontal { height: 6px; background: transparent; }
QScrollBar::handle:horizontal { background: #5a5554; border-radius: 3px; }

/* ── Misc ── */
QFrame[frameShape="4"], QFrame[frameShape="5"] { background: #5a5554; max-height: 1px; border: none; }
QSplitter::handle { background: #5a5554; }
QSplitter::handle:hover { background: #ffa987; }
QToolTip { background: #2a2a32; color: #f7ebe8; border: 1px solid #ffa987; padding: 5px 10px; border-radius: 7px; }

QMessageBox { background: #2a2a32; }
QMessageBox QLabel { color: #f7ebe8; background: transparent; }
QMessageBox QPushButton {
    background: #444140; color: #f7ebe8;
    border: 1px solid #ffa987; border-radius: 8px; padding: 7px 18px; min-width: 70px;
}
QMessageBox QPushButton:hover { background: #ffa987; color: #1e1e24; }

QScrollArea { border: none; background: #1e1e24; }

/* ── Calendar popup ── */
#calendarPopup {
    background: #2a2a32; border: 1px solid #ffa987; border-radius: 14px;
}
QCalendarWidget { background: #2a2a32; color: #f7ebe8; border-radius: 12px; }
QCalendarWidget QToolButton { background: transparent; color: #f7ebe8; border-radius: 6px; padding: 4px 8px; }
QCalendarWidget QToolButton:hover { background: #e54b4b; }
QCalendarWidget QAbstractItemView {
    background: #2a2a32; color: #f7ebe8;
    selection-background-color: #ffa987; selection-color: #1e1e24;
    gridline-color: #3a3938;
}
QCalendarWidget QWidget#qt_calendar_navigationbar { background: #3a3938; }
"""
