"""
AnimatedDatePicker — кастомный виджет выбора даты.

Дизайн:
  - Поле ввода с иконкой календаря (нажимается всё поле)
  - Всплывающий календарь с анимацией: плавное появление снизу + fade-in
  - Тема Crimson Abyss
"""
from __future__ import annotations

from PySide6.QtWidgets import (
    QWidget, QHBoxLayout, QVBoxLayout, QLabel,
    QPushButton, QCalendarWidget, QApplication,
    QSizePolicy, QGraphicsOpacityEffect
)
from PySide6.QtCore import (
    Qt, Signal, QDate, QPropertyAnimation,
    QEasingCurve, QPoint, QSize, QRect, QParallelAnimationGroup
)
from PySide6.QtGui import QFont, QColor, QPainter, QPen, QBrush, QPainterPath


# ── Стилизованный календарь ──────────────────────────────────────

CALENDAR_STYLE = """
QCalendarWidget {
    background: #181114;
    border: 1px solid #4A1C1C;
    border-radius: 14px;
    color: #F0E6D2;
}
QCalendarWidget QWidget#qt_calendar_navigationbar {
    background: #1E1519;
    border-bottom: 1px solid #2A1A1A;
    border-radius: 14px 14px 0 0;
    padding: 4px;
    min-height: 44px;
}
QCalendarWidget QToolButton {
    background: transparent;
    color: #F0E6D2;
    border: none;
    border-radius: 8px;
    padding: 6px 10px;
    font-size: 13px;
    font-weight: 600;
    min-width: 30px;
}
QCalendarWidget QToolButton:hover {
    background: #4A1C1C;
    color: #F0E6D2;
}
QCalendarWidget QToolButton#qt_calendar_prevmonth,
QCalendarWidget QToolButton#qt_calendar_nextmonth {
    color: #9B2C2C;
    font-size: 16px;
    font-weight: 700;
    min-width: 36px;
}
QCalendarWidget QToolButton::menu-indicator { image: none; }
QCalendarWidget QSpinBox {
    background: transparent;
    color: #F0E6D2;
    border: none;
    font-size: 14px;
    font-weight: 700;
}
QCalendarWidget QSpinBox::up-button,
QCalendarWidget QSpinBox::down-button { width: 0; }
QCalendarWidget QTableView {
    background: #181114;
    selection-background-color: #9B2C2C;
    selection-color: #F0E6D2;
    alternate-background-color: #1C1217;
    gridline-color: transparent;
    border-radius: 0 0 14px 14px;
    font-size: 13px;
}
QCalendarWidget QTableView::item {
    border-radius: 8px;
    padding: 4px;
    margin: 1px;
    color: #F0E6D2;
}
QCalendarWidget QTableView::item:hover {
    background: #4A1C1C;
}
QCalendarWidget QTableView::item:selected {
    background: #9B2C2C;
    color: #F0E6D2;
}
QAbstractItemView {
    outline: none;
}
"""


class PopupCalendar(QWidget):
    """Всплывающий виджет с календарём и анимацией."""

    date_selected = Signal(QDate)

    def __init__(self, parent_widget: QWidget):
        super().__init__(parent_widget.window(), Qt.WindowType.Popup)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating, False)
        self.setWindowFlags(
            Qt.WindowType.Popup |
            Qt.WindowType.FramelessWindowHint |
            Qt.WindowType.NoDropShadowWindowHint
        )

        self._parent_widget = parent_widget
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Внутренний контейнер (для округлённых углов и тени)
        self._inner = QWidget(self)
        self._inner.setObjectName("calendarPopup")
        self._inner.setStyleSheet("""
            #calendarPopup {
                background: #181114;
                border: 1px solid #4A1C1C;
                border-radius: 16px;
            }
        """)

        inner_l = QVBoxLayout(self._inner)
        inner_l.setContentsMargins(4, 4, 4, 4)
        inner_l.setSpacing(0)

        self._cal = QCalendarWidget()
        self._cal.setStyleSheet(CALENDAR_STYLE)
        self._cal.setGridVisible(False)
        self._cal.setVerticalHeaderFormat(QCalendarWidget.VerticalHeaderFormat.NoVerticalHeader)
        self._cal.setNavigationBarVisible(True)
        self._cal.setFixedSize(300, 270)
        self._cal.clicked.connect(self._on_date_clicked)

        inner_l.addWidget(self._cal)
        layout.addWidget(self._inner)

        self.setFixedSize(308, 278)

        # Opacity-эффект для fade анимации
        self._opacity = QGraphicsOpacityEffect(self)
        self._opacity.setOpacity(0.0)
        self.setGraphicsEffect(self._opacity)

    def show_animated(self, anchor: QPoint, selected: QDate):
        """Показываем с анимацией: slide-up + fade-in."""
        self._cal.setSelectedDate(selected)
        self._cal.setCurrentPage(selected.year(), selected.month())

        # Позиционируем — под полем ввода, выравниваем по левому краю
        target_pos = QPoint(anchor.x(), anchor.y() + 8)
        # Стартовая позиция — чуть ниже (для эффекта подъёма)
        start_pos  = QPoint(anchor.x(), anchor.y() + 28)

        self.move(start_pos)
        self.show()

        # Анимация позиции (slide-up)
        self._pos_anim = QPropertyAnimation(self, b"pos")
        self._pos_anim.setStartValue(start_pos)
        self._pos_anim.setEndValue(target_pos)
        self._pos_anim.setDuration(220)
        self._pos_anim.setEasingCurve(QEasingCurve.Type.OutCubic)

        # Анимация прозрачности (fade-in)
        self._fade_anim = QPropertyAnimation(self._opacity, b"opacity")
        self._fade_anim.setStartValue(0.0)
        self._fade_anim.setEndValue(1.0)
        self._fade_anim.setDuration(220)
        self._fade_anim.setEasingCurve(QEasingCurve.Type.OutCubic)

        # Запускаем параллельно
        self._group = QParallelAnimationGroup()
        self._group.addAnimation(self._pos_anim)
        self._group.addAnimation(self._fade_anim)
        self._group.start()

    def hide_animated(self):
        """Скрываем с анимацией: slide-down + fade-out."""
        curr_pos = self.pos()
        end_pos  = QPoint(curr_pos.x(), curr_pos.y() + 20)

        self._hide_pos = QPropertyAnimation(self, b"pos")
        self._hide_pos.setStartValue(curr_pos)
        self._hide_pos.setEndValue(end_pos)
        self._hide_pos.setDuration(160)
        self._hide_pos.setEasingCurve(QEasingCurve.Type.InCubic)

        self._hide_fade = QPropertyAnimation(self._opacity, b"opacity")
        self._hide_fade.setStartValue(1.0)
        self._hide_fade.setEndValue(0.0)
        self._hide_fade.setDuration(160)
        self._hide_fade.setEasingCurve(QEasingCurve.Type.InCubic)

        self._hide_group = QParallelAnimationGroup()
        self._hide_group.addAnimation(self._hide_pos)
        self._hide_group.addAnimation(self._hide_fade)
        self._hide_group.finished.connect(self.hide)
        self._hide_group.start()

    def _on_date_clicked(self, date: QDate):
        self.date_selected.emit(date)
        self.hide_animated()

    def get_date(self) -> QDate:
        return self._cal.selectedDate()


# ── Поле ввода с кнопкой-триггером ──────────────────────────────

FIELD_STYLE = """
    background: #0C0A0B;
    border: 1.5px solid #2A1A1A;
    border-radius: 10px;
    color: #F0E6D2;
    font-size: 13px;
"""

FIELD_STYLE_ACTIVE = """
    background: #10090A;
    border: 1.5px solid #9B2C2C;
    border-radius: 10px;
    color: #F0E6D2;
    font-size: 13px;
"""


class AnimatedDatePicker(QWidget):
    """
    Замена QDateEdit.
    Использование: picker.get_date() → QDate, picker.set_date(QDate)
    Сигнал: date_changed(QDate)
    """
    date_changed = Signal(QDate)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._date   = QDate.currentDate()
        self._popup  = None
        self._open   = False
        self.setFixedHeight(42)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self._setup_ui()

    def _setup_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Кликабельное поле — нажимается ВСЁ, не только иконка
        self._btn = QPushButton()
        self._btn.setFixedHeight(42)
        self._btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self._btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._btn.clicked.connect(self._toggle)
        self._btn.setStyleSheet(FIELD_STYLE + """
            QPushButton {
                background: #0C0A0B;
                border: 1.5px solid #2A1A1A;
                border-radius: 10px;
                color: #F0E6D2;
                font-size: 13px;
                text-align: left;
                padding: 0 14px;
            }
            QPushButton:hover {
                border-color: #4A1C1C;
                background: #100C0D;
            }
        """)
        self._update_text()
        layout.addWidget(self._btn)

    def _update_text(self):
        d = self._date
        day_names = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
        day_of_week = day_names[d.dayOfWeek() - 1]
        months = ["янв","фев","мар","апр","май","июн",
                  "июл","авг","сен","окт","ноя","дек"]
        text = f"📅   {d.day()} {months[d.month()-1]} {d.year()}  ({day_of_week})"
        self._btn.setText(text)

    def _toggle(self):
        if self._open:
            self._close_popup()
        else:
            self._open_popup()

    def _open_popup(self):
        self._open = True
        self._btn.setStyleSheet("""
            QPushButton {
                background: #10090A;
                border: 1.5px solid #9B2C2C;
                border-radius: 10px;
                color: #F0E6D2;
                font-size: 13px;
                text-align: left;
                padding: 0 14px;
            }
        """)

        if self._popup is None:
            self._popup = PopupCalendar(self)
            self._popup.date_selected.connect(self._on_date_selected)

        # Вычисляем позицию в глобальных координатах
        global_pos = self.mapToGlobal(QPoint(0, self.height()))
        self._popup.show_animated(global_pos, self._date)

        # Слушаем закрытие popup
        self._popup.installEventFilter(self)

    def _close_popup(self):
        self._open = False
        self._btn.setStyleSheet("""
            QPushButton {
                background: #0C0A0B;
                border: 1.5px solid #2A1A1A;
                border-radius: 10px;
                color: #F0E6D2;
                font-size: 13px;
                text-align: left;
                padding: 0 14px;
            }
            QPushButton:hover {
                border-color: #4A1C1C;
                background: #100C0D;
            }
        """)
        if self._popup and self._popup.isVisible():
            self._popup.hide_animated()

    def _on_date_selected(self, date: QDate):
        self._date = date
        self._open = False
        self._update_text()
        self._btn.setStyleSheet("""
            QPushButton {
                background: #0C0A0B;
                border: 1.5px solid #2A1A1A;
                border-radius: 10px;
                color: #F0E6D2;
                font-size: 13px;
                text-align: left;
                padding: 0 14px;
            }
            QPushButton:hover { border-color: #4A1C1C; }
        """)
        self.date_changed.emit(date)

    # ── Public API ────────────────────────────────────────────────

    def get_date(self) -> QDate:
        return self._date

    def set_date(self, date: QDate):
        self._date = date
        self._update_text()

    def toString(self, fmt: str = "dd.MM.yyyy") -> str:
        return self._date.toString(fmt)
