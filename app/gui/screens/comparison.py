"""
Экран «Сравнение объектов».

UX-принципы:
  • Список объектов = тот же, что в «Анализе цен» (active properties).
  • Удобства/описание показываются из кэша БД. Если кэша нет — карточка
    показывает плейсхолдер и кнопку «Загрузить».
  • Кнопка «Загрузить удобства для всех» — массовый фетч в фоне, без
    блокировки UI. Парсинг цен при этом не затрагивается вообще.
  • Фильтры по ключевым словам из категорий скриншота
    (Популярные / Общее / Апартаменты / Интернет / Развлечения / Парковка
    / Дети / Животные). AND-логика: показываются объекты, у которых
    в строке всех удобств содержатся все выбранные ключевые слова.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple, Optional

from PySide6.QtCore import Qt, Signal, QTimer
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QScrollArea, QFrame, QCheckBox, QLineEdit,
    QMessageBox, QSizePolicy,
)

from app.gui.api_client import ApiClient

FilterDef = Dict[str, Any]


# Группы фильтров. Текстовые фильтры ищутся по удобствам, описанию и коротким
# фактам; числовые фильтры читают значения из key_facts/category/guest_capacity.
FILTER_GROUPS: List[Tuple[str, List[FilterDef]]] = [
    ("⭐ Популярные", [
        {"label": "Wi-Fi / интернет",       "mode": "any", "keywords": ["wifi", "wi fi", "вайфай", "вай фай", "интернет"]},
        {"label": "Парковка",               "mode": "any", "keywords": ["парковк", "parking"]},
        {
            "label": "Подходит для детей",
            "mode": "any",
            "keywords": [
                "для детей", "подходит для дет", "можно с детьми", "с детьми",
                "детская площадка", "детская игровая площадка", "игровая площадка",
                "площадка для детей", "детская комната", "игровая комната",
                "детский клуб", "клуб для детей", "детские кроватки", "детская кроватка",
                "кроватка для ребенка", "детский стульчик", "стульчик для кормления",
                "детское меню", "детская зона", "детская мебель", "детская анимация",
                "анимация для детей", "услуги няни", "няня", "манеж", "детская ванночка",
                "горшок", "защитные барьеры", "семейные номера", "семейный номер",
                "kids", "children", "playground", "kids club", "family room",
                "family friendly", "kid friendly", "child friendly",
                "baby cot", "crib", "high chair", "baby bath", "babysitting",
            ],
            "keyword_groups": [
                ["детск", "детей", "ребен", "ребенк"],
                [
                    "площадк", "игров", "кроват", "стульчик", "кормлен",
                    "клуб", "комнат", "меню", "зон", "мебел", "анимац",
                    "нян", "манеж", "ванноч", "горш", "барьер", "семейн",
                ],
            ],
            "exclude_patterns": [
                r"(?:дет|ребен|children|kids).{0,45}(?:запрещ|нельзя|не допуска|не разреш|не подход)",
                r"(?:запрещ|нельзя|не допуска|не разреш|не подход).{0,45}(?:дет|ребен|children|kids)",
            ],
        },
        {
            "label": "С животными",
            "mode": "any",
            "keywords": ["с животными", "с домашними животными", "размещение с животными", "можно с животными", "pet friendly", "pets allowed"],
            "exclude_patterns": [
                r"(?:животн|питомц|pet).{0,45}(?:запрещ|нельзя|не допуска|не разреш|запрет)",
                r"(?:запрещ|нельзя|не допуска|не разреш|запрет).{0,45}(?:животн|питомц|pet)",
            ],
        },
    ]),
    ("🏠 Общее", [
        {"label": "Магазины поблизости",    "mode": "any", "keywords": ["магазин"]},
        {"label": "Для некурящих",          "mode": "any", "keywords": ["некурящ", "non smoking"]},
        {"label": "Сад",                    "mode": "any", "keywords": ["сад", "garden"]},
        {"label": "Ускоренная регистрация", "mode": "any", "keywords": ["ускоренная регистрация", "express check"]},
    ]),
    ("🛏 В апартаментах", [
        {"label": "Собственная ванная",     "mode": "any", "keywords": ["собственная ванн", "ванная комната", "private bathroom"]},
        {"label": "Кухня",                  "mode": "any", "keywords": ["кухн", "kitchen"]},
        {"label": "Кондиционер",            "mode": "any", "keywords": ["кондиционер", "air conditioning"]},
        {"label": "Стиральная машина",      "mode": "any", "keywords": ["стиральн", "washing machine"]},
    ]),
    ("📶 Интернет", [
        {"label": "Wi-Fi",                  "mode": "any", "keywords": ["wifi", "wi fi", "вайфай", "вай фай"]},
        {"label": "Бесплатный интернет",    "mode": "all_groups", "keyword_groups": [["бесплатн", "free"], ["интернет", "wifi", "wi fi", "вайфай", "вай фай"]]},
    ]),
    ("🛌 Тип кровати", [
        {"label": "1-спальная",             "mode": "any", "keywords": ["односпальн", "1 спальн", "single bed", "twin bed"]},
        {"label": "2-спальная",             "mode": "any", "keywords": ["двуспальн", "2 спальн", "double bed", "queen", "king"]},
        {"label": "Диван-кровать",          "mode": "any", "keywords": ["диван кровать", "sofa bed"]},
        {"label": "1 кровать",              "field": "beds", "value": 1},
        {"label": "2 кровати",              "field": "beds", "value": 2},
        {"label": "3+ кровати",             "field": "beds", "min": 3},
    ]),
    ("📐 Метраж", [
        {"label": "до 30 кв.м",              "field": "area", "max": 30},
        {"label": "30-49 кв.м",              "field": "area", "min": 30, "max": 49},
        {"label": "50-69 кв.м",              "field": "area", "min": 50, "max": 69},
        {"label": "70+ кв.м",                "field": "area", "min": 70},
    ]),
    ("🚪 Комнаты", [
        {"label": "1 комната",               "field": "rooms", "value": 1},
        {"label": "2 комнаты",               "field": "rooms", "value": 2},
        {"label": "3 комнаты",               "field": "rooms", "value": 3},
        {"label": "4+ комнаты",              "field": "rooms", "min": 4},
    ]),
    ("👥 Гости", [
        {"label": "до 2 гостей",             "field": "guests", "max": 2},
        {"label": "3-4 гостя",               "field": "guests", "min": 3, "max": 4},
        {"label": "5-6 гостей",              "field": "guests", "min": 5, "max": 6},
        {"label": "7+ гостей",               "field": "guests", "min": 7},
    ]),
    ("🎯 Развлечения", [
        {"label": "Барбекю",                "mode": "any", "keywords": ["барбекю", "bbq"]},
        {"label": "Бассейн",                "mode": "any", "keywords": ["бассейн", "pool"]},
        {"label": "Спортзал / фитнес",      "mode": "any", "keywords": ["спортзал", "фитнес", "gym"]},
        {"label": "Сауна / баня",           "mode": "any", "keywords": ["сауна", "баня"]},
    ]),
    ("🚗 Парковка", [
        {"label": "Парковка",               "mode": "any", "keywords": ["парковк", "parking"]},
        {"label": "Бесплатная парковка",    "mode": "all_groups", "keyword_groups": [["бесплатн", "free"], ["парковк", "parking"]]},
    ]),
    ("👶 Дети", [
        {
            "label": "Подходит для детей",
            "mode": "any",
            "keywords": [
                "для детей", "подходит для дет", "можно с детьми", "с детьми",
                "детская площадка", "детская игровая площадка", "игровая площадка",
                "площадка для детей", "детская комната", "игровая комната",
                "детский клуб", "клуб для детей", "детские кроватки", "детская кроватка",
                "кроватка для ребенка", "детский стульчик", "стульчик для кормления",
                "детское меню", "детская зона", "детская мебель", "детская анимация",
                "анимация для детей", "услуги няни", "няня", "манеж", "детская ванночка",
                "горшок", "защитные барьеры", "семейные номера", "семейный номер",
                "kids", "children", "playground", "kids club", "family room",
                "family friendly", "kid friendly", "child friendly",
                "baby cot", "crib", "high chair", "baby bath", "babysitting",
            ],
            "keyword_groups": [
                ["детск", "детей", "ребен", "ребенк"],
                [
                    "площадк", "игров", "кроват", "стульчик", "кормлен",
                    "клуб", "комнат", "меню", "зон", "мебел", "анимац",
                    "нян", "манеж", "ванноч", "горш", "барьер", "семейн",
                ],
            ],
            "exclude_patterns": [
                r"(?:дет|ребен|children|kids).{0,45}(?:запрещ|нельзя|не допуска|не разреш|не подход)",
                r"(?:запрещ|нельзя|не допуска|не разреш|не подход).{0,45}(?:дет|ребен|children|kids)",
            ],
        },
        {"label": "Детские телеканалы",     "mode": "any", "keywords": ["детские телеканалы", "телеканалы для детей", "детское тв", "kids tv", "children television"]},
    ]),
    ("🐾 Животные", [
        {
            "label": "С домашними животными",
            "mode": "any",
            "keywords": ["с животными", "с домашними животными", "размещение с животными", "можно с животными", "pet friendly", "pets allowed"],
            "exclude_patterns": [
                r"(?:животн|питомц|pet).{0,45}(?:запрещ|нельзя|не допуска|не разреш|запрет)",
                r"(?:запрещ|нельзя|не допуска|не разреш|запрет).{0,45}(?:животн|питомц|pet)",
            ],
        },
    ]),
]


class _PropertyCard(QFrame):
    """Карточка одного объекта в разделе «Сравнение»."""

    refresh_requested = Signal(int)
    collapse_toggled  = Signal(int, bool)   # (prop_id, new_collapsed_state)

    def __init__(self, data: Dict[str, Any], collapsed: bool = False, parent=None):
        super().__init__(parent)
        self.setObjectName("ownCard" if data.get("is_own") else "card")
        self.setSizePolicy(QSizePolicy.Policy.Expanding,
                           QSizePolicy.Policy.Preferred)
        self._data = data
        # Виджеты, которые скрываются при свёрнутом состоянии. Заполняется в _build.
        self._collapsible: List[QWidget] = []
        self._collapsed = bool(collapsed)
        self._build()
        # Применяем стартовое состояние свёрнутости после построения
        self._apply_collapse()

    def _build(self) -> None:
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 14, 16, 14)
        lay.setSpacing(8)

        # ── Шапка: название + категория + chevron
        head = QHBoxLayout(); head.setSpacing(8)
        title = QLabel(self._data.get("title") or "Без названия")
        title.setObjectName("cardTitle")
        title.setWordWrap(True)
        head.addWidget(title, stretch=1)
        cat = self._data.get("category")
        if cat:
            cat_lbl = QLabel(cat)
            cat_lbl.setObjectName("categoryBadge")
            cat_lbl.setStyleSheet(
                "background:#3a3938;color:#ffa987;border:1px solid #5a5554;"
                "border-radius:8px;padding:3px 8px;font-size:10px;font-weight:700;"
            )
            head.addWidget(cat_lbl)

        # Chevron для сворачивания (всегда виден)
        self._chevron_btn = QPushButton("▾")
        self._chevron_btn.setFixedSize(26, 26)
        self._chevron_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._chevron_btn.setStyleSheet(
            "QPushButton { background:transparent; color:#9a8a84; border:1px solid transparent;"
            " border-radius:5px; font-size:14px; padding:0; }"
            "QPushButton:hover { color:#ffa987; border-color:#5a5554; background:#3a3938; }"
        )
        self._chevron_btn.clicked.connect(self._on_chevron_clicked)
        head.addWidget(self._chevron_btn)
        lay.addLayout(head)

        addr = self._data.get("address")
        if addr:
            a = QLabel(addr); a.setObjectName("cardSub"); a.setWordWrap(True)
            lay.addWidget(a)

        amenities = self._data.get("amenities") or {}
        description = self._data.get("description")
        key_facts = self._data.get("key_facts") or []
        fetched = self._data.get("amenities_fetched_at")

        if not fetched and not amenities and not description and not key_facts:
            # Удобства ещё не загружены — заголовок и плейсхолдер; в плейсхолдере
            # тоже регистрируем свёртываемые элементы.
            self._build_placeholder(lay)
            self._build_footer(lay, fetched=None)
            return

        # ── ОБ АПАРТАМЕНТАХ (короткие факты-бейджи)
        if key_facts:
            sec = self._section_label("ОБ АПАРТАМЕНТАХ")
            lay.addWidget(sec); self._collapsible.append(sec)
            badges_w = self._build_badges_row(key_facts)
            lay.addWidget(badges_w); self._collapsible.append(badges_w)

        # ── ПОДРОБНОЕ ОПИСАНИЕ (длинный текст)
        if description:
            sec = self._section_label("ПОДРОБНОЕ ОПИСАНИЕ")
            lay.addWidget(sec); self._collapsible.append(sec)
            desc_lbl = QLabel(self._truncate(description, 600))
            desc_lbl.setWordWrap(True)
            desc_lbl.setStyleSheet("color:#d0c8c4;font-size:12px;")
            lay.addWidget(desc_lbl); self._collapsible.append(desc_lbl)

        # ── УСЛУГИ И УДОБСТВА (группы)
        if amenities:
            sec = self._section_label("УСЛУГИ И УДОБСТВА")
            lay.addWidget(sec); self._collapsible.append(sec)
            for group_name, items in amenities.items():
                if not items:
                    continue
                gl = QLabel(group_name)
                gl.setStyleSheet("color:#ffa987;font-weight:600;font-size:11px;"
                                 "margin-top:4px;")
                lay.addWidget(gl); self._collapsible.append(gl)
                items_lbl = QLabel(" • " + "  • ".join(items))
                items_lbl.setWordWrap(True)
                items_lbl.setStyleSheet("color:#d0c8c4;font-size:12px;")
                lay.addWidget(items_lbl); self._collapsible.append(items_lbl)
        elif fetched and not key_facts:
            empty = QLabel("Удобства не найдены на странице объекта.")
            empty.setStyleSheet("color:#b0a09a;font-size:12px;font-style:italic;")
            lay.addWidget(empty); self._collapsible.append(empty)

        self._build_footer(lay, fetched=fetched)

    @staticmethod
    def _build_badges_row(facts: List[str]) -> QWidget:
        """Готовый виджет-строка с фактами-«пилюльками».
        Аналог «Об апартаментах» на сайте Ostrovok (см. скриншот).
        Используется один QLabel с rich-text, чтобы получить word-wrap
        при узкой карточке без сторонних FlowLayout."""
        w = QWidget(); w.setStyleSheet("background:transparent;")
        lay = QHBoxLayout(w)
        lay.setContentsMargins(0, 2, 0, 4); lay.setSpacing(0)

        html_parts = []
        for f in facts:
            safe = (
                f.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
            )
            html_parts.append(
                f'<span style="background:#3a3938;color:#f7ebe8;'
                f'border:1px solid #5a5554;border-radius:10px;'
                f'padding:3px 10px;margin-right:4px;font-size:12px;">'
                f'{safe}</span>'
            )
        lbl = QLabel("&nbsp;".join(html_parts))
        lbl.setTextFormat(Qt.TextFormat.RichText)
        lbl.setWordWrap(True)
        lbl.setStyleSheet("background:transparent;")
        lay.addWidget(lbl)
        return w

    def _build_footer(self, lay: QVBoxLayout, *, fetched: Optional[str]) -> None:
        """Footer всегда виден (даже в свёрнутом состоянии) — кнопка
        обновления и статус загрузки. Текст кнопки зависит от того, есть ли
        кэш: «Загрузить» при отсутствии, «⟳ Обновить» при наличии."""
        foot = QHBoxLayout(); foot.setSpacing(8)
        if fetched:
            t = QLabel(f"⟳ {fetched[:10]}")
            t.setStyleSheet("color:#7a6a64;font-size:10px;")
            foot.addWidget(t)
        foot.addStretch()
        status = self._data.get("fetch_status") or "idle"
        if status in ("running", "queued"):
            s = QLabel("Загрузка удобств…")
            s.setStyleSheet("color:#ffa987;font-size:11px;")
            foot.addWidget(s)
        else:
            label = "⟳ Обновить удобства" if fetched else "Загрузить удобства"
            btn = QPushButton(label)
            btn.setStyleSheet(self._BTN_SMALL)
            btn.setFixedHeight(28)
            btn.clicked.connect(lambda: self.refresh_requested.emit(self._data["id"]))
            foot.addWidget(btn)
        lay.addLayout(foot)

    def _on_chevron_clicked(self) -> None:
        self._collapsed = not self._collapsed
        self._apply_collapse()
        self.collapse_toggled.emit(self._data["id"], self._collapsed)

    def _apply_collapse(self) -> None:
        for w in self._collapsible:
            try:
                w.setVisible(not self._collapsed)
            except RuntimeError:
                continue
        self._chevron_btn.setText("▸" if self._collapsed else "▾")
        self._chevron_btn.setToolTip(
            "Развернуть карточку" if self._collapsed else "Свернуть карточку"
        )

    def _build_placeholder(self, lay: QVBoxLayout) -> None:
        # Только информационное сообщение. Кнопка «Загрузить удобства» появится
        # автоматически в _build_footer, который вызывается сразу после.
        msg = QLabel("Удобства ещё не загружены.")
        msg.setStyleSheet("color:#b0a09a;font-size:12px;")
        lay.addWidget(msg)
        # Сообщение тоже свёртываемое — в свёрнутом виде показываем только
        # шапку и footer.
        self._collapsible.append(msg)

    @staticmethod
    def _section_label(text: str) -> QLabel:
        lbl = QLabel(text)
        lbl.setStyleSheet(
            "color:#b0a09a;font-size:10px;font-weight:700;"
            "letter-spacing:1.5px;margin-top:6px;"
        )
        return lbl

    @staticmethod
    def _truncate(text: str, n: int) -> str:
        text = (text or "").strip()
        return text if len(text) <= n else text[:n].rstrip() + "…"

    _BTN_SMALL = (
        "QPushButton {"
        " background:#3a3938;color:#ffa987;border:1px solid #5a5554;"
        " border-radius:6px;padding:4px 10px;font-size:11px;}"
        "QPushButton:hover { background:#4a4544;border-color:#ffa987; }"
    )


class ComparisonScreen(QWidget):
    """Главный виджет раздела «Сравнение объектов»."""

    _CB_STYLE = (
        "QCheckBox { color:#d0c8c4; font-size:12px; background:transparent;"
        " padding:3px 0; spacing:8px; }"
        "QCheckBox::indicator { width:14px; height:14px; border-radius:3px;"
        " border:1px solid #5a5554; background:#2a2a32; }"
        "QCheckBox::indicator:hover { border-color:#ffa987; }"
        "QCheckBox::indicator:checked { background:#e54b4b; border-color:#e54b4b; }"
        "QCheckBox:checked { color:#f7ebe8; }"
    )

    def __init__(self, api: ApiClient, parent=None):
        super().__init__(parent)
        self.api = api
        self._all_data: List[Dict[str, Any]] = []  # сырой список из api
        self._cards: Dict[int, _PropertyCard] = {}
        self._active_filters: List[FilterDef] = []
        self._search_query: str = ""
        # ID объектов с свёрнутыми карточками. Хранится здесь, а не в карточке,
        # потому что карточки полностью пересоздаются при каждом polling-цикле.
        self._collapsed_ids: set[int] = set()
        self._poll_timer = QTimer(self)
        self._poll_timer.setInterval(1500)
        self._poll_timer.timeout.connect(self._poll_status)
        self._setup_ui()

    # ── Жизненный цикл (вызывается из main_window при показе) ───

    def refresh(self) -> None:
        try:
            self._all_data = self.api.list_comparison() or []
        except Exception as e:
            QMessageBox.warning(self, "Ошибка загрузки", str(e))
            self._all_data = []
        self._render_cards()

    # ── UI ──────────────────────────────────────────────────────

    def _setup_ui(self) -> None:
        root = QHBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0); root.setSpacing(0)

        root.addWidget(self._build_filters_panel())
        root.addWidget(self._build_main_panel(), stretch=1)

    def _build_filters_panel(self) -> QWidget:
        panel = QFrame()
        panel.setFixedWidth(260)
        panel.setStyleSheet(
            "QFrame { background:#252530; border-right:1px solid #5a5554; }"
        )
        outer = QVBoxLayout(panel)
        outer.setContentsMargins(14, 18, 14, 18); outer.setSpacing(10)

        title = QLabel("ФИЛЬТРЫ")
        title.setStyleSheet("color:#ffa987;font-size:11px;font-weight:700;"
                            "letter-spacing:2px;background:transparent;")
        outer.addWidget(title)

        # Поиск по названию
        self.inp_search = QLineEdit()
        self.inp_search.setPlaceholderText("Поиск по названию…")
        self.inp_search.setStyleSheet(
            "QLineEdit { background:#2a2a32; border:1px solid #5a5554;"
            " border-radius:6px; padding:6px 10px; color:#f7ebe8; font-size:12px; }"
            "QLineEdit:focus { border-color:#ffa987; }"
        )
        self.inp_search.textChanged.connect(self._on_search_changed)
        outer.addWidget(self.inp_search)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setStyleSheet("QScrollArea { background:transparent; border:none; }")

        cont = QWidget(); cont.setStyleSheet("background:transparent;")
        cl = QVBoxLayout(cont); cl.setContentsMargins(0, 0, 6, 0); cl.setSpacing(8)

        self._filter_checkboxes: List[Tuple[QCheckBox, FilterDef]] = []
        for group_name, items in FILTER_GROUPS:
            gl = QLabel(group_name)
            gl.setStyleSheet("color:#f7ebe8;font-weight:700;font-size:12px;"
                             "background:transparent;margin-top:6px;")
            cl.addWidget(gl)
            for filter_def in items:
                cb = QCheckBox(filter_def["label"])
                cb.setStyleSheet(self._CB_STYLE)
                cb.toggled.connect(self._on_filter_toggled)
                cl.addWidget(cb)
                self._filter_checkboxes.append((cb, filter_def))

        cl.addStretch()
        scroll.setWidget(cont)
        outer.addWidget(scroll, stretch=1)

        btn_clear = QPushButton("Сбросить фильтры")
        btn_clear.setStyleSheet(
            "QPushButton { background:#3a3938; color:#f7ebe8; border:1px solid #5a5554;"
            " border-radius:6px; padding:6px 10px; font-size:11px; }"
            "QPushButton:hover { border-color:#ffa987; }"
        )
        btn_clear.clicked.connect(self._clear_filters)
        outer.addWidget(btn_clear)

        return panel

    def _build_main_panel(self) -> QWidget:
        panel = QWidget()
        outer = QVBoxLayout(panel)
        outer.setContentsMargins(28, 22, 28, 22); outer.setSpacing(0)

        # Шапка: заголовок + кнопка «Загрузить для всех»
        hdr = QHBoxLayout(); hdr.setSpacing(10)
        col = QVBoxLayout(); col.setSpacing(2)
        pt = QLabel("СРАВНЕНИЕ ОБЪЕКТОВ"); pt.setObjectName("pageTitle")
        self.sub_lbl = QLabel("ЗАГРУЗКА…"); self.sub_lbl.setObjectName("sectionTitle")
        col.addWidget(pt); col.addWidget(self.sub_lbl)
        hdr.addLayout(col); hdr.addStretch()

        self.btn_collapse_all = QPushButton("▾  Свернуть все")
        self.btn_collapse_all.setFixedHeight(38)
        self.btn_collapse_all.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_collapse_all.setStyleSheet(
            "QPushButton {"
            " background:#444140; color:#f7ebe8;"
            " border:1.5px solid #5a5554; border-radius:9px;"
            " min-width:160px; padding:0 18px;"
            " font-size:13px; font-weight:500;"
            " text-align:center;"
            "}"
            "QPushButton:hover {"
            " background:#3a3938; color:#ffa987; border-color:#ffa987;"
            "}"
            "QPushButton:pressed { background:#2e2c2b; }"
        )
        self.btn_collapse_all.clicked.connect(self._toggle_collapse_all)
        hdr.addWidget(self.btn_collapse_all)

        self.btn_refresh_all = QPushButton("  ↻  Загрузить удобства для всех  ")
        self.btn_refresh_all.setObjectName("primaryBtn")
        self.btn_refresh_all.setFixedHeight(38)
        self.btn_refresh_all.clicked.connect(self._refresh_all)
        hdr.addWidget(self.btn_refresh_all)

        outer.addLayout(hdr)

        div = QFrame(); div.setFrameShape(QFrame.Shape.HLine)
        div.setStyleSheet("background:#5a5554;max-height:1px;margin:14px 0 12px 0;")
        outer.addWidget(div)

        # Контейнер карточек
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        cont = QWidget(); cont.setStyleSheet("background:transparent;")
        self._cards_layout = QVBoxLayout(cont)
        self._cards_layout.setContentsMargins(0, 0, 6, 0)
        self._cards_layout.setSpacing(10)
        self._cards_layout.addStretch()
        scroll.setWidget(cont)
        outer.addWidget(scroll, stretch=1)

        return panel

    # ── Render / filter ─────────────────────────────────────────

    def _render_cards(self) -> None:
        # Снести всё, кроме финального stretch
        while self._cards_layout.count() > 1:
            item = self._cards_layout.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()
        self._cards.clear()

        filtered = self._apply_filters(self._all_data)
        self.sub_lbl.setText(
            f"{len(filtered)} из {len(self._all_data)} объектов"
        )

        for item in filtered:
            card = _PropertyCard(
                item,
                collapsed=item["id"] in self._collapsed_ids,
            )
            card.refresh_requested.connect(self._refresh_one)
            card.collapse_toggled.connect(self._on_card_collapse_toggled)
            # Вставляем перед stretch
            self._cards_layout.insertWidget(self._cards_layout.count() - 1, card)
            self._cards[item["id"]] = card
        self._update_collapse_all_btn()

    def _apply_filters(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        q = self._normalize_text(self._search_query)
        result = []
        for it in items:
            if q and q not in self._normalize_text(it.get("title") or ""):
                continue
            if self._active_filters:
                haystack = self._build_haystack(it)
                ok = all(self._matches_filter(it, haystack, f) for f in self._active_filters)
                if not ok:
                    continue
            result.append(it)
        return result

    @classmethod
    def _build_haystack(cls, item: Dict[str, Any]) -> str:
        parts: List[str] = []
        amenities = item.get("amenities") or {}
        for group, ams in amenities.items():
            parts.append(group)
            parts.extend(ams)
        parts.extend(item.get("key_facts") or [])
        if item.get("category"):
            parts.append(item["category"])
        if item.get("description"):
            parts.append(item["description"])
        return cls._normalize_text(" ".join(parts))

    @staticmethod
    def _normalize_text(text: Any, *, expand_wifi: bool = True) -> str:
        value = str(text or "").lower().replace("ё", "е").replace("м²", "м2")
        value = re.sub(r"[\u2010-\u2015–—\-_/]+", " ", value)
        value = re.sub(r"[^\w\s.]+", " ", value, flags=re.UNICODE)
        value = re.sub(r"\s+", " ", value).strip()
        if expand_wifi and (
            "wi fi" in value or "wifi" in value or "вай фай" in value or "вайфай" in value
        ):
            value += " wifi интернет"
        return value

    def _matches_filter(self, item: Dict[str, Any], haystack: str, filter_def: FilterDef) -> bool:
        if self._has_excluded_context(haystack, filter_def):
            return False

        field = filter_def.get("field")
        if field:
            value = self._extract_numeric_fact(item, field)
            if value is None:
                return False
            if "value" in filter_def:
                return value == filter_def["value"]
            if "min" in filter_def and value < filter_def["min"]:
                return False
            if "max" in filter_def and value > filter_def["max"]:
                return False
            return True

        mode = filter_def.get("mode", "any")
        if mode == "all":
            return all(
                self._normalize_text(kw, expand_wifi=False) in haystack
                for kw in filter_def.get("keywords", [])
            )
        if mode == "all_groups":
            return all(
                any(self._normalize_text(kw, expand_wifi=False) in haystack for kw in group)
                for group in filter_def.get("keyword_groups", [])
            )
        keyword_match = any(
            self._normalize_text(kw, expand_wifi=False) in haystack
            for kw in filter_def.get("keywords", [])
        )
        group_match = bool(filter_def.get("keyword_groups")) and all(
            any(self._normalize_text(kw, expand_wifi=False) in haystack for kw in group)
            for group in filter_def.get("keyword_groups", [])
        )
        return keyword_match or group_match

    @classmethod
    def _has_excluded_context(cls, haystack: str, filter_def: FilterDef) -> bool:
        for phrase in filter_def.get("exclude_keywords", []):
            if cls._normalize_text(phrase, expand_wifi=False) in haystack:
                return True
        for pattern in filter_def.get("exclude_patterns", []):
            if re.search(cls._normalize_pattern(pattern), haystack):
                return True
        return False

    @staticmethod
    def _normalize_pattern(pattern: str) -> str:
        return pattern.lower().replace("ё", "е")

    @classmethod
    def _extract_numeric_fact(cls, item: Dict[str, Any], field: str) -> Optional[int]:
        facts = item.get("key_facts") or []
        haystack = cls._normalize_text(" ".join([str(x) for x in facts] + [item.get("category") or ""]))

        if field == "guests":
            match = re.search(r"(?:до\s*)?(\d{1,2})\s*гост", haystack)
            if match:
                return int(match.group(1))
            capacity = item.get("guest_capacity")
            return capacity if isinstance(capacity, int) and capacity > 0 else None

        if field == "rooms":
            match = re.search(r"(\d{1,2})\s*комнат", haystack)
            return int(match.group(1)) if match else None

        if field == "area":
            match = re.search(r"(\d{1,3})\s*(?:кв\s*\.?\s*м|м2|м\s*2)", haystack)
            return int(match.group(1)) if match else None

        if field == "beds":
            match = re.search(r"(\d{1,2})\s*кроват", haystack)
            return int(match.group(1)) if match else None

        return None

    def _on_filter_toggled(self) -> None:
        self._active_filters = [
            filter_def for cb, filter_def in self._filter_checkboxes if cb.isChecked()
        ]
        self._render_cards()

    def _clear_filters(self) -> None:
        for cb, _ in self._filter_checkboxes:
            cb.blockSignals(True); cb.setChecked(False); cb.blockSignals(False)
        self._active_filters = []
        self.inp_search.blockSignals(True); self.inp_search.setText(""); self.inp_search.blockSignals(False)
        self._search_query = ""
        self._render_cards()

    def _on_search_changed(self, text: str) -> None:
        self._search_query = text
        self._render_cards()

    # ── Collapse actions ────────────────────────────────────────

    def _on_card_collapse_toggled(self, prop_id: int, collapsed: bool) -> None:
        """Карточка переключила своё состояние — синхронизируем с экраном."""
        if collapsed:
            self._collapsed_ids.add(prop_id)
        else:
            self._collapsed_ids.discard(prop_id)
        self._update_collapse_all_btn()

    def _toggle_collapse_all(self) -> None:
        """Если хоть одна карточка развёрнута — сворачиваем все, иначе разворачиваем."""
        visible_ids = [it["id"] for it in self._apply_filters(self._all_data)]
        any_expanded = any(pid not in self._collapsed_ids for pid in visible_ids)
        if any_expanded:
            self._collapsed_ids.update(visible_ids)
        else:
            for pid in visible_ids:
                self._collapsed_ids.discard(pid)
        self._render_cards()
        self._update_collapse_all_btn()

    def _update_collapse_all_btn(self) -> None:
        if not hasattr(self, "btn_collapse_all"):
            return
        visible_ids = [it["id"] for it in self._apply_filters(self._all_data)]
        any_expanded = any(pid not in self._collapsed_ids for pid in visible_ids)
        self.btn_collapse_all.setText(
            "▾  Свернуть все" if any_expanded else "▸  Развернуть все"
        )

    # ── Refresh actions ─────────────────────────────────────────

    def _refresh_one(self, prop_id: int) -> None:
        # Помечаем как running локально и сразу перерисовываем
        for it in self._all_data:
            if it["id"] == prop_id:
                it["fetch_status"] = "running"
                break
        self._render_cards()
        self._poll_timer.start()
        # Запускаем фетч в фоне (отдельный поток через api_client._run)
        from threading import Thread

        def _do():
            try:
                self.api.refresh_comparison_one(prop_id, force=True)
            except Exception as e:
                # Помечаем ошибку
                for it in self._all_data:
                    if it["id"] == prop_id:
                        it["fetch_status"] = f"error:{str(e)[:60]}"
                        break

        Thread(target=_do, daemon=True).start()

    def _refresh_all(self) -> None:
        try:
            res = self.api.refresh_comparison_all(force=False)
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", str(e))
            return
        n = res.get("queued", 0)
        QMessageBox.information(
            self, "Загрузка запущена",
            f"Загрузка удобств для {n} объектов запущена в фоне.\n"
            f"Карточки будут обновляться автоматически."
        )
        for it in self._all_data:
            if not it.get("amenities_fetched_at"):
                it["fetch_status"] = "queued"
        self._render_cards()
        self._poll_timer.start()

    def _poll_status(self) -> None:
        """Опрос: подтягиваем свежие данные из БД, обновляем карточки.
        Останавливаем таймер, когда нет ни одного in-progress."""
        try:
            fresh = self.api.list_comparison() or []
        except Exception:
            return
        # Mapping id -> data
        fresh_by_id = {it["id"]: it for it in fresh}
        any_running = False
        for it in self._all_data:
            new = fresh_by_id.get(it["id"])
            if not new:
                continue
            it.update(new)
            if it.get("fetch_status") in ("queued", "running"):
                any_running = True
        # Перерисовка
        self._render_cards()
        if not any_running:
            self._poll_timer.stop()
