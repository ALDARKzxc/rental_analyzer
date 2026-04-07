"""Price chart — Crimson Abyss dark theme."""
from __future__ import annotations
from typing import List, Dict
from datetime import datetime

from PySide6.QtWidgets import QWidget, QVBoxLayout
import matplotlib
matplotlib.use("QtAgg")
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import numpy as np

BG      = "#0C0A0B"
SURFACE = "#181114"
ACCENT  = "#9B2C2C"
ACCENT2 = "#4A1C1C"
GLOW    = "#C53030"
TEXT    = "#F0E6D2"
MUTED   = "#7A6A5C"
BORDER  = "#2A1A1A"


class PriceChartWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.fig = Figure(figsize=(10, 3.2), dpi=96)
        self.fig.patch.set_facecolor(BG)
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setMinimumHeight(240)
        self.canvas.setStyleSheet(f"background: {BG};")
        layout.addWidget(self.canvas)

        self._draw_empty()

    def _draw_empty(self):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.set_facecolor(SURFACE)
        self.fig.patch.set_facecolor(BG)
        ax.text(0.5, 0.5, "НЕТ ДАННЫХ ДЛЯ ОТОБРАЖЕНИЯ",
                ha="center", va="center",
                fontsize=11, color=MUTED, fontweight="bold",
                transform=ax.transAxes)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.set_xticks([])
        ax.set_yticks([])
        self.fig.tight_layout(pad=0.5)
        self.canvas.draw()

    def plot(self, price_records: List[Dict]):
        self.fig.clear()
        valid = [r for r in reversed(price_records) if r.get("price") is not None]
        if not valid:
            self._draw_empty()
            return

        dates, prices = [], []
        for r in valid:
            ds = r.get("recorded_at", "")
            try:
                dt = datetime.fromisoformat(ds.replace("T", " ").split(".")[0])
                dates.append(dt)
                prices.append(r["price"])
            except Exception:
                pass
        if not dates:
            self._draw_empty()
            return

        ax = self.fig.add_subplot(111)
        ax.set_facecolor(SURFACE)
        self.fig.patch.set_facecolor(BG)

        # Grid
        ax.grid(axis="y", color=BORDER, linewidth=0.8, linestyle="-", zorder=0)
        ax.grid(axis="x", color=BORDER, linewidth=0.5, linestyle=":", zorder=0)

        # Fill under line
        ax.fill_between(dates, prices, color=ACCENT2, alpha=0.25, zorder=1)

        # Main line
        ax.plot(dates, prices, color=ACCENT, linewidth=2.5, zorder=3,
                solid_capstyle="round", solid_joinstyle="round")

        # Dots
        ax.scatter(dates, prices, color=GLOW, s=55, zorder=5,
                   edgecolors=BG, linewidths=1.5)

        # Trend line
        if len(dates) >= 3:
            xn = mdates.date2num(dates)
            c = np.polyfit(xn, prices, 1)
            tv = np.polyval(c, xn)
            t_color = "#2D6A4F" if c[0] >= 0 else ACCENT
            ax.plot(dates, tv, color=t_color, linewidth=1.5,
                    linestyle="--", alpha=0.7, zorder=2, label="Тренд")

        # Mean line
        mean = np.mean(prices)
        mean_label = f"Среднее: {mean:,.0f} \u20bd".replace(",", "\u202f")
        ax.axhline(mean, color=MUTED, linewidth=1.0, linestyle=":",
                   alpha=0.8, zorder=2, label=mean_label)

        # Unavailable markers (×)
        for r in reversed(price_records):
            if r.get("price") is None:
                ds = r.get("recorded_at", "")
                try:
                    dt = datetime.fromisoformat(ds.replace("T", " ").split(".")[0])
                    ymin = min(prices) * 0.97
                    ax.scatter([dt], [ymin], marker="x", color=ACCENT,
                               s=70, zorder=4, linewidths=2)
                except Exception:
                    pass

        # Axes format
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=10))
        self.fig.autofmt_xdate(rotation=30, ha="right")

        ax.yaxis.set_major_formatter(
            ticker.FuncFormatter(
                lambda x, _: f"{x:,.0f}\u20bd".replace(",", "\u202f")
            )
        )

        # Spines
        for side in ["top", "right"]:
            ax.spines[side].set_visible(False)
        ax.spines["left"].set_color(BORDER)
        ax.spines["bottom"].set_color(BORDER)
        ax.tick_params(colors=MUTED, labelsize=9)

        # Title
        ax.set_title("ИСТОРИЯ ЦЕН", fontsize=10, fontweight="bold",
                     color=MUTED, pad=10, loc="left")

        # Legend
        ax.legend(fontsize=9, loc="upper left",
                  framealpha=0.85, edgecolor=BORDER,
                  facecolor=SURFACE, labelcolor=TEXT)

        self.fig.tight_layout(pad=1.0)
        self.canvas.draw()
