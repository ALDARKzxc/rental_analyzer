"""
Analytics engine — computes stats, trends, recommendations.
"""
from __future__ import annotations

import statistics
from typing import List, Optional, Dict, Any

from app.utils.config import PRICE_HIGH_THRESHOLD, PRICE_LOW_THRESHOLD


class AnalyticsEngine:

    @staticmethod
    def compute(property_id: int, records) -> Dict[str, Any]:
        """
        Compute analytics from a list of PriceRecord ORM objects.
        Returns dict matching AnalyticsOut schema.
        """
        prices = [r.price for r in records if r.price is not None]

        base = {
            "property_id": property_id,
            "current_price": None,
            "avg_price": None,
            "min_price": None,
            "max_price": None,
            "price_change_pct": None,
            "trend": "insufficient_data",
            "recommendation": "Недостаточно данных для анализа",
            "records_count": len(records),
        }

        if not prices:
            return base

        current = prices[0]  # records are ordered desc
        base["current_price"] = current
        base["records_count"] = len(prices)

        if len(prices) >= 2:
            avg = statistics.mean(prices)
            base["avg_price"] = round(avg, 2)
            base["min_price"] = min(prices)
            base["max_price"] = max(prices)

            # Price change: current vs oldest available
            oldest = prices[-1]
            if oldest and oldest != 0:
                change_pct = (current - oldest) / oldest
                base["price_change_pct"] = round(change_pct * 100, 2)

            # Trend (last 5 values)
            recent = prices[:min(5, len(prices))]
            if len(recent) >= 3:
                trend = AnalyticsEngine._compute_trend(recent)
                base["trend"] = trend

            # Recommendation vs market average
            if avg and avg != 0:
                deviation = (current - avg) / avg
                if deviation > PRICE_HIGH_THRESHOLD:
                    base["recommendation"] = (
                        f"⬇️ Цена выше рынка на {deviation*100:.1f}%. "
                        f"Рекомендуем снизить до ~{avg:,.0f} ₽"
                    )
                elif deviation < PRICE_LOW_THRESHOLD:
                    base["recommendation"] = (
                        f"⬆️ Цена ниже рынка на {abs(deviation)*100:.1f}%. "
                        f"Можно повысить до ~{avg:,.0f} ₽"
                    )
                else:
                    base["recommendation"] = (
                        f"✅ Цена в рыночном диапазоне (±{abs(deviation)*100:.1f}% от среднего)"
                    )
        elif len(prices) == 1:
            base["avg_price"] = current
            base["min_price"] = current
            base["max_price"] = current
            base["recommendation"] = "Накопите больше данных для точного анализа"

        return base

    @staticmethod
    def _compute_trend(prices: List[float]) -> str:
        if len(prices) < 2:
            return "stable"

        # Simple linear regression slope
        n = len(prices)
        x = list(range(n))
        x_mean = sum(x) / n
        y_mean = sum(prices) / n

        numerator = sum((x[i] - x_mean) * (prices[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return "stable"

        slope = numerator / denominator
        # Normalize by mean price
        if y_mean != 0:
            rel_slope = slope / y_mean
        else:
            return "stable"

        if rel_slope > 0.02:
            return "up"
        elif rel_slope < -0.02:
            return "down"
        return "stable"
