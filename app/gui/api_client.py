"""API client v2 — прямые вызовы + категории + даты парсинга."""
from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional
from loguru import logger

_backend_loop = None
_backend_loop_lock = threading.Lock()


def register_backend_loop(loop):
    global _backend_loop
    with _backend_loop_lock:
        _backend_loop = loop
    logger.info("Backend loop registered")


def _run(coro) -> Any:
    import asyncio
    with _backend_loop_lock:
        loop = _backend_loop
    if loop is None or not loop.is_running():
        raise RuntimeError("Backend not ready")
    return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=60)


class ApiError(Exception):
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


def _w(coro) -> Any:
    try:
        return _run(coro)
    except Exception as e:
        raise ApiError(str(e))


class ApiClient:

    # ── Properties ───────────────────────────────────────────────

    def get_properties(self, category: str = None) -> List[Dict]:
        from app.backend.database import PropertyRepository, PriceRepository

        async def _():
            props  = await PropertyRepository.get_all(category)
            latest = await PriceRepository.get_all_latest()
            result = []
            for p in props:
                rec = latest.get(p.id)
                result.append({
                    "id": p.id, "title": p.title, "url": p.url,
                    "site": p.site, "category": p.category or "Квартиры",
                    "parse_dates": p.parse_dates,
                    "address": p.address,
                    "guest_capacity": p.guest_capacity,
                    "preview_path": p.preview_path,
                    "notes": p.notes,
                    "is_active": p.is_active,
                    "title_locked": getattr(p, "title_locked", False),
                    "is_own": bool(getattr(p, "is_own", False)),
                    "created_at": p.created_at.isoformat(),
                    "latest_price":  rec.price      if rec else None,
                    "latest_status": rec.status     if rec else None,
                    "latest_dates":  rec.parse_dates if rec else None,
                })
            return result
        return _w(_())

    def create_property(self, title: str, url: str,
                        category: str = "Квартиры",
                        notes: str = None,
                        title_locked: bool = False,
                        is_own: bool = False) -> Dict:
        from app.backend.database import PropertyRepository
        from app.backend.property_service import enrich_property_metadata
        from app.parser.dispatcher import ParserDispatcher

        async def _():
            # Активный объект с таким URL — настоящий дубликат
            existing_active = await PropertyRepository.get_by_url(url)
            if existing_active:
                raise ValueError("Property with this URL already exists")

            # Мягко-удалённый объект (is_active=False) — реактивируем
            any_existing = await PropertyRepository.get_by_url_any(url)
            if any_existing and not any_existing.is_active:
                prop = await PropertyRepository.update(
                    any_existing.id,
                    title=title, category=category, notes=notes,
                    title_locked=title_locked, is_own=is_own, is_active=True
                )
                prop = await enrich_property_metadata(
                    prop.id,
                    allow_title_update=not title_locked,
                ) or prop
                return {"id": prop.id, "title": prop.title, "url": prop.url,
                        "site": prop.site, "category": prop.category,
                        "parse_dates": prop.parse_dates,
                        "address": prop.address,
                        "guest_capacity": prop.guest_capacity,
                        "preview_path": prop.preview_path,
                        "title_locked": getattr(prop, "title_locked", False),
                        "is_own": bool(getattr(prop, "is_own", False)),
                        "is_active": prop.is_active,
                        "created_at": prop.created_at.isoformat(),
                        "latest_price": None, "latest_status": None}

            # Создаём новый
            site = ParserDispatcher().detect_site(url)
            prop = await PropertyRepository.create(
                title=title, url=url, site=site,
                category=category, notes=notes,
                title_locked=title_locked,
                is_own=is_own,
            )
            prop = await enrich_property_metadata(
                prop.id,
                allow_title_update=not title_locked,
            ) or prop
            return {"id": prop.id, "title": prop.title, "url": prop.url,
                    "site": prop.site, "category": prop.category,
                    "parse_dates": prop.parse_dates,
                    "address": prop.address,
                    "guest_capacity": prop.guest_capacity,
                    "preview_path": prop.preview_path,
                    "title_locked": getattr(prop, "title_locked", False),
                    "is_own": bool(getattr(prop, "is_own", False)),
                    "is_active": prop.is_active,
                    "created_at": prop.created_at.isoformat(),
                    "latest_price": None, "latest_status": None}
        return _w(_())

    def update_property(self, prop_id: int, **kwargs) -> Dict:
        from app.backend.database import PropertyRepository

        async def _():
            prop = await PropertyRepository.update(prop_id, **kwargs)
            if not prop: raise ValueError("Not found")
            return {"id": prop.id, "title": prop.title}
        return _w(_())

    def delete_property(self, prop_id: int) -> Dict:
        from app.backend.database import PropertyRepository

        async def _():
            ok = await PropertyRepository.delete(prop_id)
            if not ok: raise ValueError("Not found")
            return {"deleted": True}
        return _w(_())

    # ── Dates ────────────────────────────────────────────────────

    def set_category_dates(self, category: str, dates: str) -> Dict:
        """Установить даты парсинга для всей категории."""
        from app.backend.database import PropertyRepository

        async def _():
            count = await PropertyRepository.set_category_dates(category, dates)
            return {"updated": count}
        return _w(_())

    def set_all_dates(self, dates: str) -> Dict:
        """Установить даты парсинга для всех активных объектов."""
        from app.backend.database import PropertyRepository

        async def _():
            count = await PropertyRepository.set_all_dates(dates)
            return {"updated": count}
        return _w(_())

    def set_property_dates(self, prop_id: int, dates: str) -> Dict:
        from app.backend.database import PropertyRepository

        async def _():
            await PropertyRepository.set_parse_dates(prop_id, dates)
            return {"updated": True}
        return _w(_())

    # ── Prices ───────────────────────────────────────────────────

    def get_prices(self, prop_id: int) -> List[Dict]:
        from app.backend.database import PriceRepository

        async def _():
            recs = await PriceRepository.get_history(prop_id, 100)
            return [{
                "id": r.id, "property_id": r.property_id,
                "price": r.price, "currency": r.currency,
                "status": r.status, "error_message": r.error_message,
                "parse_dates": r.parse_dates,
                "recorded_at": r.recorded_at.isoformat(),
            } for r in recs]
        return _w(_())

    # ── Parse ────────────────────────────────────────────────────

    def trigger_parse(self, prop_id: int = None, category: str = None) -> List[Dict]:
        from app.backend import api as backend_api
        from app.backend.database import PropertyRepository
        import asyncio

        async def _():
            if prop_id is not None:
                prop_ids = [int(prop_id)]
            elif category and category != "Все":
                props    = await PropertyRepository.get_all(category)
                prop_ids = [p.id for p in props]
            else:
                props    = await PropertyRepository.get_all()
                prop_ids = [p.id for p in props]

            loop = asyncio.get_event_loop()
            for pid in prop_ids:
                loop.create_task(backend_api._run_parse(pid))
            return [{"property_id": pid, "status": "queued"} for pid in prop_ids]
        return _w(_())

    def get_parse_status(self, prop_id: int) -> Dict:
        from app.backend import api as backend_api

        async def _():
            return {"property_id": prop_id,
                    "status": backend_api._parse_tasks.get(prop_id, "idle")}
        return _w(_())

    # ── Analytics ────────────────────────────────────────────────

    def get_analytics(self, prop_id: int) -> Dict:
        from app.backend.database import PropertyRepository, PriceRepository
        from app.analytics.engine import AnalyticsEngine

        async def _():
            prop = await PropertyRepository.get_by_id(prop_id)
            if not prop: raise ValueError("Not found")
            records = await PriceRepository.get_history(prop_id, 60)
            return AnalyticsEngine.compute(prop_id, records)
        return _w(_())

    # ── Deep Analysis ────────────────────────────────────────────

    def start_deep_analysis(self, prop_ids: List[int]) -> None:
        """
        Запускает глубокий анализ как фоновую asyncio-задачу.
        Возвращается немедленно — анализ работает в фоне.
        """
        async def _schedule():
            from app.backend.deep_analysis import start_task
            await start_task(prop_ids)

        _run(_schedule())  # _run возвращается быстро — start_task создаёт task и выходит

    def get_deep_analysis_state(self) -> Dict:
        """
        Читает текущее состояние анализа.
        Потокобезопасно (только чтение Python dict под GIL).
        """
        from app.backend import deep_analysis as da
        return da.get_state()

    def cancel_deep_analysis(self) -> None:
        """Запрашивает отмену анализа через asyncio-поток (thread-safe)."""
        with _backend_loop_lock:
            loop = _backend_loop
        if loop and loop.is_running():
            from app.backend.deep_analysis import request_cancel
            loop.call_soon_threadsafe(request_cancel)

    # ── Health ───────────────────────────────────────────────────

    def health(self) -> bool:
        with _backend_loop_lock:
            loop = _backend_loop
        return loop is not None and loop.is_running()
