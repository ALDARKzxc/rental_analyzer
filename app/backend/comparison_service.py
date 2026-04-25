"""
Сервис раздела «Сравнение объектов».

Архитектура:
  • Удобства и описание парсятся ОТДЕЛЬНО от цен — отдельный кодпуть,
    отдельный вызов парсера. Не влияет на скорость parse-флоу.
  • Кэшируются в БД навсегда (Property.amenities, Property.description).
    Повторный фетч — только по явной команде пользователя.
  • На уровне сервиса есть общий semaphore (concurrency=3), чтобы массовая
    загрузка удобств не насиловала сеть.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

from loguru import logger

from app.backend.database import PropertyRepository
from app.parser.dispatcher import ParserDispatcher

# Ограничиваем параллелизм фетчей удобств — не насилуем сеть
_AMENITIES_SEMAPHORE = asyncio.Semaphore(3)
_dispatcher = ParserDispatcher()

# Статусы in-progress fetch'ей для UI
_fetch_status: dict[int, str] = {}  # prop_id -> "queued" | "running" | "done" | "error:..."


def _decode_amenities(raw: Optional[str]) -> Dict[str, List[str]]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    # Только string-ключи и list-of-strings
    result: Dict[str, List[str]] = {}
    for k, v in data.items():
        if not isinstance(k, str) or not isinstance(v, list):
            continue
        items = [str(x) for x in v if isinstance(x, str) and x.strip()]
        if items:
            result[k] = items
    return result


def _encode_amenities(groups: Dict[str, List[str]]) -> Optional[str]:
    if not groups:
        return None
    try:
        return json.dumps(groups, ensure_ascii=False)
    except Exception:
        return None


def comparison_dict(prop) -> Dict[str, Any]:
    """Сериализация одного объекта для UI «Сравнение»."""
    return {
        "id": prop.id,
        "title": prop.title,
        "url": prop.url,
        "site": prop.site,
        "category": prop.category,
        "address": prop.address,
        "guest_capacity": prop.guest_capacity,
        "preview_path": prop.preview_path,
        "is_own": bool(getattr(prop, "is_own", False)),
        "amenities": _decode_amenities(getattr(prop, "amenities", None)),
        "description": getattr(prop, "description", None),
        "amenities_fetched_at": (
            prop.amenities_fetched_at.isoformat()
            if getattr(prop, "amenities_fetched_at", None) else None
        ),
        "fetch_status": _fetch_status.get(prop.id, "idle"),
    }


async def list_for_comparison() -> List[Dict[str, Any]]:
    """Все активные объекты с уже кэшированными удобствами.
    Никаких сетевых запросов — просто читаем БД."""
    props = await PropertyRepository.get_all()
    return [comparison_dict(p) for p in props]


async def fetch_amenities_for(prop_id: int, force: bool = False) -> Dict[str, Any]:
    """
    Загрузка удобств для одного объекта.
    Если уже закэшировано и force=False — возвращаем кэш мгновенно.
    """
    prop = await PropertyRepository.get_by_id(prop_id)
    if not prop:
        return {"ok": False, "error": "Not found"}

    if not force and getattr(prop, "amenities_fetched_at", None):
        return {"ok": True, "cached": True, "data": comparison_dict(prop)}

    if _fetch_status.get(prop_id) == "running":
        return {"ok": True, "cached": False, "queued": True}

    _fetch_status[prop_id] = "queued"
    async with _AMENITIES_SEMAPHORE:
        _fetch_status[prop_id] = "running"
        try:
            result = await _dispatcher.fetch_amenities(prop.url)
            groups = result.get("amenities") or {}
            description = result.get("description")
            await PropertyRepository.update_amenities(
                prop_id=prop_id,
                amenities_json=_encode_amenities(groups),
                description=description,
            )
            _fetch_status[prop_id] = "done"
            prop = await PropertyRepository.get_by_id(prop_id)
            return {"ok": True, "cached": False, "data": comparison_dict(prop)}
        except Exception as e:
            logger.warning(f"comparison_service: fetch error for {prop_id}: {e}")
            _fetch_status[prop_id] = f"error:{str(e)[:80]}"
            return {"ok": False, "error": str(e)[:200]}


async def fetch_amenities_bulk(prop_ids: List[int], force: bool = False) -> None:
    """
    Параллельный фетч с общим семафором. Запускает таски и сразу возвращается —
    UI должен опрашивать статус через list_for_comparison() / get_fetch_status().
    """
    loop = asyncio.get_event_loop()
    for pid in prop_ids:
        if _fetch_status.get(pid) in ("running", "queued"):
            continue
        _fetch_status[pid] = "queued"
        loop.create_task(fetch_amenities_for(pid, force=force))


def get_fetch_status(prop_id: int) -> str:
    return _fetch_status.get(prop_id, "idle")
