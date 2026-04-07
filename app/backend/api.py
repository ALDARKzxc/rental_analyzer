"""FastAPI local API v2 — категории, даты парсинга, фильтрация."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from app.backend.database import PropertyRepository, PriceRepository, CATEGORIES
from app.analytics.engine import AnalyticsEngine

app = FastAPI(title="Rental Price Analyzer API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

_parse_tasks: dict[int, str] = {}
_parse_semaphore = asyncio.Semaphore(5)


async def _body(request: Request) -> dict:
    try:
        raw = await request.body()
        if not raw: return {}
        text = raw.decode("utf-8").strip()
        if not text: return {}
        data = json.loads(text)
        if isinstance(data, str): data = json.loads(data)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error(f"_body error: {e}")
        return {}


async def _run_parse(property_id: int):
    if _parse_tasks.get(property_id) == "running":
        return
    _parse_tasks[property_id] = "queued"
    async with _parse_semaphore:
        _parse_tasks[property_id] = "running"
        prop = await PropertyRepository.get_by_id(property_id)
        if not prop:
            _parse_tasks[property_id] = "error:not_found"
            return
        try:
            from app.parser.dispatcher import ParserDispatcher
            # Строим URL с датами парсинга
            url = _build_parse_url(prop.url, prop.parse_dates)
            logger.info(f"Parsing property {property_id}: {url[:80]}")
            result = await ParserDispatcher().parse(url)

            price  = result.get("price")
            status = result.get("status", "ok")
            error  = result.get("error")
            title  = result.get("title")

            if title and title != prop.title and len(title) > 2:
                await PropertyRepository.update(property_id, title=title)

            await PriceRepository.add_record(
                property_id=property_id,
                price=price, status=status, error_message=error,
                parse_dates=prop.parse_dates
            )
            logger.info(f"Parsed {property_id}: price={price} status={status} dates={prop.parse_dates}")
            _parse_tasks[property_id] = "done"
        except Exception as e:
            logger.error(f"Parse failed {property_id}: {e}")
            await PriceRepository.add_record(
                property_id=property_id, price=None,
                status="error", error_message=str(e)[:500],
                parse_dates=prop.parse_dates if prop else None
            )
            _parse_tasks[property_id] = f"error:{str(e)[:100]}"


def _build_parse_url(base_url: str, parse_dates: Optional[str]) -> str:
    """Добавляем даты в URL для парсинга."""
    clean = base_url.split("?")[0]
    if not parse_dates:
        return clean
    # parse_dates формат: "DD.MM.YYYY-DD.MM.YYYY"
    return f"{clean}?dates={parse_dates}&guests=2"


def _prop_out(p, rec=None) -> dict:
    d = {
        "id": p.id, "title": p.title, "url": p.url,
        "site": p.site, "category": p.category or "Квартиры",
        "parse_dates": p.parse_dates,
        "notes": p.notes, "is_active": p.is_active,
        "created_at": p.created_at.isoformat(),
        "latest_price": None, "latest_status": None, "latest_dates": None,
    }
    if rec:
        d["latest_price"]  = rec.price
        d["latest_status"] = rec.status
        d["latest_dates"]  = rec.parse_dates
    return d


def _rec_out(r) -> dict:
    return {
        "id": r.id, "property_id": r.property_id,
        "price": r.price, "currency": r.currency,
        "status": r.status, "error_message": r.error_message,
        "parse_dates": r.parse_dates,
        "recorded_at": r.recorded_at.isoformat(),
    }


# ── Health ──────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


# ── Categories ──────────────────────────────────────────────────
@app.get("/categories")
async def get_categories():
    return {"categories": CATEGORIES}


# ── Properties ──────────────────────────────────────────────────
@app.get("/properties")
async def list_properties(category: str = None):
    props  = await PropertyRepository.get_all(category)
    latest = await PriceRepository.get_all_latest()
    return [_prop_out(p, latest.get(p.id)) for p in props]


@app.post("/properties", status_code=201)
async def create_property(request: Request):
    data = await _body(request)
    title    = data.get("title", "").strip()
    url      = data.get("url", "").strip()
    category = data.get("category", "Квартиры")
    notes    = data.get("notes")

    if not title or not url:
        raise HTTPException(422, f"title and url required, got: {data}")

    existing = await PropertyRepository.get_by_url(url)
    if existing:
        raise HTTPException(400, "Property with this URL already exists")

    from app.parser.dispatcher import ParserDispatcher
    site = ParserDispatcher().detect_site(url)
    prop = await PropertyRepository.create(
        title=title, url=url, site=site,
        category=category, notes=notes
    )
    return _prop_out(prop)


@app.get("/properties/{prop_id}")
async def get_property(prop_id: int):
    prop = await PropertyRepository.get_by_id(prop_id)
    if not prop: raise HTTPException(404, "Not found")
    rec = await PriceRepository.get_latest(prop_id)
    return _prop_out(prop, rec)


@app.put("/properties/{prop_id}")
async def update_property(prop_id: int, request: Request):
    data = await _body(request)
    prop = await PropertyRepository.update(prop_id, **{
        k: v for k, v in data.items()
        if k in ("title", "url", "category", "notes", "parse_dates")
    })
    if not prop: raise HTTPException(404, "Not found")
    return _prop_out(prop)


@app.delete("/properties/{prop_id}")
async def delete_property(prop_id: int):
    ok = await PropertyRepository.delete(prop_id)
    if not ok: raise HTTPException(404, "Not found")
    return {"deleted": True}


# ── Dates ────────────────────────────────────────────────────────
@app.post("/dates/category")
async def set_category_dates(request: Request):
    """Устанавливаем даты парсинга для всей категории."""
    data     = await _body(request)
    category = data.get("category", "").strip()
    dates    = data.get("dates", "").strip()
    if not category or not dates:
        raise HTTPException(422, "category and dates required")
    count = await PropertyRepository.set_category_dates(category, dates)
    return {"updated": count, "category": category, "dates": dates}


@app.post("/dates/property/{prop_id}")
async def set_property_dates(prop_id: int, request: Request):
    """Устанавливаем даты для одного объекта."""
    data  = await _body(request)
    dates = data.get("dates", "").strip()
    if not dates: raise HTTPException(422, "dates required")
    ok = await PropertyRepository.set_parse_dates(prop_id, dates)
    if not ok: raise HTTPException(404, "Not found")
    return {"updated": True, "dates": dates}


# ── Prices ───────────────────────────────────────────────────────
@app.get("/prices/{prop_id}")
async def get_prices(prop_id: int, limit: int = 100):
    records = await PriceRepository.get_history(prop_id, limit)
    return [_rec_out(r) for r in records]


# ── Parse ────────────────────────────────────────────────────────
@app.post("/parse")
async def parse_properties(request: Request, background_tasks: BackgroundTasks):
    data     = await _body(request)
    prop_id  = data.get("property_id")
    category = data.get("category")

    if prop_id is not None:
        prop_ids = [int(prop_id)]
    elif category and category != "Все":
        props    = await PropertyRepository.get_all(category)
        prop_ids = [p.id for p in props]
    else:
        props    = await PropertyRepository.get_all()
        prop_ids = [p.id for p in props]

    for pid in prop_ids:
        background_tasks.add_task(_run_parse, pid)

    return [{"property_id": pid, "status": "queued"} for pid in prop_ids]


@app.get("/parse/status/{prop_id}")
async def parse_status(prop_id: int):
    return {"property_id": prop_id, "status": _parse_tasks.get(prop_id, "idle")}


# ── Analytics ────────────────────────────────────────────────────
@app.get("/analytics/{prop_id}")
async def get_analytics(prop_id: int):
    prop = await PropertyRepository.get_by_id(prop_id)
    if not prop: raise HTTPException(404, "Not found")
    records = await PriceRepository.get_history(prop_id, 60)
    return AnalyticsEngine.compute(prop_id, records)
