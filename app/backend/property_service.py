"""Shared property workflows: metadata enrichment and preview caching."""
from __future__ import annotations

import random
import re
from pathlib import Path
from urllib.parse import urlparse

import httpx
from loguru import logger

from app.backend.database import PropertyRepository
from app.parser.base_parser import _detect_system_proxy
from app.utils.config import PREVIEWS_DIR, PARSER_USER_AGENTS

_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
_CONTENT_TYPE_SUFFIXES = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/bmp": ".bmp",
}
_IMAGE_SIGNATURES = (
    b"\xff\xd8\xff",
    b"\x89PNG\r\n\x1a\n",
    b"GIF87a",
    b"GIF89a",
    b"RIFF",
    b"BM",
)


def _clean_text(value: object, *, limit: int) -> str | None:
    if not isinstance(value, str):
        return None

    cleaned = " ".join(value.split()).strip()
    if len(cleaned) < 2:
        return None
    return cleaned[:limit]


def _clean_guest_capacity(value: object) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        guest_capacity = int(value)
    elif isinstance(value, str):
        match = re.search(r"\d{1,2}", value)
        if not match:
            return None
        guest_capacity = int(match.group(0))
    else:
        return None

    if 1 <= guest_capacity <= 30:
        return guest_capacity
    return None


def _guess_image_suffix(image_url: str, content_type: str) -> str:
    content_type = (content_type or "").split(";")[0].strip().lower()
    if content_type in _CONTENT_TYPE_SUFFIXES:
        return _CONTENT_TYPE_SUFFIXES[content_type]

    suffix = Path(urlparse(image_url).path).suffix.lower()
    if suffix in _IMAGE_SUFFIXES:
        return suffix

    return ".jpg"


def _looks_like_image_payload(content_type: str, image_url: str, payload: bytes) -> bool:
    content_type = (content_type or "").split(";")[0].strip().lower()
    if content_type.startswith("image/"):
        return True

    suffix = Path(urlparse(image_url).path).suffix.lower()
    if suffix in _IMAGE_SUFFIXES and payload:
        return True

    return any(payload.startswith(signature) for signature in _IMAGE_SIGNATURES)


async def _cache_preview_image(property_id: int, image_url: str) -> str | None:
    PREVIEWS_DIR.mkdir(parents=True, exist_ok=True)

    headers = {
        "User-Agent": random.choice(PARSER_USER_AGENTS),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Referer": "https://ostrovok.ru/",
        "Origin": "https://ostrovok.ru",
    }

    proxy = _detect_system_proxy()
    proxies_to_try = [proxy, None] if proxy else [None]
    response = None
    for attempt_proxy in proxies_to_try:
        kwargs: dict = {
            "timeout": httpx.Timeout(15.0, connect=5.0),
            "follow_redirects": True,
            "trust_env": False,
        }
        if attempt_proxy:
            kwargs["proxy"] = attempt_proxy
        try:
            async with httpx.AsyncClient(**kwargs) as client:
                response = await client.get(image_url, headers=headers)
            break
        except Exception as exc:
            logger.debug(
                f"Preview download (proxy={bool(attempt_proxy)}) failed for "
                f"property {property_id}: {exc}"
            )
            response = None

    if response is None:
        logger.warning(f"Preview download failed for property {property_id}: all attempts errored")
        return None

    content_type = response.headers.get("content-type", "")
    if response.status_code != 200 or not _looks_like_image_payload(
        content_type,
        image_url,
        response.content,
    ):
        logger.debug(
            "Preview download skipped for property {}: status={} content-type={}",
            property_id,
            response.status_code,
            content_type,
        )
        return None

    suffix = _guess_image_suffix(image_url, content_type)
    target_path = PREVIEWS_DIR / f"property_{property_id}{suffix}"

    try:
        for existing in PREVIEWS_DIR.glob(f"property_{property_id}.*"):
            if existing != target_path:
                existing.unlink(missing_ok=True)
        target_path.write_bytes(response.content)
    except Exception as exc:
        logger.warning(f"Failed to save preview for property {property_id}: {exc}")
        return None

    return str(target_path)


async def enrich_property_metadata(
    property_id: int,
    *,
    allow_title_update: bool,
) -> object | None:
    """Fetch metadata from the listing page and persist what we can."""
    from app.parser.dispatcher import ParserDispatcher

    prop = await PropertyRepository.get_by_id(property_id)
    if not prop or not prop.is_active:
        return prop

    dispatcher = ParserDispatcher()
    try:
        metadata = await dispatcher.fetch_metadata(prop.url)
    except Exception as exc:
        logger.warning(f"Metadata enrichment failed for property {property_id}: {exc}")
        return prop

    if not metadata:
        return prop

    updates: dict[str, object] = {}

    title = _clean_text(metadata.get("title"), limit=300)
    if allow_title_update and title and title != prop.title:
        updates["title"] = title

    address = _clean_text(metadata.get("address"), limit=500)
    if address and address != prop.address:
        updates["address"] = address

    guest_capacity = _clean_guest_capacity(metadata.get("guest_capacity"))
    if guest_capacity and guest_capacity != prop.guest_capacity:
        updates["guest_capacity"] = guest_capacity

    image_url = _clean_text(metadata.get("image_url"), limit=2000)
    if image_url:
        preview_path = await _cache_preview_image(property_id, image_url)
        if preview_path and preview_path != prop.preview_path:
            updates["preview_path"] = preview_path

    if not updates:
        return prop

    return await PropertyRepository.update(property_id, **updates)
