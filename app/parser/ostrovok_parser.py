"""
Ostrovok.ru parser.

Стратегии (по приоритету):
1. Playwright + XHR-перехват /hotel/search → самая надёжная.
   page.goto с wait_until="commit" (резолвится сразу как сервер начал отвечать).
   XHR с ценами приходит через 5–20 сек пока страница грузится в фоне.
   Если page.goto таймаутит — проверяем, есть ли уже XHR-цены и возвращаем их.
2. httpx — резерв без браузера (пробует с прокси и без).
"""
from __future__ import annotations

import html as html_lib
import re
import json
import asyncio
import random
import uuid
from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin, urlparse, parse_qs, quote

import httpx
from loguru import logger

from app.parser.base_parser import BaseParser, BlockedError, CaptchaError
from app.utils.config import PARSER_USER_AGENTS

_PW_NAV_TIMEOUT = 20_000   # ms: "commit" резолвится быстро, это safety-net
_XHR_WAIT       = 12       # секунд ждём XHR (если нет — fallback на DOM/HTML)
_MAP_BUTTON_LABELS = ("Показать на карте", "Show on map")
_ADDRESS_SECTION_TITLES = ("Расположение", "Location")
_ADDRESS_STOP_LABELS = (
    "Что есть рядом",
    "What's nearby",
    "Достопримечательности",
    "Places of interest",
    "Популярные удобства",
    "Popular amenities",
    "Доступные номера",
    "Available rooms",
    "Посмотреть цены",
    "See prices",
)
_BAD_IMAGE_TOKENS = (
    "logo",
    "icon",
    "avatar",
    "appstore",
    "googleplay",
    "google-play",
    "playstore",
    "huawei",
    "favicon",
    "sprite",
    "placeholder",
)

# "N-местный" → guest count (Ostrovok room names)
_ROOM_NAME_CAPACITY = {
    "одноместн":  1,
    "двухместн":  2,
    "трехместн":  3,
    "трёхместн":  3,
    "четырехместн": 4,
    "четырёхместн": 4,
    "пятиместн":  5,
    "шестиместн": 6,
    "семиместн":  7,
    "восьмиместн": 8,
    "девятиместн": 9,
    "десятиместн": 10,
}


class OstrovokParser(BaseParser):

    async def _fetch_metadata_once(self, url: str) -> Dict[str, Any]:
        """
        Метаданные Ostrovok — server-rendered, поэтому достаём через httpx:
          • JSON-LD <script type="application/ld+json"> @type:Hotel  → name, address, photo
          • __NEXT_DATA__ props.pageProps.hotel.roomGroups[]         → guest_capacity
          • <meta property="og:image">                               → fallback изображение
          • <h1>                                                     → fallback название

        ~2 сек против ~20 сек у Playwright, без таймаутов SPA.
        Playwright оставляем как fallback на случай блокировок/CDN-челленджей.
        """
        clean_url = url.split("?")[0]
        html = await self._httpx_fetch_html(clean_url)
        if html and not self._detect_block(html):
            metadata = self._extract_metadata_from_html(html, clean_url)
            if metadata.get("title") or metadata.get("address") or metadata.get("image_url"):
                logger.info(
                    f"OstrovokParser metadata (httpx): "
                    f"title={bool(metadata.get('title'))} "
                    f"addr={bool(metadata.get('address'))} "
                    f"img={bool(metadata.get('image_url'))} "
                    f"guests={metadata.get('guest_capacity')}"
                )
                return metadata
            logger.warning("OstrovokParser metadata (httpx): empty result, fallback Playwright")

        return await self._playwright_metadata(clean_url)

    async def _httpx_fetch_html(self, url: str) -> Optional[str]:
        """Тянем HTML с системным прокси (если есть), иначе напрямую."""
        for proxy in [self._proxy, None] if self._proxy else [None]:
            try:
                kwargs: dict = {
                    "timeout": httpx.Timeout(15.0, connect=5.0),
                    "follow_redirects": True,
                    "trust_env": False,
                }
                if proxy:
                    kwargs["proxy"] = proxy
                async with httpx.AsyncClient(**kwargs) as client:
                    resp = await client.get(url, headers=self._headers())
                if resp.status_code == 200 and resp.text and len(resp.text) > 5000:
                    return resp.text
                logger.debug(
                    f"OstrovokParser httpx metadata (proxy={bool(proxy)}): "
                    f"status={resp.status_code} len={len(resp.text or '')}"
                )
            except Exception as e:
                logger.debug(
                    f"OstrovokParser httpx metadata (proxy={bool(proxy)}) error: {e}"
                )
        return None

    def _extract_metadata_from_html(self, html: str, url: str) -> Dict[str, Any]:
        """Достаём всё из server-rendered HTML — без браузера."""
        next_data = self._parse_next_data(html)
        hotel = self._next_data_hotel(next_data)
        jsonld_hotel = self._jsonld_hotel(html)

        title = self._meta_title(hotel, jsonld_hotel, html)
        address = self._meta_address(hotel, jsonld_hotel)
        image_url = self._meta_image(hotel, jsonld_hotel, html, url)
        guest_capacity = self._meta_guest_capacity(hotel, html)

        return {
            "title":          title,
            "image_url":      image_url,
            "address":        address,
            "guest_capacity": guest_capacity,
        }

    def _parse_next_data(self, html: str) -> Optional[dict]:
        m = re.search(
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
            html,
            re.S,
        )
        if not m:
            return None
        try:
            return json.loads(m.group(1))
        except Exception:
            return None

    def _next_data_hotel(self, next_data: Optional[dict]) -> Optional[dict]:
        if not isinstance(next_data, dict):
            return None
        try:
            hotel = next_data["props"]["pageProps"]["hotel"]
        except (KeyError, TypeError):
            return None
        return hotel if isinstance(hotel, dict) else None

    def _jsonld_hotel(self, html: str) -> Optional[dict]:
        for obj in self._jsonld_objects(html):
            if isinstance(obj, dict) and obj.get("@type") == "Hotel":
                return obj
        return None

    def _meta_title(
        self, hotel: Optional[dict], jsonld: Optional[dict], html: str,
    ) -> Optional[str]:
        if hotel:
            name = hotel.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()[:300]
        if jsonld:
            name = jsonld.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()[:300]
        return self._html_title(html)

    def _meta_address(
        self, hotel: Optional[dict], jsonld: Optional[dict],
    ) -> Optional[str]:
        # JSON-LD streetAddress — самый надёжный источник
        if jsonld:
            addr = jsonld.get("address")
            if isinstance(addr, dict):
                street = addr.get("streetAddress")
                if isinstance(street, str) and street.strip():
                    return " ".join(street.split())[:500]

        # __NEXT_DATA__ location.address
        if hotel:
            loc = hotel.get("location")
            if isinstance(loc, dict):
                for key in ("address", "fullAddress", "displayAddress"):
                    val = loc.get(key)
                    if isinstance(val, str) and val.strip():
                        return " ".join(val.split())[:500]

        return None

    def _meta_image(
        self,
        hotel: Optional[dict],
        jsonld: Optional[dict],
        html: str,
        url: str,
    ) -> Optional[str]:
        candidates: List[str] = []

        if jsonld:
            photo = jsonld.get("photo")
            if isinstance(photo, str):
                candidates.append(photo)
            images = jsonld.get("image")
            if isinstance(images, list):
                for img in images:
                    if isinstance(img, str):
                        candidates.append(img)
                    elif isinstance(img, dict) and isinstance(img.get("url"), str):
                        candidates.append(img["url"])

        if hotel:
            for key in ("images", "photos"):
                images = hotel.get(key)
                if not isinstance(images, list):
                    continue
                for img in images[:10]:
                    if isinstance(img, str):
                        candidates.append(img)
                    elif isinstance(img, dict):
                        for url_key in ("src", "url", "originalUrl", "previewUrl"):
                            v = img.get(url_key)
                            if isinstance(v, str):
                                candidates.append(v)

        # og:image / twitter:image
        for pattern in (
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        ):
            for m in re.finditer(pattern, html, re.IGNORECASE):
                candidates.append(m.group(1))

        # CDN URLs из самого HTML — на случай если ничего выше не сработало
        for m in re.finditer(
            r'(?:https?:)?//cdn\.worldota\.net/[^"\'>\s]+',
            html,
            re.IGNORECASE,
        ):
            candidates.append(m.group(0))

        return self._pick_ostrovok_image(candidates, url)

    def _meta_guest_capacity(
        self, hotel: Optional[dict], html: str,
    ) -> Optional[int]:
        capacities: List[int] = []

        if hotel:
            # roomGroups[].nameStruct.mainName: "Двухместный ...", "Четырёхместный ..."
            for room in (hotel.get("roomGroups") or []):
                if not isinstance(room, dict):
                    continue
                ns = room.get("nameStruct")
                name = ns.get("mainName") if isinstance(ns, dict) else None
                if not isinstance(name, str):
                    continue
                cap = self._capacity_from_room_name(name)
                if cap:
                    capacities.append(cap)

            # apartmentsInfo.capacity (часто = 0, но на всякий случай)
            ai = hotel.get("apartmentsInfo")
            if isinstance(ai, dict):
                cap_val = ai.get("capacity")
                if isinstance(cap_val, (int, float)) and 1 <= cap_val <= 30:
                    capacities.append(int(cap_val))

        # HTML-уровневый fallback (для случаев когда __NEXT_DATA__ не пришёл)
        if not capacities:
            html_cap = self._extract_ostrovok_guest_capacity_from_html(html)
            if html_cap:
                capacities.append(html_cap)

        return max(capacities) if capacities else None

    @staticmethod
    def _capacity_from_room_name(name: str) -> Optional[int]:
        lower = name.lower()
        for token, value in _ROOM_NAME_CAPACITY.items():
            if token in lower:
                return value
        return None

    async def _playwright_metadata(self, url: str) -> Dict[str, Any]:
        """Fallback на случай если httpx был заблокирован/CDN-челлендж."""
        context = await self._new_context()
        page = await context.new_page()
        try:
            try:
                response = await page.goto(
                    url,
                    timeout=_PW_NAV_TIMEOUT,
                    wait_until="domcontentloaded",
                )
                if response and response.status in (403, 429, 503):
                    raise BlockedError(f"HTTP {response.status}")
            except BlockedError:
                raise
            except Exception as exc:
                logger.debug(f"OstrovokParser playwright nav: {exc}")

            try:
                await page.wait_for_load_state("networkidle", timeout=6_000)
            except Exception:
                pass
            await self._human_delay(0.3, 0.6)

            try:
                html = await page.content()
            except Exception:
                html = ""

            if html and self._detect_block(html):
                raise BlockedError("Blocked")

            metadata = self._extract_metadata_from_html(html, url) if html else {}

            # Доп. шанс: если streetAddress не вытащился — берём из DOM
            if not metadata.get("address"):
                metadata["address"] = self._extract_ostrovok_address_from_html(html or "")
            logger.info(
                f"OstrovokParser metadata (playwright fallback): "
                f"title={bool(metadata.get('title'))} "
                f"img={bool(metadata.get('image_url'))}"
            )
            return metadata
        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass

    def _extract_ostrovok_address_from_html(self, html: str) -> Optional[str]:
        if not html:
            return None

        patterns = (
            r"(?:Показать на карте|Show on map)\s*</[^>]+>\s*<[^>]+>\s*([^<]{5,180})<",
            r"(?:Расположение|Location)</[^>]+>\s*<[^>]+>\s*([^<]{5,180})<",
            r"([^<>]{5,180}?)(?:Показать на карте|Show on map)",
        )
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE | re.S)
            if not match:
                continue
            candidate = self._clean_ostrovok_address(match.group(1))
            if candidate:
                return candidate

        return None

    def _clean_ostrovok_address(self, value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None

        text = html_lib.unescape(value).replace("\\/", "/")
        text = re.sub(r"\[(?:Button|Кнопка):\s*", " ", text, flags=re.IGNORECASE)
        text = text.replace("]", " ")
        for label in _MAP_BUTTON_LABELS:
            text = re.sub(re.escape(label), " ", text, flags=re.IGNORECASE)

        text = re.sub(
            r"\b\d+[,.]?\d*\s*(?:м|км|m|km)\s+(?:от центра|from the city center).*$",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = " ".join(text.split()).strip(" ,;|-")

        if not text or len(text) < 5 or len(text) > 180:
            return None
        if not self._looks_like_ostrovok_address(text):
            return None
        return text[:500]

    def _looks_like_ostrovok_address(self, text: str) -> bool:
        lower = text.lower()

        if any(label.lower() in lower for label in _MAP_BUTTON_LABELS):
            return False
        if any(label.lower() in lower for label in _ADDRESS_STOP_LABELS):
            return False
        if lower in {label.lower() for label in _ADDRESS_SECTION_TITLES}:
            return False
        if "от центра" in lower or "from the city center" in lower:
            return False
        if any(
            bad in lower for bad in (
                "канатной дороги",
                "горнолыжного подъёмника",
                "горнолыжного подъемника",
                "ski lift",
                "что вокруг",
            )
        ):
            return False

        has_number = bool(re.search(r"\d", text))
        has_hint = bool(re.search(
            r"(?:\bул\.?\b|улиц|ulitsa|street|str\.|road\b|проспект|пер\.?|переулок|проезд|набереж|шоссе|бульвар|дом\b|д\.\s*\d|километр|kilometer)",
            lower,
            re.IGNORECASE,
        ))
        has_comma = "," in text
        has_city = any(city in lower for city in ("терскол", "terskol"))
        return (has_number and (has_hint or has_comma)) or (has_city and has_number and has_comma)

    def _extract_ostrovok_guest_capacity_from_html(self, html: str) -> Optional[int]:
        if not html:
            return None

        candidates: List[int] = []
        patterns = (
            r'"(?:max_guests|maxGuests|max_persons|maxPersons|max_occupancy|maxOccupancy|guest_capacity|guestCapacity|room_capacity|roomCapacity|placement_capacity|placementCapacity)"\s*:\s*"?(\d{1,2})"?',
            r"(?:до|для|на|максимум|макс\.?|вмещает(?:\s+до)?|up to|for)\s*(\d{1,2})\s*(?:гост[а-я]*|guests?|persons?)",
            r"(\d{1,2})\s*[- ]?\s*(?:местн(?:ый|ая|ое|ые)?|гост[а-я]*|guests?|persons?)",
        )
        for pattern in patterns:
            for match in re.finditer(pattern, html, re.IGNORECASE):
                value = int(match.group(1))
                if 1 <= value <= 30:
                    candidates.append(value)

        return max(candidates) if candidates else None

    def _pick_ostrovok_image(self, candidates: List[str], page_url: str) -> Optional[str]:
        scored: List[tuple[int, str]] = []

        for raw in candidates:
            candidate = self._normalize_ostrovok_image_url(raw, page_url)
            if not candidate:
                continue

            lower = candidate.lower()
            if any(token in lower for token in _BAD_IMAGE_TOKENS):
                continue

            score = 0
            if "cdn.worldota.net" in lower:
                score += 50
            if "/content/" in lower:
                score += 20
            if any(size in lower for size in ("640x400", "1024x768", "1200x800", "1280x720")):
                score += 10
            if lower.endswith((".jpg", ".jpeg", ".png", ".webp")):
                score += 5

            scored.append((score, candidate))

        if not scored:
            return None

        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1]

    def _normalize_ostrovok_image_url(self, value: Any, page_url: str) -> Optional[str]:
        if not isinstance(value, str):
            return None

        candidate = html_lib.unescape(value).replace("\\/", "/").strip()
        if not candidate:
            return None

        candidate = candidate.split(",")[0].strip().split()[0]
        if candidate.startswith("//"):
            candidate = f"https:{candidate}"
        candidate = self._absolute_url(candidate, page_url)

        if not candidate.startswith(("http://", "https://")):
            return None
        return candidate

    def _absolute_url(self, candidate: str, page_url: str) -> str:
        if candidate.startswith(("http://", "https://")):
            return candidate
        return urljoin(page_url, candidate)

    # ── Amenities (раздел «Сравнение») ──────────────────────────
    # Без браузера — httpx + парсинг __NEXT_DATA__. Если httpx
    # заблокирован — fallback на Playwright. Не влияет на цены: вызывается
    # только из comparison-флоу по явному действию пользователя.

    async def _fetch_amenities_once(self, url: str) -> Dict[str, Any]:
        clean_url = url.split("?")[0]
        html = await self._httpx_fetch_html(clean_url)
        if not html or self._detect_block(html):
            html = await self._playwright_fetch_html_for_amenities(clean_url)
        if not html:
            return {"amenities": {}, "description": None, "key_facts": []}

        next_data = self._parse_next_data(html)
        hotel = self._next_data_hotel(next_data)

        groups = self._ostrovok_amenities(hotel) if hotel else {}
        description = self._ostrovok_description(hotel) if hotel else None
        key_facts = self._ostrovok_key_facts(hotel) if hotel else []

        return {
            "amenities":   groups,
            "description": description,
            "key_facts":   key_facts,
        }

    async def _playwright_fetch_html_for_amenities(self, url: str) -> Optional[str]:
        try:
            context = await self._new_context()
            page = await context.new_page()
            try:
                try:
                    await page.goto(url, timeout=_PW_NAV_TIMEOUT,
                                    wait_until="domcontentloaded")
                except Exception:
                    pass
                try:
                    await page.wait_for_load_state("networkidle", timeout=6_000)
                except Exception:
                    pass
                try:
                    return await page.content()
                except Exception:
                    return None
            finally:
                try: await page.close()
                except Exception: pass
                try: await context.close()
                except Exception: pass
        except Exception as e:
            logger.debug(f"OstrovokParser amenities playwright fallback error: {e}")
            return None

    @staticmethod
    def _ostrovok_amenities(hotel: dict) -> Dict[str, List[str]]:
        """
        Защитно ищем удобства по нескольким возможным путям в __NEXT_DATA__.
        Структура Ostrovok меняется со временем, поэтому пробуем list-альтернатив.
        """
        groups: Dict[str, List[str]] = {}

        # Кандидаты-ключи, под которыми может лежать список групп удобств
        for path_key in (
            "amenityGroups", "amenity_groups",
            "facilityGroups", "facility_groups",
            "amenitiesGroups", "amenities_groups",
            "serviceGroups",   "service_groups",
        ):
            value = hotel.get(path_key)
            if isinstance(value, list):
                OstrovokParser._merge_amenity_groups(value, groups)

        # Плоский список — иногда удобства идут одним массивом
        if not groups:
            for flat_key in ("amenities", "facilities", "services"):
                value = hotel.get(flat_key)
                if isinstance(value, list):
                    items = OstrovokParser._extract_amenity_items(value)
                    if items:
                        groups["Удобства"] = items

        return groups

    @staticmethod
    def _merge_amenity_groups(raw_groups: list, out: Dict[str, List[str]]) -> None:
        for grp in raw_groups:
            if not isinstance(grp, dict):
                continue
            name = (
                grp.get("name") or grp.get("title")
                or grp.get("label") or grp.get("groupName")
                or "Удобства"
            )
            raw_items = (
                grp.get("items") or grp.get("amenities")
                or grp.get("facilities") or grp.get("list")
                or grp.get("values") or []
            )
            items = OstrovokParser._extract_amenity_items(raw_items)
            if not items:
                continue
            key = str(name).strip()[:80] or "Удобства"
            existing = out.setdefault(key, [])
            for it in items:
                if it not in existing:
                    existing.append(it)

    @staticmethod
    def _extract_amenity_items(raw: list) -> List[str]:
        items: List[str] = []
        for it in raw:
            if isinstance(it, str):
                txt = it.strip()
                if txt:
                    items.append(txt[:120])
            elif isinstance(it, dict):
                for key in ("name", "title", "label", "value", "text"):
                    v = it.get(key)
                    if isinstance(v, str) and v.strip():
                        items.append(v.strip()[:120])
                        break
        return items

    @staticmethod
    def _ostrovok_description(hotel: dict) -> Optional[str]:
        """
        "Об апартаментах" — несколько возможных путей. Берём первый
        непустой результат, склеиваем абзацы если описание разбито.
        """
        # Сначала пробуем единичные ключи
        for key in (
            "description", "summary", "shortDescription",
            "fullDescription", "aboutHotel", "about",
        ):
            value = hotel.get(key)
            text = OstrovokParser._description_to_text(value)
            if text:
                return text[:5000]

        # Возможен список абзацев
        for key in ("descriptionStruct", "description_struct", "descriptions"):
            value = hotel.get(key)
            if isinstance(value, list):
                parts: List[str] = []
                for item in value:
                    text = OstrovokParser._description_to_text(item)
                    if text:
                        parts.append(text)
                if parts:
                    return "\n\n".join(parts)[:5000]
            elif isinstance(value, dict):
                text = OstrovokParser._description_to_text(value)
                if text:
                    return text[:5000]
        return None

    @staticmethod
    def _description_to_text(value: Any) -> Optional[str]:
        if isinstance(value, str):
            text = " ".join(value.split())
            return text or None
        if isinstance(value, dict):
            title_str = ""
            t = value.get("title")
            if isinstance(t, str) and t.strip():
                title_str = t.strip() + ": "

            # 1. Прямые text-поля
            for sub in ("text", "value", "content", "html",
                        "description", "paragraph"):
                v = value.get(sub)
                if isinstance(v, str):
                    text = " ".join(v.split())
                    if text:
                        return f"{title_str}{text}"

            # 2. Реальная схема Ostrovok: { title, paragraphs: [str, str, ...] }
            paragraphs = value.get("paragraphs")
            if isinstance(paragraphs, list):
                parts: List[str] = []
                for p in paragraphs:
                    if isinstance(p, str):
                        ptext = " ".join(p.split())
                        if ptext:
                            parts.append(ptext)
                    elif isinstance(p, dict):
                        sub = OstrovokParser._description_to_text(p)
                        if sub:
                            parts.append(sub)
                if parts:
                    return f"{title_str}{' '.join(parts)}"
        return None

    @staticmethod
    def _ostrovok_key_facts(hotel: dict) -> List[str]:
        """
        Короткие факты «Об апартаментах»: «До 6 гостей», «2 комнаты», «55 кв.м»,
        «7 этаж», «Бесконтактное заселение» и т.п.

        Извлекаются из реальной схемы Ostrovok (__NEXT_DATA__.props.pageProps.hotel):
          • apartmentsInfo.{capacity, bedroomsQuantity, space, floor, bedCount*}
          • roomGroups[].size  (fallback для площади)
          • roomGroups[].nameStruct.mainName  (fallback для гостей по «N-местный»)
          • facts.floorsNumber, facts.yearBuilt
          • isContactless

        Никаких новых сетевых запросов: данные берутся из уже скачанного HTML.
        """
        facts: List[str] = []

        ai      = hotel.get("apartmentsInfo") or {}
        rgs     = hotel.get("roomGroups") or []
        f_dict  = hotel.get("facts") or {}

        # ── Гости ────────────────────────────────────────────────
        capacity = OstrovokParser._coerce_positive_int(ai.get("capacity"))
        if not capacity:
            # Fallback на roomGroups[].nameStruct.mainName ("N-местный...")
            for r in rgs:
                if not isinstance(r, dict):
                    continue
                ns = r.get("nameStruct") or {}
                name = ns.get("mainName") if isinstance(ns, dict) else None
                if isinstance(name, str):
                    cap = OstrovokParser._capacity_from_room_name(name)
                    if cap:
                        capacity = max(capacity or 0, cap)
        if capacity:
            facts.append(f"До {capacity} гостей")

        # ── Комнаты ──────────────────────────────────────────────
        rooms = OstrovokParser._coerce_positive_int(ai.get("bedroomsQuantity"))
        if not rooms:
            rooms = OstrovokParser._coerce_positive_int(f_dict.get("roomsNumber"))
        if rooms:
            if rooms == 1:
                facts.append("1 комната")
            elif 2 <= rooms <= 4:
                facts.append(f"{rooms} комнаты")
            else:
                facts.append(f"{rooms} комнат")

        # ── Площадь ──────────────────────────────────────────────
        space = OstrovokParser._coerce_positive_int(ai.get("space"))
        if not space:
            # Fallback на размер первой комнаты
            for r in rgs:
                if not isinstance(r, dict):
                    continue
                size = OstrovokParser._coerce_positive_int(r.get("size"))
                if size:
                    space = size
                    break
        if space:
            facts.append(f"{space} кв.м")

        # ── Этаж конкретного объекта ────────────────────────────
        floor = OstrovokParser._coerce_positive_int(ai.get("floor"))
        if floor:
            facts.append(f"{floor} этаж")
        else:
            # Этажность здания (если этажа объекта нет)
            floors_total = OstrovokParser._coerce_positive_int(
                f_dict.get("floorsNumber")
            )
            if floors_total:
                facts.append(f"Этажей: {floors_total}")

        # ── Бесконтактное заселение ──────────────────────────────
        if hotel.get("isContactless") is True:
            facts.append("Бесконтактное заселение")

        # ── Кровати (если есть) ──────────────────────────────────
        bed_total = 0
        for k in ("bedCountDouble", "bedCountSingle", "bedCountKing",
                  "bedCountQueen", "bedCountBunk", "bedCountSofa",
                  "bedCountChair"):
            n = OstrovokParser._coerce_positive_int(ai.get(k))
            if n:
                bed_total += n
        if bed_total:
            if bed_total == 1:
                facts.append("1 кровать")
            elif 2 <= bed_total <= 4:
                facts.append(f"{bed_total} кровати")
            else:
                facts.append(f"{bed_total} кроватей")

        # ── Год постройки ────────────────────────────────────────
        year = OstrovokParser._coerce_positive_int(f_dict.get("yearBuilt"))
        if year and 1800 <= year <= 2100:
            facts.append(f"Построен в {year}")

        # Уникализация
        seen: set = set()
        unique: List[str] = []
        for f in facts:
            text = f.strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            unique.append(text[:100])
        return unique[:12]

    @staticmethod
    def _coerce_positive_int(v: Any) -> int:
        """Принимает int/float/str → возвращает int>0 или 0.
        В Ostrovok JSON часть полей приходят как пустая строка вместо null,
        часть — как строки с числом. Унифицируем."""
        if isinstance(v, bool):
            return 0
        if isinstance(v, (int, float)):
            n = int(v)
            return n if n > 0 else 0
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return 0
            try:
                n = int(float(s))
                return n if n > 0 else 0
            except (ValueError, TypeError):
                return 0
        return 0

    # ── Entry point ──────────────────────────────────────────────

    async def _fetch_once(self, url: str) -> Dict[str, Any]:
        hotel_id          = self._extract_hotel_id(url)
        hotel_slug        = self._extract_slug(url)
        checkin, checkout = self._extract_dates(url)
        fetch_url         = self._normalize_url(url, checkin, checkout)
        logger.info(
            f"OstrovokParser: hotel_id={hotel_id} slug={hotel_slug} "
            f"url={fetch_url[:90]}"
        )

        # ── Прямой API /hotel/search/v1/site/hp/search ───────────
        # Быстрый путь без браузера. Авторитетно ловит rates=null
        # (max-stay превышен) и не тратит 12–22 сек на ожидание XHR.
        if hotel_slug and checkin and checkout:
            try:
                nights = (
                    date.fromisoformat(checkout) - date.fromisoformat(checkin)
                ).days
            except Exception:
                nights = 0
            if nights > 0:
                try:
                    async with httpx.AsyncClient(
                        timeout=httpx.Timeout(8.0, connect=4.0),
                        trust_env=False,
                        headers=self._headers(),
                        proxy=self._proxy if getattr(self, "_proxy", None) else None,
                    ) as client:
                        res = await self._api_search_direct(
                            client, hotel_slug, checkin, checkout, nights,
                        )
                    st = res.get("status")
                    if st == "ok":
                        price = min(res["prices"])
                        logger.info(f"OstrovokParser API direct: price={price}")
                        return {
                            "price":       price,
                            "title":       None,
                            "external_id": hotel_id,
                            "status":      "ok",
                            "error":       None,
                        }
                    if st == "sold_out":
                        logger.info("OstrovokParser API direct: sold_out/max_stay")
                        return {
                            "price":       None,
                            "title":       None,
                            "external_id": hotel_id,
                            "status":      "not_found",
                            "error":       "Нет доступных предложений на выбранные даты",
                        }
                    logger.debug(
                        f"OstrovokParser API direct: {res.get('error')} — "
                        "fallback Playwright"
                    )
                except Exception as e:
                    logger.debug(
                        f"OstrovokParser API direct exception: {e} — fallback Playwright"
                    )

        # Playwright — XHR перехват (fallback при ошибке API)
        result = None
        try:
            result = await self._playwright_strategy(fetch_url, hotel_id)
        except (BlockedError, CaptchaError):
            raise
        except Exception as e:
            logger.warning(f"OstrovokParser: playwright failed ({e.__class__.__name__}): {e}")

        if result and result.get("price"):
            return result

        # httpx — резерв (без браузера, пробует с прокси и без)
        result2 = await self._httpx_strategy(fetch_url, hotel_id)
        if result2 and result2.get("price"):
            return result2

        if result and not result.get("price") and result.get("status") in {
            "not_found",
            "blocked",
            "captcha",
        }:
            status = str(result.get("status") or "not_found")
            default_error = {
                "not_found": "Нет доступных предложений на выбранные даты",
                "blocked":   "Доступ к сайту временно заблокирован",
                "captcha":   "Требуется прохождение капчи",
            }.get(status, "Нет доступных предложений на выбранные даты")
            return {
                "price":       None,
                "title":       result.get("title"),
                "external_id": hotel_id,
                "status":      status,
                "error":       result.get("error") or default_error,
            }

        best = result or result2 or {}
        return {
            "price":       None,
            "title":       best.get("title"),
            "external_id": hotel_id,
            "status":      "not_found",
            "error":       "Нет доступных предложений на выбранные даты",
        }

    # ── Playwright strategy ──────────────────────────────────────

    async def _playwright_strategy(self, url: str, hotel_id: Optional[str]) -> Optional[Dict]:
        # Определяем кол-во запрошенных ночей для фильтрации rates в XHR
        _ci, _co = self._extract_dates(url)
        _nights = 0
        if _ci and _co:
            try:
                _nights = (date.fromisoformat(_co) - date.fromisoformat(_ci)).days
                if _nights < 0:
                    _nights = 0
            except Exception:
                pass

        context = await self._new_context()
        page    = await context.new_page()

        xhr_prices: List[float]  = []
        xhr_event                = asyncio.Event()  # fires on any definitive XHR answer
        xhr_no_avail             = False            # True = XHR came, rates empty = занято

        async def on_response(response):
            nonlocal xhr_no_avail
            try:
                if response.status != 200:
                    return
                rurl = response.url
                if not any(ep in rurl for ep in [
                    "/hotel/search/v1/site/hp/search",
                    "/hotel/search/v2/site/hp/rates",
                    "/hotel/search/v1/site/hp/rates",
                ]):
                    return
                if "json" not in response.headers.get("content-type", ""):
                    return
                data   = await response.json()
                prices = self._prices_from_xhr(data, _nights)
                if prices:
                    xhr_prices.extend(prices)
                    xhr_event.set()
                    logger.debug(f"OstrovokParser XHR prices={prices} from {rurl[:70]}")
                elif "rates" in data:
                    # XHR пришёл с полем rates, но извлекаемых цен нет.
                    # Два подслучая, оба = "нет предложений на эти даты":
                    #   rates=None/[]            — объект занят/непродажа на эти даты
                    #   rates=[{...}, ...]       — max-stay превышен, либо все тарифы без цен
                    rates_field = data.get("rates")
                    rates_len = len(rates_field) if isinstance(rates_field, list) else 0
                    xhr_no_avail = True
                    xhr_event.set()
                    logger.debug(
                        f"OstrovokParser XHR: rates_len={rates_len}, no extractable prices "
                        f"→ no availability ({rurl[:70]})"
                    )
            except Exception as e:
                logger.debug(f"OstrovokParser XHR handler error: {e}")

        page.on("response", on_response)

        nav_ok = False
        html   = ""

        try:
            # "commit" = резолвится как только сервер начал слать ответ (~1-2 сек).
            # Страница продолжает грузиться в фоне, JS файрит XHR — on_response работает.
            try:
                resp = await page.goto(url, timeout=_PW_NAV_TIMEOUT, wait_until="commit")
                if resp and resp.status in (403, 429, 503):
                    raise BlockedError(f"HTTP {resp.status}")
                nav_ok = True
            except BlockedError:
                raise
            except Exception as nav_err:
                # Даже если goto таймаутит/падает — XHR мог уже прийти.
                # Ждём ещё немного на случай если XHR в пути.
                if not xhr_event.is_set():
                    try:
                        await asyncio.wait_for(xhr_event.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass
                if xhr_prices:
                    logger.info(
                        f"OstrovokParser: nav error [{nav_err.__class__.__name__}] "
                        f"but {len(xhr_prices)} XHR prices already captured — returning OK"
                    )
                    # Возвращаем XHR цены, навигация не нужна
                    return {
                        "price":       min(xhr_prices),
                        "title":       None,
                        "external_id": hotel_id,
                        "status":      "ok",
                        "error":       None,
                    }
                raise  # XHR тоже нет — пробрасываем ошибку

            # Навигация прошла — ждём XHR (JS грузит данные после mount)
            if not xhr_event.is_set():
                try:
                    await asyncio.wait_for(xhr_event.wait(), timeout=_XHR_WAIT)
                except asyncio.TimeoutError:
                    logger.debug("OstrovokParser: XHR wait timeout — trying DOM/HTML fallback")

            # Быстрый выход: XHR пришёл, но rates пустые → объект занят/нет предложений
            if xhr_no_avail and not xhr_prices:
                return {
                    "price":       None,
                    "title":       None,
                    "external_id": hotel_id,
                    "status":      "not_found",
                    "error":       "Нет доступных предложений на выбранные даты",
                }

            # Лёгкий скролл для lazy-load (только если XHR не ответил)
            if not xhr_prices:
                try:
                    await page.evaluate("window.scrollBy(0, 600)")
                    await asyncio.sleep(0.8)
                except Exception:
                    pass

            try:
                html = await page.content()
            except Exception:
                html = ""

            if len(html) < 3000 and not xhr_prices:
                raise BlockedError("HTML слишком короткий")

            # ── Экстракция цены ──────────────────────────────────
            price = min(xhr_prices) if xhr_prices else None
            logger.debug(f"OstrovokParser XHR collected: {xhr_prices}")

            if not price and html:
                price = await self._dom_rub_price(page)
                logger.debug(f"OstrovokParser DOM price: {price}")

            if not price and html:
                price = self._next_data_price(html)
                logger.debug(f"OstrovokParser __NEXT_DATA__ price: {price}")

            if not price and html:
                price = self._strict_json_price(html)
                logger.debug(f"OstrovokParser strict-json price: {price}")

            title = None
            try:
                title = await self._dom_title(page)
                if not title and html:
                    title = self._html_title(html)
            except Exception:
                if html:
                    title = self._html_title(html)

            return {
                "price":       price,
                "title":       title,
                "external_id": hotel_id,
                "status":      "ok" if price else "not_found",
                "error":       None if price else "Цена не найдена",
            }

        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass
            # Браузер НЕ закрываем — singleton dispatcher переиспользует его

    # ── httpx strategy ───────────────────────────────────────────

    async def _httpx_strategy(self, url: str, hotel_id: Optional[str]) -> Optional[Dict]:
        """Пробует запрос с прокси, потом без — работает при любом типе соединения."""
        proxies_to_try = [self._proxy, None] if self._proxy else [None]

        for proxy in proxies_to_try:
            try:
                kwargs: dict = {"timeout": 18, "follow_redirects": True, "trust_env": False}
                if proxy:
                    kwargs["proxy"] = proxy

                async with httpx.AsyncClient(**kwargs) as client:
                    await asyncio.sleep(random.uniform(0.2, 0.6))
                    resp = await client.get(url, headers=self._headers())

                if resp.status_code not in (200, 201):
                    logger.debug(f"OstrovokParser httpx (proxy={bool(proxy)}): HTTP {resp.status_code}")
                    continue

                html = resp.text
                if len(html) < 3000 or self._detect_block(html):
                    continue

                price = self._next_data_price(html) or self._strict_json_price(html)
                title = self._html_title(html)
                label = f"proxy={proxy}" if proxy else "direct"
                logger.debug(f"OstrovokParser httpx ({label}): price={price}")

                if price:
                    return {
                        "price":       price,
                        "title":       title,
                        "external_id": hotel_id,
                        "status":      "ok",
                        "error":       None,
                    }
                # Нет цены — пробуем без прокси
                continue

            except Exception as e:
                logger.debug(f"OstrovokParser httpx (proxy={bool(proxy)}) error: {e}")
                continue

        return None

    # ── Direct API (no browser) ──────────────────────────────────
    #
    # Ostrovok отдаёт цены через /hotel/search/v1/site/hp/search с
    # query-параметром ?body={JSON}. Эндпоинт отвечает JSON-ом даже без
    # cookies/CSRF/Cloudflare-токенов, поэтому его можно дергать чистым
    # httpx — на порядок быстрее браузерной навигации и сразу даёт
    # однозначный сигнал «rates=null → max-stay превышен».
    #
    # Используется:
    #   • в _fetch_once как первичная стратегия (до Playwright);
    #   • в deep_analysis._api_phase как массовый пул запросов.

    def _extract_slug(self, url: str) -> Optional[str]:
        """Slug из URL вида /hotel/russia/<region>/mid<id>/<slug>/ — это
        поле `hotel` в API-запросе (ota_hotel_id)."""
        m = re.search(r'/mid\d+/([^/?#]+)', url)
        return m.group(1) if m else None

    def _build_api_search_url(
        self, slug: str, checkin: str, checkout: str, adults: int = 2,
    ) -> str:
        """
        Прямой URL /hotel/search/v1/site/hp/search.
        Ostrovok ожидает browser-like body c arrival/departure_date и paxes.
        """
        body = {
            "arrival_date":   checkin,
            "departure_date": checkout,
            "hotel":          slug,
            "currency":       "RUB",
            "lang":           "ru",
            "paxes":          [{"adults": adults}],
            "search_uuid":    str(uuid.uuid4()),
        }
        body_json = json.dumps(body, separators=(",", ":"))
        return (
            "https://ostrovok.ru/hotel/search/v1/site/hp/search"
            f"?body={quote(body_json)}"
        )

    async def _api_search_direct(
        self,
        client: httpx.AsyncClient,
        slug: str,
        checkin: str,
        checkout: str,
        nights: int = 0,
    ) -> Dict[str, Any]:
        """
        Возвращает:
          {"status":"ok",       "prices":[...], "data": dict}
          {"status":"sold_out", "prices":[],    "data": dict}  # rates=null/[] (max-stay / непродажа)
          {"status":"error",    "error":<msg>}                 # network/5xx/HTML
        """
        url = self._build_api_search_url(slug, checkin, checkout)
        try:
            resp = await client.get(url)
        except Exception as e:
            return {"status": "error", "error": f"net:{e.__class__.__name__}"}
        if resp.status_code != 200:
            return {"status": "error", "error": f"http:{resp.status_code}"}
        ct = resp.headers.get("content-type", "")
        if "json" not in ct:
            return {"status": "error", "error": f"ct:{ct[:30]}"}
        try:
            data = resp.json()
        except Exception as e:
            return {"status": "error", "error": f"json:{e.__class__.__name__}"}
        prices = self._prices_from_xhr(data, nights) if isinstance(data, dict) else []
        if prices:
            return {"status": "ok", "prices": prices, "data": data}
        if not isinstance(data, dict):
            return {"status": "error", "error": "schema:not_dict"}

        if any(k in data for k in ("error", "errors", "message")):
            keys = ",".join(sorted(str(k) for k in list(data.keys())[:5]))
            return {"status": "error", "error": f"api:{keys or 'message'}"}

        if "rates" not in data:
            keys = ",".join(sorted(str(k) for k in list(data.keys())[:5]))
            return {"status": "error", "error": f"schema:no_rates:{keys or 'empty'}"}

        rates = data.get("rates")
        if rates is None:
            return {"status": "sold_out", "prices": [], "data": data}
        if isinstance(rates, list):
            # Непустой список без извлекаемых цен (обычно — запрошенная длина стоя
            # превышает max-stay отеля, либо все rates — "not available" заглушки).
            # Для пользователя это эквивалентно "нет предложений на эти даты".
            # Возвращаем sold_out консистентно с Playwright XHR-веткой.
            return {"status": "sold_out", "prices": [], "data": data}
        return {"status": "error", "error": f"schema:rates_type:{type(rates).__name__}"}

    # ── XHR price extraction ─────────────────────────────────────

    def _prices_from_xhr(self, data: dict, nights: int = 0) -> List[float]:
        """
        Парсим JSON-ответ /hotel/search/v1/site/hp/search.
        Структура: { rates: [ { payment_options: { payment_types: [ {show_amount: 9589} ] } } ] }

        nights > 0: фильтруем по точному совпадению с кол-вом запрошенных ночей,
        чтобы min() не выбирал 1-ночную ставку для многодневных запросов.
        Если точного совпадения нет (field отсутствует или нет matching rate) —
        fallback: берём все rates (старое поведение, без регрессий).
        """

        def _rate_night_count(r: dict) -> int:
            """Кол-во ночей у rate (0 = неизвестно)."""
            for f in ("nights", "min_nights", "nights_count",
                      "min_stay", "length_of_stay", "stay_nights"):
                v = r.get(f)
                if isinstance(v, (int, float)) and v > 0:
                    return int(v)
            return 0

        def _collect_rate_prices(rate: dict) -> List[float]:
            prices: List[float] = []
            po = rate.get("payment_options", {})
            if isinstance(po, dict):
                for pt in po.get("payment_types", []):
                    if not isinstance(pt, dict):
                        continue
                    for field in ("show_amount", "amount", "price"):
                        p = self._to_price(pt.get(field))
                        if p:
                            prices.append(p)
            for field in ("price", "amount", "show_amount",
                          "base_amount", "total_price", "night_price"):
                p = self._to_price(rate.get(field))
                if p:
                    prices.append(p)
            return prices

        found: List[float] = []
        try:
            rates = data.get("rates", [])
            if not isinstance(rates, list):
                rates = []
            valid_rates = [r for r in rates if isinstance(r, dict)]

            if nights > 0 and valid_rates:
                annotated = [(r, _rate_night_count(r)) for r in valid_rates]
                has_info  = any(n > 0 for _, n in annotated)

                if has_info:
                    exact = [r for r, n in annotated if n == nights]
                    if exact:
                        logger.debug(
                            f"_prices_from_xhr: nights={nights}, "
                            f"exact-match rates={len(exact)}/{len(valid_rates)}"
                        )
                        for rate in exact:
                            found.extend(_collect_rate_prices(rate))
                        return [p for p in found if 500 <= p <= 300_000]
                    else:
                        available = sorted({n for _, n in annotated if n > 0})
                        logger.debug(
                            f"_prices_from_xhr: nights={nights}, no exact match, "
                            f"available={available} — fallback to all rates"
                        )
                        # Нет точного совпадения → fallback ниже

            # Fallback: берём все rates (текущее поведение)
            for rate in valid_rates:
                found.extend(_collect_rate_prices(rate))

        except Exception as e:
            logger.debug(f"_prices_from_xhr error: {e}")

        return [p for p in found if 500 <= p <= 300_000]

    # ── HTML price extraction ─────────────────────────────────────

    def _next_data_price(self, html: str) -> Optional[float]:
        m = re.search(
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
            html, re.S
        )
        if not m:
            return None
        try:
            data = json.loads(m.group(1))
            return self._dig_price(data, depth=0, max_depth=14)
        except Exception:
            return None

    def _strict_json_price(self, html: str) -> Optional[float]:
        KEYS = [
            "show_amount", "min_price", "minPrice",
            "base_amount", "night_price", "from_price",
            "total_price", "per_night", "min_rate",
        ]
        candidates = []
        for key in KEYS:
            pat = rf'"{re.escape(key)}"\s*:\s*"?(\d{{4,6}})"?'
            for m in re.finditer(pat, html):
                p = self._to_price(m.group(1))
                if p:
                    candidates.append(p)

        text_patterns = [
            r'(\d[\d\xa0\u202f\s]{2,6})\s*[₽]\s*/\s*ноч',
            r'(\d[\d\xa0\u202f\s]{2,6})\s*[₽]\s*за\s*ноч',
            r'от\s+(\d[\d\xa0\u202f\s]{2,6})\s*[₽](?:\s*/\s*ноч|\s*за\s*ноч)',
        ]
        for pat in text_patterns:
            for m in re.finditer(pat, html, re.IGNORECASE):
                raw = re.sub(r'[\xa0\u202f\s]', '', m.group(1))
                p = self._to_price(raw)
                if p:
                    candidates.append(p)

        valid = [p for p in candidates if 500 <= p <= 300_000]
        return min(valid) if valid else None

    # ── DOM extraction ────────────────────────────────────────────

    async def _dom_rub_price(self, page) -> Optional[float]:
        try:
            texts = await page.evaluate("""
                () => {
                    const results = [];
                    const all = document.querySelectorAll('span, div, p, strong, b');
                    for (const el of all) {
                        const childEls = el.querySelectorAll('*');
                        if (childEls.length > 2) continue;
                        const text = (el.innerText || el.textContent || '').trim();
                        if (text.includes('₽') && text.length >= 4 && text.length <= 25) {
                            results.push(text);
                        }
                    }
                    return results.slice(0, 50);
                }
            """)
        except Exception as e:
            logger.debug(f"OstrovokParser DOM JS error: {e}")
            return None

        logger.debug(f"OstrovokParser DOM ₽ texts (first 10): {texts[:10]}")
        prices = [p for t in texts for p in [self._parse_rub_text(t)] if p]
        valid = [p for p in prices if 500 <= p <= 300_000]
        return min(valid) if valid else None

    def _parse_rub_text(self, text: str) -> Optional[float]:
        if not text or '₽' not in text:
            return None
        before_rub = text.split('₽')[0]
        digits_only = re.sub(r'[^\d]', '', before_rub)
        if not digits_only:
            return None
        try:
            val = float(digits_only)
            if 500 <= val <= 300_000:
                return val
        except (ValueError, TypeError):
            pass
        return None

    async def _dom_title(self, page) -> Optional[str]:
        for sel in ["h1[class*='hotel']", "h1[class*='Hotel']",
                    "h1[class*='title']", "h1[itemprop='name']",
                    "[class*='HotelTitle']", "h1"]:
            try:
                el = await page.query_selector(sel)
                if el:
                    t = (await el.inner_text()).strip()
                    if t and len(t) > 2:
                        return t[:300]
            except Exception:
                pass
        return None

    def _html_title(self, html: str) -> Optional[str]:
        for p in [r'"hotelName"\s*:\s*"([^"]{3,200})"',
                  r'"name"\s*:\s*"([^"]{3,200})"',
                  r'<h1[^>]*>\s*([^<]{3,200}?)\s*</h1>',
                  r'<title>\s*([^<–|]{3,100})']:
            m = re.search(p, html, re.S)
            if m:
                t = m.group(1).strip()
                t = re.sub(r'\s*[|–—]\s*[Oo]strovok.*', '', t).strip()
                if len(t) > 2:
                    return t[:300]
        return None

    # ── JSON price digger ─────────────────────────────────────────

    def _dig_price(self, obj, depth: int, max_depth: int = 12) -> Optional[float]:
        if depth > max_depth:
            return None
        PRICE_KEYS = frozenset({
            "show_amount", "min_price", "minprice", "base_amount",
            "night_price", "from_price", "per_night", "min_rate",
            "lowest_price", "best_price", "total_price",
        })
        if isinstance(obj, dict):
            for key, val in obj.items():
                if key.lower() in PRICE_KEYS:
                    p = self._to_price(val)
                    if p and 500 <= p <= 300_000:
                        return p
            for val in obj.values():
                r = self._dig_price(val, depth + 1, max_depth)
                if r:
                    return r
        elif isinstance(obj, list):
            for item in obj[:30]:
                r = self._dig_price(item, depth + 1, max_depth)
                if r:
                    return r
        return None

    # ── Helpers ──────────────────────────────────────────────────

    def _to_price(self, v) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v) if v > 0 else None
        if isinstance(v, str):
            clean = re.sub(r'[\xa0\u202f\s,]', '', v).replace(',', '.')
            try:
                return float(clean) or None
            except (ValueError, TypeError):
                pass
        return None

    def _extract_hotel_id(self, url: str) -> Optional[str]:
        m = re.search(r'/mid(\d+)/', url)
        return m.group(1) if m else None

    def _extract_dates(self, url: str):
        qs = parse_qs(urlparse(url).query)
        ci = qs.get("checkin", [None])[0]
        co = qs.get("checkout", [None])[0]
        if ci and co:
            return ci, co
        ds = qs.get("dates", [None])[0]
        if ds:
            m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})-(\d{2})\.(\d{2})\.(\d{4})', ds)
            if m:
                d1, m1, y1, d2, m2, y2 = m.groups()
                return f"{y1}-{m1}-{d1}", f"{y2}-{m2}-{d2}"
        return None, None

    def _normalize_url(self, url: str, ci: Optional[str], co: Optional[str]) -> str:
        if "checkin=" in url or "dates=" in url:
            return url
        if not ci:
            ci = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        if not co:
            co = (datetime.strptime(ci, "%Y-%m-%d") + timedelta(days=2)).strftime("%Y-%m-%d")
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}checkin={ci}&checkout={co}&guests=2"

    def _headers(self) -> dict:
        return {
            "User-Agent":      random.choice(PARSER_USER_AGENTS),
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer":         "https://www.google.com/",
        }
