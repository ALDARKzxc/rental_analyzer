"""Database layer v2 — с автомиграцией для старых БД."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional, List

from sqlalchemy import Column, Integer, String, Float, DateTime, Text, ForeignKey, Boolean, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.future import select
from loguru import logger

from app.utils.config import DB_PATH, DATA_DIR

DATABASE_URL = f"sqlite+aiosqlite:///{DB_PATH}"
Base = declarative_base()

CATEGORIES = [
    "Квартиры",
    "Квартира - 1 комната",
    "Квартира - 2 комнаты",
    "Квартира - 3 комнаты",
    "Апартаменты",
    "Дома",
    "Дом - до 2 человек",
    "Дом - до 4 человек",
    "Дом - до 6 человек",
    "Дом - до 8 человек",
    "Дом - до 10 человек",
    "Дом - свыше 10 человек",
    "Коттеджи",
]

# Группы-«зонтики» в фильтре главного экрана: при выборе показываются объекты
# всех вложенных подкатегорий + сама группа (для совместимости со старыми записями).
CATEGORY_GROUPS = {
    "Квартиры": [
        "Квартиры",
        "Квартира - 1 комната",
        "Квартира - 2 комнаты",
        "Квартира - 3 комнаты",
    ],
    "Дома": [
        "Дома",
        "Дом - до 2 человек",
        "Дом - до 4 человек",
        "Дом - до 6 человек",
        "Дом - до 8 человек",
        "Дом - до 10 человек",
        "Дом - свыше 10 человек",
    ],
}

# Категории, доступные для выбора при добавлении объекта (без зонтичных групп).
ADD_CATEGORIES = [c for c in CATEGORIES if c not in CATEGORY_GROUPS]


class Property(Base):
    __tablename__ = "properties"
    id           = Column(Integer, primary_key=True, autoincrement=True)
    title        = Column(String(500), nullable=False)
    url          = Column(String(2000), nullable=False)
    site         = Column(String(50),  nullable=True)
    external_id  = Column(String(200), nullable=True)
    category     = Column(String(50),  nullable=True, default="Квартиры")
    parse_dates  = Column(String(30),  nullable=True)
    notes        = Column(Text,        nullable=True)
    address      = Column(String(500), nullable=True)
    guest_capacity = Column(Integer,   nullable=True)
    preview_path = Column(String(500), nullable=True)
    is_active    = Column(Boolean, default=True)
    title_locked = Column(Boolean, default=False)   # если True — парсер не перезаписывает название
    is_own       = Column(Boolean, default=False)   # отметка «свой объект» — ✅ + зелёная рамка
    # Раздел "Сравнение объектов": кэш удобств и описания со страницы
    amenities    = Column(Text,        nullable=True)  # JSON: {"groups": [{"name", "items": [...]}]}
    description  = Column(Text,        nullable=True)
    key_facts    = Column(Text,        nullable=True)  # JSON-list: ["До 6 гостей", "55 кв.м", ...]
    amenities_fetched_at = Column(DateTime, nullable=True)
    created_at   = Column(DateTime, default=datetime.utcnow)
    updated_at   = Column(DateTime, default=datetime.utcnow)

    price_records = relationship(
        "PriceRecord", back_populates="property",
        cascade="all, delete-orphan",
        order_by="desc(PriceRecord.recorded_at)"
    )


class PriceRecord(Base):
    __tablename__ = "price_records"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    property_id = Column(Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)
    price       = Column(Float,   nullable=True)
    currency    = Column(String(10), default="RUB")
    status      = Column(String(50), default="ok")
    parse_dates = Column(String(30), nullable=True)
    error_message = Column(Text, nullable=True)
    recorded_at = Column(DateTime, default=datetime.utcnow)

    property = relationship("Property", back_populates="price_records")


engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _migrate(conn):
    """Добавляем новые колонки в существующую БД если их нет."""
    migrations = [
        ("properties", "category",     "TEXT DEFAULT 'Квартиры'"),
        ("properties", "parse_dates",  "TEXT"),
        ("properties", "address",      "TEXT"),
        ("properties", "guest_capacity", "INTEGER"),
        ("properties", "preview_path", "TEXT"),
        ("properties", "title_locked", "INTEGER DEFAULT 0"),
        ("properties", "is_own",       "INTEGER DEFAULT 0"),
        ("properties", "amenities",    "TEXT"),
        ("properties", "description",  "TEXT"),
        ("properties", "key_facts",    "TEXT"),
        ("properties", "amenities_fetched_at", "DATETIME"),
        ("price_records", "parse_dates", "TEXT"),
    ]
    for table, col, col_def in migrations:
        try:
            await conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"))
            logger.info(f"Migration: added {table}.{col}")
        except Exception:
            pass  # Колонка уже существует


async def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _migrate(conn)
    logger.info(f"Database ready: {DB_PATH}")


class PropertyRepository:

    @staticmethod
    async def get_all(category: str = None) -> List[Property]:
        async with AsyncSessionLocal() as s:
            q = select(Property).where(Property.is_active == True)
            if category and category != "Все":
                if category in CATEGORY_GROUPS:
                    q = q.where(Property.category.in_(CATEGORY_GROUPS[category]))
                else:
                    q = q.where(Property.category == category)
            q = q.order_by(Property.created_at.desc())
            return (await s.execute(q)).scalars().all()

    @staticmethod
    async def get_by_id(prop_id: int) -> Optional[Property]:
        async with AsyncSessionLocal() as s:
            return (await s.execute(
                select(Property).where(Property.id == prop_id)
            )).scalar_one_or_none()

    @staticmethod
    async def get_by_url(url: str) -> Optional[Property]:
        """Возвращает только активный объект с таким URL."""
        base = url.split("?")[0]
        async with AsyncSessionLocal() as s:
            props = (await s.execute(
                select(Property).where(Property.is_active == True)
            )).scalars().all()
            for p in props:
                if p.url.split("?")[0] == base:
                    return p
            return None

    @staticmethod
    async def get_by_url_any(url: str) -> Optional[Property]:
        """Возвращает объект с таким URL независимо от is_active (включая удалённые)."""
        base = url.split("?")[0]
        async with AsyncSessionLocal() as s:
            props = (await s.execute(select(Property))).scalars().all()
            for p in props:
                if p.url.split("?")[0] == base:
                    return p
            return None

    @staticmethod
    async def create(title: str, url: str, site: str = None,
                     category: str = "Квартиры", notes: str = None,
                     title_locked: bool = False,
                     is_own: bool = False) -> Property:
        async with AsyncSessionLocal() as s:
            prop = Property(
                title=title, url=url.split("?")[0],
                site=site, category=category, notes=notes,
                title_locked=title_locked,
                is_own=is_own,
            )
            s.add(prop)
            await s.commit()
            await s.refresh(prop)
            logger.info(f"Created property id={prop.id} cat={category}")
            return prop

    @staticmethod
    async def update(prop_id: int, **kwargs) -> Optional[Property]:
        async with AsyncSessionLocal() as s:
            prop = (await s.execute(
                select(Property).where(Property.id == prop_id)
            )).scalar_one_or_none()
            if prop:
                for k, v in kwargs.items():
                    if hasattr(prop, k):
                        setattr(prop, k, v)
                prop.updated_at = datetime.utcnow()
                await s.commit()
                await s.refresh(prop)
            return prop

    @staticmethod
    async def set_parse_dates(prop_id: int, dates_str: str) -> bool:
        async with AsyncSessionLocal() as s:
            prop = (await s.execute(
                select(Property).where(Property.id == prop_id)
            )).scalar_one_or_none()
            if prop:
                prop.parse_dates = dates_str
                prop.updated_at  = datetime.utcnow()
                await s.commit()
                return True
            return False

    @staticmethod
    async def set_category_dates(category: str, dates_str: str) -> int:
        async with AsyncSessionLocal() as s:
            q = select(Property).where(Property.is_active == True)
            if category in CATEGORY_GROUPS:
                q = q.where(Property.category.in_(CATEGORY_GROUPS[category]))
            else:
                q = q.where(Property.category == category)
            props = (await s.execute(q)).scalars().all()
            for p in props:
                p.parse_dates = dates_str
                p.updated_at  = datetime.utcnow()
            await s.commit()
            return len(props)

    @staticmethod
    async def update_amenities(prop_id: int, amenities_json: Optional[str],
                               description: Optional[str],
                               key_facts_json: Optional[str] = None) -> bool:
        async with AsyncSessionLocal() as s:
            prop = (await s.execute(
                select(Property).where(Property.id == prop_id)
            )).scalar_one_or_none()
            if not prop:
                return False
            prop.amenities = amenities_json
            prop.description = description
            prop.key_facts = key_facts_json
            prop.amenities_fetched_at = datetime.utcnow()
            prop.updated_at = datetime.utcnow()
            await s.commit()
            return True

    @staticmethod
    async def set_all_dates(dates_str: str) -> int:
        async with AsyncSessionLocal() as s:
            props = (await s.execute(
                select(Property).where(Property.is_active == True)
            )).scalars().all()
            for p in props:
                p.parse_dates = dates_str
                p.updated_at  = datetime.utcnow()
            await s.commit()
            return len(props)

    @staticmethod
    async def delete(prop_id: int) -> bool:
        async with AsyncSessionLocal() as s:
            prop = (await s.execute(
                select(Property).where(Property.id == prop_id)
            )).scalar_one_or_none()
            if prop:
                preview_path = prop.preview_path
                await s.delete(prop)
                await s.commit()
                PropertyRepository._delete_preview_file(preview_path)
                return True
            return False

    @staticmethod
    def _delete_preview_file(preview_path: Optional[str]) -> None:
        if not preview_path:
            return

        try:
            path = Path(preview_path)
            if not path.is_absolute():
                path = DATA_DIR / path
            if path.exists():
                path.unlink()
        except Exception:
            logger.warning(f"Failed to remove preview file: {preview_path}")


class PriceRepository:

    @staticmethod
    async def add_record(property_id: int, price: Optional[float],
                         status: str = "ok", error_message: str = None,
                         parse_dates: str = None,
                         currency: str = "RUB") -> PriceRecord:
        async with AsyncSessionLocal() as s:
            rec = PriceRecord(
                property_id=property_id, price=price,
                status=status, error_message=error_message,
                parse_dates=parse_dates, currency=currency
            )
            s.add(rec)
            await s.commit()
            await s.refresh(rec)
            return rec

    @staticmethod
    async def get_history(property_id: int, limit: int = 100) -> List[PriceRecord]:
        async with AsyncSessionLocal() as s:
            return (await s.execute(
                select(PriceRecord)
                .where(PriceRecord.property_id == property_id)
                .order_by(PriceRecord.recorded_at.desc())
                .limit(limit)
            )).scalars().all()

    @staticmethod
    async def get_latest(property_id: int) -> Optional[PriceRecord]:
        async with AsyncSessionLocal() as s:
            return (await s.execute(
                select(PriceRecord)
                .where(PriceRecord.property_id == property_id)
                .order_by(PriceRecord.recorded_at.desc())
                .limit(1)
            )).scalar_one_or_none()

    @staticmethod
    async def get_all_latest() -> dict:
        props = await PropertyRepository.get_all()
        result = {}
        for p in props:
            rec = await PriceRepository.get_latest(p.id)
            result[p.id] = rec
        return result
