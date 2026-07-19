from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import text
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

engine = create_async_engine(settings.database_url)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def create_all() -> None:
    from app import models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            text("ALTER TABLE ads ADD COLUMN IF NOT EXISTS image VARCHAR NOT NULL DEFAULT ''")
        )
        await conn.execute(
            text("ALTER TABLE ads ADD COLUMN IF NOT EXISTS image_position VARCHAR")
        )
        await conn.execute(
            text("ALTER TABLE ads ADD COLUMN IF NOT EXISTS category VARCHAR NOT NULL DEFAULT 'Geral'")
        )
        await conn.execute(
            text("ALTER TABLE ads ADD COLUMN IF NOT EXISTS condition VARCHAR NOT NULL DEFAULT 'usado'")
        )
        await conn.execute(
            text("ALTER TABLE ads ADD COLUMN IF NOT EXISTS location VARCHAR NOT NULL DEFAULT 'Nao informado'")
        )
        await conn.execute(
            text("ALTER TABLE ads ADD COLUMN IF NOT EXISTS trade_terms VARCHAR")
        )
