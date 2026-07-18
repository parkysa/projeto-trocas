import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Ad
from app.schemas import AdCreate, AdUpdate


async def get_ad(db: AsyncSession, ad_id: uuid.UUID) -> Ad | None:
    result = await db.execute(select(Ad).where(Ad.id == ad_id))
    return result.scalar_one_or_none()


async def list_ads(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Ad]:
    result = await db.execute(select(Ad).offset(skip).limit(limit))
    return list(result.scalars().all())


async def create_ad(db: AsyncSession, data: AdCreate) -> Ad:
    ad = Ad(
        title=data.title,
        description=data.description,
        owner_id=data.owner_id,
        address=data.address,
        publication_date=data.publication_date,
        accept_terms=data.accept_terms,
        item_condition=data.item_condition,
    )
    db.add(ad)
    try:
        await db.commit()
        await db.refresh(ad)
    except Exception:
        await db.rollback()
        raise
    return ad


async def update_ad(db: AsyncSession, ad: Ad, data: AdUpdate) -> Ad:
    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(ad, field, value)
    try:
        await db.commit()
        await db.refresh(ad)
    except Exception:
        await db.rollback()
        raise
    return ad


async def delete_ad(db: AsyncSession, ad: Ad) -> None:
    await db.delete(ad)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise
