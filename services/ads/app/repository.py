import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Ad


class AdRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, owner_id: str, title: str, description: str) -> Ad:
        ad = Ad(owner_id=uuid.UUID(owner_id), title=title, description=description)
        self.session.add(ad)
        await self.session.commit()
        await self.session.refresh(ad)
        return ad

    async def list_by_owner(self, owner_id: str) -> list[Ad]:
        result = await self.session.scalars(
            select(Ad).where(Ad.owner_id == uuid.UUID(owner_id))
        )
        return list(result)

    async def get_by_id(self, ad_id: str) -> Ad | None:
        return await self.session.get(Ad, uuid.UUID(ad_id))

    async def list_available(self, exclude_owner_id: str) -> list[Ad]:
        result = await self.session.scalars(
            select(Ad)
            .where(Ad.owner_id != uuid.UUID(exclude_owner_id))
            .where(Ad.is_available.is_(True))
        )
        return list(result)

    async def search_by_title(self, query: str, exclude_owner_id: str) -> list[Ad]:
        result = await self.session.scalars(
            select(Ad)
            .where(Ad.owner_id != uuid.UUID(exclude_owner_id))
            .where(Ad.is_available.is_(True))
            .where(Ad.title.ilike(f"%{query}%"))
        )
        return list(result)

    async def update(self, ad: Ad, title: str, description: str) -> Ad:
        ad.title = title
        ad.description = description
        await self.session.commit()
        await self.session.refresh(ad)
        return ad

    async def delete(self, ad: Ad) -> None:
        await self.session.delete(ad)
        await self.session.commit()

    async def mark_unavailable(self, ad_id: str) -> None:
        ad = await self.get_by_id(ad_id)
        if ad is not None:
            ad.is_available = False
            await self.session.commit()
