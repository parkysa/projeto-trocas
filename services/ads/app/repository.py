import uuid

from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Ad, FavoriteAd


class AdRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(
        self,
        owner_id: str,
        title: str,
        description: str,
        image: str,
        image_position: str | None,
        category: str,
        condition: str,
        location: str,
        trade_terms: str | None,
    ) -> Ad:
        ad = Ad(
            owner_id=uuid.UUID(owner_id),
            title=title,
            description=description,
            image=image,
            image_position=image_position,
            category=category,
            condition=condition,
            location=location,
            trade_terms=trade_terms,
        )
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
        pattern = f"%{query}%"
        result = await self.session.scalars(
            select(Ad)
            .where(Ad.owner_id != uuid.UUID(exclude_owner_id))
            .where(Ad.is_available.is_(True))
            .where(
                or_(
                    Ad.title.ilike(pattern),
                    Ad.description.ilike(pattern),
                    Ad.category.ilike(pattern),
                    Ad.location.ilike(pattern),
                    Ad.trade_terms.ilike(pattern),
                )
            )
        )
        return list(result)

    async def update(
        self,
        ad: Ad,
        title: str,
        description: str,
        image: str,
        image_position: str | None,
        category: str,
        condition: str,
        location: str,
        trade_terms: str | None,
    ) -> Ad:
        ad.title = title
        ad.description = description
        ad.image = image
        ad.image_position = image_position
        ad.category = category
        ad.condition = condition
        ad.location = location
        ad.trade_terms = trade_terms
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

    async def favorite(self, user_id: str, ad_id: str) -> None:
        existing = await self.session.scalar(
            select(FavoriteAd)
            .where(FavoriteAd.user_id == uuid.UUID(user_id))
            .where(FavoriteAd.ad_id == uuid.UUID(ad_id))
        )
        if existing is not None:
            return

        favorite = FavoriteAd(user_id=uuid.UUID(user_id), ad_id=uuid.UUID(ad_id))
        self.session.add(favorite)
        await self.session.commit()

    async def unfavorite(self, user_id: str, ad_id: str) -> None:
        await self.session.execute(
            delete(FavoriteAd)
            .where(FavoriteAd.user_id == uuid.UUID(user_id))
            .where(FavoriteAd.ad_id == uuid.UUID(ad_id))
        )
        await self.session.commit()

    async def list_favorite_ids(self, user_id: str) -> list[str]:
        result = await self.session.scalars(
            select(FavoriteAd.ad_id).where(FavoriteAd.user_id == uuid.UUID(user_id))
        )
        return [str(ad_id) for ad_id in result]
