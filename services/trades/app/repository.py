import uuid

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Trade


class TradeRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(
        self, requester_id: str, requester_ad_id: str, target_ad_id: str
    ) -> Trade:
        trade = Trade(
            requester_id=uuid.UUID(requester_id),
            requester_ad_id=uuid.UUID(requester_ad_id),
            target_ad_id=uuid.UUID(target_ad_id),
            status="PENDING",
        )
        self.session.add(trade)
        await self.session.commit()
        await self.session.refresh(trade)
        return trade

    async def get_by_id(self, trade_id: str) -> Trade | None:
        return await self.session.get(Trade, uuid.UUID(trade_id))

    async def list_by_requester(self, requester_id: str) -> list[Trade]:
        result = await self.session.scalars(
            select(Trade)
            .where(Trade.requester_id == uuid.UUID(requester_id))
            .order_by(Trade.created_at.desc())
        )
        return list(result)

    async def list_all(self) -> list[Trade]:
        result = await self.session.scalars(
            select(Trade).order_by(Trade.created_at.desc())
        )
        return list(result)

    async def has_accepted_trade_for_ad(self, ad_id: str) -> bool:
        ad_uuid = uuid.UUID(ad_id)
        stmt = select(Trade).where(
            Trade.status == "ACCEPTED",
            or_(Trade.requester_ad_id == ad_uuid, Trade.target_ad_id == ad_uuid),
        )
        result = await self.session.scalars(stmt)
        return result.first() is not None

    async def has_non_cancelled_trade_for_same_items(self, ad_a: str, ad_b: str) -> bool:
        ad_a_uuid = uuid.UUID(ad_a)
        ad_b_uuid = uuid.UUID(ad_b)
        stmt = select(Trade).where(
            Trade.status != "CANCELLED",
            or_(
                (Trade.requester_ad_id == ad_a_uuid) & (Trade.target_ad_id == ad_b_uuid),
                (Trade.requester_ad_id == ad_b_uuid) & (Trade.target_ad_id == ad_a_uuid),
            ),
        )
        result = await self.session.scalars(stmt)
        return result.first() is not None

    async def update_status(self, trade: Trade, status: str) -> Trade:
        trade.status = status
        await self.session.commit()
        await self.session.refresh(trade)
        return trade

    async def cancel_other_pending_for_ads(
        self, accepted_trade_id: str, ad_ids: list[str]
    ) -> None:
        ad_uuids = [uuid.UUID(ad_id) for ad_id in ad_ids]
        stmt = (
            update(Trade)
            .where(Trade.id != uuid.UUID(accepted_trade_id))
            .where(Trade.status == "PENDING")
            .where(
                or_(
                    Trade.requester_ad_id.in_(ad_uuids),
                    Trade.target_ad_id.in_(ad_uuids),
                )
            )
            .values(status="CANCELLED")
        )
        await self.session.execute(stmt)
