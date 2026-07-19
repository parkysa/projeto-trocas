import uuid

from sqlalchemy import or_, select
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

    async def has_accepted_trade_for_ad(self, ad_id: str) -> bool:
        ad_uuid = uuid.UUID(ad_id)
        stmt = select(Trade).where(
            Trade.status == "ACCEPTED",
            or_(Trade.requester_ad_id == ad_uuid, Trade.target_ad_id == ad_uuid),
        )
        result = await self.session.scalars(stmt)
        return result.first() is not None

    async def update_status(self, trade: Trade, status: str) -> Trade:
        trade.status = status
        await self.session.commit()
        await self.session.refresh(trade)
        return trade
