import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Trade
from app.schemas import TradeCreate, TradeUpdate


async def get_trade(db: AsyncSession, trade_id: uuid.UUID) -> Trade | None:
    result = await db.execute(select(Trade).where(Trade.id == trade_id))
    return result.scalar_one_or_none()


async def list_trades(db: AsyncSession, skip: int = 0, limit: int = 100) -> list[Trade]:
    result = await db.execute(select(Trade).offset(skip).limit(limit))
    return list(result.scalars().all())


async def create_trade(db: AsyncSession, data: TradeCreate) -> Trade:
    trade = Trade(
        ad_id=data.ad_id,
        proposer_id=data.proposer_id,
        offered_ad_id=data.offered_ad_id,
        status=data.status,
        purpose_date=data.purpose_date,
        answer_date=data.answer_date,
    )
    db.add(trade)
    try:
        await db.commit()
        await db.refresh(trade)
    except Exception:
        await db.rollback()
        raise
    return trade


async def update_trade(db: AsyncSession, trade: Trade, data: TradeUpdate) -> Trade:
    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(trade, field, value)
    try:
        await db.commit()
        await db.refresh(trade)
    except Exception:
        await db.rollback()
        raise
    return trade


async def delete_trade(db: AsyncSession, trade: Trade) -> None:
    await db.delete(trade)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise
