import uuid

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.models import Trade


class TradeRepository:
    def __init__(self, session: Session):
        self.session = session

    def create(self, requester_id: str, requester_ad_id: str, target_ad_id: str) -> Trade:
        trade = Trade(
            requester_id=uuid.UUID(requester_id),
            requester_ad_id=uuid.UUID(requester_ad_id),
            target_ad_id=uuid.UUID(target_ad_id),
            status="PENDING",
        )
        self.session.add(trade)
        self.session.commit()
        self.session.refresh(trade)
        return trade

    def get_by_id(self, trade_id: str) -> Trade | None:
        return self.session.get(Trade, uuid.UUID(trade_id))

    def has_accepted_trade_for_ad(self, ad_id: str) -> bool:
        ad_uuid = uuid.UUID(ad_id)
        stmt = select(Trade).where(
            Trade.status == "ACCEPTED",
            or_(Trade.requester_ad_id == ad_uuid, Trade.target_ad_id == ad_uuid),
        )
        return self.session.scalars(stmt).first() is not None

    def update_status(self, trade: Trade, status: str) -> Trade:
        trade.status = status
        self.session.commit()
        self.session.refresh(trade)
        return trade
