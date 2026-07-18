import uuid

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
