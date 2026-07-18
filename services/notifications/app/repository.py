import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Notification


class NotificationRepository:
    def __init__(self, session: Session):
        self.session = session

    def create(self, user_id: str, type: str, message: str) -> Notification:
        notification = Notification(user_id=uuid.UUID(user_id), type=type, message=message)
        self.session.add(notification)
        self.session.commit()
        self.session.refresh(notification)
        return notification

    def list_by_user(self, user_id: str) -> list[Notification]:
        return list(
            self.session.scalars(
                select(Notification)
                .where(Notification.user_id == uuid.UUID(user_id))
                .order_by(Notification.created_at.desc())
            )
        )
