import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Notification


class NotificationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, user_id: str, type: str, message: str) -> Notification:
        notification = Notification(user_id=uuid.UUID(user_id), type=type, message=message)
        self.session.add(notification)
        await self.session.commit()
        await self.session.refresh(notification)
        return notification

    async def list_by_user(self, user_id: str) -> list[Notification]:
        result = await self.session.scalars(
            select(Notification)
            .where(Notification.user_id == uuid.UUID(user_id))
            .order_by(Notification.created_at.desc())
        )
        return list(result)
