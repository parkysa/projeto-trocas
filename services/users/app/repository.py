import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import User


class UserRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, name: str, email: str, phone: str, password_hash: str) -> User:
        user = User(name=name, email=email, phone=phone, password_hash=password_hash)
        self.session.add(user)
        await self.session.commit()
        await self.session.refresh(user)
        return user

    async def get_by_email(self, email: str) -> User | None:
        return await self.session.scalar(select(User).where(User.email == email))

    async def get_by_id(self, user_id: str) -> User | None:
        return await self.session.get(User, uuid.UUID(user_id))

    async def update(self, user: User, name: str, email: str, phone: str) -> User:
        user.name = name
        user.email = email
        user.phone = phone
        await self.session.commit()
        await self.session.refresh(user)
        return user
