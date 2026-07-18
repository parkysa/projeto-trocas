import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Ad


class AdRepository:
    def __init__(self, session: Session):
        self.session = session

    def create(self, owner_id: str, title: str, description: str) -> Ad:
        ad = Ad(owner_id=uuid.UUID(owner_id), title=title, description=description)
        self.session.add(ad)
        self.session.commit()
        self.session.refresh(ad)
        return ad

    def list_by_owner(self, owner_id: str) -> list[Ad]:
        return list(
            self.session.scalars(select(Ad).where(Ad.owner_id == uuid.UUID(owner_id)))
        )

    def get_by_id(self, ad_id: str) -> Ad | None:
        return self.session.get(Ad, uuid.UUID(ad_id))

    def update(self, ad: Ad, title: str, description: str) -> Ad:
        ad.title = title
        ad.description = description
        self.session.commit()
        self.session.refresh(ad)
        return ad

    def delete(self, ad: Ad) -> None:
        self.session.delete(ad)
        self.session.commit()
