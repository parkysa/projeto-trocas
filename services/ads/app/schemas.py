from pydantic import BaseModel


class CreateAdCommand(BaseModel):
    owner_id: str
    title: str
    description: str


class ListAdsByOwnerCommand(BaseModel):
    owner_id: str


class UpdateAdCommand(BaseModel):
    ad_id: str
    owner_id: str
    title: str
    description: str


class DeleteAdCommand(BaseModel):
    ad_id: str
    owner_id: str


class AdItem(BaseModel):
    id: str
    title: str
    description: str


class AdCreatedEvent(BaseModel):
    id: str
    title: str
    description: str


class AdsListedEvent(BaseModel):
    ads: list[AdItem]


class AdUpdatedEvent(BaseModel):
    id: str
    title: str
    description: str


class AdDeletedEvent(BaseModel):
    id: str


class AdOperationFailedEvent(BaseModel):
    reason: str
