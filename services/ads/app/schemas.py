from pydantic import BaseModel


class CreateAdCommand(BaseModel):
    owner_id: str
    title: str
    description: str
    image: str = ""
    image_position: str | None = None
    category: str = "Geral"
    condition: str = "usado"
    location: str = "Nao informado"
    trade_terms: str | None = None


class ListAdsByOwnerCommand(BaseModel):
    owner_id: str


class UpdateAdCommand(BaseModel):
    ad_id: str
    owner_id: str
    title: str
    description: str
    image: str = ""
    image_position: str | None = None
    category: str = "Geral"
    condition: str = "usado"
    location: str = "Nao informado"
    trade_terms: str | None = None


class DeleteAdCommand(BaseModel):
    ad_id: str
    owner_id: str
    keep_record: bool = False


class AdItem(BaseModel):
    id: str
    title: str
    description: str
    is_available: bool
    image: str
    image_position: str | None
    category: str
    condition: str
    location: str
    trade_terms: str | None


class AdCreatedEvent(BaseModel):
    id: str
    title: str
    description: str
    is_available: bool
    image: str
    image_position: str | None
    category: str
    condition: str
    location: str
    trade_terms: str | None


class AdsListedEvent(BaseModel):
    ads: list[AdItem]


class AdUpdatedEvent(BaseModel):
    id: str
    title: str
    description: str
    is_available: bool
    image: str
    image_position: str | None
    category: str
    condition: str
    location: str
    trade_terms: str | None


class AdDeletedEvent(BaseModel):
    id: str


class AdOperationFailedEvent(BaseModel):
    reason: str


class ListAvailableAdsCommand(BaseModel):
    owner_id: str


class SearchAdsCommand(BaseModel):
    owner_id: str
    query: str


class AvailableAdItem(BaseModel):
    id: str
    title: str
    description: str
    owner_id: str
    image: str
    image_position: str | None
    category: str
    condition: str
    location: str
    trade_terms: str | None


class GetAdByIdCommand(BaseModel):
    ad_id: str


class AdFoundEvent(BaseModel):
    id: str
    owner_id: str
    title: str
    description: str
    is_available: bool
    image: str
    image_position: str | None
    category: str
    condition: str
    location: str
    trade_terms: str | None


class AdNotFoundEvent(BaseModel):
    ad_id: str


class MarkAdUnavailableCommand(BaseModel):
    ad_id: str


class FavoriteAdCommand(BaseModel):
    user_id: str
    ad_id: str


class ListFavoriteAdsCommand(BaseModel):
    user_id: str


class FavoriteAdEvent(BaseModel):
    ad_id: str


class FavoriteAdsListedEvent(BaseModel):
    ad_ids: list[str]
