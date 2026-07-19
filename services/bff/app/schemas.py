from pydantic import BaseModel, EmailStr


class RegisterRequest(BaseModel):
    name: str
    email: EmailStr
    phone: str
    password: str


class RegisterResponse(BaseModel):
    id: str
    name: str
    email: str
    phone: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str


class ProfileResponse(BaseModel):
    id: str
    name: str
    email: str
    phone: str


class UpdateProfileRequest(BaseModel):
    name: str
    email: EmailStr
    phone: str


class CreateAdRequest(BaseModel):
    title: str
    description: str
    image: str = ""
    image_position: str | None = None
    category: str = "Geral"
    condition: str = "usado"
    location: str = "Nao informado"
    trade_terms: str | None = None


class UpdateAdRequest(BaseModel):
    id: str
    title: str
    description: str
    image: str = ""
    image_position: str | None = None
    category: str = "Geral"
    condition: str = "usado"
    location: str = "Nao informado"
    trade_terms: str | None = None


class AdIdRequest(BaseModel):
    id: str


class SearchAdsRequest(BaseModel):
    q: str


class FavoriteAdRequest(BaseModel):
    ad_id: str


class AdResponse(BaseModel):
    id: str
    title: str
    description: str
    is_available: bool = True
    image: str
    image_position: str | None
    category: str
    condition: str
    location: str
    trade_terms: str | None


class AdSearchResult(BaseModel):
    id: str
    title: str
    description: str
    owner_id: str
    owner_name: str
    image: str
    image_position: str | None
    category: str
    condition: str
    location: str
    trade_terms: str | None


class CreateTradeRequest(BaseModel):
    requester_ad_id: str
    target_ad_id: str


class TradeResponse(BaseModel):
    id: str
    status: str


class TradeDecisionResponse(BaseModel):
    status: str


class TradeItemDetailResponse(BaseModel):
    nome: str
    descricao: str
    condicao: str
    localizacao: str
    imagem: str | None = None


class TradeContactResponse(BaseModel):
    telefone: str
    email: str


class TradeViewResponse(BaseModel):
    id: str
    itemDeId: str
    itemParaId: str
    itemDe: str
    itemPara: str
    meuItem: TradeItemDetailResponse
    itemFulano: TradeItemDetailResponse
    status: str
    dataSolicitacao: str
    dataRespostaCancelamento: str | None = None
    contraparte: str
    contatoContraparte: TradeContactResponse | None = None
    direcao: str


class NotificationResponse(BaseModel):
    id: str
    type: str
    message: str
    created_at: str


class FavoriteIdsResponse(BaseModel):
    ad_ids: list[str]
