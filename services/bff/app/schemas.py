from pydantic import BaseModel, EmailStr


class RegisterRequest(BaseModel):
    name: str
    email: EmailStr
    password: str


class RegisterResponse(BaseModel):
    id: str
    name: str
    email: str


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


class UpdateProfileRequest(BaseModel):
    name: str
    email: EmailStr


class CreateAdRequest(BaseModel):
    title: str
    description: str


class UpdateAdRequest(BaseModel):
    title: str
    description: str


class AdResponse(BaseModel):
    id: str
    title: str
    description: str


class AdSearchResult(BaseModel):
    id: str
    title: str
    description: str
    owner_id: str


class CreateTradeRequest(BaseModel):
    requester_ad_id: str
    target_ad_id: str


class TradeResponse(BaseModel):
    id: str
    status: str


class TradeDecisionResponse(BaseModel):
    status: str


class NotificationResponse(BaseModel):
    id: str
    type: str
    message: str
    created_at: str
