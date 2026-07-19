from pydantic import BaseModel, EmailStr


class RegisterCommand(BaseModel):
    name: str
    email: EmailStr
    phone: str
    password: str


class LoginCommand(BaseModel):
    email: EmailStr
    password: str


class RegisteredEvent(BaseModel):
    user_id: str
    email: str
    phone: str


class RegistrationFailedEvent(BaseModel):
    email: str
    reason: str


class AuthenticatedEvent(BaseModel):
    user_id: str
    token: str


class AuthenticationFailedEvent(BaseModel):
    email: str
    reason: str


class GetProfileCommand(BaseModel):
    user_id: str


class UpdateProfileCommand(BaseModel):
    user_id: str
    name: str
    email: EmailStr
    phone: str


class ProfileFoundEvent(BaseModel):
    id: str
    name: str
    email: str
    phone: str


class ProfileUpdatedEvent(BaseModel):
    id: str
    name: str
    email: str
    phone: str


class ProfileUpdateFailedEvent(BaseModel):
    reason: str
