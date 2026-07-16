from pydantic import BaseModel, EmailStr


class RegisterCommand(BaseModel):
    name: str
    email: EmailStr
    password: str


class LoginCommand(BaseModel):
    email: EmailStr
    password: str


class RegisteredEvent(BaseModel):
    user_id: str
    email: str


class RegistrationFailedEvent(BaseModel):
    email: str
    reason: str


class AuthenticatedEvent(BaseModel):
    user_id: str
    token: str


class AuthenticationFailedEvent(BaseModel):
    email: str
    reason: str
