import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field


# ---------- entrada ----------

class UserCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    email: EmailStr
    password: str = Field(..., min_length=6)
    nickname: str = Field(..., min_length=1, max_length=50)
    phone_number: str = Field(..., min_length=1, max_length=20)


class UserUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    is_active: bool | None = None
    nickname: str | None = Field(default=None, min_length=1, max_length=50)
    phone_number: str | None = Field(default=None, min_length=1, max_length=20)


# ---------- saída ----------

class UserOut(BaseModel):
    id: uuid.UUID
    name: str
    email: str
    is_active: bool
    nickname: str
    phone_number: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
