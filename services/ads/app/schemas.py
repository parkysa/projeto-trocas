import uuid
from datetime import date, datetime

from pydantic import BaseModel, Field


# ---------- entrada ----------

class AdCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    description: str = Field(..., min_length=1, max_length=2000)
    owner_id: uuid.UUID
    address: str = Field(..., min_length=1, max_length=500)
    publication_date: date
    accept_terms: str = Field(..., min_length=1, max_length=500)
    item_condition: str = Field(..., min_length=1, max_length=20)


class AdUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, min_length=1, max_length=2000)
    address: str | None = Field(default=None, min_length=1, max_length=500)
    publication_date: date | None = None
    accept_terms: str | None = Field(default=None, min_length=1, max_length=500)
    item_condition: str | None = Field(default=None, min_length=1, max_length=20)


# ---------- saída ----------

class AdOut(BaseModel):
    id: uuid.UUID
    title: str
    description: str
    owner_id: uuid.UUID
    address: str
    publication_date: date
    accept_terms: str
    item_condition: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
