import uuid
from datetime import date, datetime

from pydantic import BaseModel, Field


# ---------- entrada ----------

class TradeCreate(BaseModel):
    ad_id: uuid.UUID
    proposer_id: uuid.UUID
    offered_ad_id: uuid.UUID
    status: str = Field(default="pending", min_length=1, max_length=50)
    purpose_date: date
    answer_date: date | None = None


class TradeUpdate(BaseModel):
    status: str | None = Field(default=None, min_length=1, max_length=50)
    answer_date: date | None = None


# ---------- saída ----------

class TradeOut(BaseModel):
    id: uuid.UUID
    ad_id: uuid.UUID
    proposer_id: uuid.UUID
    offered_ad_id: uuid.UUID
    status: str
    purpose_date: date
    answer_date: date | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
