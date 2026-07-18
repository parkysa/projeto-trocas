from pydantic import BaseModel


class RequestTradeCommand(BaseModel):
    requester_id: str
    requester_ad_id: str
    target_ad_id: str


class TradeRequestedEvent(BaseModel):
    trade_id: str
    status: str


class TradeRequestFailedEvent(BaseModel):
    reason: str
