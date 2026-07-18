from pydantic import BaseModel


class RequestTradeCommand(BaseModel):
    requester_id: str
    requester_ad_id: str
    target_ad_id: str


class TradeRequestedEvent(BaseModel):
    trade_id: str
    status: str
    target_owner_id: str


class TradeRequestFailedEvent(BaseModel):
    reason: str


class AcceptTradeCommand(BaseModel):
    trade_id: str
    decider_id: str


class RejectTradeCommand(BaseModel):
    trade_id: str
    decider_id: str


class TradeAcceptedEvent(BaseModel):
    trade_id: str
    status: str
    requester_id: str


class TradeRejectedEvent(BaseModel):
    trade_id: str
    status: str
    requester_id: str


class TradeDecisionFailedEvent(BaseModel):
    reason: str


class CancelTradeCommand(BaseModel):
    trade_id: str
    canceler_id: str


class TradeCancelledEvent(BaseModel):
    trade_id: str
    status: str
    target_owner_id: str


class TradeCancelFailedEvent(BaseModel):
    reason: str
