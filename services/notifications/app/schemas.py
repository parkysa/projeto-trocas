from pydantic import BaseModel


class UserRegisteredEvent(BaseModel):
    user_id: str
    email: str


class TradeRequestedEvent(BaseModel):
    trade_id: str
    status: str
    target_owner_id: str


class TradeAcceptedEvent(BaseModel):
    trade_id: str
    status: str
    requester_id: str


class TradeRejectedEvent(BaseModel):
    trade_id: str
    status: str
    requester_id: str


class TradeCancelledEvent(BaseModel):
    trade_id: str
    status: str
    target_owner_id: str


class ListNotificationsCommand(BaseModel):
    user_id: str


class NotificationItem(BaseModel):
    id: str
    type: str
    message: str
    created_at: str
