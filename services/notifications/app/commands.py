import asyncio

from pydantic import ValidationError

from app.database import SessionLocal
from app.kafka_producer import producer
from app.models import Notification
from app.repository import NotificationRepository
from app.schemas import (
    ListNotificationsCommand,
    NotificationItem,
    TradeAcceptedEvent,
    TradeCancelledEvent,
    TradeRejectedEvent,
    TradeRequestedEvent,
    UserRegisteredEvent,
)

TOPIC_LISTED = "notifications.listed"


def _create_notification(user_id: str, type: str, message: str) -> None:
    session = SessionLocal()
    try:
        NotificationRepository(session).create(user_id=user_id, type=type, message=message)
    finally:
        session.close()


async def handle_user_registered(payload: dict, correlation_id: str | None) -> None:
    try:
        event = UserRegisteredEvent.model_validate(payload)
    except ValidationError:
        return

    await asyncio.to_thread(
        _create_notification, event.user_id, "USER_REGISTERED", "Cadastro realizado com sucesso."
    )


async def handle_trade_requested(payload: dict, correlation_id: str | None) -> None:
    try:
        event = TradeRequestedEvent.model_validate(payload)
    except ValidationError:
        return

    await asyncio.to_thread(
        _create_notification,
        event.target_owner_id,
        "TRADE_REQUEST",
        "Você recebeu uma nova solicitação de troca.",
    )


async def handle_trade_accepted(payload: dict, correlation_id: str | None) -> None:
    try:
        event = TradeAcceptedEvent.model_validate(payload)
    except ValidationError:
        return

    await asyncio.to_thread(
        _create_notification,
        event.requester_id,
        "TRADE_ACCEPTED",
        "Sua solicitação de troca foi aceita.",
    )


async def handle_trade_rejected(payload: dict, correlation_id: str | None) -> None:
    try:
        event = TradeRejectedEvent.model_validate(payload)
    except ValidationError:
        return

    await asyncio.to_thread(
        _create_notification,
        event.requester_id,
        "TRADE_REJECTED",
        "Sua solicitação de troca foi recusada.",
    )


async def handle_trade_cancelled(payload: dict, correlation_id: str | None) -> None:
    try:
        event = TradeCancelledEvent.model_validate(payload)
    except ValidationError:
        return

    await asyncio.to_thread(
        _create_notification,
        event.target_owner_id,
        "TRADE_CANCELLED",
        "Uma solicitação de troca foi cancelada.",
    )


def _list_notifications(user_id: str) -> list[Notification]:
    session = SessionLocal()
    try:
        return NotificationRepository(session).list_by_user(user_id)
    finally:
        session.close()


async def handle_list(payload: dict, correlation_id: str | None) -> None:
    try:
        command = ListNotificationsCommand.model_validate(payload)
    except ValidationError:
        return

    notifications = await asyncio.to_thread(_list_notifications, command.user_id)

    items = [
        NotificationItem(
            id=str(n.id),
            type=n.type,
            message=n.message,
            created_at=n.created_at.isoformat(),
        ).model_dump()
        for n in notifications
    ]
    await producer.publish(TOPIC_LISTED, items, correlation_id)
