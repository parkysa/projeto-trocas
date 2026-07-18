import asyncio

from pydantic import ValidationError

from app.ads_client import ads_client
from app.database import SessionLocal
from app.kafka_producer import producer
from app.models import Trade
from app.repository import TradeRepository
from app.schemas import (
    RequestTradeCommand,
    TradeRequestedEvent,
    TradeRequestFailedEvent,
)

TOPIC_REQUESTED = "trades.requested"
TOPIC_REQUEST_FAILED = "trades.request_failed"


def _create_trade(command: RequestTradeCommand) -> Trade:
    session = SessionLocal()
    try:
        return TradeRepository(session).create(
            requester_id=command.requester_id,
            requester_ad_id=command.requester_ad_id,
            target_ad_id=command.target_ad_id,
        )
    finally:
        session.close()


async def _publish_failure(reason: str, correlation_id: str | None) -> None:
    event = TradeRequestFailedEvent(reason=reason)
    await producer.publish(TOPIC_REQUEST_FAILED, event.model_dump(), correlation_id)


async def handle_request(payload: dict, correlation_id: str | None) -> None:
    try:
        command = RequestTradeCommand.model_validate(payload)
    except ValidationError:
        return

    requester_ad = await ads_client.get_ad_by_id(command.requester_ad_id)
    if requester_ad is None:
        await _publish_failure("requester_ad_not_found", correlation_id)
        return

    target_ad = await ads_client.get_ad_by_id(command.target_ad_id)
    if target_ad is None:
        await _publish_failure("target_ad_not_found", correlation_id)
        return

    if target_ad["owner_id"] == command.requester_id:
        await _publish_failure("cannot_request_own_ad", correlation_id)
        return

    trade = await asyncio.to_thread(_create_trade, command)

    event = TradeRequestedEvent(trade_id=str(trade.id), status=trade.status)
    await producer.publish(TOPIC_REQUESTED, event.model_dump(), correlation_id)
