import asyncio

from pydantic import ValidationError

from app.ads_client import ads_client
from app.database import SessionLocal
from app.kafka_producer import producer
from app.models import Trade
from app.repository import TradeRepository
from app.schemas import (
    AcceptTradeCommand,
    CancelTradeCommand,
    RejectTradeCommand,
    RequestTradeCommand,
    TradeAcceptedEvent,
    TradeCancelFailedEvent,
    TradeCancelledEvent,
    TradeDecisionFailedEvent,
    TradeRejectedEvent,
    TradeRequestedEvent,
    TradeRequestFailedEvent,
)

TOPIC_REQUESTED = "trades.requested"
TOPIC_REQUEST_FAILED = "trades.request_failed"
TOPIC_ACCEPTED = "trades.accepted"
TOPIC_REJECTED = "trades.rejected"
TOPIC_DECISION_FAILED = "trades.decision_failed"
TOPIC_CANCELLED = "trades.cancelled"
TOPIC_CANCEL_FAILED = "trades.cancel_failed"


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


def _load_trade(trade_id: str) -> Trade | None:
    session = SessionLocal()
    try:
        return TradeRepository(session).get_by_id(trade_id)
    finally:
        session.close()


def _accept_trade(trade_id: str) -> tuple[bool, Trade | str]:
    """Re-validates status/conflicts and updates to ACCEPTED, atomically within one session."""
    session = SessionLocal()
    try:
        repository = TradeRepository(session)
        trade = repository.get_by_id(trade_id)
        if trade is None:
            return False, "trade_not_found"
        if trade.status != "PENDING":
            return False, "trade_not_pending"
        if repository.has_accepted_trade_for_ad(
            str(trade.requester_ad_id)
        ) or repository.has_accepted_trade_for_ad(str(trade.target_ad_id)):
            return False, "ad_already_traded"
        return True, repository.update_status(trade, status="ACCEPTED")
    finally:
        session.close()


def _reject_trade(trade_id: str) -> tuple[bool, Trade | str]:
    session = SessionLocal()
    try:
        repository = TradeRepository(session)
        trade = repository.get_by_id(trade_id)
        if trade is None:
            return False, "trade_not_found"
        if trade.status != "PENDING":
            return False, "trade_not_pending"
        return True, repository.update_status(trade, status="REJECTED")
    finally:
        session.close()


async def _publish_decision_failed(reason: str, correlation_id: str | None) -> None:
    event = TradeDecisionFailedEvent(reason=reason)
    await producer.publish(TOPIC_DECISION_FAILED, event.model_dump(), correlation_id)


async def _authorize_decision(trade: Trade, decider_id: str) -> str | None:
    """Returns a failure reason if the decider is not the target ad's owner, else None."""
    target_ad = await ads_client.get_ad_by_id(str(trade.target_ad_id))
    if target_ad is None:
        return "target_ad_not_found"
    if target_ad["owner_id"] != decider_id:
        return "forbidden"
    return None


async def handle_accept(payload: dict, correlation_id: str | None) -> None:
    try:
        command = AcceptTradeCommand.model_validate(payload)
    except ValidationError:
        return

    trade = await asyncio.to_thread(_load_trade, command.trade_id)
    if trade is None:
        await _publish_decision_failed("trade_not_found", correlation_id)
        return
    if trade.status != "PENDING":
        await _publish_decision_failed("trade_not_pending", correlation_id)
        return

    failure_reason = await _authorize_decision(trade, command.decider_id)
    if failure_reason is not None:
        await _publish_decision_failed(failure_reason, correlation_id)
        return

    success, result = await asyncio.to_thread(_accept_trade, command.trade_id)
    if not success:
        await _publish_decision_failed(result, correlation_id)
        return

    await ads_client.mark_unavailable(str(result.requester_ad_id))
    await ads_client.mark_unavailable(str(result.target_ad_id))

    event = TradeAcceptedEvent(trade_id=str(result.id), status=result.status)
    await producer.publish(TOPIC_ACCEPTED, event.model_dump(), correlation_id)


async def handle_reject(payload: dict, correlation_id: str | None) -> None:
    try:
        command = RejectTradeCommand.model_validate(payload)
    except ValidationError:
        return

    trade = await asyncio.to_thread(_load_trade, command.trade_id)
    if trade is None:
        await _publish_decision_failed("trade_not_found", correlation_id)
        return
    if trade.status != "PENDING":
        await _publish_decision_failed("trade_not_pending", correlation_id)
        return

    failure_reason = await _authorize_decision(trade, command.decider_id)
    if failure_reason is not None:
        await _publish_decision_failed(failure_reason, correlation_id)
        return

    success, result = await asyncio.to_thread(_reject_trade, command.trade_id)
    if not success:
        await _publish_decision_failed(result, correlation_id)
        return

    event = TradeRejectedEvent(trade_id=str(result.id), status=result.status)
    await producer.publish(TOPIC_REJECTED, event.model_dump(), correlation_id)


def _cancel_trade(command: CancelTradeCommand) -> tuple[bool, Trade | str]:
    session = SessionLocal()
    try:
        repository = TradeRepository(session)
        trade = repository.get_by_id(command.trade_id)
        if trade is None:
            return False, "trade_not_found"
        if trade.status != "PENDING":
            return False, "trade_not_pending"
        if str(trade.requester_id) != command.canceler_id:
            return False, "forbidden"
        return True, repository.update_status(trade, status="CANCELLED")
    finally:
        session.close()


async def handle_cancel(payload: dict, correlation_id: str | None) -> None:
    try:
        command = CancelTradeCommand.model_validate(payload)
    except ValidationError:
        return

    success, result = await asyncio.to_thread(_cancel_trade, command)
    if not success:
        event = TradeCancelFailedEvent(reason=result)
        await producer.publish(TOPIC_CANCEL_FAILED, event.model_dump(), correlation_id)
        return

    event = TradeCancelledEvent(trade_id=str(result.id), status=result.status)
    await producer.publish(TOPIC_CANCELLED, event.model_dump(), correlation_id)
