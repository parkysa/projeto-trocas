from pydantic import ValidationError

from app.ads_client import ads_client
from app.database import SessionLocal
from app.kafka_producer import producer
from app.models import Trade
from app.repository import TradeRepository
from app.schemas import (
    AcceptTradeCommand,
    CancelTradeCommand,
    ListTradesByRequesterCommand,
    ListTradesForTargetOwnerCommand,
    RejectTradeCommand,
    RequestTradeCommand,
    TradeAcceptedEvent,
    TradeCancelFailedEvent,
    TradeCancelledEvent,
    TradeDecisionFailedEvent,
    TradeListItem,
    TradeRejectedEvent,
    TradesListedEvent,
    TradeRequestedEvent,
    TradeRequestFailedEvent,
)

TOPIC_REQUESTED = "trades.troca.solicitada"
TOPIC_REQUEST_FAILED = "trades.troca.solicitacao_falhou"
TOPIC_ACCEPTED = "trades.troca.aprovada"
TOPIC_REJECTED = "trades.troca.recusada"
TOPIC_DECISION_FAILED = "trades.troca.decisao_falhou"
TOPIC_CANCELLED = "trades.troca.cancelada"
TOPIC_CANCEL_FAILED = "trades.troca.cancelamento_falhou"
TOPIC_LISTED_BY_REQUESTER = "trades.troca.de_mim_listadas"
TOPIC_LISTED_FOR_TARGET_OWNER = "trades.troca.para_mim_listadas"


async def _create_trade(command: RequestTradeCommand) -> Trade:
    async with SessionLocal() as session:
        return await TradeRepository(session).create(
            requester_id=command.requester_id,
            requester_ad_id=command.requester_ad_id,
            target_ad_id=command.target_ad_id,
        )


async def _has_accepted_trade_for_any_ad(*ad_ids: str) -> bool:
    async with SessionLocal() as session:
        repository = TradeRepository(session)
        for ad_id in ad_ids:
            if await repository.has_accepted_trade_for_ad(ad_id):
                return True
        return False


async def _has_non_cancelled_trade_for_same_items(ad_a: str, ad_b: str) -> bool:
    async with SessionLocal() as session:
        repository = TradeRepository(session)
        return await repository.has_non_cancelled_trade_for_same_items(ad_a, ad_b)


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
    if not requester_ad.get("is_available", True):
        await _publish_failure("requester_ad_unavailable", correlation_id)
        return

    target_ad = await ads_client.get_ad_by_id(command.target_ad_id)
    if target_ad is None:
        await _publish_failure("target_ad_not_found", correlation_id)
        return
    if not target_ad.get("is_available", True):
        await _publish_failure("target_ad_unavailable", correlation_id)
        return

    if target_ad["owner_id"] == command.requester_id:
        await _publish_failure("cannot_request_own_ad", correlation_id)
        return

    if await _has_accepted_trade_for_any_ad(
        command.requester_ad_id, command.target_ad_id
    ):
        await _publish_failure("ad_already_traded", correlation_id)
        return

    if await _has_non_cancelled_trade_for_same_items(
        command.requester_ad_id, command.target_ad_id
    ):
        await _publish_failure("duplicate_trade_not_allowed", correlation_id)
        return

    trade = await _create_trade(command)

    event = TradeRequestedEvent(
        trade_id=str(trade.id), status=trade.status, target_owner_id=target_ad["owner_id"]
    )
    await producer.publish(TOPIC_REQUESTED, event.model_dump(), correlation_id)


async def _load_trade(trade_id: str) -> Trade | None:
    async with SessionLocal() as session:
        return await TradeRepository(session).get_by_id(trade_id)


async def _accept_trade(trade_id: str) -> tuple[bool, Trade | str]:
    """Re-validates status/conflicts and updates to ACCEPTED, atomically within one session."""
    async with SessionLocal() as session:
        repository = TradeRepository(session)
        trade = await repository.get_by_id(trade_id)
        if trade is None:
            return False, "trade_not_found"
        if trade.status != "PENDING":
            return False, "trade_not_pending"
        if await repository.has_accepted_trade_for_ad(
            str(trade.requester_ad_id)
        ) or await repository.has_accepted_trade_for_ad(str(trade.target_ad_id)):
            return False, "ad_already_traded"
        trade.status = "ACCEPTED"
        await repository.cancel_other_pending_for_ads(
            accepted_trade_id=str(trade.id),
            ad_ids=[str(trade.requester_ad_id), str(trade.target_ad_id)],
        )
        await session.commit()
        await session.refresh(trade)
        return True, trade


async def _reject_trade(trade_id: str) -> tuple[bool, Trade | str]:
    async with SessionLocal() as session:
        repository = TradeRepository(session)
        trade = await repository.get_by_id(trade_id)
        if trade is None:
            return False, "trade_not_found"
        if trade.status != "PENDING":
            return False, "trade_not_pending"
        return True, await repository.update_status(trade, status="REJECTED")


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

    trade = await _load_trade(command.trade_id)
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

    success, result = await _accept_trade(command.trade_id)
    if not success:
        await _publish_decision_failed(result, correlation_id)
        return

    await ads_client.mark_unavailable(str(result.requester_ad_id))
    await ads_client.mark_unavailable(str(result.target_ad_id))

    event = TradeAcceptedEvent(
        trade_id=str(result.id), status=result.status, requester_id=str(result.requester_id)
    )
    await producer.publish(TOPIC_ACCEPTED, event.model_dump(), correlation_id)


async def handle_reject(payload: dict, correlation_id: str | None) -> None:
    try:
        command = RejectTradeCommand.model_validate(payload)
    except ValidationError:
        return

    trade = await _load_trade(command.trade_id)
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

    success, result = await _reject_trade(command.trade_id)
    if not success:
        await _publish_decision_failed(result, correlation_id)
        return

    event = TradeRejectedEvent(
        trade_id=str(result.id), status=result.status, requester_id=str(result.requester_id)
    )
    await producer.publish(TOPIC_REJECTED, event.model_dump(), correlation_id)


async def _cancel_trade(command: CancelTradeCommand) -> tuple[bool, Trade | str]:
    async with SessionLocal() as session:
        repository = TradeRepository(session)
        trade = await repository.get_by_id(command.trade_id)
        if trade is None:
            return False, "trade_not_found"
        if trade.status != "PENDING":
            return False, "trade_not_pending"
        if str(trade.requester_id) != command.canceler_id:
            return False, "forbidden"
        return True, await repository.update_status(trade, status="CANCELLED")


async def handle_cancel(payload: dict, correlation_id: str | None) -> None:
    try:
        command = CancelTradeCommand.model_validate(payload)
    except ValidationError:
        return

    success, result = await _cancel_trade(command)
    if not success:
        event = TradeCancelFailedEvent(reason=result)
        await producer.publish(TOPIC_CANCEL_FAILED, event.model_dump(), correlation_id)
        return

    target_ad = await ads_client.get_ad_by_id(str(result.target_ad_id))
    target_owner_id = target_ad["owner_id"] if target_ad is not None else ""

    event = TradeCancelledEvent(
        trade_id=str(result.id), status=result.status, target_owner_id=target_owner_id
    )
    await producer.publish(TOPIC_CANCELLED, event.model_dump(), correlation_id)


async def handle_list_by_requester(payload: dict, correlation_id: str | None) -> None:
    try:
        command = ListTradesByRequesterCommand.model_validate(payload)
    except ValidationError:
        return

    async with SessionLocal() as session:
        trades = await TradeRepository(session).list_by_requester(command.requester_id)

    event = TradesListedEvent(
        trades=[
            TradeListItem(
                id=str(trade.id),
                requester_id=str(trade.requester_id),
                requester_ad_id=str(trade.requester_ad_id),
                target_ad_id=str(trade.target_ad_id),
                status=trade.status,
                created_at=trade.created_at.date().isoformat(),
            )
            for trade in trades
        ]
    )
    await producer.publish(TOPIC_LISTED_BY_REQUESTER, event.model_dump(), correlation_id)


async def handle_list_for_target_owner(payload: dict, correlation_id: str | None) -> None:
    try:
        command = ListTradesForTargetOwnerCommand.model_validate(payload)
    except ValidationError:
        return

    async with SessionLocal() as session:
        trades = await TradeRepository(session).list_all()

    visible: list[TradeListItem] = []
    for trade in trades:
        target_ad = await ads_client.get_ad_by_id(str(trade.target_ad_id))
        if target_ad is None:
            continue
        if str(target_ad.get("owner_id")) != command.owner_id:
            continue

        visible.append(
            TradeListItem(
                id=str(trade.id),
                requester_id=str(trade.requester_id),
                requester_ad_id=str(trade.requester_ad_id),
                target_ad_id=str(trade.target_ad_id),
                status=trade.status,
                created_at=trade.created_at.date().isoformat(),
            )
        )

    event = TradesListedEvent(trades=visible)
    await producer.publish(
        TOPIC_LISTED_FOR_TARGET_OWNER, event.model_dump(), correlation_id
    )
