import asyncio

from pydantic import ValidationError

from app.database import SessionLocal
from app.kafka_producer import producer
from app.models import Ad
from app.repository import AdRepository
from app.schemas import (
    AdCreatedEvent,
    AdDeletedEvent,
    AdItem,
    AdOperationFailedEvent,
    AdsListedEvent,
    AdUpdatedEvent,
    CreateAdCommand,
    DeleteAdCommand,
    ListAdsByOwnerCommand,
    UpdateAdCommand,
)

TOPIC_CREATED = "ads.created"
TOPIC_LISTED = "ads.listed"
TOPIC_UPDATED = "ads.updated"
TOPIC_DELETED = "ads.deleted"
TOPIC_OPERATION_FAILED = "ads.operation_failed"


def _create_ad(command: CreateAdCommand) -> Ad:
    session = SessionLocal()
    try:
        return AdRepository(session).create(
            owner_id=command.owner_id,
            title=command.title,
            description=command.description,
        )
    finally:
        session.close()


def _list_ads_by_owner(owner_id: str) -> list[Ad]:
    session = SessionLocal()
    try:
        return AdRepository(session).list_by_owner(owner_id)
    finally:
        session.close()


def _update_ad(command: UpdateAdCommand) -> tuple[bool, Ad | str]:
    """Returns (True, updated Ad) on success, or (False, failure reason)."""
    session = SessionLocal()
    try:
        repository = AdRepository(session)
        ad = repository.get_by_id(command.ad_id)
        if ad is None:
            return False, "ad_not_found"
        if str(ad.owner_id) != command.owner_id:
            return False, "forbidden"
        return True, repository.update(
            ad, title=command.title, description=command.description
        )
    finally:
        session.close()


def _delete_ad(command: DeleteAdCommand) -> tuple[bool, str]:
    """Returns (True, deleted ad id) on success, or (False, failure reason)."""
    session = SessionLocal()
    try:
        repository = AdRepository(session)
        ad = repository.get_by_id(command.ad_id)
        if ad is None:
            return False, "ad_not_found"
        if str(ad.owner_id) != command.owner_id:
            return False, "forbidden"
        ad_id = str(ad.id)
        repository.delete(ad)
        return True, ad_id
    finally:
        session.close()


async def handle_create(payload: dict, correlation_id: str | None) -> None:
    try:
        command = CreateAdCommand.model_validate(payload)
    except ValidationError:
        return

    ad = await asyncio.to_thread(_create_ad, command)

    event = AdCreatedEvent(id=str(ad.id), title=ad.title, description=ad.description)
    await producer.publish(TOPIC_CREATED, event.model_dump(), correlation_id)


async def handle_list_by_owner(payload: dict, correlation_id: str | None) -> None:
    try:
        command = ListAdsByOwnerCommand.model_validate(payload)
    except ValidationError:
        return

    ads = await asyncio.to_thread(_list_ads_by_owner, command.owner_id)

    event = AdsListedEvent(
        ads=[
            AdItem(id=str(ad.id), title=ad.title, description=ad.description)
            for ad in ads
        ]
    )
    await producer.publish(TOPIC_LISTED, event.model_dump(), correlation_id)


async def handle_update(payload: dict, correlation_id: str | None) -> None:
    try:
        command = UpdateAdCommand.model_validate(payload)
    except ValidationError:
        return

    success, result = await asyncio.to_thread(_update_ad, command)

    if not success:
        event = AdOperationFailedEvent(reason=result)
        await producer.publish(
            TOPIC_OPERATION_FAILED, event.model_dump(), correlation_id
        )
        return

    event = AdUpdatedEvent(
        id=str(result.id), title=result.title, description=result.description
    )
    await producer.publish(TOPIC_UPDATED, event.model_dump(), correlation_id)


async def handle_delete(payload: dict, correlation_id: str | None) -> None:
    try:
        command = DeleteAdCommand.model_validate(payload)
    except ValidationError:
        return

    success, result = await asyncio.to_thread(_delete_ad, command)

    if not success:
        event = AdOperationFailedEvent(reason=result)
        await producer.publish(
            TOPIC_OPERATION_FAILED, event.model_dump(), correlation_id
        )
        return

    event = AdDeletedEvent(id=result)
    await producer.publish(TOPIC_DELETED, event.model_dump(), correlation_id)
