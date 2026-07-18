import asyncio

from pydantic import ValidationError

from app.database import SessionLocal
from app.kafka_producer import producer
from app.models import Ad
from app.repository import AdRepository
from app.schemas import (
    AdCreatedEvent,
    AdDeletedEvent,
    AdFoundEvent,
    AdItem,
    AdNotFoundEvent,
    AdOperationFailedEvent,
    AdsListedEvent,
    AdUpdatedEvent,
    AvailableAdItem,
    CreateAdCommand,
    DeleteAdCommand,
    GetAdByIdCommand,
    ListAdsByOwnerCommand,
    ListAvailableAdsCommand,
    MarkAdUnavailableCommand,
    SearchAdsCommand,
    UpdateAdCommand,
)

TOPIC_CREATED = "ads.created"
TOPIC_LISTED = "ads.listed"
TOPIC_UPDATED = "ads.updated"
TOPIC_DELETED = "ads.deleted"
TOPIC_OPERATION_FAILED = "ads.operation_failed"
TOPIC_AVAILABLE_LIST = "ads.available_list"
TOPIC_SEARCH_RESULT = "ads.search_result"
TOPIC_FOUND = "ads.found"
TOPIC_NOT_FOUND = "ads.not_found"


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


def _list_available(exclude_owner_id: str) -> list[Ad]:
    session = SessionLocal()
    try:
        return AdRepository(session).list_available(exclude_owner_id=exclude_owner_id)
    finally:
        session.close()


def _search_ads(command: SearchAdsCommand) -> list[Ad]:
    session = SessionLocal()
    try:
        return AdRepository(session).search_by_title(
            query=command.query, exclude_owner_id=command.owner_id
        )
    finally:
        session.close()


async def handle_list_available(payload: dict, correlation_id: str | None) -> None:
    try:
        command = ListAvailableAdsCommand.model_validate(payload)
    except ValidationError:
        return

    ads = await asyncio.to_thread(_list_available, command.owner_id)

    items = [
        AvailableAdItem(
            id=str(ad.id),
            title=ad.title,
            description=ad.description,
            owner_id=str(ad.owner_id),
        ).model_dump()
        for ad in ads
    ]
    await producer.publish(TOPIC_AVAILABLE_LIST, items, correlation_id)


async def handle_search(payload: dict, correlation_id: str | None) -> None:
    try:
        command = SearchAdsCommand.model_validate(payload)
    except ValidationError:
        return

    ads = await asyncio.to_thread(_search_ads, command)

    items = [
        AvailableAdItem(
            id=str(ad.id),
            title=ad.title,
            description=ad.description,
            owner_id=str(ad.owner_id),
        ).model_dump()
        for ad in ads
    ]
    await producer.publish(TOPIC_SEARCH_RESULT, items, correlation_id)


def _get_ad(ad_id: str) -> Ad | None:
    session = SessionLocal()
    try:
        return AdRepository(session).get_by_id(ad_id)
    finally:
        session.close()


async def handle_get_by_id(payload: dict, correlation_id: str | None) -> None:
    """Internal lookup used by other services (e.g. Trades) to resolve an ad by id."""
    try:
        command = GetAdByIdCommand.model_validate(payload)
    except ValidationError:
        return

    ad = await asyncio.to_thread(_get_ad, command.ad_id)

    if ad is None:
        event = AdNotFoundEvent(ad_id=command.ad_id)
        await producer.publish(TOPIC_NOT_FOUND, event.model_dump(), correlation_id)
        return

    event = AdFoundEvent(
        id=str(ad.id), owner_id=str(ad.owner_id), title=ad.title, description=ad.description
    )
    await producer.publish(TOPIC_FOUND, event.model_dump(), correlation_id)


def _mark_unavailable(ad_id: str) -> None:
    session = SessionLocal()
    try:
        AdRepository(session).mark_unavailable(ad_id)
    finally:
        session.close()


async def handle_mark_unavailable(payload: dict, correlation_id: str | None) -> None:
    """Internal, fire-and-forget: used by Trades to flag an ad as no longer tradeable."""
    try:
        command = MarkAdUnavailableCommand.model_validate(payload)
    except ValidationError:
        return

    await asyncio.to_thread(_mark_unavailable, command.ad_id)
