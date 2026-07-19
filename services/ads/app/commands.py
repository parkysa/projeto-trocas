from pydantic import ValidationError

from app.database import SessionLocal
from app.kafka_producer import producer
from app.models import Ad
from app.repository import AdRepository
from app.schemas import (
    AdCreatedEvent,
    AdDeletedEvent,
    FavoriteAdCommand,
    FavoriteAdEvent,
    FavoriteAdsListedEvent,
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
    ListFavoriteAdsCommand,
    ListAdsByOwnerCommand,
    ListAvailableAdsCommand,
    MarkAdUnavailableCommand,
    SearchAdsCommand,
    UpdateAdCommand,
)

TOPIC_CREATED = "ads.anuncio.criado"
TOPIC_LISTED = "ads.anuncio.listado"
TOPIC_UPDATED = "ads.anuncio.atualizado"
TOPIC_DELETED = "ads.anuncio.removido"
TOPIC_OPERATION_FAILED = "ads.anuncio.operacao_falhou"
TOPIC_AVAILABLE_LIST = "ads.anuncio.disponiveis_listados"
TOPIC_SEARCH_RESULT = "ads.anuncio.busca_concluida"
TOPIC_FOUND = "ads.anuncio.encontrado"
TOPIC_NOT_FOUND = "ads.anuncio.nao_encontrado"
TOPIC_FAVORITED = "ads.anuncio.favoritado"
TOPIC_UNFAVORITED = "ads.anuncio.desfavoritado"
TOPIC_FAVORITES_LISTED = "ads.anuncio.favoritos_listados"


async def _create_ad(command: CreateAdCommand) -> Ad:
    async with SessionLocal() as session:
        return await AdRepository(session).create(
            owner_id=command.owner_id,
            title=command.title,
            description=command.description,
            image=command.image,
            image_position=command.image_position,
            category=command.category,
            condition=command.condition,
            location=command.location,
            trade_terms=command.trade_terms,
        )


async def _list_ads_by_owner(owner_id: str) -> list[Ad]:
    async with SessionLocal() as session:
        return await AdRepository(session).list_by_owner(owner_id)


async def _update_ad(command: UpdateAdCommand) -> tuple[bool, Ad | str]:
    """Returns (True, updated Ad) on success, or (False, failure reason)."""
    async with SessionLocal() as session:
        repository = AdRepository(session)
        ad = await repository.get_by_id(command.ad_id)
        if ad is None:
            return False, "ad_not_found"
        if str(ad.owner_id) != command.owner_id:
            return False, "forbidden"
        if not ad.is_available:
            return False, "ad_unavailable"
        return True, await repository.update(
            ad,
            title=command.title,
            description=command.description,
            image=command.image,
            image_position=command.image_position,
            category=command.category,
            condition=command.condition,
            location=command.location,
            trade_terms=command.trade_terms,
        )


async def _delete_ad(command: DeleteAdCommand) -> tuple[bool, str]:
    """Returns (True, deleted ad id) on success, or (False, failure reason)."""
    async with SessionLocal() as session:
        repository = AdRepository(session)
        ad = await repository.get_by_id(command.ad_id)
        if ad is None:
            return False, "ad_not_found"
        if str(ad.owner_id) != command.owner_id:
            return False, "forbidden"

        if command.keep_record:
            await repository.mark_unavailable(command.ad_id)
            return True, str(ad.id)

        if not ad.is_available:
            return False, "ad_unavailable"
        ad_id = str(ad.id)
        await repository.delete(ad)
        return True, ad_id


async def handle_create(payload: dict, correlation_id: str | None) -> None:
    try:
        command = CreateAdCommand.model_validate(payload)
    except ValidationError:
        return

    ad = await _create_ad(command)

    event = AdCreatedEvent(
        id=str(ad.id),
        title=ad.title,
        description=ad.description,
        is_available=ad.is_available,
        image=ad.image,
        image_position=ad.image_position,
        category=ad.category,
        condition=ad.condition,
        location=ad.location,
        trade_terms=ad.trade_terms,
    )
    await producer.publish(TOPIC_CREATED, event.model_dump(), correlation_id)


async def handle_list_by_owner(payload: dict, correlation_id: str | None) -> None:
    try:
        command = ListAdsByOwnerCommand.model_validate(payload)
    except ValidationError:
        return

    ads = await _list_ads_by_owner(command.owner_id)

    event = AdsListedEvent(
        ads=[
            AdItem(
                id=str(ad.id),
                title=ad.title,
                description=ad.description,
                is_available=ad.is_available,
                image=ad.image,
                image_position=ad.image_position,
                category=ad.category,
                condition=ad.condition,
                location=ad.location,
                trade_terms=ad.trade_terms,
            )
            for ad in ads
        ]
    )
    await producer.publish(TOPIC_LISTED, event.model_dump(), correlation_id)


async def handle_update(payload: dict, correlation_id: str | None) -> None:
    try:
        command = UpdateAdCommand.model_validate(payload)
    except ValidationError:
        return

    success, result = await _update_ad(command)

    if not success:
        event = AdOperationFailedEvent(reason=result)
        await producer.publish(
            TOPIC_OPERATION_FAILED, event.model_dump(), correlation_id
        )
        return

    event = AdUpdatedEvent(
        id=str(result.id),
        title=result.title,
        description=result.description,
        is_available=result.is_available,
        image=result.image,
        image_position=result.image_position,
        category=result.category,
        condition=result.condition,
        location=result.location,
        trade_terms=result.trade_terms,
    )
    await producer.publish(TOPIC_UPDATED, event.model_dump(), correlation_id)


async def handle_delete(payload: dict, correlation_id: str | None) -> None:
    try:
        command = DeleteAdCommand.model_validate(payload)
    except ValidationError:
        return

    success, result = await _delete_ad(command)

    if not success:
        event = AdOperationFailedEvent(reason=result)
        await producer.publish(
            TOPIC_OPERATION_FAILED, event.model_dump(), correlation_id
        )
        return

    event = AdDeletedEvent(id=result)
    await producer.publish(TOPIC_DELETED, event.model_dump(), correlation_id)


async def _list_available(exclude_owner_id: str) -> list[Ad]:
    async with SessionLocal() as session:
        return await AdRepository(session).list_available(
            exclude_owner_id=exclude_owner_id
        )


async def _search_ads(command: SearchAdsCommand) -> list[Ad]:
    async with SessionLocal() as session:
        return await AdRepository(session).search_by_title(
            query=command.query, exclude_owner_id=command.owner_id
        )


async def handle_list_available(payload: dict, correlation_id: str | None) -> None:
    try:
        command = ListAvailableAdsCommand.model_validate(payload)
    except ValidationError:
        return

    ads = await _list_available(command.owner_id)

    items = [
        AvailableAdItem(
            id=str(ad.id),
            title=ad.title,
            description=ad.description,
            owner_id=str(ad.owner_id),
            image=ad.image,
            image_position=ad.image_position,
            category=ad.category,
            condition=ad.condition,
            location=ad.location,
            trade_terms=ad.trade_terms,
        ).model_dump()
        for ad in ads
    ]
    await producer.publish(TOPIC_AVAILABLE_LIST, items, correlation_id)


async def handle_search(payload: dict, correlation_id: str | None) -> None:
    try:
        command = SearchAdsCommand.model_validate(payload)
    except ValidationError:
        return

    ads = await _search_ads(command)

    items = [
        AvailableAdItem(
            id=str(ad.id),
            title=ad.title,
            description=ad.description,
            owner_id=str(ad.owner_id),
            image=ad.image,
            image_position=ad.image_position,
            category=ad.category,
            condition=ad.condition,
            location=ad.location,
            trade_terms=ad.trade_terms,
        ).model_dump()
        for ad in ads
    ]
    await producer.publish(TOPIC_SEARCH_RESULT, items, correlation_id)


async def _get_ad(ad_id: str) -> Ad | None:
    async with SessionLocal() as session:
        return await AdRepository(session).get_by_id(ad_id)


async def handle_get_by_id(payload: dict, correlation_id: str | None) -> None:
    """Internal lookup used by other services (e.g. Trades) to resolve an ad by id."""
    try:
        command = GetAdByIdCommand.model_validate(payload)
    except ValidationError:
        return

    ad = await _get_ad(command.ad_id)

    if ad is None:
        event = AdNotFoundEvent(ad_id=command.ad_id)
        await producer.publish(TOPIC_NOT_FOUND, event.model_dump(), correlation_id)
        return

    event = AdFoundEvent(
        id=str(ad.id),
        owner_id=str(ad.owner_id),
        title=ad.title,
        description=ad.description,
        is_available=ad.is_available,
        image=ad.image,
        image_position=ad.image_position,
        category=ad.category,
        condition=ad.condition,
        location=ad.location,
        trade_terms=ad.trade_terms,
    )
    await producer.publish(TOPIC_FOUND, event.model_dump(), correlation_id)


async def _mark_unavailable(ad_id: str) -> None:
    async with SessionLocal() as session:
        await AdRepository(session).mark_unavailable(ad_id)


async def handle_mark_unavailable(payload: dict, correlation_id: str | None) -> None:
    """Internal, fire-and-forget: used by Trades to flag an ad as no longer tradeable."""
    try:
        command = MarkAdUnavailableCommand.model_validate(payload)
    except ValidationError:
        return

    await _mark_unavailable(command.ad_id)


async def handle_favorite(payload: dict, correlation_id: str | None) -> None:
    try:
        command = FavoriteAdCommand.model_validate(payload)
    except ValidationError:
        return

    async with SessionLocal() as session:
        repository = AdRepository(session)
        ad = await repository.get_by_id(command.ad_id)
        if ad is None:
            event = AdOperationFailedEvent(reason="ad_not_found")
            await producer.publish(TOPIC_OPERATION_FAILED, event.model_dump(), correlation_id)
            return

        if not ad.is_available:
            event = AdOperationFailedEvent(reason="ad_unavailable")
            await producer.publish(TOPIC_OPERATION_FAILED, event.model_dump(), correlation_id)
            return

        if str(ad.owner_id) == command.user_id:
            event = AdOperationFailedEvent(reason="cannot_favorite_own_ad")
            await producer.publish(TOPIC_OPERATION_FAILED, event.model_dump(), correlation_id)
            return

        await repository.favorite(command.user_id, command.ad_id)

    event = FavoriteAdEvent(ad_id=command.ad_id)
    await producer.publish(TOPIC_FAVORITED, event.model_dump(), correlation_id)


async def handle_unfavorite(payload: dict, correlation_id: str | None) -> None:
    try:
        command = FavoriteAdCommand.model_validate(payload)
    except ValidationError:
        return

    async with SessionLocal() as session:
        await AdRepository(session).unfavorite(command.user_id, command.ad_id)

    event = FavoriteAdEvent(ad_id=command.ad_id)
    await producer.publish(TOPIC_UNFAVORITED, event.model_dump(), correlation_id)


async def handle_list_favorites(payload: dict, correlation_id: str | None) -> None:
    try:
        command = ListFavoriteAdsCommand.model_validate(payload)
    except ValidationError:
        return

    async with SessionLocal() as session:
        ad_ids = await AdRepository(session).list_favorite_ids(command.user_id)

    event = FavoriteAdsListedEvent(ad_ids=ad_ids)
    await producer.publish(TOPIC_FAVORITES_LISTED, event.model_dump(), correlation_id)
