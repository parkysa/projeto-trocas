import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from app.kafka_client import client
from app.schemas import (
    AdIdRequest,
    AdResponse,
    AdSearchResult,
    CreateAdRequest,
    CreateTradeRequest,
    FavoriteAdRequest,
    FavoriteIdsResponse,
    LoginRequest,
    LoginResponse,
    NotificationResponse,
    ProfileResponse,
    RegisterRequest,
    RegisterResponse,
    SearchAdsRequest,
    TradeDecisionResponse,
    TradeItemDetailResponse,
    TradeViewResponse,
    TradeResponse,
    UpdateAdRequest,
    UpdateProfileRequest,
)
from app.security import AuthenticationError, decode_user_id

TOPIC_REGISTER = "users.usuario.cadastrar"
TOPIC_REGISTRATION_FAILED = "users.usuario.cadastro_falhou"
TOPIC_LOGIN = "users.usuario.autenticar"
TOPIC_AUTHENTICATION_FAILED = "users.usuario.autenticacao_falhou"
TOPIC_GET_PROFILE = "users.perfil.consultar"
TOPIC_UPDATE_PROFILE = "users.perfil.atualizar"
TOPIC_PROFILE_UPDATE_FAILED = "users.perfil.atualizacao_falhou"
TOPIC_ADS_CREATE = "ads.anuncio.criar"
TOPIC_ADS_LIST_BY_OWNER = "ads.anuncio.consultar_proprios"
TOPIC_ADS_UPDATE = "ads.anuncio.atualizar"
TOPIC_ADS_DELETE = "ads.anuncio.remover"
TOPIC_ADS_OPERATION_FAILED = "ads.anuncio.operacao_falhou"
TOPIC_ADS_LIST_AVAILABLE = "ads.anuncio.consultar_disponiveis"
TOPIC_ADS_SEARCH = "ads.anuncio.buscar"
TOPIC_ADS_FAVORITE = "ads.anuncio.favoritar"
TOPIC_ADS_UNFAVORITE = "ads.anuncio.desfavoritar"
TOPIC_ADS_LIST_FAVORITES = "ads.anuncio.consultar_favoritos"
TOPIC_TRADES_REQUEST = "trades.troca.solicitar"
TOPIC_TRADES_REQUEST_FAILED = "trades.troca.solicitacao_falhou"
TOPIC_TRADES_ACCEPT = "trades.troca.aceitar"
TOPIC_TRADES_REJECT = "trades.troca.recusar"
TOPIC_TRADES_DECISION_FAILED = "trades.troca.decisao_falhou"
TOPIC_TRADES_CANCEL = "trades.troca.cancelar"
TOPIC_TRADES_CANCEL_FAILED = "trades.troca.cancelamento_falhou"
TOPIC_TRADES_LIST_BY_REQUESTER = "trades.troca.consultar_de_mim"
TOPIC_TRADES_LIST_FOR_TARGET_OWNER = "trades.troca.consultar_para_mim"
TOPIC_ADS_GET_BY_ID = "ads.anuncio.consultar_por_id"
TOPIC_NOTIFICATIONS_LIST = "notifications.notificacao.consultar"

_PUBLIC_TOPICS = {TOPIC_REGISTER, TOPIC_LOGIN}


def _nickname_from_name(name: str) -> str:
    trimmed = name.strip()
    if not trimmed:
        return "Usuario"
    parts = trimmed.split()
    return " ".join(parts[:2])


async def _resolve_owner_name(owner_id: str) -> str:
    try:
        topic, response = await client.request(
            TOPIC_GET_PROFILE, {"user_id": owner_id}, tipo="Consulta"
        )
        if topic == "users.perfil.encontrado":
            return _nickname_from_name(str(response.get("name", "")))
    except TimeoutError:
        pass
    return "Usuario"


async def _attach_owner_name(ads: list[dict]) -> list[dict]:
    cache: dict[str, str] = {}
    enriched: list[dict] = []

    for ad in ads:
        owner_id = str(ad.get("owner_id", ""))
        if owner_id not in cache:
            cache[owner_id] = await _resolve_owner_name(owner_id)
        enriched.append({**ad, "owner_name": cache[owner_id]})

    return enriched


async def _handle_register(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = RegisterRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_REGISTER, request.model_dump(), tipo="Comando"
    )
    if topic == TOPIC_REGISTRATION_FAILED:
        return topic, response
    return topic, RegisterResponse(
        id=response["user_id"],
        name=request.name,
        email=response["email"],
        phone=response.get("phone", request.phone),
    ).model_dump()


async def _handle_login(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = LoginRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_LOGIN, request.model_dump(), tipo="Comando"
    )
    if topic == TOPIC_AUTHENTICATION_FAILED:
        return topic, response
    return topic, LoginResponse(
        access_token=response["token"], token_type="bearer"
    ).model_dump()


async def _handle_get_profile(user_id: str | None, payload: dict) -> tuple[str, dict]:
    topic, response = await client.request(
        TOPIC_GET_PROFILE, {"user_id": user_id}, tipo="Consulta"
    )
    return topic, ProfileResponse(**response).model_dump()


async def _handle_update_profile(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = UpdateProfileRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_UPDATE_PROFILE,
        {
            "user_id": user_id,
            "name": request.name,
            "email": request.email,
            "phone": request.phone,
        },
        tipo="Comando",
    )
    if topic == TOPIC_PROFILE_UPDATE_FAILED:
        return topic, response
    return topic, ProfileResponse(**response).model_dump()


async def _handle_create_ad(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = CreateAdRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_ADS_CREATE,
        {
            "owner_id": user_id,
            "title": request.title,
            "description": request.description,
            "image": request.image,
            "image_position": request.image_position,
            "category": request.category,
            "condition": request.condition,
            "location": request.location,
            "trade_terms": request.trade_terms,
        },
        tipo="Comando",
    )
    return topic, AdResponse(**response).model_dump()


async def _handle_list_ads(user_id: str | None, payload: dict) -> tuple[str, list]:
    topic, response = await client.request(
        TOPIC_ADS_LIST_BY_OWNER, {"owner_id": user_id}, tipo="Consulta"
    )
    return topic, [AdResponse(**ad).model_dump() for ad in response["ads"]]


async def _handle_update_ad(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = UpdateAdRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_ADS_UPDATE,
        {
            "ad_id": request.id,
            "owner_id": user_id,
            "title": request.title,
            "description": request.description,
            "image": request.image,
            "image_position": request.image_position,
            "category": request.category,
            "condition": request.condition,
            "location": request.location,
            "trade_terms": request.trade_terms,
        },
        tipo="Comando",
    )
    if topic == TOPIC_ADS_OPERATION_FAILED:
        return topic, response
    return topic, AdResponse(**response).model_dump()


async def _handle_delete_ad(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = AdIdRequest.model_validate(payload)

    topic_from_me, response_from_me = await client.request(
        TOPIC_TRADES_LIST_BY_REQUESTER,
        {"requester_id": user_id},
        tipo="Consulta",
    )
    topic_for_me, response_for_me = await client.request(
        TOPIC_TRADES_LIST_FOR_TARGET_OWNER,
        {"owner_id": user_id},
        tipo="Consulta",
    )

    trades_from_me = (
        response_from_me.get("trades", [])
        if topic_from_me == "trades.troca.de_mim_listadas" and isinstance(response_from_me, dict)
        else []
    )
    trades_for_me = (
        response_for_me.get("trades", [])
        if topic_for_me == "trades.troca.para_mim_listadas" and isinstance(response_for_me, dict)
        else []
    )

    pending_from_me_ids = [
        str(trade.get("id", ""))
        for trade in trades_from_me
        if str(trade.get("status", "")).upper() == "PENDING"
        and str(trade.get("requester_ad_id", "")) == request.id
        and str(trade.get("id", ""))
    ]
    pending_for_me_ids = [
        str(trade.get("id", ""))
        for trade in trades_for_me
        if str(trade.get("status", "")).upper() == "PENDING"
        and str(trade.get("target_ad_id", "")) == request.id
        and str(trade.get("id", ""))
    ]

    for trade_id in pending_from_me_ids:
        cancel_topic, _ = await client.request(
            TOPIC_TRADES_CANCEL,
            {"trade_id": trade_id, "canceler_id": user_id},
            tipo="Comando",
        )
        if cancel_topic not in {"trades.troca.cancelada", "trades.troca.cancelamento_falhou"}:
            return cancel_topic, {"reason": "trade_cancel_dispatch_failed"}

    for trade_id in pending_for_me_ids:
        reject_topic, _ = await client.request(
            TOPIC_TRADES_REJECT,
            {"trade_id": trade_id, "decider_id": user_id},
            tipo="Comando",
        )
        if reject_topic not in {"trades.troca.recusada", "trades.troca.decisao_falhou"}:
            return reject_topic, {"reason": "trade_reject_dispatch_failed"}

    has_trade_history = any(
        str(trade.get("requester_ad_id", "")) == request.id
        or str(trade.get("target_ad_id", "")) == request.id
        for trade in [*trades_from_me, *trades_for_me]
    )

    topic, response = await client.request(
        TOPIC_ADS_DELETE,
        {
            "ad_id": request.id,
            "owner_id": user_id,
            "keep_record": has_trade_history,
        },
        tipo="Comando",
    )
    return topic, response


async def _handle_search_available(user_id: str | None, payload: dict) -> tuple[str, list]:
    topic, response = await client.request(
        TOPIC_ADS_LIST_AVAILABLE, {"owner_id": user_id}, tipo="Consulta"
    )
    ads = await _attach_owner_name(response)
    return topic, [AdSearchResult(**ad).model_dump() for ad in ads]


async def _handle_search_ads(user_id: str | None, payload: dict) -> tuple[str, list]:
    request = SearchAdsRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_ADS_SEARCH, {"owner_id": user_id, "query": request.q}, tipo="Consulta"
    )
    ads = await _attach_owner_name(response)
    return topic, [AdSearchResult(**ad).model_dump() for ad in ads]


async def _handle_favorite_ad(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = FavoriteAdRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_ADS_FAVORITE,
        {"user_id": user_id, "ad_id": request.ad_id},
        tipo="Comando",
    )
    return topic, response


async def _handle_unfavorite_ad(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = FavoriteAdRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_ADS_UNFAVORITE,
        {"user_id": user_id, "ad_id": request.ad_id},
        tipo="Comando",
    )
    return topic, response


async def _handle_list_favorite_ads(user_id: str | None, payload: dict) -> tuple[str, dict]:
    topic, response = await client.request(
        TOPIC_ADS_LIST_FAVORITES,
        {"user_id": user_id},
        tipo="Consulta",
    )
    return topic, FavoriteIdsResponse(**response).model_dump()


async def _handle_create_trade(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = CreateTradeRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_TRADES_REQUEST,
        {
            "requester_id": user_id,
            "requester_ad_id": request.requester_ad_id,
            "target_ad_id": request.target_ad_id,
        },
        tipo="Comando",
    )
    if topic == TOPIC_TRADES_REQUEST_FAILED:
        return topic, response
    return topic, TradeResponse(id=response["trade_id"], status=response["status"]).model_dump()


async def _handle_accept_trade(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = AdIdRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_TRADES_ACCEPT, {"trade_id": request.id, "decider_id": user_id}, tipo="Comando"
    )
    if topic == TOPIC_TRADES_DECISION_FAILED:
        return topic, response
    return topic, TradeDecisionResponse(status=response["status"]).model_dump()


async def _handle_reject_trade(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = AdIdRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_TRADES_REJECT, {"trade_id": request.id, "decider_id": user_id}, tipo="Comando"
    )
    if topic == TOPIC_TRADES_DECISION_FAILED:
        return topic, response
    return topic, TradeDecisionResponse(status=response["status"]).model_dump()


async def _handle_cancel_trade(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = AdIdRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_TRADES_CANCEL, {"trade_id": request.id, "canceler_id": user_id}, tipo="Comando"
    )
    if topic == TOPIC_TRADES_CANCEL_FAILED:
        return topic, response
    return topic, TradeDecisionResponse(status=response["status"]).model_dump()


def _to_front_status(status: str) -> str:
    mapping = {
        "PENDING": "pendente",
        "ACCEPTED": "aceita",
        "REJECTED": "recusada",
        "CANCELLED": "cancelada",
    }
    return mapping.get(status.upper(), "pendente")


async def _get_ad(ad_id: str) -> dict | None:
    topic, payload = await client.request(
        TOPIC_ADS_GET_BY_ID, {"ad_id": ad_id}, tipo="Consulta"
    )
    if topic == "ads.anuncio.encontrado":
        return payload
    return None


async def _get_profile(user_id: str) -> dict | None:
    topic, payload = await client.request(
        TOPIC_GET_PROFILE, {"user_id": user_id}, tipo="Consulta"
    )
    if topic == "users.perfil.encontrado":
        return payload
    return None


def _to_item_detail(ad: dict | None) -> TradeItemDetailResponse:
    if ad is None:
        return TradeItemDetailResponse(
            nome="Item indisponivel",
            descricao="Sem descricao",
            condicao="Nao informado",
            localizacao="Nao informado",
            imagem=None,
        )

    return TradeItemDetailResponse(
        nome=str(ad.get("title", "Item")),
        descricao=str(ad.get("description", "Sem descricao")),
        condicao=str(ad.get("condition", "Nao informado")),
        localizacao=str(ad.get("location", "Nao informado")),
        imagem=(str(ad.get("image")) if ad.get("image") else None),
    )


async def _to_trade_view(trade: dict, current_user_id: str) -> TradeViewResponse:
    requester_id = str(trade.get("requester_id", ""))
    requester_ad_id = str(trade.get("requester_ad_id", ""))
    target_ad_id = str(trade.get("target_ad_id", ""))

    requester_ad, target_ad, requester_profile = await asyncio.gather(
        _get_ad(requester_ad_id),
        _get_ad(target_ad_id),
        _get_profile(requester_id),
    )

    target_owner_id = str((target_ad or {}).get("owner_id", ""))
    target_owner_profile = await _get_profile(target_owner_id) if target_owner_id else None

    direction = "de_mim" if requester_id == current_user_id else "para_mim"
    status = _to_front_status(str(trade.get("status", "PENDING")))
    date_requested = str(trade.get("created_at", ""))

    requester_name = _nickname_from_name(str((requester_profile or {}).get("name", "Usuario")))
    target_owner_name = _nickname_from_name(str((target_owner_profile or {}).get("name", "Usuario")))

    if direction == "de_mim":
        meu_item = _to_item_detail(requester_ad)
        item_fulano = _to_item_detail(target_ad)
        contraparte = target_owner_name
        contact_source = target_owner_profile
    else:
        meu_item = _to_item_detail(target_ad)
        item_fulano = _to_item_detail(requester_ad)
        contraparte = requester_name
        contact_source = requester_profile

    contact = None
    if status == "aceita" and contact_source is not None:
        contact = {
            "telefone": str(contact_source.get("phone", "Telefone nao informado")),
            "email": str(contact_source.get("email", "Email nao informado")),
        }

    return TradeViewResponse(
        id=str(trade.get("id", "")),
        itemDeId=requester_ad_id,
        itemParaId=target_ad_id,
        itemDe=str((requester_ad or {}).get("title", "Item")),
        itemPara=str((target_ad or {}).get("title", "Item")),
        meuItem=meu_item,
        itemFulano=item_fulano,
        status=status,
        dataSolicitacao=date_requested,
        dataRespostaCancelamento=(date_requested if status != "pendente" else None),
        contraparte=contraparte,
        contatoContraparte=contact,
        direcao=direction,
    )


async def _handle_list_trades_by_requester(user_id: str | None, payload: dict) -> tuple[str, list]:
    topic, response = await client.request(
        TOPIC_TRADES_LIST_BY_REQUESTER,
        {"requester_id": user_id},
        tipo="Consulta",
    )
    trades = response.get("trades", []) if isinstance(response, dict) else []
    mapped = [await _to_trade_view(trade, str(user_id or "")) for trade in trades]
    return topic, [trade.model_dump() for trade in mapped]


async def _handle_list_trades_for_target_owner(user_id: str | None, payload: dict) -> tuple[str, list]:
    topic, response = await client.request(
        TOPIC_TRADES_LIST_FOR_TARGET_OWNER,
        {"owner_id": user_id},
        tipo="Consulta",
    )
    trades = response.get("trades", []) if isinstance(response, dict) else []
    mapped = [await _to_trade_view(trade, str(user_id or "")) for trade in trades]
    return topic, [trade.model_dump() for trade in mapped]


async def _handle_list_notifications(user_id: str | None, payload: dict) -> tuple[str, list]:
    topic, response = await client.request(
        TOPIC_NOTIFICATIONS_LIST, {"user_id": user_id}, tipo="Consulta"
    )
    return topic, [NotificationResponse(**n).model_dump() for n in response]


_HANDLERS = {
    TOPIC_REGISTER: _handle_register,
    TOPIC_LOGIN: _handle_login,
    TOPIC_GET_PROFILE: _handle_get_profile,
    TOPIC_UPDATE_PROFILE: _handle_update_profile,
    TOPIC_ADS_CREATE: _handle_create_ad,
    TOPIC_ADS_LIST_BY_OWNER: _handle_list_ads,
    TOPIC_ADS_UPDATE: _handle_update_ad,
    TOPIC_ADS_DELETE: _handle_delete_ad,
    TOPIC_ADS_LIST_AVAILABLE: _handle_search_available,
    TOPIC_ADS_SEARCH: _handle_search_ads,
    TOPIC_ADS_FAVORITE: _handle_favorite_ad,
    TOPIC_ADS_UNFAVORITE: _handle_unfavorite_ad,
    TOPIC_ADS_LIST_FAVORITES: _handle_list_favorite_ads,
    TOPIC_TRADES_REQUEST: _handle_create_trade,
    TOPIC_TRADES_ACCEPT: _handle_accept_trade,
    TOPIC_TRADES_REJECT: _handle_reject_trade,
    TOPIC_TRADES_CANCEL: _handle_cancel_trade,
    TOPIC_TRADES_LIST_BY_REQUESTER: _handle_list_trades_by_requester,
    TOPIC_TRADES_LIST_FOR_TARGET_OWNER: _handle_list_trades_for_target_owner,
    TOPIC_NOTIFICATIONS_LIST: _handle_list_notifications,
}


async def _dispatch(
    message: dict, user_id: str | None, auth_error: str | None
) -> tuple[str, dict | list]:
    topico = message.get("topico")
    payload = message.get("payload") or {}

    handler = _HANDLERS.get(topico)
    if handler is None:
        return "sistema.mensagem.nao_reconhecida", {"reason": "topico_desconhecido"}

    if topico not in _PUBLIC_TOPICS and user_id is None:
        return f"{topico}_nao_autorizado", {"reason": auth_error or "missing_token"}

    try:
        return await handler(user_id, payload)
    except ValidationError:
        return f"{topico}_falhou", {"reason": "invalid_payload"}
    except TimeoutError:
        return f"{topico}_falhou", {"reason": "timeout"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await client.start()
    yield
    await client.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    token = websocket.query_params.get("token")
    user_id: str | None = None
    auth_error: str | None = "missing_token"
    if token is not None:
        try:
            user_id = decode_user_id(token)
            auth_error = None
        except AuthenticationError as exc:
            auth_error = exc.reason

    try:
        while True:
            message = await websocket.receive_json()
            topico, response_payload = await _dispatch(message, user_id, auth_error)
            await websocket.send_json(
                {"tipo": "Evento", "topico": topico, "payload": response_payload}
            )
    except WebSocketDisconnect:
        pass
