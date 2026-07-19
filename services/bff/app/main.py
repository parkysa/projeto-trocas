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
    LoginRequest,
    LoginResponse,
    NotificationResponse,
    ProfileResponse,
    RegisterRequest,
    RegisterResponse,
    SearchAdsRequest,
    TradeDecisionResponse,
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
TOPIC_TRADES_REQUEST = "trades.troca.solicitar"
TOPIC_TRADES_REQUEST_FAILED = "trades.troca.solicitacao_falhou"
TOPIC_TRADES_ACCEPT = "trades.troca.aceitar"
TOPIC_TRADES_REJECT = "trades.troca.recusar"
TOPIC_TRADES_DECISION_FAILED = "trades.troca.decisao_falhou"
TOPIC_TRADES_CANCEL = "trades.troca.cancelar"
TOPIC_TRADES_CANCEL_FAILED = "trades.troca.cancelamento_falhou"
TOPIC_NOTIFICATIONS_LIST = "notifications.notificacao.consultar"

_PUBLIC_TOPICS = {TOPIC_REGISTER, TOPIC_LOGIN}


async def _handle_register(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = RegisterRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_REGISTER, request.model_dump(), tipo="Comando"
    )
    if topic == TOPIC_REGISTRATION_FAILED:
        return topic, response
    return topic, RegisterResponse(
        id=response["user_id"], name=request.name, email=response["email"]
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
        {"user_id": user_id, "name": request.name, "email": request.email},
        tipo="Comando",
    )
    if topic == TOPIC_PROFILE_UPDATE_FAILED:
        return topic, response
    return topic, ProfileResponse(**response).model_dump()


async def _handle_create_ad(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = CreateAdRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_ADS_CREATE,
        {"owner_id": user_id, "title": request.title, "description": request.description},
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
        },
        tipo="Comando",
    )
    if topic == TOPIC_ADS_OPERATION_FAILED:
        return topic, response
    return topic, AdResponse(**response).model_dump()


async def _handle_delete_ad(user_id: str | None, payload: dict) -> tuple[str, dict]:
    request = AdIdRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_ADS_DELETE, {"ad_id": request.id, "owner_id": user_id}, tipo="Comando"
    )
    return topic, response


async def _handle_search_available(user_id: str | None, payload: dict) -> tuple[str, list]:
    topic, response = await client.request(
        TOPIC_ADS_LIST_AVAILABLE, {"owner_id": user_id}, tipo="Consulta"
    )
    return topic, [AdSearchResult(**ad).model_dump() for ad in response]


async def _handle_search_ads(user_id: str | None, payload: dict) -> tuple[str, list]:
    request = SearchAdsRequest.model_validate(payload)
    topic, response = await client.request(
        TOPIC_ADS_SEARCH, {"owner_id": user_id, "query": request.q}, tipo="Consulta"
    )
    return topic, [AdSearchResult(**ad).model_dump() for ad in response]


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
    TOPIC_TRADES_REQUEST: _handle_create_trade,
    TOPIC_TRADES_ACCEPT: _handle_accept_trade,
    TOPIC_TRADES_REJECT: _handle_reject_trade,
    TOPIC_TRADES_CANCEL: _handle_cancel_trade,
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
