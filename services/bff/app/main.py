from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Response

from app.kafka_client import client
from app.schemas import (
    AdResponse,
    AdSearchResult,
    CreateAdRequest,
    CreateTradeRequest,
    LoginRequest,
    LoginResponse,
    ProfileResponse,
    RegisterRequest,
    RegisterResponse,
    TradeDecisionResponse,
    TradeResponse,
    UpdateAdRequest,
    UpdateProfileRequest,
)
from app.security import get_current_user_id

TOPIC_REGISTER = "users.register"
TOPIC_LOGIN = "users.login"
TOPIC_GET_PROFILE = "users.get_profile"
TOPIC_UPDATE_PROFILE = "users.update_profile"
TOPIC_ADS_CREATE = "ads.create"
TOPIC_ADS_LIST_BY_OWNER = "ads.list_by_owner"
TOPIC_ADS_UPDATE = "ads.update"
TOPIC_ADS_DELETE = "ads.delete"
TOPIC_ADS_LIST_AVAILABLE = "ads.list_available"
TOPIC_ADS_SEARCH = "ads.search"
TOPIC_TRADES_REQUEST = "trades.request"
TOPIC_TRADES_ACCEPT = "trades.accept"
TOPIC_TRADES_REJECT = "trades.reject"
TOPIC_TRADES_CANCEL = "trades.cancel"


def _ad_operation_status_code(reason: str) -> int:
    return 404 if reason == "ad_not_found" else 403


def _trade_request_failed_status_code(reason: str) -> int:
    return 404 if reason in ("requester_ad_not_found", "target_ad_not_found") else 400


def _trade_decision_failed_status_code(reason: str) -> int:
    if reason in ("trade_not_found", "target_ad_not_found"):
        return 404
    if reason == "forbidden":
        return 403
    return 409


def _trade_cancel_failed_status_code(reason: str) -> int:
    if reason == "trade_not_found":
        return 404
    if reason == "forbidden":
        return 403
    return 409


@asynccontextmanager
async def lifespan(app: FastAPI):
    await client.start()
    yield
    await client.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/register", response_model=RegisterResponse, status_code=201)
async def register(request: RegisterRequest):
    try:
        topic, payload = await client.request(TOPIC_REGISTER, request.model_dump())
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Users service")

    if topic == "users.registration_failed":
        raise HTTPException(status_code=409, detail=payload["reason"])

    return RegisterResponse(id=payload["user_id"], name=request.name, email=payload["email"])


@app.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    try:
        topic, payload = await client.request(TOPIC_LOGIN, request.model_dump())
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Users service")

    if topic == "users.authentication_failed":
        raise HTTPException(status_code=401, detail=payload["reason"])

    return LoginResponse(access_token=payload["token"], token_type="bearer")


@app.get("/me", response_model=ProfileResponse)
async def get_profile(user_id: str = Depends(get_current_user_id)):
    try:
        topic, payload = await client.request(TOPIC_GET_PROFILE, {"user_id": user_id})
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Users service")

    return ProfileResponse(id=payload["id"], name=payload["name"], email=payload["email"])


@app.put("/me", response_model=ProfileResponse)
async def update_profile(
    request: UpdateProfileRequest, user_id: str = Depends(get_current_user_id)
):
    try:
        topic, payload = await client.request(
            TOPIC_UPDATE_PROFILE,
            {"user_id": user_id, "name": request.name, "email": request.email},
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Users service")

    if topic == "users.profile_update_failed":
        raise HTTPException(status_code=409, detail=payload["reason"])

    return ProfileResponse(id=payload["id"], name=payload["name"], email=payload["email"])


@app.post("/ads", response_model=AdResponse, status_code=201)
async def create_ad(request: CreateAdRequest, user_id: str = Depends(get_current_user_id)):
    try:
        topic, payload = await client.request(
            TOPIC_ADS_CREATE,
            {"owner_id": user_id, "title": request.title, "description": request.description},
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Ads service")

    return AdResponse(id=payload["id"], title=payload["title"], description=payload["description"])


@app.get("/ads", response_model=list[AdResponse])
async def list_ads(user_id: str = Depends(get_current_user_id)):
    try:
        topic, payload = await client.request(TOPIC_ADS_LIST_BY_OWNER, {"owner_id": user_id})
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Ads service")

    return [AdResponse(**ad) for ad in payload["ads"]]


@app.put("/ads/{ad_id}", response_model=AdResponse)
async def update_ad(
    ad_id: str, request: UpdateAdRequest, user_id: str = Depends(get_current_user_id)
):
    try:
        topic, payload = await client.request(
            TOPIC_ADS_UPDATE,
            {
                "ad_id": ad_id,
                "owner_id": user_id,
                "title": request.title,
                "description": request.description,
            },
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Ads service")

    if topic == "ads.operation_failed":
        raise HTTPException(
            status_code=_ad_operation_status_code(payload["reason"]), detail=payload["reason"]
        )

    return AdResponse(id=payload["id"], title=payload["title"], description=payload["description"])


@app.delete("/ads/{ad_id}", status_code=204)
async def delete_ad(ad_id: str, user_id: str = Depends(get_current_user_id)):
    try:
        topic, payload = await client.request(
            TOPIC_ADS_DELETE, {"ad_id": ad_id, "owner_id": user_id}
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Ads service")

    if topic == "ads.operation_failed":
        raise HTTPException(
            status_code=_ad_operation_status_code(payload["reason"]), detail=payload["reason"]
        )

    return Response(status_code=204)


@app.get("/ads/search", response_model=list[AdSearchResult])
async def search_ads(q: str | None = None, user_id: str = Depends(get_current_user_id)):
    try:
        if q:
            topic, payload = await client.request(
                TOPIC_ADS_SEARCH, {"owner_id": user_id, "query": q}
            )
        else:
            topic, payload = await client.request(
                TOPIC_ADS_LIST_AVAILABLE, {"owner_id": user_id}
            )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Ads service")

    return [AdSearchResult(**ad) for ad in payload]


@app.post("/trades", response_model=TradeResponse, status_code=201)
async def create_trade(request: CreateTradeRequest, user_id: str = Depends(get_current_user_id)):
    try:
        topic, payload = await client.request(
            TOPIC_TRADES_REQUEST,
            {
                "requester_id": user_id,
                "requester_ad_id": request.requester_ad_id,
                "target_ad_id": request.target_ad_id,
            },
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Trades service")

    if topic == "trades.request_failed":
        raise HTTPException(
            status_code=_trade_request_failed_status_code(payload["reason"]),
            detail=payload["reason"],
        )

    return TradeResponse(id=payload["trade_id"], status=payload["status"])


@app.post("/trades/{trade_id}/accept", response_model=TradeDecisionResponse)
async def accept_trade(trade_id: str, user_id: str = Depends(get_current_user_id)):
    try:
        topic, payload = await client.request(
            TOPIC_TRADES_ACCEPT, {"trade_id": trade_id, "decider_id": user_id}
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Trades service")

    if topic == "trades.decision_failed":
        raise HTTPException(
            status_code=_trade_decision_failed_status_code(payload["reason"]),
            detail=payload["reason"],
        )

    return TradeDecisionResponse(status=payload["status"])


@app.post("/trades/{trade_id}/cancel", response_model=TradeDecisionResponse)
async def cancel_trade(trade_id: str, user_id: str = Depends(get_current_user_id)):
    try:
        topic, payload = await client.request(
            TOPIC_TRADES_CANCEL, {"trade_id": trade_id, "canceler_id": user_id}
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Trades service")

    if topic == "trades.cancel_failed":
        raise HTTPException(
            status_code=_trade_cancel_failed_status_code(payload["reason"]),
            detail=payload["reason"],
        )

    return TradeDecisionResponse(status=payload["status"])


@app.post("/trades/{trade_id}/reject", response_model=TradeDecisionResponse)
async def reject_trade(trade_id: str, user_id: str = Depends(get_current_user_id)):
    try:
        topic, payload = await client.request(
            TOPIC_TRADES_REJECT, {"trade_id": trade_id, "decider_id": user_id}
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for Trades service")

    if topic == "trades.decision_failed":
        raise HTTPException(
            status_code=_trade_decision_failed_status_code(payload["reason"]),
            detail=payload["reason"],
        )

    return TradeDecisionResponse(status=payload["status"])
