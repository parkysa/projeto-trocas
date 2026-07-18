from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException

from app.kafka_client import client
from app.schemas import (
    LoginRequest,
    LoginResponse,
    ProfileResponse,
    RegisterRequest,
    RegisterResponse,
    UpdateProfileRequest,
)
from app.security import get_current_user_id

TOPIC_REGISTER = "users.register"
TOPIC_LOGIN = "users.login"
TOPIC_GET_PROFILE = "users.get_profile"
TOPIC_UPDATE_PROFILE = "users.update_profile"


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
