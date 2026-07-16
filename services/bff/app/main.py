from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from app.kafka_client import client
from app.schemas import LoginRequest, LoginResponse, RegisterRequest, RegisterResponse

TOPIC_REGISTER = "users.register"
TOPIC_LOGIN = "users.login"


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
