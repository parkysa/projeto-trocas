import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import Base, engine, get_db
from app.repository import (
    create_user,
    delete_user,
    get_user,
    get_user_by_email,
    list_users,
    update_user,
)
from app.schemas import UserCreate, UserOut, UserUpdate


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Cria as tabelas ao iniciar (suficiente para desenvolvimento)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Users Service", lifespan=lifespan)


# ---------- health ----------

@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}


# ---------- users ----------

@app.post("/users", response_model=UserOut, status_code=status.HTTP_201_CREATED, tags=["users"])
async def create(data: UserCreate, db: AsyncSession = Depends(get_db)):
    if await get_user_by_email(db, data.email):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="E-mail já cadastrado.",
        )
    return await create_user(db, data)


@app.get("/users", response_model=list[UserOut], tags=["users"])
async def list_all(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    return await list_users(db, skip=skip, limit=limit)


@app.get("/users/{user_id}", response_model=UserOut, tags=["users"])
async def get_one(user_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    user = await get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuário não encontrado.")
    return user


@app.patch("/users/{user_id}", response_model=UserOut, tags=["users"])
async def update(
    user_id: uuid.UUID,
    data: UserUpdate,
    db: AsyncSession = Depends(get_db),
):
    user = await get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuário não encontrado.")
    return await update_user(db, user, data)


@app.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["users"])
async def delete(user_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    user = await get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuário não encontrado.")
    await delete_user(db, user)
