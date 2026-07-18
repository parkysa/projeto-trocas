import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import Base, engine, get_db
from app.repository import (
    create_trade,
    delete_trade,
    get_trade,
    list_trades,
    update_trade,
)
from app.schemas import TradeCreate, TradeOut, TradeUpdate


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Cria as tabelas ao iniciar (suficiente para desenvolvimento)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Trades Service", lifespan=lifespan)


# ---------- health ----------

@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}


# ---------- trades ----------

@app.post("/trades", response_model=TradeOut, status_code=status.HTTP_201_CREATED, tags=["trades"])
async def create(data: TradeCreate, db: AsyncSession = Depends(get_db)):
    return await create_trade(db, data)


@app.get("/trades", response_model=list[TradeOut], tags=["trades"])
async def list_all(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    return await list_trades(db, skip=skip, limit=limit)


@app.get("/trades/{trade_id}", response_model=TradeOut, tags=["trades"])
async def get_one(trade_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    trade = await get_trade(db, trade_id)
    if not trade:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proposta não encontrada.")
    return trade


@app.patch("/trades/{trade_id}", response_model=TradeOut, tags=["trades"])
async def update(
    trade_id: uuid.UUID,
    data: TradeUpdate,
    db: AsyncSession = Depends(get_db),
):
    trade = await get_trade(db, trade_id)
    if not trade:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proposta não encontrada.")
    return await update_trade(db, trade, data)


@app.delete("/trades/{trade_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["trades"])
async def delete(trade_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    trade = await get_trade(db, trade_id)
    if not trade:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proposta não encontrada.")
    await delete_trade(db, trade)
