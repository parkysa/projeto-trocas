import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import Base, engine, get_db
from app.repository import (
    create_ad,
    delete_ad,
    get_ad,
    list_ads,
    update_ad,
)
from app.schemas import AdCreate, AdOut, AdUpdate


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Cria as tabelas ao iniciar (suficiente para desenvolvimento)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Ads Service", lifespan=lifespan)


# ---------- health ----------

@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}


# ---------- ads ----------

@app.post("/ads", response_model=AdOut, status_code=status.HTTP_201_CREATED, tags=["ads"])
async def create(data: AdCreate, db: AsyncSession = Depends(get_db)):
    return await create_ad(db, data)


@app.get("/ads", response_model=list[AdOut], tags=["ads"])
async def list_all(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    return await list_ads(db, skip=skip, limit=limit)


@app.get("/ads/{ad_id}", response_model=AdOut, tags=["ads"])
async def get_one(ad_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    ad = await get_ad(db, ad_id)
    if not ad:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Anúncio não encontrado.")
    return ad


@app.patch("/ads/{ad_id}", response_model=AdOut, tags=["ads"])
async def update(
    ad_id: uuid.UUID,
    data: AdUpdate,
    db: AsyncSession = Depends(get_db),
):
    ad = await get_ad(db, ad_id)
    if not ad:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Anúncio não encontrado.")
    return await update_ad(db, ad, data)


@app.delete("/ads/{ad_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["ads"])
async def delete(ad_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    ad = await get_ad(db, ad_id)
    if not ad:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Anúncio não encontrado.")
    await delete_ad(db, ad)
