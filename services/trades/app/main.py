import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.ads_client import ads_client
from app.database import create_all
from app.kafka_consumer import consumer
from app.kafka_producer import producer


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_all()
    await producer.start()
    await consumer.start()
    await ads_client.start()
    consumer_task = asyncio.create_task(consumer.run())

    yield

    consumer_task.cancel()
    await ads_client.stop()
    await consumer.stop()
    await producer.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}
