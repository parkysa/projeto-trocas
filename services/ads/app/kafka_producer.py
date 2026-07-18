import json

from aiokafka import AIOKafkaProducer

from app.config import settings


class KafkaProducer:
    def __init__(self):
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

    async def publish(
        self, topic: str, payload: dict, correlation_id: str | None
    ) -> None:
        headers = []
        if correlation_id is not None:
            headers.append(("correlation_id", correlation_id.encode("utf-8")))
        await self._producer.send_and_wait(
            topic, json.dumps(payload).encode("utf-8"), headers=headers
        )


producer = KafkaProducer()
