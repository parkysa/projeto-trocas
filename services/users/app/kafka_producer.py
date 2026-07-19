import json
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

from app.config import settings

SERVICE_NAME = "users"


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
        """Publishes a result event (this service only ever publishes events)."""
        envelope = {"tipo": "Evento", "topico": topic, "payload": payload}
        headers = [
            ("event_id", str(uuid.uuid4()).encode("utf-8")),
            ("timestamp", datetime.now(timezone.utc).isoformat().encode("utf-8")),
            ("producer", SERVICE_NAME.encode("utf-8")),
            ("version", b"1.0"),
            ("topic", topic.encode("utf-8")),
        ]
        if correlation_id is not None:
            headers.append(("correlation_id", correlation_id.encode("utf-8")))
        await self._producer.send_and_wait(
            topic, json.dumps(envelope).encode("utf-8"), headers=headers
        )


producer = KafkaProducer()
