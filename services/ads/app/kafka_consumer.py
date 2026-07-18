import json
import logging

from aiokafka import AIOKafkaConsumer

from app.commands import (
    handle_create,
    handle_delete,
    handle_list_by_owner,
    handle_update,
)
from app.config import settings

TOPIC_CREATE = "ads.create"
TOPIC_LIST_BY_OWNER = "ads.list_by_owner"
TOPIC_UPDATE = "ads.update"
TOPIC_DELETE = "ads.delete"

_HANDLERS = {
    TOPIC_CREATE: handle_create,
    TOPIC_LIST_BY_OWNER: handle_list_by_owner,
    TOPIC_UPDATE: handle_update,
    TOPIC_DELETE: handle_delete,
}


class KafkaCommandConsumer:
    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            TOPIC_CREATE,
            TOPIC_LIST_BY_OWNER,
            TOPIC_UPDATE,
            TOPIC_DELETE,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="ads-service",
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()

    async def run(self) -> None:
        async for message in self._consumer:
            correlation_id = None
            for key, value in message.headers:
                if key == "correlation_id":
                    correlation_id = value.decode("utf-8")
                    break

            payload = json.loads(message.value.decode("utf-8"))
            handler = _HANDLERS.get(message.topic)
            if handler is not None:
                try:
                    await handler(payload, correlation_id)
                except Exception:
                    logging.exception(
                        "Failed to process message from topic %s", message.topic
                    )


consumer = KafkaCommandConsumer()
