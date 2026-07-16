import json
import logging

from aiokafka import AIOKafkaConsumer

from app.commands import handle_login, handle_register
from app.config import settings

TOPIC_REGISTER = "users.register"
TOPIC_LOGIN = "users.login"

_HANDLERS = {
    TOPIC_REGISTER: handle_register,
    TOPIC_LOGIN: handle_login,
}


class KafkaCommandConsumer:
    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            TOPIC_REGISTER,
            TOPIC_LOGIN,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="users-service",
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
