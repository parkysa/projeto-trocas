import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from app.commands import (
    handle_get_profile,
    handle_login,
    handle_register,
    handle_update_profile,
)
from app.config import settings
from app.kafka_producer import producer

TOPIC_REGISTER = "users.register"
TOPIC_LOGIN = "users.login"
TOPIC_GET_PROFILE = "users.get_profile"
TOPIC_UPDATE_PROFILE = "users.update_profile"

_HANDLERS = {
    TOPIC_REGISTER: handle_register,
    TOPIC_LOGIN: handle_login,
    TOPIC_GET_PROFILE: handle_get_profile,
    TOPIC_UPDATE_PROFILE: handle_update_profile,
}


class KafkaCommandConsumer:
    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None
        self._processed_offsets: set[tuple[str, int, int]] = set()

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            TOPIC_REGISTER,
            TOPIC_LOGIN,
            TOPIC_GET_PROFILE,
            TOPIC_UPDATE_PROFILE,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="users-service",
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()

    async def _handle_with_retry(
        self, handler, topic: str, payload: dict, correlation_id: str | None
    ) -> None:
        for attempt in range(1, settings.kafka_retry_attempts + 1):
            try:
                await handler(payload, correlation_id)
                return
            except Exception:
                logging.exception(
                    "Attempt %s/%s failed processing message from topic %s",
                    attempt,
                    settings.kafka_retry_attempts,
                    topic,
                )
                if attempt < settings.kafka_retry_attempts:
                    await asyncio.sleep(settings.kafka_retry_delay_seconds)

        logging.error(
            "Exceeded %s retry attempts for topic %s; sending message to DLQ topic %s",
            settings.kafka_retry_attempts,
            topic,
            settings.kafka_dlq_topic,
        )
        await producer.publish(
            settings.kafka_dlq_topic,
            {"original_topic": topic, "payload": payload, "reason": "max_retries_exceeded"},
            correlation_id,
        )

    async def run(self) -> None:
        async for message in self._consumer:
            offset_key = (message.topic, message.partition, message.offset)
            if offset_key in self._processed_offsets:
                logging.warning(
                    "Skipping already processed message from topic %s "
                    "(partition=%s, offset=%s)",
                    message.topic,
                    message.partition,
                    message.offset,
                )
                continue

            correlation_id = None
            for key, value in message.headers:
                if key == "correlation_id":
                    correlation_id = value.decode("utf-8")
                    break

            payload = json.loads(message.value.decode("utf-8"))
            handler = _HANDLERS.get(message.topic)
            if handler is not None:
                await self._handle_with_retry(
                    handler, message.topic, payload, correlation_id
                )

            self._processed_offsets.add(offset_key)


consumer = KafkaCommandConsumer()
