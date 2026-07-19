import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from app.commands import (
    handle_create,
    handle_delete,
    handle_get_by_id,
    handle_list_available,
    handle_list_by_owner,
    handle_mark_unavailable,
    handle_search,
    handle_update,
)
from app.config import settings
from app.kafka_producer import producer

TOPIC_CREATE = "ads.anuncio.criar"
TOPIC_LIST_BY_OWNER = "ads.anuncio.consultar_proprios"
TOPIC_UPDATE = "ads.anuncio.atualizar"
TOPIC_DELETE = "ads.anuncio.remover"
TOPIC_LIST_AVAILABLE = "ads.anuncio.consultar_disponiveis"
TOPIC_SEARCH = "ads.anuncio.buscar"
TOPIC_GET_BY_ID = "ads.anuncio.consultar_por_id"
TOPIC_MARK_UNAVAILABLE = "ads.anuncio.marcar_indisponivel"

_HANDLERS = {
    TOPIC_CREATE: handle_create,
    TOPIC_LIST_BY_OWNER: handle_list_by_owner,
    TOPIC_UPDATE: handle_update,
    TOPIC_DELETE: handle_delete,
    TOPIC_LIST_AVAILABLE: handle_list_available,
    TOPIC_SEARCH: handle_search,
    TOPIC_GET_BY_ID: handle_get_by_id,
    TOPIC_MARK_UNAVAILABLE: handle_mark_unavailable,
}


class KafkaCommandConsumer:
    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None
        self._processed_offsets: set[tuple[str, int, int]] = set()

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            TOPIC_CREATE,
            TOPIC_LIST_BY_OWNER,
            TOPIC_UPDATE,
            TOPIC_DELETE,
            TOPIC_LIST_AVAILABLE,
            TOPIC_SEARCH,
            TOPIC_GET_BY_ID,
            TOPIC_MARK_UNAVAILABLE,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="ads-service",
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

            envelope = json.loads(message.value.decode("utf-8"))
            payload = envelope["payload"]
            handler = _HANDLERS.get(message.topic)
            if handler is not None:
                await self._handle_with_retry(
                    handler, message.topic, payload, correlation_id
                )

            self._processed_offsets.add(offset_key)


consumer = KafkaCommandConsumer()
