import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from app.commands import (
    handle_list,
    handle_trade_accepted,
    handle_trade_cancelled,
    handle_trade_rejected,
    handle_trade_requested,
    handle_user_registered,
)
from app.config import settings
from app.kafka_producer import producer

TOPIC_USER_REGISTERED = "users.usuario.cadastrado"
TOPIC_TRADE_REQUESTED = "trades.troca.solicitada"
TOPIC_TRADE_ACCEPTED = "trades.troca.aprovada"
TOPIC_TRADE_REJECTED = "trades.troca.recusada"
TOPIC_TRADE_CANCELLED = "trades.troca.cancelada"
TOPIC_LIST = "notifications.notificacao.consultar"

_HANDLERS = {
    TOPIC_USER_REGISTERED: handle_user_registered,
    TOPIC_TRADE_REQUESTED: handle_trade_requested,
    TOPIC_TRADE_ACCEPTED: handle_trade_accepted,
    TOPIC_TRADE_REJECTED: handle_trade_rejected,
    TOPIC_TRADE_CANCELLED: handle_trade_cancelled,
    TOPIC_LIST: handle_list,
}


class KafkaCommandConsumer:
    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None
        self._processed_offsets: set[tuple[str, int, int]] = set()

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            TOPIC_USER_REGISTERED,
            TOPIC_TRADE_REQUESTED,
            TOPIC_TRADE_ACCEPTED,
            TOPIC_TRADE_REJECTED,
            TOPIC_TRADE_CANCELLED,
            TOPIC_LIST,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="notifications-service",
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
