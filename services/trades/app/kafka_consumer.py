import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from app.commands import handle_accept, handle_cancel, handle_reject, handle_request
from app.config import settings
from app.kafka_producer import producer

TOPIC_REQUEST = "trades.troca.solicitar"
TOPIC_ACCEPT = "trades.troca.aceitar"
TOPIC_REJECT = "trades.troca.recusar"
TOPIC_CANCEL = "trades.troca.cancelar"

_HANDLERS = {
    TOPIC_REQUEST: handle_request,
    TOPIC_ACCEPT: handle_accept,
    TOPIC_REJECT: handle_reject,
    TOPIC_CANCEL: handle_cancel,
}


class KafkaCommandConsumer:
    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None
        self._processed_offsets: set[tuple[str, int, int]] = set()

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            TOPIC_REQUEST,
            TOPIC_ACCEPT,
            TOPIC_REJECT,
            TOPIC_CANCEL,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="trades-service",
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
