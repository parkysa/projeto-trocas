import asyncio
import json
import logging
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.config import settings

REPLY_TOPICS = (
    "users.registered",
    "users.registration_failed",
    "users.authenticated",
    "users.authentication_failed",
    "users.profile_found",
    "users.profile_updated",
    "users.profile_update_failed",
    "ads.created",
    "ads.listed",
    "ads.updated",
    "ads.deleted",
    "ads.operation_failed",
    "ads.available_list",
    "ads.search_result",
    "trades.requested",
    "trades.request_failed",
    "trades.accepted",
    "trades.rejected",
    "trades.decision_failed",
    "trades.cancelled",
    "trades.cancel_failed",
    "notifications.listed",
)


class KafkaRequestReplyClient:
    def __init__(self):
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._consumer_task: asyncio.Task | None = None
        self._pending: dict[str, asyncio.Future] = {}

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers
        )
        await self._producer.start()

        self._consumer = AIOKafkaConsumer(
            *REPLY_TOPICS,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="bff-service",
        )
        await self._consumer.start()
        self._consumer_task = asyncio.create_task(self._consume_replies())

    async def stop(self) -> None:
        if self._consumer_task is not None:
            self._consumer_task.cancel()
        if self._consumer is not None:
            await self._consumer.stop()
        if self._producer is not None:
            await self._producer.stop()

    async def _consume_replies(self) -> None:
        async for message in self._consumer:
            try:
                correlation_id = None
                for key, value in message.headers:
                    if key == "correlation_id":
                        correlation_id = value.decode("utf-8")
                        break

                future = self._pending.get(correlation_id)
                if future is not None and not future.done():
                    payload = json.loads(message.value.decode("utf-8"))
                    future.set_result((message.topic, payload))
            except Exception:
                logging.exception(
                    "Failed to process reply from topic %s", message.topic
                )

    async def request(
        self, command_topic: str, payload: dict
    ) -> tuple[str, dict | list]:
        correlation_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[correlation_id] = future

        try:
            await self._producer.send_and_wait(
                command_topic,
                json.dumps(payload).encode("utf-8"),
                headers=[("correlation_id", correlation_id.encode("utf-8"))],
            )
            return await asyncio.wait_for(
                future, timeout=settings.bff_kafka_reply_timeout_seconds
            )
        finally:
            self._pending.pop(correlation_id, None)


client = KafkaRequestReplyClient()
