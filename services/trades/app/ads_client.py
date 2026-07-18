import asyncio
import json
import logging
import uuid

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.kafka_producer import producer

TOPIC_GET_BY_ID = "ads.get_by_id"
TOPIC_MARK_UNAVAILABLE = "ads.mark_unavailable"
REPLY_TOPICS = ("ads.found", "ads.not_found")


class AdsClient:
    """Kafka request/reply client used by Trades to resolve ad data from the Ads service."""

    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None
        self._consumer_task: asyncio.Task | None = None
        self._pending: dict[str, asyncio.Future] = {}

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *REPLY_TOPICS,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="trades-service-ads-client",
        )
        await self._consumer.start()
        self._consumer_task = asyncio.create_task(self._consume_replies())

    async def stop(self) -> None:
        if self._consumer_task is not None:
            self._consumer_task.cancel()
        if self._consumer is not None:
            await self._consumer.stop()

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

    async def get_ad_by_id(self, ad_id: str) -> dict | None:
        """Returns the ad's data (id, owner_id, title, description), or None if not found."""
        correlation_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[correlation_id] = future

        try:
            await producer.publish(TOPIC_GET_BY_ID, {"ad_id": ad_id}, correlation_id)
            topic, payload = await asyncio.wait_for(
                future, timeout=settings.trades_kafka_reply_timeout_seconds
            )
        finally:
            self._pending.pop(correlation_id, None)

        if topic == "ads.not_found":
            return None
        return payload

    async def mark_unavailable(self, ad_id: str) -> None:
        """Fire-and-forget: tells Ads to flag the ad as no longer tradeable."""
        await producer.publish(TOPIC_MARK_UNAVAILABLE, {"ad_id": ad_id}, None)


ads_client = AdsClient()
