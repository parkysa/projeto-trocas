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

TOPIC_USER_REGISTERED = "users.registered"
TOPIC_TRADE_REQUESTED = "trades.requested"
TOPIC_TRADE_ACCEPTED = "trades.accepted"
TOPIC_TRADE_REJECTED = "trades.rejected"
TOPIC_TRADE_CANCELLED = "trades.cancelled"
TOPIC_LIST = "notifications.list"

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
