import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.config import settings

SERVICE_NAME = "bff"

REPLY_TOPICS = (
    "users.usuario.cadastrado",
    "users.usuario.cadastro_falhou",
    "users.usuario.autenticado",
    "users.usuario.autenticacao_falhou",
    "users.perfil.encontrado",
    "users.perfil.atualizado",
    "users.perfil.atualizacao_falhou",
    "ads.anuncio.criado",
    "ads.anuncio.listado",
    "ads.anuncio.atualizado",
    "ads.anuncio.removido",
    "ads.anuncio.operacao_falhou",
    "ads.anuncio.disponiveis_listados",
    "ads.anuncio.busca_concluida",
    "trades.troca.solicitada",
    "trades.troca.solicitacao_falhou",
    "trades.troca.aprovada",
    "trades.troca.recusada",
    "trades.troca.decisao_falhou",
    "trades.troca.cancelada",
    "trades.troca.cancelamento_falhou",
    "notifications.notificacao.listada",
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
                    envelope = json.loads(message.value.decode("utf-8"))
                    future.set_result((message.topic, envelope["payload"]))
            except Exception:
                logging.exception(
                    "Failed to process reply from topic %s", message.topic
                )

    async def request(
        self, command_topic: str, payload: dict, tipo: str
    ) -> tuple[str, dict | list]:
        correlation_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[correlation_id] = future

        envelope = {"tipo": tipo, "topico": command_topic, "payload": payload}
        headers = [
            ("event_id", str(uuid.uuid4()).encode("utf-8")),
            ("timestamp", datetime.now(timezone.utc).isoformat().encode("utf-8")),
            ("producer", SERVICE_NAME.encode("utf-8")),
            ("version", b"1.0"),
            ("topic", command_topic.encode("utf-8")),
            ("correlation_id", correlation_id.encode("utf-8")),
        ]

        try:
            await self._producer.send_and_wait(
                command_topic,
                json.dumps(envelope).encode("utf-8"),
                headers=headers,
            )
            return await asyncio.wait_for(
                future, timeout=settings.bff_kafka_reply_timeout_seconds
            )
        finally:
            self._pending.pop(correlation_id, None)


client = KafkaRequestReplyClient()
