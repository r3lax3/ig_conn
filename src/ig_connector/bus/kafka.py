"""Kafka bus (aiokafka): SASL_PLAINTEXT + SCRAM-SHA-256, topics and group from channel_type.

Offsets are committed only when the runtime says so (no auto commit), so a command read
but not answered before a stop comes again after the restart.
"""

import logging
from collections.abc import AsyncIterator, Iterable
from typing import TYPE_CHECKING, Any, Self

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener, TopicPartition
from pydantic import SecretStr

from ig_connector.bus.port import Delivery, commands_topic, events_topic
from ig_connector.contract.codec import ContractViolationError, serialize
from ig_connector.contract.envelope import EventEnvelope

if TYPE_CHECKING:
    from ig_connector.settings import Settings

log = logging.getLogger(__name__)

_POLL_MS = 500


def kafka_auth(bootstrap_servers: str, username: str, password: SecretStr) -> dict[str, Any]:
    return {
        "bootstrap_servers": bootstrap_servers,
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_plain_username": username,
        "sasl_plain_password": password.get_secret_value(),
    }


class _Session(ConsumerRebalanceListener):  # type: ignore[misc]
    def __init__(self, consumer: AIOKafkaConsumer) -> None:
        self.consumer = consumer
        self.lost = False
        self.committed: dict[int, int] = {}

    def on_partitions_revoked(self, revoked: Iterable[TopicPartition]) -> None:
        # One replica per channel (contract §3): losing partitions means another consumer
        # joined or our session expired. Records in flight would come again in this same
        # session and break offset tracking, so the stream ends and the process restarts.
        if revoked:
            self.lost = True

    def on_partitions_assigned(self, assigned: Iterable[TopicPartition]) -> None:
        log.info("partitions assigned", extra={"partitions": sorted(tp.partition for tp in assigned)})


class KafkaBus:
    def __init__(
        self,
        *,
        channel_type: str,
        bootstrap_servers: str,
        username: str,
        password: SecretStr,
        group_id: str,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self.channel_type = channel_type
        self.commands_topic = commands_topic(channel_type)
        self.events_topic = events_topic(channel_type)
        self._servers = bootstrap_servers
        self._username = username
        self._password = password
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._producer: AIOKafkaProducer | None = None
        self._session: _Session | None = None

    @classmethod
    def from_settings(cls, settings: "Settings", *, group_id: str | None = None) -> Self:
        return cls(
            channel_type=settings.channel_type,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            username=settings.kafka_username,
            password=settings.kafka_password,
            group_id=group_id or settings.consumer_group,
        )

    async def start(self) -> None:
        producer = AIOKafkaProducer(**self._auth(), enable_idempotence=True, acks="all")
        try:
            await producer.start()
        except BaseException:
            await producer.stop()
            raise
        self._producer = producer

    async def stop(self) -> None:
        await self._end_session()
        if self._producer is not None:
            producer, self._producer = self._producer, None
            await producer.stop()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    def is_consuming(self) -> bool:
        """A consumer session is up and holds partitions."""
        session = self._session
        return session is not None and not session.lost and bool(session.consumer.assignment())

    async def deliveries(self) -> AsyncIterator[Delivery]:
        await self._end_session()
        consumer = AIOKafkaConsumer(
            **self._auth(),
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset=self._auto_offset_reset,
        )
        session = _Session(consumer)
        consumer.subscribe([self.commands_topic], listener=session)
        self._session = session
        try:
            await consumer.start()
            while self._session is session and not session.lost:
                batches = await consumer.getmany(timeout_ms=_POLL_MS)
                for records in batches.values():
                    for record in records:
                        if self._session is not session or session.lost:
                            return
                        yield Delivery(
                            record.topic, record.partition, record.offset, record.key, record.value
                        )
        finally:
            if self._session is session:
                await self._end_session()

    async def publish(self, event: EventEnvelope) -> None:
        if event.channel_type != self.channel_type:
            raise ContractViolationError(
                f"channel_type {event.channel_type!r} does not match topic {self.events_topic}"
            )
        if self._producer is None:
            raise RuntimeError("the bus is not started")
        await self._producer.send_and_wait(
            self.events_topic, serialize(event), key=str(event.source_id).encode()
        )

    async def commit(self, delivery: Delivery) -> None:
        session = self._session
        if session is None or session.lost:
            raise RuntimeError(f"p{delivery.partition}@{delivery.offset}: no consumer session holds it")
        last = session.committed.get(delivery.partition, -1)
        if delivery.offset < last:
            raise ValueError(
                f"commit going backwards in partition {delivery.partition}: {delivery.offset} < {last}"
            )
        # Kafka stores the next offset to read
        tp = TopicPartition(delivery.topic, delivery.partition)
        await session.consumer.commit({tp: delivery.offset + 1})
        session.committed[delivery.partition] = delivery.offset

    def _auth(self) -> dict[str, Any]:
        # unwrapped only for the client being built, never kept in plain text
        return kafka_auth(self._servers, self._username, self._password)

    async def _end_session(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            await session.consumer.stop()
