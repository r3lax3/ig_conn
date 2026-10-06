# The service as deployed, against Kafka and Postgres of the local stand: uv run pytest -m integration

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from pydantic import SecretStr

from ig_connector.app import Infrastructure, Wire, Wiring, default_wiring, serve
from ig_connector.bus import kafka_auth
from ig_connector.bus.kafka import KafkaBus
from ig_connector.contract.codec import parse_event
from ig_connector.contract.envelope import CommandType, EventEnvelope, EventType
from ig_connector.contract.errors import ResultErrorCode
from ig_connector.contract.events import EventPayload, ResultPayload
from ig_connector.devtools.fake_crm import FakeCrmSettings, build_command
from ig_connector.health import is_healthy
from ig_connector.runtime import CommandContext, Handler
from ig_connector.settings import Settings

pytestmark = pytest.mark.integration


@pytest.fixture
def crm_settings() -> FakeCrmSettings:
    return FakeCrmSettings()  # filled from .env


@pytest.fixture
def settings(crm_settings: FakeCrmSettings, postgres_dsn: str, tmp_path: Path) -> Settings:
    return Settings.model_validate(
        {
            **crm_settings.model_dump(),
            "database_url": SecretStr(postgres_dsn),
            "health_file": tmp_path / "health",
        }
    )


@pytest.fixture
async def group(settings: Settings) -> str:
    """A fresh consumer group already positioned at the end of the commands topic."""
    group_id = f"{settings.consumer_group}.test-{uuid4().hex[:8]}"
    consumer = AIOKafkaConsumer(
        settings.commands_topic,
        **_connector_auth(settings),
        group_id=group_id,
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        await consumer.getmany(timeout_ms=2_000)
        partitions = consumer.assignment()
        assert partitions, "the test group got no partitions"
        await consumer.seek_to_end(*partitions)
        await consumer.commit({tp: await consumer.position(tp) for tp in partitions})
    finally:
        await consumer.stop()
    return group_id


@pytest.fixture
async def crm_events(crm_settings: FakeCrmSettings) -> AsyncIterator[AIOKafkaConsumer]:
    consumer = AIOKafkaConsumer(
        crm_settings.events_topic,
        **crm_settings.kafka(),
        group_id=f"fake-crm-test-{uuid4().hex[:8]}",
        auto_offset_reset="latest",
    )
    await consumer.start()
    await consumer.getmany(timeout_ms=2_000)  # join and pin the offsets before anything is published
    yield consumer
    await consumer.stop()


def _connector_auth(settings: Settings) -> dict[str, Any]:
    return kafka_auth(settings.kafka_bootstrap_servers, settings.kafka_username, settings.kafka_password)


@asynccontextmanager
async def running_service(
    settings: Settings, group: str, wire: Wire = default_wiring
) -> AsyncIterator[asyncio.Task[None]]:
    """The service as main() runs it; leaving the block is a SIGTERM."""
    bus = KafkaBus.from_settings(settings, group_id=group)
    task = asyncio.create_task(serve(settings, bus=bus, wire=wire))
    try:
        yield task
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def _send_resolve(crm_settings: FakeCrmSettings) -> tuple[UUID, TopicPartition, int]:
    source_id = uuid4()
    command = build_command(
        crm_settings,
        CommandType.RESOLVE_RECIPIENT,
        {"recipient_kind": "username", "value": "someone"},
        source_id=source_id,
    )
    producer = AIOKafkaProducer(**crm_settings.kafka())
    await producer.start()
    try:
        sent = await producer.send_and_wait(
            crm_settings.commands_topic, json.dumps(command).encode(), key=str(source_id).encode()
        )
    finally:
        await producer.stop()
    return UUID(command["operation_id"]), TopicPartition(sent.topic, sent.partition), sent.offset


async def _events_of(
    consumer: AIOKafkaConsumer, operation_id: UUID, count: int, *, within: float = 30.0
) -> list[tuple[EventEnvelope, EventPayload]]:
    found: list[tuple[EventEnvelope, EventPayload]] = []
    async with asyncio.timeout(within):
        while len(found) < count:
            for batch in (await consumer.getmany(timeout_ms=500)).values():
                for record in batch:
                    envelope, payload = parse_event(record.value)
                    if envelope.operation_id == operation_id:
                        found.append((envelope, payload))
    return found


async def _committed(settings: Settings, group: str, partition: TopicPartition) -> int | None:
    consumer = AIOKafkaConsumer(**_connector_auth(settings), group_id=group, enable_auto_commit=False)
    await consumer.start()
    try:
        committed: int | None = await consumer.committed(partition)
        return committed
    finally:
        await consumer.stop()


async def _eventually(condition: Callable[[], Awaitable[bool]], *, within: float = 15.0) -> None:
    async with asyncio.timeout(within):
        while not await condition():  # noqa: ASYNC110 - polls Kafka and files
            await asyncio.sleep(0.2)


async def test_crm_command_gets_ack_and_result_and_its_offset_is_committed(
    settings: Settings, crm_settings: FakeCrmSettings, group: str, crm_events: AIOKafkaConsumer
) -> None:
    async with running_service(settings, group):
        operation_id, partition, offset = await _send_resolve(crm_settings)

        [(ack, _), (result, payload)] = await _events_of(crm_events, operation_id, 2)

        assert ack.type is EventType.ACK
        assert result.type is EventType.RESULT
        assert isinstance(payload, ResultPayload)
        # nothing can bring an Account up yet: an honest offline answer
        assert payload.error_code is ResultErrorCode.SOURCE_OFFLINE
        assert result.channel_type == settings.channel_type

        async def committed() -> bool:
            return await _committed(settings, group, partition) == offset + 1

        await _eventually(committed)


async def test_unanswered_command_comes_again_after_a_restart(
    settings: Settings, crm_settings: FakeCrmSettings, group: str, crm_events: AIOKafkaConsumer
) -> None:
    started = asyncio.Event()

    async def hangs(ctx: CommandContext) -> ResultPayload:
        started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def wire_hanging(infra: Infrastructure) -> Wiring:
        wiring = await default_wiring(infra)
        handlers = {
            **wiring.handlers,
            CommandType.RESOLVE_RECIPIENT: Handler(hangs, needs_active_source=False),
        }
        return Wiring(sources=wiring.sources, handlers=handlers)

    async with running_service(settings, group, wire_hanging):
        operation_id, partition, offset = await _send_resolve(crm_settings)
        [(ack, _)] = await _events_of(crm_events, operation_id, 1)
        assert ack.type is EventType.ACK
        async with asyncio.timeout(10):
            await started.wait()

    assert await _committed(settings, group, partition) == offset  # still before the command

    async with running_service(settings, group):
        events = await _events_of(crm_events, operation_id, 2)
        assert [envelope.type for envelope, _ in events] == [EventType.ACK, EventType.RESULT]

        async def committed() -> bool:
            return await _committed(settings, group, partition) == offset + 1

        await _eventually(committed)


async def test_stop_lets_the_running_command_finish_and_commit(
    settings: Settings, crm_settings: FakeCrmSettings, group: str, crm_events: AIOKafkaConsumer
) -> None:
    started, release = asyncio.Event(), asyncio.Event()

    async def slow(ctx: CommandContext) -> ResultPayload:
        started.set()
        await release.wait()
        return ResultPayload(ok=False, error_code=ResultErrorCode.CHANNEL_REJECTED, error_text="slow")

    async def wire_slow(infra: Infrastructure) -> Wiring:
        wiring = await default_wiring(infra)
        handlers = {
            **wiring.handlers,
            CommandType.RESOLVE_RECIPIENT: Handler(slow, needs_active_source=False),
        }
        return Wiring(sources=wiring.sources, handlers=handlers)

    stop = asyncio.Event()
    bus = KafkaBus.from_settings(settings, group_id=group)
    service = asyncio.create_task(serve(settings, bus=bus, wire=wire_slow, stop=stop))
    try:
        operation_id, partition, offset = await _send_resolve(crm_settings)
        async with asyncio.timeout(10):
            await started.wait()
        stop.set()  # SIGTERM
        await asyncio.sleep(0.5)
        assert not service.done()
        release.set()
        async with asyncio.timeout(10):
            with suppress(asyncio.CancelledError):
                await service
    finally:
        service.cancel()
        with suppress(asyncio.CancelledError):
            await service

    events = await _events_of(crm_events, operation_id, 2)
    assert [envelope.type for envelope, _ in events] == [EventType.ACK, EventType.RESULT]
    assert await _committed(settings, group, partition) == offset + 1


async def test_health_follows_the_service(settings: Settings, group: str) -> None:
    assert not is_healthy(settings.health_file)

    async with running_service(settings, group):

        async def healthy() -> bool:
            return is_healthy(settings.health_file)

        await _eventually(healthy)

    assert not is_healthy(settings.health_file)
