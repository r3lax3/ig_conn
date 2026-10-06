# Needs a test bus from .env incl. FAKE_CRM_KAFKA_*: uv run pytest -m integration

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ig_connector.bus import kafka_auth
from ig_connector.contract.codec import parse_command, parse_event, reply_to, serialize
from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.events import AckPayload
from ig_connector.devtools.fake_crm import FakeCrmSettings, build_command

pytestmark = pytest.mark.integration


@pytest.fixture
def settings() -> FakeCrmSettings:
    return FakeCrmSettings()  # filled from .env


async def _first(consumer: AIOKafkaConsumer, timeout_ms: int = 15_000) -> bytes:
    batches = await consumer.getmany(timeout_ms=timeout_ms, max_records=1)
    records = [record for batch in batches.values() for record in batch]
    assert records, "no message within timeout"
    value: bytes = records[0].value
    return value


async def _joined(consumer: AIOKafkaConsumer) -> AIOKafkaConsumer:
    await consumer.start()
    await consumer.getmany(timeout_ms=2_000)  # join the group and pin offsets before publishing
    return consumer


@pytest.fixture
async def connector_commands(settings: FakeCrmSettings) -> AsyncIterator[AIOKafkaConsumer]:
    consumer = AIOKafkaConsumer(
        settings.commands_topic,
        **kafka_auth(settings.kafka_bootstrap_servers, settings.kafka_username, settings.kafka_password),
        group_id=f"{settings.consumer_group}.test-{uuid4().hex[:8]}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    yield await _joined(consumer)
    await consumer.stop()


@pytest.fixture
async def crm_events(settings: FakeCrmSettings) -> AsyncIterator[AIOKafkaConsumer]:
    consumer = AIOKafkaConsumer(
        settings.events_topic, **settings.kafka(), group_id=f"fake-crm-test-{uuid4().hex[:8]}"
    )
    yield await _joined(consumer)
    await consumer.stop()


async def test_command_ack_round_trip(
    settings: FakeCrmSettings, connector_commands: AIOKafkaConsumer, crm_events: AIOKafkaConsumer
) -> None:
    crm = AIOKafkaProducer(**settings.kafka())
    connector = AIOKafkaProducer(
        **kafka_auth(settings.kafka_bootstrap_servers, settings.kafka_username, settings.kafka_password)
    )
    await crm.start()
    await connector.start()
    try:
        source_id = uuid4()
        payload = {"recipient_kind": "username", "value": "someone"}
        command_json = build_command(settings, CommandType.RESOLVE_RECIPIENT, payload, source_id=source_id)
        await crm.send_and_wait(
            settings.commands_topic, json.dumps(command_json).encode(), key=str(source_id).encode()
        )

        command = parse_command(await _first(connector_commands))
        assert command.envelope.source_id == source_id
        await connector.send_and_wait(
            settings.events_topic,
            serialize(reply_to(command, AckPayload(), occurred_at=datetime.now(UTC))),
            key=str(source_id).encode(),
        )

        envelope, _ = parse_event(await _first(crm_events))
        assert envelope.type is EventType.ACK
        assert envelope.operation_id == command.envelope.operation_id
    finally:
        await crm.stop()
        await connector.stop()
