import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from ig_connector.bus import Bus, Delivery
from ig_connector.contract.codec import ContractViolationError, build_event, parse_command, reply_to
from ig_connector.contract.envelope import CONTRACT_VERSION, CommandType, EventType
from ig_connector.contract.events import AckPayload, StatusPayload
from tests.support.crm import CHANNEL_TYPE, command
from tests.support.memory_bus import MemoryBus

NOW = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
RESOLVE = {"recipient_kind": "username", "value": "someone"}


def _resolve(source_id: UUID) -> dict[str, Any]:
    return command(CommandType.RESOLVE_RECIPIENT, RESOLVE, source_id=source_id)


async def _take(deliveries: AsyncIterator[Delivery], count: int) -> list[Delivery]:
    async with asyncio.timeout(1):
        return [await anext(deliveries) for _ in range(count)]


async def test_commands_of_one_source_land_in_one_partition_in_order() -> None:
    bus = MemoryBus(CHANNEL_TYPE, partitions=4)
    source = uuid4()
    for _ in range(3):
        bus.submit(_resolve(source))

    received = await _take(bus.deliveries(), 3)

    assert {d.partition for d in received} == {received[0].partition}
    assert [d.offset for d in received] == [0, 1, 2]
    assert {d.topic for d in received} == {f"crm.connector.commands.{CHANNEL_TYPE}"}
    assert {d.key for d in received} == {str(source).encode()}
    assert parse_command(received[0].value).envelope.source_id == source


async def test_consumer_waits_for_commands_submitted_later() -> None:
    bus = MemoryBus(CHANNEL_TYPE)
    pending = asyncio.ensure_future(_take(bus.deliveries(), 1))
    await asyncio.sleep(0)
    sent = bus.submit(_resolve(uuid4()))
    assert await pending == [sent]


async def test_published_event_is_keyed_by_source_and_readable_by_type() -> None:
    bus = MemoryBus(CHANNEL_TYPE)
    delivery = bus.submit(_resolve(uuid4()))
    cmd = parse_command(delivery.value)

    port: Bus = bus
    await port.publish(reply_to(cmd, AckPayload(), occurred_at=NOW))

    [ack] = bus.events(EventType.ACK)
    assert ack.topic == f"crm.connector.events.{CHANNEL_TYPE}"
    assert ack.key == str(cmd.envelope.source_id).encode()
    assert ack.envelope.operation_id == cmd.envelope.operation_id
    assert isinstance(ack.payload, AckPayload)
    assert bus.events(EventType.RESULT) == []


async def test_event_breaking_the_contract_is_refused() -> None:
    bus = MemoryBus(CHANNEL_TYPE)
    broken = build_event(
        payload=StatusPayload(status="active"),
        operation_id=uuid4(),
        source_id=uuid4(),
        channel_type=CHANNEL_TYPE,
        contract_version=CONTRACT_VERSION,
        occurred_at=NOW,
    ).model_copy(update={"payload": {"status": "error"}})  # error without error_code

    with pytest.raises(ContractViolationError):
        await bus.publish(broken)
    assert bus.events() == []


async def test_reopened_consumer_resumes_after_the_committed_offset() -> None:
    bus = MemoryBus(CHANNEL_TYPE, partitions=1)
    first, second, third = (bus.submit(_resolve(uuid4())) for _ in range(3))
    before_restart = bus.deliveries()
    taken_first, *_ = await _take(before_restart, 3)

    await bus.commit(taken_first)
    assert bus.is_committed(first)
    assert not bus.is_committed(second)

    after_restart = bus.deliveries()
    assert await _take(after_restart, 2) == [second, third]
    with pytest.raises(StopAsyncIteration):
        await anext(before_restart)


async def test_commit_cannot_go_backwards() -> None:
    bus = MemoryBus(CHANNEL_TYPE, partitions=1)
    bus.submit(_resolve(uuid4()))
    bus.submit(_resolve(uuid4()))
    first, second = await _take(bus.deliveries(), 2)
    await bus.commit(second)
    with pytest.raises(ValueError, match="backwards"):
        await bus.commit(first)


async def test_commit_needs_a_delivery_of_the_current_session() -> None:
    bus = MemoryBus(CHANNEL_TYPE, partitions=1)
    bus.submit(_resolve(uuid4()))
    never_delivered = bus.submit(_resolve(uuid4()))
    [stale] = await _take(bus.deliveries(), 1)
    with pytest.raises(ValueError, match="not delivered"):
        await bus.commit(never_delivered)

    await _take(bus.deliveries(), 2)  # restart: a new session took over
    with pytest.raises(ValueError, match="not delivered"):
        await bus.commit(stale)


async def test_event_on_a_foreign_channel_is_refused() -> None:
    bus = MemoryBus(CHANNEL_TYPE)
    event = reply_to(parse_command(bus.submit(_resolve(uuid4())).value), AckPayload(), occurred_at=NOW)
    with pytest.raises(ContractViolationError, match="channel_type"):
        await bus.publish(event.model_copy(update={"channel_type": "max_bot"}))


async def test_wait_until_reports_what_was_published_on_timeout() -> None:
    bus = MemoryBus(CHANNEL_TYPE)
    with pytest.raises(AssertionError, match="not met"):
        await bus.wait_until(lambda: bool(bus.events(EventType.RESULT)), within=0.05)


async def test_wait_until_returns_once_the_condition_holds() -> None:
    bus = MemoryBus(CHANNEL_TYPE)
    delivery = bus.submit(_resolve(uuid4()))

    async def connector() -> None:
        await asyncio.sleep(0.01)
        await bus.commit(await anext(bus.deliveries()))

    task = asyncio.create_task(connector())
    await bus.wait_until(lambda: bus.is_committed(delivery))
    await task


async def test_listener_sees_every_bus_step() -> None:
    seen: list[tuple[str, dict[str, Any]]] = []
    bus = MemoryBus(CHANNEL_TYPE, listener=lambda kind, fields: seen.append((kind, fields)))
    bus.submit(_resolve(uuid4()))
    [delivery] = await _take(bus.deliveries(), 1)
    await bus.publish(reply_to(parse_command(delivery.value), AckPayload(), occurred_at=NOW))
    await bus.commit(delivery)

    assert [kind for kind, _ in seen] == ["bus.command", "bus.deliver", "bus.publish", "bus.commit"]
    assert seen[0][1]["command"]["type"] == "command.resolve_recipient"
    assert seen[2][1]["event"]["type"] == "ack"
    assert seen[3][1] == {"partition": delivery.partition, "offset": delivery.offset}
