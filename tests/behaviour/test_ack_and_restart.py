"""Ack, result and commit on the real runtime, and a restart that redoes only the open command.

Also the shape of a behaviour test: CRM submits commands, the connector runs over the
bus port, Postgres and the clock; the test checks what reached the bus and what got
committed, then rebuilds the connector over the same database and bus (a restart). The platform here is a handler that "talks
to Instagram" for five seconds of connector time.
"""

from uuid import UUID, uuid4

from ig_connector.contract.codec import parse_command
from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.events import ResultPayload
from ig_connector.runtime import (
    CommandContext,
    Handler,
    SourceCondition,
    SourceState,
    default_handlers,
)
from tests.support.connector import eventually, running_connector
from tests.support.crm import command
from tests.support.fake_clock import FakeClock
from tests.support.memory_bus import MemoryBus

LOOKUP = {"recipient_kind": "username", "value": "someone"}


class AllActive:
    async def state(self, source_id: UUID) -> SourceState:
        return SourceState(source_id, SourceCondition.ACTIVE)


def slow_lookup(clock: FakeClock, calls: list[UUID]) -> dict[CommandType, Handler]:
    async def lookup(ctx: CommandContext) -> ResultPayload:
        calls.append(ctx.command.envelope.operation_id)
        await clock.sleep(5)
        return ResultPayload(ok=True, external_chat_id="1789", display_name="Someone")

    return {**default_handlers(), CommandType.RESOLVE_RECIPIENT: Handler(lookup)}


async def test_command_gets_ack_then_result_and_is_committed(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str
) -> None:
    source = uuid4()
    delivery = bus.submit(command(CommandType.RESOLVE_RECIPIENT, LOOKUP, source_id=source))
    operation_id = parse_command(delivery.value).envelope.operation_id
    calls: list[UUID] = []
    handlers = slow_lookup(clock, calls)

    async with running_connector(bus, clock, postgres_dsn, sources=AllActive(), handlers=handlers):
        await bus.wait_until(lambda: bool(bus.events(EventType.ACK)))
        # ack goes out on receipt; the handler starts at the command's turn
        await eventually(lambda: bool(calls))
        assert bus.events(EventType.RESULT) == []
        assert not bus.is_committed(delivery)

        await clock.advance(5)
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [ack] = bus.events(EventType.ACK, operation_id=operation_id)
    [result] = bus.events(EventType.RESULT, operation_id=operation_id)
    assert ack.key == str(source).encode()
    assert result.payload == ResultPayload(ok=True, external_chat_id="1789", display_name="Someone")
    assert (result.envelope.occurred_at - ack.envelope.occurred_at).total_seconds() == 5


async def test_rebuilt_connector_redoes_only_the_uncommitted_command(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str
) -> None:
    calls: list[UUID] = []
    handlers = slow_lookup(clock, calls)
    first = bus.submit(command(CommandType.RESOLVE_RECIPIENT, LOOKUP, source_id=uuid4()))
    first_id = parse_command(first.value).envelope.operation_id

    async with running_connector(bus, clock, postgres_dsn, sources=AllActive(), handlers=handlers):
        await eventually(lambda: first_id in calls)
        await clock.advance(5)
        await bus.wait_until(lambda: bus.is_committed(first))
        second = bus.submit(command(CommandType.RESOLVE_RECIPIENT, LOOKUP, source_id=uuid4()))
        second_id = parse_command(second.value).envelope.operation_id
        await eventually(lambda: second_id in calls)
    # the process died while `second` was talking to the platform

    async with running_connector(bus, clock, postgres_dsn, sources=AllActive(), handlers=handlers):
        await eventually(lambda: calls.count(second_id) == 2)
        await clock.advance(5)
        await bus.wait_until(lambda: bus.is_committed(second))

    assert calls.count(first_id) == 1
    assert calls[-1] == second_id
    assert len(bus.events(EventType.RESULT, operation_id=first_id)) == 1
    assert len(bus.events(EventType.RESULT, operation_id=second_id)) == 1
