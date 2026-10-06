"""Command lifecycle: every CRM command gets ack, exactly one result, and a committed offset.

Until the platform port exists, "the platform" here is a resolve_recipient handler that
the test plugs into the registry: it records calls and can be held open with a gate.
"""

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from ig_connector.contract.codec import parse_command, parse_envelope
from ig_connector.contract.commands import ResolveRecipientPayload
from ig_connector.contract.envelope import CommandEnvelope, CommandType, EventType
from ig_connector.contract.errors import ResultErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.runtime import CommandContext, Handler, SourceCondition, SourceState, default_handlers
from ig_connector.store import Operation
from ig_connector.store.postgres import PostgresStore
from tests.support.connector import running_connector
from tests.support.crm import CHANNEL_TYPE, command
from tests.support.fake_clock import FakeClock
from tests.support.memory_bus import MemoryBus
from tests.support.tracing import Trace

EDIT = {"message_id": "m1", "external_chat_id": "1789", "external_message_id": "x1", "text": "fixed"}
DELETE = {"message_id": "m1", "external_chat_id": "1789", "external_message_id": "x1"}
SEND = {"message_id": "m1", "external_chat_id": "1789", "text": "hello"}


def resolve(value: str) -> dict[str, Any]:
    return {"recipient_kind": "username", "value": value}


class Sources:
    """Source states as the test sets them; offline unless said otherwise."""

    def __init__(self) -> None:
        self.conditions: dict[UUID, SourceCondition] = {}

    async def state(self, source_id: UUID) -> SourceState:
        return SourceState(source_id, self.conditions.get(source_id, SourceCondition.OFFLINE))


class Platform:
    """Answers resolve_recipient; a lookup for a gated value waits until the gate opens."""

    def __init__(self, trace: Trace) -> None:
        self.calls: list[str] = []
        self.gates: dict[str, asyncio.Event] = {}
        self._trace = trace

    def gate(self, value: str) -> asyncio.Event:
        return self.gates.setdefault(value, asyncio.Event())

    async def resolve(self, ctx: CommandContext) -> ResultPayload:
        assert isinstance(ctx.command.payload, ResolveRecipientPayload)
        value = ctx.command.payload.value
        self.calls.append(value)
        self._trace.record("platform.resolve", {"value": value})
        if value in self.gates:
            await self.gates[value].wait()
        if value == "boom":
            raise RuntimeError("bug in the handler, secret text inside")
        return ResultPayload(ok=True, external_chat_id=f"id-{value}", display_name=value)

    def handlers(self) -> dict[CommandType, Handler]:
        return {**default_handlers(), CommandType.RESOLVE_RECIPIENT: Handler(self.resolve)}


@pytest.fixture
def sources() -> Sources:
    return Sources()


@pytest.fixture
def platform(trace: Trace) -> Platform:
    return Platform(trace)


def results(bus: MemoryBus, operation_id: UUID) -> list[ResultPayload]:
    out = []
    for event in bus.events(EventType.RESULT, operation_id=operation_id):
        assert isinstance(event.payload, ResultPayload)
        out.append(event.payload)
    return out


def operation_of(delivery_value: bytes) -> UUID:
    return parse_envelope(delivery_value).operation_id


async def eventually(condition: Callable[[], bool], within: float = 2.0) -> None:
    """For conditions outside the bus (bus.wait_until only wakes on bus changes)."""
    async with asyncio.timeout(within):
        while not condition():  # noqa: ASYNC110  the fakes have no event to wait on
            await asyncio.sleep(0.01)


async def test_edit_is_acked_then_answered_edit_unsupported(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str
) -> None:
    source = uuid4()
    delivery = bus.submit(command(CommandType.EDIT, EDIT, source_id=source))
    sent = parse_command(delivery.value).envelope

    async with running_connector(bus, clock, postgres_dsn):
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [ack, result] = bus.events(operation_id=sent.operation_id)
    assert (ack.envelope.type, result.envelope.type) == (EventType.ACK, EventType.RESULT)
    assert isinstance(result.payload, ResultPayload)
    assert result.payload.ok is False
    assert result.payload.error_code == ResultErrorCode.EDIT_UNSUPPORTED
    for reply in (ack, result):
        assert reply.key == str(source).encode()
        assert reply.envelope.source_id == source
        assert reply.envelope.channel_type == CHANNEL_TYPE
        assert reply.envelope.contract_version == sent.contract_version
        assert reply.envelope.occurred_at == clock.now()


async def test_delete_is_rejected_by_the_channel(bus: MemoryBus, clock: FakeClock, postgres_dsn: str) -> None:
    delivery = bus.submit(command(CommandType.DELETE, DELETE, source_id=uuid4()))

    async with running_connector(bus, clock, postgres_dsn):
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = results(bus, operation_of(delivery.value))
    assert result.error_code == ResultErrorCode.CHANNEL_REJECTED
    assert len(bus.events(EventType.ACK)) == 1


@pytest.mark.parametrize(
    ("command_type", "payload"), [(CommandType.SEND, SEND), (CommandType.RESOLVE_RECIPIENT, resolve("x"))]
)
async def test_command_to_a_source_without_session_gets_source_offline(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    command_type: CommandType,
    payload: dict[str, Any],
) -> None:
    delivery = bus.submit(command(command_type, payload, source_id=uuid4()))

    async with running_connector(bus, clock, postgres_dsn):
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = results(bus, operation_of(delivery.value))
    assert result.error_code == ResultErrorCode.SOURCE_OFFLINE
    assert len(bus.events(EventType.ACK)) == 1


async def test_restricted_source_gets_peer_flood_without_touching_the_platform(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    source = uuid4()
    sources.conditions[source] = SourceCondition.LIMITED
    delivery = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("x"), source_id=source))

    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = results(bus, operation_of(delivery.value))
    assert result.error_code == ResultErrorCode.PEER_FLOOD
    assert platform.calls == []


async def test_invalid_payload_of_a_known_command_is_an_internal_error(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str
) -> None:
    delivery = bus.submit(command(CommandType.SEND, {"text": "no chat, no message id"}, source_id=uuid4()))

    async with running_connector(bus, clock, postgres_dsn):
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = results(bus, operation_of(delivery.value))
    assert result.error_code == ResultErrorCode.INTERNAL_ERROR
    assert result.error_text is not None
    assert "no chat" not in result.error_text
    assert len(bus.events(EventType.ACK)) == 1


async def test_handler_bug_is_an_internal_error_and_the_source_keeps_working(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    source = uuid4()
    sources.conditions[source] = SourceCondition.ACTIVE
    broken = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("boom"), source_id=source))
    fine = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("ok"), source_id=source))

    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: bus.is_committed(fine))

    [failed] = results(bus, operation_of(broken.value))
    assert failed.error_code == ResultErrorCode.INTERNAL_ERROR
    assert "secret" not in (failed.error_text or "")
    [ok] = results(bus, operation_of(fine.value))
    assert ok.ok is True


async def test_repeated_delivery_of_a_finished_command_returns_the_saved_result(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    source = uuid4()
    sources.conditions[source] = SourceCondition.ACTIVE
    sent = command(CommandType.RESOLVE_RECIPIENT, resolve("anna"), source_id=source)
    first = bus.submit(sent)

    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: bus.is_committed(first))
        await clock.advance(30)
        repeated = bus.submit(sent)
        await bus.wait_until(lambda: bus.is_committed(repeated))

    operation_id = operation_of(first.value)
    assert platform.calls == ["anna"]
    assert len(bus.events(EventType.ACK, operation_id=operation_id)) == 2
    first_result, second_result = results(bus, operation_id)
    assert second_result == first_result


async def test_saved_result_survives_a_restart(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    source = uuid4()
    sources.conditions[source] = SourceCondition.ACTIVE
    sent = command(CommandType.RESOLVE_RECIPIENT, resolve("anna"), source_id=source)
    first = bus.submit(sent)
    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: bus.is_committed(first))

    repeated = bus.submit(sent)
    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: bus.is_committed(repeated))

    assert platform.calls == ["anna"]
    first_result, second_result = results(bus, operation_of(first.value))
    assert second_result == first_result


async def test_crm_retry_after_a_retryable_failure_gets_a_fresh_attempt(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    source = uuid4()
    sent = command(CommandType.RESOLVE_RECIPIENT, resolve("anna"), source_id=source)
    first = bus.submit(sent)

    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: bus.is_committed(first))
        sources.conditions[source] = SourceCondition.ACTIVE
        await clock.advance(30)  # CRM backoff before the retry
        retry = bus.submit(sent)
        await bus.wait_until(lambda: bus.is_committed(retry))

    offline, retried = results(bus, operation_of(first.value))
    assert offline.error_code == ResultErrorCode.SOURCE_OFFLINE
    assert retried.ok is True
    assert platform.calls == ["anna"]


async def test_command_with_foreign_channel_type_is_skipped_and_committed(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    foreign = bus.submit(
        command(CommandType.EDIT, EDIT, source_id=uuid4(), channel_type="individual_x_account")
    )

    with caplog.at_level(logging.ERROR, logger="ig_connector"):
        async with running_connector(bus, clock, postgres_dsn):
            await bus.wait_until(lambda: bus.is_committed(foreign))

    assert bus.events() == []
    assert any("channel_type" in record.getMessage() for record in caplog.records)


async def test_unreadable_record_is_skipped_and_committed(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str
) -> None:
    garbage = bus.submit(b"not a command", key=str(uuid4()))

    async with running_connector(bus, clock, postgres_dsn):
        await bus.wait_until(lambda: bus.is_committed(garbage))

    assert bus.events() == []


async def test_commands_of_one_source_run_in_order_and_others_do_not_wait(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    slow_source, other_source = uuid4(), uuid4()
    sources.conditions |= {slow_source: SourceCondition.ACTIVE, other_source: SourceCondition.ACTIVE}
    gate = platform.gate("slow")
    slow = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("slow"), source_id=slow_source))
    after_slow = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("next"), source_id=slow_source))
    other = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("other"), source_id=other_source))

    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: bool(results(bus, operation_of(other.value))))
        await eventually(lambda: "slow" in platform.calls)
        assert "next" not in platform.calls
        assert results(bus, operation_of(after_slow.value)) == []

        gate.set()
        await bus.wait_until(lambda: bus.is_committed(after_slow))

    assert platform.calls.index("slow") < platform.calls.index("next")
    [slow_event] = bus.events(EventType.RESULT, operation_id=operation_of(slow.value))
    [next_event] = bus.events(EventType.RESULT, operation_id=operation_of(after_slow.value))
    assert bus.events().index(slow_event) < bus.events().index(next_event)


async def test_offset_stays_behind_an_unfinished_command_of_the_partition(
    trace: Trace, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    bus = MemoryBus(CHANNEL_TYPE, partitions=1, listener=trace.record)
    slow_source, fast_source = uuid4(), uuid4()
    sources.conditions |= {slow_source: SourceCondition.ACTIVE, fast_source: SourceCondition.ACTIVE}
    gate = platform.gate("slow")
    slow = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("slow"), source_id=slow_source))
    fast = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("fast"), source_id=fast_source))

    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: bool(results(bus, operation_of(fast.value))))
        await asyncio.sleep(0.05)  # room for a wrong commit to happen
        assert not bus.is_committed(fast)

        gate.set()
        await bus.wait_until(lambda: bus.is_committed(fast))

    assert bus.is_committed(slow)


async def test_parallel_sources_are_capped(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    first_source, second_source = uuid4(), uuid4()
    sources.conditions |= {first_source: SourceCondition.ACTIVE, second_source: SourceCondition.ACTIVE}
    gate = platform.gate("slow")
    bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("slow"), source_id=first_source))
    waiting = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("waiting"), source_id=second_source))

    async with running_connector(
        bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers(), max_parallel_sources=1
    ):
        await eventually(lambda: platform.calls == ["slow"])
        await asyncio.sleep(0.05)
        assert platform.calls == ["slow"]

        gate.set()
        await bus.wait_until(lambda: bus.is_committed(waiting))

    assert platform.calls == ["slow", "waiting"]


async def test_unknown_command_type_still_gets_ack_and_internal_error(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str
) -> None:
    sent = command(CommandType.EDIT, EDIT, source_id=uuid4())
    sent["type"] = "command.typing"
    delivery = bus.submit(sent)

    async with running_connector(bus, clock, postgres_dsn):
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = results(bus, operation_of(delivery.value))
    assert result.error_code == ResultErrorCode.INTERNAL_ERROR
    assert len(bus.events(EventType.ACK)) == 1


class StoreFailingToSaveResults(PostgresStore):
    async def finish_operation(
        self, operation: Operation, result: ResultPayload, *, now: datetime
    ) -> Operation:
        raise ConnectionError("database went away")


async def test_outcome_is_reported_even_when_saving_it_fails(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    source = uuid4()
    sources.conditions[source] = SourceCondition.ACTIVE
    delivery = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("anna"), source_id=source))

    async with running_connector(
        bus,
        clock,
        postgres_dsn,
        sources=sources,
        handlers=platform.handlers(),
        store_type=StoreFailingToSaveResults,
    ):
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = results(bus, operation_of(delivery.value))
    assert result.ok is True


async def test_connector_fails_loudly_when_the_delivery_stream_ends(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str
) -> None:
    with pytest.raises(ExceptionGroup) as failure:
        async with running_connector(bus, clock, postgres_dsn):
            delivery = bus.submit(command(CommandType.EDIT, EDIT, source_id=uuid4()))
            await bus.wait_until(lambda: bus.is_committed(delivery))
            takeover = asyncio.create_task(anext(aiter(bus.deliveries()), None))  # ours ends now
            await asyncio.sleep(0.1)
    takeover.cancel()
    assert failure.group_contains(RuntimeError, match="delivery stream ended")


async def test_command_is_acked_on_receipt_while_its_source_is_busy(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    source = uuid4()
    sources.conditions[source] = SourceCondition.ACTIVE
    gate = platform.gate("slow")
    slow = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("slow"), source_id=source))
    queued = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("next"), source_id=source))

    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: bool(bus.events(EventType.ACK, operation_id=operation_of(queued.value))))
        assert results(bus, operation_of(slow.value)) == []
        assert "next" not in platform.calls

        gate.set()
        await bus.wait_until(lambda: bus.is_committed(queued))

    assert len(bus.events(EventType.ACK, operation_id=operation_of(queued.value))) == 1
    [ok] = results(bus, operation_of(queued.value))
    assert ok.ok is True


class StoreThatIsDown(PostgresStore):
    async def record_operation(self, envelope: CommandEnvelope, *, now: datetime) -> Operation:
        raise ConnectionError("database went away")


async def test_command_that_cannot_be_saved_gets_source_offline_without_ack(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    source = uuid4()
    sources.conditions[source] = SourceCondition.ACTIVE
    delivery = bus.submit(command(CommandType.RESOLVE_RECIPIENT, resolve("anna"), source_id=source))

    async with running_connector(
        bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers(), store_type=StoreThatIsDown
    ):
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = results(bus, operation_of(delivery.value))
    assert result.error_code == ResultErrorCode.SOURCE_OFFLINE
    assert bus.events(EventType.ACK) == []
    assert platform.calls == []


async def test_duplicate_record_read_while_the_first_runs_is_answered_from_its_result(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, sources: Sources, platform: Platform
) -> None:
    source = uuid4()
    sources.conditions[source] = SourceCondition.ACTIVE
    gate = platform.gate("anna")
    sent = command(CommandType.RESOLVE_RECIPIENT, resolve("anna"), source_id=source)
    bus.submit(sent)
    duplicate = bus.submit(sent)  # a producer retry: the same record twice in the topic

    async with running_connector(bus, clock, postgres_dsn, sources=sources, handlers=platform.handlers()):
        await bus.wait_until(lambda: len(bus.events(EventType.ACK)) == 2)
        gate.set()
        await bus.wait_until(lambda: bus.is_committed(duplicate))

    assert platform.calls == ["anna"]
    first, second = results(bus, operation_of(duplicate.value))
    assert second == first
