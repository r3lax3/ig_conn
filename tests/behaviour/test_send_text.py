"""command.send: a text into an existing personal dialog."""

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.errors import ResultErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Failure, OutgoingText, SentMessage
from ig_connector.runtime import CommandContext, Handler, NoActiveSources, SourceStates, default_handlers
from ig_connector.runtime.deadline import DEADLINE_PASSED
from ig_connector.runtime.refusals import failure
from ig_connector.runtime.send import NO_ATTACHMENTS, NO_NEW_DIALOGS
from ig_connector.store.postgres import PostgresStore
from tests.support.connector import running_connector
from tests.support.crm import command
from tests.support.fake_clock import FakeClock
from tests.support.fake_instagram import FakeInstagram
from tests.support.login import Connected, connect
from tests.support.memory_bus import MemoryBus
from tests.support.postgres import SESSION_KEY

PEER = "5551"
ATTACHMENT = {
    "kind": "image",
    "bucket": "crm-bus",
    "key": "crm/photo.png",
    "mime": "image/png",
    "size_bytes": 1024,
}


def send(source_id: UUID, *, to: str = PEER, text: str = "Hello there", **extra: Any) -> dict[str, Any]:
    payload = {"message_id": str(uuid4()), "external_chat_id": to, "text": text, **extra}
    return command(CommandType.SEND, payload, source_id=source_id)


def results(bus: MemoryBus, sent: dict[str, Any]) -> list[ResultPayload]:
    out = []
    for event in bus.events(EventType.RESULT, operation_id=UUID(sent["operation_id"])):
        assert isinstance(event.payload, ResultPayload)
        out.append(event.payload)
    return out


def refused(code: ResultErrorCode) -> Callable[[ResultPayload], bool]:
    return lambda result: not result.ok and result.error_code == code


@asynccontextmanager
async def sending_connector(
    bus: MemoryBus,
    clock: FakeClock,
    dsn: str,
    instagram: FakeInstagram,
    *,
    sources: SourceStates | None = None,
    extra_handlers: Mapping[CommandType, Handler] | None = None,
) -> AsyncIterator[None]:
    # the handler gets its own store over the same schema, like the service wires it
    store = await PostgresStore.open(dsn, session_key=SESSION_KEY)
    handlers = {
        # the service's registry: send is real once it has the platform and the store
        **default_handlers(instagram, store),
        **(extra_handlers or {}),
    }
    try:
        async with running_connector(
            bus, clock, dsn, instagram=instagram, handlers=handlers, sources=sources
        ):
            yield
    finally:
        await store.close()


async def connected_with_dialog(bus: MemoryBus, instagram: FakeInstagram) -> Connected:
    connected = await connect(bus, instagram, login="anna.shop", external_id="1789")
    instagram.add_dialog("1789", PEER)
    return connected


async def answered(bus: MemoryBus, sent: dict[str, Any]) -> list[ResultPayload]:
    delivery = bus.submit(sent)
    await bus.wait_until(lambda: bus.is_committed(delivery))
    return results(bus, sent)


async def saved_send(dsn: str, sent: dict[str, Any]) -> tuple[str, str | None]:
    """(state, client_context) of the send's Operation as stored."""
    db = await asyncpg.connect(dsn)
    try:
        row = await db.fetchrow(
            "select state, client_context from operations where operation_id = $1 and type = $2",
            UUID(sent["operation_id"]),
            CommandType.SEND.value,
        )
    finally:
        await db.close()
    assert row is not None
    return row["state"], row["client_context"]


async def eventually(condition: Callable[[], bool], within: float = 2.0) -> None:
    async with asyncio.timeout(within):
        while not condition():  # noqa: ASYNC110  the fakes have no event to wait on
            await asyncio.sleep(0.01)


async def test_text_goes_into_an_existing_dialog_and_comes_back_ok_with_the_message_id(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id, text="Your order is ready")
        [result] = await answered(bus, sent)

    [message] = instagram.sent
    assert (message.account_id, message.peer_id, message.text) == ("1789", PEER, "Your order is ready")
    assert len(message.client_context) == 19 and message.client_context.isdigit()
    # no `delivered`: Instagram does not tell
    assert result == ResultPayload(ok=True, external_message_id=message.message_id)
    types = [e.envelope.type for e in bus.events(operation_id=UUID(sent["operation_id"]))]
    assert types == [EventType.ACK, EventType.RESULT]


async def test_each_send_gets_its_own_label(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        await answered(bus, send(source.source_id))
        await answered(bus, send(source.source_id))

    first, second = instagram.sent
    assert first.client_context != second.client_context


async def test_reply_to_a_message_still_sends(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        [result] = await answered(bus, send(source.source_id, reply_to_external_id="item-777"))

    [message] = instagram.sent
    assert message.reply_to == "item-777"
    assert result.ok and result.external_message_id == message.message_id


async def test_attachments_are_rejected_without_touching_instagram(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        result = await answered(bus, send(source.source_id, attachments=[ATTACHMENT]))

    assert result == [
        ResultPayload(ok=False, error_code=ResultErrorCode.CHANNEL_REJECTED, error_text=NO_ATTACHMENTS)
    ]
    assert instagram.sent == []


async def test_recipient_without_a_dialog_is_rejected_as_a_new_dialog(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        result = await answered(bus, send(source.source_id, to="9999"))

    assert result == [
        ResultPayload(ok=False, error_code=ResultErrorCode.CHANNEL_REJECTED, error_text=NO_NEW_DIALOGS)
    ]
    assert instagram.sent == []


@pytest.mark.parametrize(
    ("platform_failure", "code"),
    [
        (Failure.FLOOD, ResultErrorCode.PEER_FLOOD),
        (Failure.RATE_LIMITED, ResultErrorCode.RATE_LIMITED),
        (Failure.REJECTED, ResultErrorCode.CHANNEL_REJECTED),
        (Failure.SESSION_REVOKED, ResultErrorCode.SOURCE_OFFLINE),
        (Failure.CHALLENGE, ResultErrorCode.SOURCE_OFFLINE),
        (Failure.NETWORK, ResultErrorCode.NETWORK_ERROR),
    ],
)
async def test_instagram_refusing_the_send_maps_to_its_code(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    platform_failure: Failure,
    code: ResultErrorCode,
) -> None:
    instagram.send_failures[PEER] = platform_failure
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        [result] = await answered(bus, send(source.source_id))

    assert refused(code)(result)
    # fixed texts only: the adapter's detail goes to the logs
    assert result.error_text is not None and instagram.failure_detail not in result.error_text
    assert instagram.sent == []


@pytest.mark.parametrize(
    ("platform_failure", "code"),
    [
        (Failure.FLOOD, ResultErrorCode.PEER_FLOOD),
        (Failure.RATE_LIMITED, ResultErrorCode.RATE_LIMITED),
        (Failure.SESSION_REVOKED, ResultErrorCode.SOURCE_OFFLINE),
        (Failure.NETWORK, ResultErrorCode.NETWORK_ERROR),
    ],
)
async def test_instagram_refusing_the_dialog_check_maps_to_its_code(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    platform_failure: Failure,
    code: ResultErrorCode,
) -> None:
    instagram.dialog_failures[PEER] = platform_failure
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        [result] = await answered(bus, send(source.source_id))

    assert refused(code)(result)
    assert instagram.sent == []


async def test_send_with_an_unknown_outcome_found_in_the_dialog_by_its_label_is_ok(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.send_failures[PEER] = Failure.UNKNOWN_AFTER_SEND
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        [result] = await answered(bus, send(source.source_id))

    [message] = instagram.sent
    assert result == ResultPayload(ok=True, external_message_id=message.message_id)
    assert len(instagram.sends) == 1
    assert instagram.history_reads == [(source.source_id, PEER)]


async def test_send_with_an_unknown_outcome_and_no_history_is_unconfirmed_never_a_network_error(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.send_failures[PEER] = Failure.UNKNOWN_AFTER_SEND
    instagram.history_failures[PEER] = Failure.NETWORK
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        [result] = await answered(bus, sent)

    assert refused(ResultErrorCode.SEND_UNCONFIRMED)(result)
    [message] = instagram.sent
    # the label is kept for reconciliation
    assert (await saved_send(postgres_dsn, sent))[1] == message.client_context


async def test_label_is_saved_as_sending_before_instagram_is_called(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    release = instagram.hold_send[PEER] = asyncio.Event()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        delivery = bus.submit(sent)
        await eventually(lambda: PEER in _send_calls(instagram))
        state, label = await saved_send(postgres_dsn, sent)
        release.set()
        await bus.wait_until(lambda: bus.is_committed(delivery))

    assert state == "sending"
    [message] = instagram.sent
    assert label == message.client_context
    assert await saved_send(postgres_dsn, sent) == ("done", label)


class _CrashingInstagram(FakeInstagram):
    """An adapter with a bug: it blows up with something that is not a PlatformError."""

    def __init__(self) -> None:
        super().__init__()
        self.attempts: list[OutgoingText] = []

    async def send_text(self, message: OutgoingText) -> SentMessage:
        self.attempts.append(message)
        raise RuntimeError("adapter bug")


async def test_platform_crash_mid_send_is_unconfirmed_and_the_label_was_already_saved(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str
) -> None:
    instagram = _CrashingInstagram()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        [result] = await answered(bus, sent)

    # the request may have left before the adapter broke: "sent, not sure" (contract 9)
    assert refused(ResultErrorCode.SEND_UNCONFIRMED)(result)
    assert result.error_text is not None and "adapter bug" not in result.error_text
    [attempt] = instagram.attempts
    assert (await saved_send(postgres_dsn, sent))[1] == attempt.client_context


async def test_repeated_delivery_of_a_finished_send_answers_from_the_saved_result(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        await answered(bus, sent)
        first, again = await answered(bus, sent)

    assert first.ok and again == first
    assert len(instagram.sent) == 1


async def test_send_that_hangs_is_answered_unconfirmed_before_crm_gives_up(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    slow_check = instagram.hold_dialog[PEER] = asyncio.Event()
    instagram.hold_send[PEER] = asyncio.Event()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        delivery = bus.submit(sent)
        await eventually(lambda: PEER in _dialog_checks(instagram))
        started = clock.now()
        await clock.advance(29)
        slow_check.set()
        await eventually(lambda: PEER in _send_calls(instagram))
        await clock.advance(270)
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [answer] = bus.events(EventType.RESULT, operation_id=UUID(sent["operation_id"]))
    assert refused(ResultErrorCode.SEND_UNCONFIRMED)(answer.payload)  # type: ignore[arg-type]
    assert (answer.envelope.occurred_at - started).total_seconds() < 300


async def test_dialog_check_that_hangs_is_a_network_error_and_nothing_is_sent(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.hold_dialog[PEER] = asyncio.Event()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        delivery = bus.submit(sent)
        await eventually(lambda: PEER in _dialog_checks(instagram))
        await clock.advance(60)
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = results(bus, sent)
    assert refused(ResultErrorCode.NETWORK_ERROR)(result)
    assert instagram.sent == []
    assert await saved_send(postgres_dsn, sent) == ("done", None)


async def test_send_that_went_out_before_a_crash_is_found_by_its_label_after_the_restart(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.hang_after_send[PEER] = asyncio.Event()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        bus.submit(sent)
        await eventually(lambda: len(instagram.sent) == 1)
    # the process died right after the message went out; the command comes again, even
    # after CRM's timeout: reading the dialog is safe whenever it happens
    del instagram.hang_after_send[PEER]
    await clock.advance(400)
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        await bus.wait_until(lambda: len(results(bus, sent)) == 1)

    [message] = instagram.sent
    assert results(bus, sent) == [ResultPayload(ok=True, external_message_id=message.message_id)]
    assert len(_send_calls(instagram)) == 1
    assert await saved_send(postgres_dsn, sent) == ("done", message.client_context)


async def test_send_redelivered_after_a_crash_mid_send_is_never_sent_again(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.hold_send[PEER] = asyncio.Event()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        bus.submit(sent)
        await eventually(lambda: PEER in _send_calls(instagram))
    # the process died mid-send, before the message went out; the command comes again
    del instagram.hold_send[PEER]
    calls = len(_send_calls(instagram))
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        await bus.wait_until(lambda: len(results(bus, sent)) == 1)
        # a CRM retry of the unconfirmed send is answered from the saved result
        await answered(bus, sent)

    first, retried = results(bus, sent)
    assert refused(ResultErrorCode.SEND_UNCONFIRMED)(first)
    assert retried == first
    assert instagram.history_reads == [(source.source_id, PEER)]
    assert len(_send_calls(instagram)) == calls
    assert instagram.sent == []


async def test_send_redelivered_mid_send_without_history_is_unconfirmed(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.hang_after_send[PEER] = asyncio.Event()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        bus.submit(sent)
        await eventually(lambda: len(instagram.sent) == 1)
    del instagram.hang_after_send[PEER]
    instagram.history_failures[PEER] = Failure.RATE_LIMITED
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        await bus.wait_until(lambda: len(results(bus, sent)) == 1)

    [result] = results(bus, sent)
    assert refused(ResultErrorCode.SEND_UNCONFIRMED)(result)
    assert len(_send_calls(instagram)) == 1


async def test_history_that_hangs_is_unconfirmed_before_crm_gives_up(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.send_failures[PEER] = Failure.UNKNOWN_AFTER_SEND
    instagram.hold_history[PEER] = asyncio.Event()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        delivery = bus.submit(sent)
        await eventually(lambda: bool(instagram.history_reads))
        started = clock.now()
        await clock.advance(60)
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [answer] = bus.events(EventType.RESULT, operation_id=UUID(sent["operation_id"]))
    assert refused(ResultErrorCode.SEND_UNCONFIRMED)(answer.payload)  # type: ignore[arg-type]
    assert (answer.envelope.occurred_at - started).total_seconds() < 300


async def test_send_found_mid_send_while_the_source_is_offline_is_never_sent_again(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.hold_send[PEER] = asyncio.Event()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        source = await connected_with_dialog(bus, instagram)
        sent = send(source.source_id)
        bus.submit(sent)
        await eventually(lambda: PEER in _send_calls(instagram))
    del instagram.hold_send[PEER]
    # after the restart the Source is not active yet: a retried code here would let CRM's
    # retry find the Operation done and send a second time
    async with sending_connector(bus, clock, postgres_dsn, instagram, sources=NoActiveSources()):
        await bus.wait_until(lambda: len(results(bus, sent)) == 1)
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        await answered(bus, sent)

    first, retried = results(bus, sent)
    assert refused(ResultErrorCode.SEND_UNCONFIRMED)(first)
    assert retried == first
    assert len(_send_calls(instagram)) == 1
    # no working Session: the history is not read through it
    assert instagram.history_reads == []


async def test_send_whose_time_ran_out_in_the_queue_is_refused_without_touching_instagram(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    release = asyncio.Event()

    async def slow_edit(ctx: CommandContext) -> ResultPayload:
        await release.wait()
        return failure(ResultErrorCode.EDIT_UNSUPPORTED, "not supported")

    slow = {CommandType.EDIT: Handler(slow_edit, needs_active_source=False)}
    async with sending_connector(bus, clock, postgres_dsn, instagram, extra_handlers=slow):
        source = await connected_with_dialog(bus, instagram)
        edit = {"message_id": "m-1", "external_chat_id": PEER, "external_message_id": "item-1", "text": "x"}
        bus.submit(command(CommandType.EDIT, edit, source_id=source.source_id))
        sent = send(source.source_id)
        delivery = bus.submit(sent)
        await bus.wait_until(lambda: bool(bus.events(EventType.ACK, operation_id=UUID(sent["operation_id"]))))
        # CRM counts its 300 s from the ack: by the send's turn they are gone
        await clock.advance(300)
        release.set()
        await bus.wait_until(lambda: bus.is_committed(delivery))

    assert results(bus, sent) == [
        ResultPayload(ok=False, error_code=ResultErrorCode.NETWORK_ERROR, error_text=DEADLINE_PASSED)
    ]
    assert instagram.dialog_checks == [] and instagram.sends == []


async def test_send_redelivered_after_a_crash_keeps_the_time_crm_counts_from_the_first_ack(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    release = asyncio.Event()

    async def slow_edit(ctx: CommandContext) -> ResultPayload:
        await release.wait()
        return failure(ResultErrorCode.EDIT_UNSUPPORTED, "not supported")

    slow = {CommandType.EDIT: Handler(slow_edit, needs_active_source=False)}
    async with sending_connector(bus, clock, postgres_dsn, instagram, extra_handlers=slow):
        source = await connected_with_dialog(bus, instagram)
        edit = {"message_id": "m-1", "external_chat_id": PEER, "external_message_id": "item-1", "text": "x"}
        bus.submit(command(CommandType.EDIT, edit, source_id=source.source_id))
        sent = send(source.source_id)
        bus.submit(sent)
        await bus.wait_until(lambda: bool(bus.events(EventType.ACK, operation_id=UUID(sent["operation_id"]))))
        await clock.advance(250)
    # the process died with the send still queued; Kafka brings it again 50 s later, when
    # CRM's 300 s from the first ack are over: a late send would be a duplicate of a resend
    await clock.advance(50)
    release.set()
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        await bus.wait_until(lambda: len(results(bus, sent)) == 1)

    assert results(bus, sent) == [
        ResultPayload(ok=False, error_code=ResultErrorCode.NETWORK_ERROR, error_text=DEADLINE_PASSED)
    ]
    assert instagram.sends == []


async def test_send_for_a_source_that_is_not_connected_is_source_offline(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with sending_connector(bus, clock, postgres_dsn, instagram):
        [result] = await answered(bus, send(uuid4()))

    assert refused(ResultErrorCode.SOURCE_OFFLINE)(result)
    assert _send_calls(instagram) == []


def _send_calls(instagram: FakeInstagram) -> list[str]:
    return [message.peer_id for message in instagram.sends]


def _dialog_checks(instagram: FakeInstagram) -> list[str]:
    return [peer for _, peer in instagram.dialog_checks]
