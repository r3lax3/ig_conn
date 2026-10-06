"""Inbound texts: the inbox of every connected Source is polled between its commands."""

import asyncio
import json
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from ig_connector.bus import Delivery
from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.events import InboundMessagePayload
from ig_connector.instagram import Failure, InboxPositions, InboxThread, InboxUser, ThreadPosition
from ig_connector.store.postgres import PostgresStore
from tests.behaviour.conftest import START
from tests.support.connector import running_connector
from tests.support.crm import command
from tests.support.fake_clock import FakeClock
from tests.support.fake_instagram import FakeInstagram
from tests.support.login import PASSWORD, TOTP_SECRET, connect, connect_confirm, connect_start
from tests.support.memory_bus import MemoryBus
from tests.support.tracing import Trace

INTERVAL = 30.0
EDIT = {"message_id": "m1", "external_chat_id": "5551", "external_message_id": "x1", "text": "fixed"}
DELETE = {"message_id": "m2", "external_chat_id": "5551", "external_message_id": "x1"}
_ORDERED = (EventType.INBOUND_MESSAGE, EventType.RESULT)
CLIENT = InboxUser("5551", username="ivan.petrov", full_name="Ivan Petrov")


def inbound(bus: MemoryBus, source_id: UUID) -> list[InboundMessagePayload]:
    out = []
    for event in bus.events(EventType.INBOUND_MESSAGE):
        if event.envelope.source_id == source_id:
            assert isinstance(event.payload, InboundMessagePayload)
            out.append(event.payload)
    return out


def json_id(delivery: Delivery) -> str:
    return str(json.loads(delivery.value)["operation_id"])


async def eventually(condition: Callable[[], bool], within: float = 2.0) -> None:
    async with asyncio.timeout(within):
        while not condition():  # noqa: ASYNC110  the fakes have no event to wait on
            await asyncio.sleep(0.01)


async def next_poll(clock: FakeClock, instagram: FakeInstagram) -> None:
    """Let one poll interval pass and wait until the inbox was asked."""
    polls = len(instagram.inbox_polls)
    await clock.advance(INTERVAL)
    await eventually(lambda: len(instagram.inbox_polls) > polls)


async def test_inbound_text_is_published_with_the_platform_time(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        sent_at = START + timedelta(seconds=7)
        message = instagram.message("1789", peer=CLIENT, text="Hello, about my order", at=sent_at)

        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) == 1)

    [event] = bus.events(EventType.INBOUND_MESSAGE)
    assert event.envelope.occurred_at == sent_at
    assert event.payload == InboundMessagePayload(
        external_chat_id="5551",
        external_message_id=message.message_id,
        external_user_id="5551",
        user_display_name="Ivan Petrov",
        text="Hello, about my order",
        kind="text",
    )


async def test_non_text_message_is_published_as_unsupported_without_media(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        at = START + timedelta(seconds=3)
        photo = instagram.message("1789", peer=CLIENT, item_type="media", at=at)
        voice = instagram.message("1789", peer=CLIENT, item_type="voice_media", at=at + timedelta(seconds=1))

        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) == 2)

    events = inbound(bus, connected.source_id)
    assert [(e.external_message_id, e.kind, e.text, e.media) for e in events] == [
        (photo.message_id, "unsupported", "", []),
        (voice.message_id, "unsupported", "", []),
    ]


async def test_own_group_and_pending_messages_are_not_published(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    stranger = InboxUser("7007", username="stranger")
    friend = InboxUser("8008", username="friend")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        at = START + timedelta(seconds=1)
        instagram.message("1789", peer=CLIENT, text="sent from the app", by_viewer=True, at=at)
        instagram.message("1789", peer=stranger, text="a message request", pending=True, at=at)
        instagram.group_message("1789", members=[CLIENT, friend], sender=friend, text="hi all", at=at)
        last = instagram.message(
            "1789", peer=CLIENT, text="the client's answer", at=at + timedelta(seconds=1)
        )

        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) >= 1)
        await next_poll(clock, instagram)

    assert [e.external_message_id for e in inbound(bus, connected.source_id)] == [last.message_id]


async def test_history_from_before_the_connect_is_not_loaded(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        instagram.add_account("anna.shop", password=PASSWORD, totp_secret=TOTP_SECRET, external_id="1789")
        instagram.message("1789", peer=CLIENT, text="last week", at=START - timedelta(days=7))
        instagram.message("1789", peer=CLIENT, text="a minute ago", at=START - timedelta(minutes=1))
        connected = await connect(bus, instagram, external_id="1789")
        new = instagram.message(
            "1789", peer=CLIENT, text="after the connect", at=START + timedelta(seconds=2)
        )

        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) >= 1)
        await next_poll(clock, instagram)

    assert [e.external_message_id for e in inbound(bus, connected.source_id)] == [new.message_id]


async def test_restart_publishes_no_message_twice_and_polls_without_any_command(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        first = instagram.message(
            "1789", peer=CLIENT, text="before the restart", at=START + timedelta(seconds=1)
        )
        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) == 1)
        # a stop between publishing and saving the position repeats the message (by design):
        # the next poll starts only once that one is over
        await next_poll(clock, instagram)

    after = instagram.message("1789", peer=CLIENT, text="after the restart", at=START + timedelta(seconds=40))
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        # no command for the Source arrives after the restart
        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) >= 2)
        await next_poll(clock, instagram)

    assert [e.external_message_id for e in inbound(bus, connected.source_id)] == [
        first.message_id,
        after.message_id,
    ]


async def test_a_command_interrupts_a_running_poll_and_commands_keep_their_order(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        message = instagram.message("1789", peer=CLIENT, text="hello", at=START + timedelta(seconds=1))
        held = instagram.hold_inbox["1789"] = asyncio.Event()
        await next_poll(clock, instagram)
        polls = len(instagram.inbox_polls)

        # the inbox hangs, yet the commands are answered at once, in order
        edit = bus.submit(command(CommandType.EDIT, EDIT, source_id=connected.source_id))
        delete = bus.submit(command(CommandType.DELETE, DELETE, source_id=connected.source_id))
        await bus.wait_until(lambda: bus.is_committed(delete))
        assert inbound(bus, connected.source_id) == []

        # the interrupted poll is done again right after them
        await eventually(lambda: len(instagram.inbox_polls) > polls)
        held.set()
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) == 1)

    order = [
        (e.envelope.type, str(e.envelope.operation_id)) for e in bus.events() if e.envelope.type in _ORDERED
    ]
    assert [kind for kind, _ in order] == [EventType.RESULT, EventType.RESULT, EventType.INBOUND_MESSAGE]
    assert [operation for _, operation in order[:2]] == [json_id(edit), json_id(delete)]
    assert inbound(bus, connected.source_id)[0].external_message_id == message.message_id


class _NaiveTimes(FakeInstagram):
    """An adapter bug: one message comes with a time without a timezone."""

    broken: frozenset[str] = frozenset()

    async def new_messages(self, source_id: UUID, positions: InboxPositions) -> Sequence[InboxThread]:
        threads = await super().new_messages(source_id, positions)
        return [
            replace(
                thread,
                messages=[
                    replace(m, sent_at=m.sent_at.replace(tzinfo=None)) if m.message_id in self.broken else m
                    for m in thread.messages
                ],
            )
            for thread in threads
        ]


async def test_a_message_that_cannot_be_published_does_not_hold_back_the_rest(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, trace: Trace
) -> None:
    instagram = _NaiveTimes(listener=trace.record)
    other = InboxUser("6006", username="olga")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        at = START + timedelta(seconds=1)
        bad = instagram.message("1789", peer=CLIENT, text="broken", at=at)
        instagram.broken = frozenset({bad.message_id})
        after = instagram.message(
            "1789", peer=CLIENT, text="next in the thread", at=at + timedelta(seconds=1)
        )
        elsewhere = instagram.message("1789", peer=other, text="another thread", at=at + timedelta(seconds=2))

        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) >= 2)
        await next_poll(clock, instagram)

    published = sorted(e.external_message_id for e in inbound(bus, connected.source_id))
    assert published == sorted([after.message_id, elsewhere.message_id])


async def test_inbox_is_polled_once_per_configured_interval(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        await next_poll(clock, instagram)
        polls = len(instagram.inbox_polls)

        await clock.advance(INTERVAL - 1)
        await asyncio.sleep(0.3)
        assert len(instagram.inbox_polls) == polls

        await clock.advance(1)
        await eventually(lambda: len(instagram.inbox_polls) == polls + 1)

    assert set(instagram.inbox_polls) == {connected.source_id}


async def test_a_burst_longer_than_a_page_comes_over_several_polls_in_order(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.inbox_page = 2
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        burst = [
            instagram.message("1789", peer=CLIENT, text=f"part {n}", at=START + timedelta(seconds=n))
            for n in range(1, 6)
        ]
        for _ in range(3):
            await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) == 5)

    assert [e.external_message_id for e in inbound(bus, connected.source_id)] == [m.message_id for m in burst]


async def test_a_refused_poll_is_tried_again_at_the_next_interval(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        message = instagram.message("1789", peer=CLIENT, text="hello", at=START + timedelta(seconds=1))
        instagram.inbox_failures["1789"] = Failure.RATE_LIMITED
        await next_poll(clock, instagram)

        del instagram.inbox_failures["1789"]
        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) == 1)

    assert [e.external_message_id for e in inbound(bus, connected.source_id)] == [message.message_id]


class _PositionNotSaved(PostgresStore):
    """The process dies right after publishing: the position never moves (once)."""

    failures = 1

    async def move_inbox_position(
        self, source_id: UUID, thread_id: str, position: ThreadPosition, *, now: datetime
    ) -> None:
        if self.failures:
            self.failures -= 1
            raise OSError("connection lost")
        await super().move_inbox_position(source_id, thread_id, position, now=now)


async def test_a_message_whose_position_was_not_saved_comes_again_never_lost(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(
        bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL, store_type=_PositionNotSaved
    ):
        connected = await connect(bus, instagram, external_id="1789")
        message = instagram.message("1789", peer=CLIENT, text="hello", at=START + timedelta(seconds=1))
        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) == 1)
        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) == 2)
        await next_poll(clock, instagram)

    events = bus.events(EventType.INBOUND_MESSAGE)
    # the same event again (CRM deduplicates), and nothing after the position moved
    assert [e.payload for e in events] == [events[0].payload] * 2
    assert events[0].envelope.operation_id == events[1].envelope.operation_id
    assert events[0].payload.external_message_id == message.message_id  # type: ignore[union-attr]


async def _hold_a_login(bus: MemoryBus, instagram: FakeInstagram) -> asyncio.Event:
    """A third Source logging in that holds its slot until the event is set."""
    held = instagram.hold_login["carl.shop"] = asyncio.Event()
    instagram.add_account("carl.shop", password=PASSWORD, totp_secret=TOTP_SECRET, external_id="1791")
    start = connect_start(uuid4(), login="carl.shop")
    bus.submit(start)
    bus.submit(connect_confirm(start))
    await eventually(lambda: len(instagram.logins) == 3)
    return held


async def test_polls_share_the_parallelism_limit_with_commands(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(
        bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL, max_parallel_sources=1
    ):
        await connect(bus, instagram, login="anna.shop", external_id="1789")
        boris = (await connect(bus, instagram, login="boris.shop", external_id="1790")).source_id
        held = await _hold_a_login(bus, instagram)
        polls = len(instagram.inbox_polls)

        await clock.advance(INTERVAL)
        await asyncio.sleep(0.1)
        # the one slot is the login's: nobody else talks to Instagram meanwhile
        assert len(instagram.inbox_polls) == polls

        held.set()
        await eventually(lambda: boris in instagram.inbox_polls[polls:])


async def test_a_command_gets_a_free_slot_before_waiting_polls(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    lookup = instagram.hold_lookup["bob.customer"] = asyncio.Event()
    async with running_connector(
        bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL, max_parallel_sources=1
    ):
        anna = (await connect(bus, instagram, login="anna.shop", external_id="1789")).source_id
        boris = (await connect(bus, instagram, login="boris.shop", external_id="1790")).source_id
        held = await _hold_a_login(bus, instagram)
        polls = len(instagram.inbox_polls)
        await clock.advance(INTERVAL)
        await asyncio.sleep(0.1)  # both polls wait for the slot now
        payload = {"recipient_kind": "username", "value": "bob.customer"}
        bus.submit(command(CommandType.RESOLVE_RECIPIENT, payload, source_id=anna))
        await asyncio.sleep(0.05)

        held.set()
        await eventually(lambda: len(instagram.lookups) == 1)
        # queued after Boris's poll, yet first
        assert boris not in instagram.inbox_polls[polls:]

        lookup.set()
        await eventually(lambda: boris in instagram.inbox_polls[polls:])


async def test_a_link_is_published_as_text(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        connected = await connect(bus, instagram, external_id="1789")
        at = START + timedelta(seconds=3)
        instagram.message("1789", peer=CLIENT, item_type="link", text="look https://example.com", at=at)

        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(inbound(bus, connected.source_id)) == 1)

    [event] = inbound(bus, connected.source_id)
    assert (event.kind, event.text) == ("text", "look https://example.com")
