"""Source statuses and restart: Sessions come back without a login, statuses only on change."""

import asyncio
from collections.abc import Callable
from datetime import datetime
from uuid import UUID

from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.errors import ResultErrorCode, StatusErrorCode
from ig_connector.contract.events import ResultPayload, StatusPayload
from ig_connector.instagram import Device, Failure
from ig_connector.store import SourceStatus
from ig_connector.store.postgres import PostgresStore
from tests.support.connector import running_connector
from tests.support.crm import command
from tests.support.fake_clock import FakeClock
from tests.support.fake_instagram import FakeInstagram
from tests.support.fake_proxy import FakeProxyService
from tests.support.login import connect, statuses
from tests.support.memory_bus import MemoryBus

INTERVAL = 30.0


def resolve(source_id: UUID, value: str = "bob.customer") -> dict[str, object]:
    return command(
        CommandType.RESOLVE_RECIPIENT, {"recipient_kind": "username", "value": value}, source_id=source_id
    )


async def ask(bus: MemoryBus, sent: dict[str, object]) -> ResultPayload:
    delivery = bus.submit(sent)
    await bus.wait_until(lambda: bus.is_committed(delivery))
    [event] = bus.events(EventType.RESULT, operation_id=UUID(str(sent["operation_id"])))
    assert isinstance(event.payload, ResultPayload)
    return event.payload


async def settled(bus: MemoryBus, source_id: UUID) -> None:
    """Wait until everything before it in the Source's queue is done (the login flow saved)."""
    await ask(bus, resolve(source_id, "settled"))


async def eventually(condition: Callable[[], bool], within: float = 2.0) -> None:
    async with asyncio.timeout(within):
        while not condition():  # noqa: ASYNC110  the fakes have no event to wait on
            await asyncio.sleep(0.01)


async def test_restart_with_a_live_session_and_saved_active_publishes_no_status(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    proxies: FakeProxyService,
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        source = (await connect(bus, instagram)).source_id
        await settled(bus, source)
    instagram.restart()  # the process is gone, its live Sessions with it

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        result = await ask(bus, resolve(source))

    assert result.ok
    assert statuses(bus, source) == [StatusPayload(status="active")]
    assert len(instagram.logins) == 1
    assert [r.source_id for r in instagram.restores] == [source]
    assert instagram.checks == [source]
    # the same assignment as before the restart: the Session's IP does not jump
    [restored] = instagram.restores
    assert restored.assignment_id == proxies.assignments[source].assignment_id
    assert restored.device == Device(b"device-1")  # the one the login created


async def next_poll(clock: FakeClock, instagram: FakeInstagram) -> None:
    polls = len(instagram.inbox_polls)
    await clock.advance(INTERVAL)
    await eventually(lambda: len(instagram.inbox_polls) > polls)


async def pass_time(clock: FakeClock, seconds: float) -> None:
    """Let time pass one poll interval at a time, each with room for the database work it causes."""
    for _ in range(int(seconds // INTERVAL)):
        await clock.advance(INTERVAL)
        await asyncio.sleep(0.02)


async def test_restart_with_saved_error_and_a_live_session_publishes_active(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        source = (await connect(bus, instagram, external_id="1789")).source_id
        instagram.inbox_failures["1789"] = Failure.FLOOD
        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(statuses(bus, source)) == 2)
    instagram.restart()
    del instagram.inbox_failures["1789"]

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await bus.wait_until(lambda: len(statuses(bus, source)) == 3)

    assert statuses(bus, source) == [
        StatusPayload(status="active"),
        StatusPayload(
            status="error",
            error_code=StatusErrorCode.PEER_FLOOD,
            error_text="Instagram restricted the account",
        ),
        StatusPayload(status="active"),
    ]


async def test_revoked_session_on_a_poll_needs_reconnect_and_leaves_other_sources_working(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        revoked = (await connect(bus, instagram, login="anna.shop", external_id="1789")).source_id
        working = (await connect(bus, instagram, login="boris.shop", external_id="1790")).source_id
        instagram.inbox_failures["1789"] = Failure.SESSION_REVOKED

        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(statuses(bus, revoked)) == 2)
        refused = await ask(bus, resolve(revoked))
        answered = await ask(bus, resolve(working))

    assert statuses(bus, revoked)[-1] == StatusPayload(
        status="needs_reconnect",
        error_code=StatusErrorCode.DEAUTHORIZED,
        error_text="Instagram no longer accepts the session",
    )
    assert refused.error_code is ResultErrorCode.SOURCE_OFFLINE
    assert answered.ok
    assert statuses(bus, working) == [StatusPayload(status="active")]


async def test_the_same_status_twice_in_a_row_is_published_once(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, inbound_interval=INTERVAL):
        source = (await connect(bus, instagram, external_id="1789")).source_id
        instagram.inbox_failures["1789"] = Failure.FLOOD
        await next_poll(clock, instagram)
        await bus.wait_until(lambda: len(statuses(bus, source)) == 2)

        # still restricted when it is asked again
        instagram.check_failures["1789"] = Failure.FLOOD
        await pass_time(clock, 16 * 60)
        # and then it is over
        del instagram.check_failures["1789"]
        del instagram.inbox_failures["1789"]
        await pass_time(clock, 16 * 60)
        await bus.wait_until(lambda: len(statuses(bus, source)) == 3)

    assert [s.status for s in statuses(bus, source)] == ["active", "error", "active"]
    # a restricted Account is asked rarely, not on every poll
    assert len(instagram.checks) == 2


async def test_restart_with_a_revoked_session_needs_reconnect_without_a_login(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram, external_id="1789")).source_id
        await settled(bus, source)
    instagram.restart()
    instagram.check_failures["1789"] = Failure.CHALLENGE

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        refused = await ask(bus, resolve(source))

    assert refused.error_code is ResultErrorCode.SOURCE_OFFLINE
    assert [(s.status, s.error_code) for s in statuses(bus, source)] == [
        ("active", None),
        ("needs_reconnect", StatusErrorCode.DEAUTHORIZED),
    ]
    assert len(instagram.logins) == 1


async def test_restart_without_a_proxy_never_goes_to_instagram(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    proxies: FakeProxyService,
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        source = (await connect(bus, instagram, external_id="1789")).source_id
        await settled(bus, source)
    instagram.restart()
    proxies.unreachable = True

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        refused = await ask(bus, resolve(source))

    assert refused.error_code is ResultErrorCode.SOURCE_OFFLINE
    assert statuses(bus, source)[-1] == StatusPayload(
        status="error", error_code=StatusErrorCode.SOURCE_OFFLINE, error_text="no proxy or network failure"
    )
    assert instagram.restores == []
    assert instagram.checks == []


async def test_commands_after_a_restart_wait_for_the_session_instead_of_being_refused(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram, external_id="1789")).source_id
        await settled(bus, source)
    instagram.restart()
    checking = instagram.hold_check["1789"] = asyncio.Event()

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await eventually(lambda: len(instagram.checks) == 1)
        sent = resolve(source)
        delivery = bus.submit(sent)
        await asyncio.sleep(0.05)
        assert not bus.is_committed(delivery)
        checking.set()
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = bus.events(EventType.RESULT, operation_id=UUID(str(sent["operation_id"])))
    assert isinstance(result.payload, ResultPayload)
    assert result.payload.ok


async def test_a_command_refused_for_flood_puts_the_source_in_error(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.lookup_failures["bob.customer"] = Failure.FLOOD
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram, external_id="1789")).source_id
        first = await ask(bus, resolve(source))
        second = await ask(bus, resolve(source, "someone.else"))

    assert first.error_code is ResultErrorCode.PEER_FLOOD
    assert [(s.status, s.error_code) for s in statuses(bus, source)] == [
        ("active", None),
        ("error", StatusErrorCode.PEER_FLOOD),
    ]
    # restricted now: refused without asking Instagram
    assert second.error_code is ResultErrorCode.PEER_FLOOD
    assert [lookup.value for lookup in instagram.lookups] == ["bob.customer"]


async def test_stopping_lets_the_running_command_finish_and_takes_no_new_ones(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    held = instagram.hold_lookup["bob.customer"] = asyncio.Event()
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram) as connector:
        source = (await connect(bus, instagram, external_id="1789")).source_id
        running = resolve(source)
        running_delivery = bus.submit(running)
        await eventually(lambda: len(instagram.lookups) == 1)

        draining = asyncio.create_task(connector.drain(grace=5.0))
        await asyncio.sleep(0.02)
        later = bus.submit(resolve(source, "someone.else"))
        await asyncio.sleep(0.02)
        assert not draining.done()
        held.set()
        await draining

        assert bus.is_committed(running_delivery)
        assert not bus.is_committed(later)
    assert [lookup.value for lookup in instagram.lookups] == ["bob.customer"]
    assert bus.events(EventType.RESULT, operation_id=UUID(str(running["operation_id"])))


async def test_stopping_gives_up_on_a_command_after_the_grace_period(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    instagram.hold_lookup["bob.customer"] = asyncio.Event()
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram) as connector:
        source = (await connect(bus, instagram, external_id="1789")).source_id
        delivery = bus.submit(resolve(source))
        await eventually(lambda: len(instagram.lookups) == 1)

        draining = asyncio.create_task(connector.drain(grace=5.0))
        await asyncio.sleep(0.02)
        await clock.advance(5.0)
        await draining

        # cancelled with the process: not committed, it comes again after the restart
        assert not bus.is_committed(delivery)


async def test_a_network_failure_brings_the_session_back_on_the_next_proxy(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with running_connector(
        bus, clock, postgres_dsn, instagram=instagram, proxies=proxies, inbound_interval=INTERVAL
    ):
        source = (await connect(bus, instagram, external_id="1789")).source_id
        first = proxies.assignments[source]
        instagram.inbox_failures["1789"] = Failure.NETWORK
        await next_poll(clock, instagram)
        del instagram.inbox_failures["1789"]
        await bus.wait_until(lambda: len(statuses(bus, source)) == 3)

    assert [(s.status, s.error_code) for s in statuses(bus, source)] == [
        ("active", None),
        ("error", StatusErrorCode.SOURCE_OFFLINE),
        ("active", None),
    ]
    # a confirmed network failure changes the proxy: reported, then a new one
    assert [f.assignment_id for f in proxies.failures] == [first.assignment_id]
    assert [r.assignment_id for r in instagram.restores] == [proxies.assignments[source].assignment_id]
    assert proxies.assignments[source] != first


async def test_an_unexpected_restore_error_takes_back_the_active_of_the_last_process(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram, external_id="1789")).source_id
        await settled(bus, source)
    instagram.restart()

    async def broken(*args: object) -> None:
        raise RuntimeError("an adapter bug")

    instagram.restore = broken  # type: ignore[method-assign,assignment]
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await bus.wait_until(lambda: len(statuses(bus, source)) == 2)

    assert statuses(bus, source)[-1].error_code is StatusErrorCode.SOURCE_OFFLINE


class StoreFailingToSaveActiveOnce(PostgresStore):
    # set by the test: the next `active` is published but not saved
    armed = False

    async def save_status(self, source_id: UUID, status: SourceStatus, *, now: datetime) -> None:
        if status.status == "active" and StoreFailingToSaveActiveOnce.armed:
            StoreFailingToSaveActiveOnce.armed = False
            raise ConnectionError("database went away")
        await super().save_status(source_id, status, now=now)


async def test_a_status_that_was_published_but_not_saved_still_counts_as_the_last_one(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(
        bus,
        clock,
        postgres_dsn,
        instagram=instagram,
        inbound_interval=INTERVAL,
        store_type=StoreFailingToSaveActiveOnce,
    ):
        source = (await connect(bus, instagram, external_id="1789")).source_id
        StoreFailingToSaveActiveOnce.armed = True
        instagram.inbox_failures["1789"] = Failure.NETWORK
        await next_poll(clock, instagram)
        # error, then active again at once on the next proxy: that one not saved
        await bus.wait_until(lambda: len(statuses(bus, source)) == 3)
        await asyncio.sleep(0.05)
        # the store still says error: the next error must not look like no change
        await pass_time(clock, INTERVAL)
        await bus.wait_until(lambda: len(statuses(bus, source)) == 4)

    assert [s.status for s in statuses(bus, source)] == ["active", "error", "active", "error"]


async def test_stopping_does_not_start_a_command_that_waits_for_a_slot(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    held = instagram.hold_lookup["bob.customer"] = asyncio.Event()
    async with running_connector(
        bus, clock, postgres_dsn, instagram=instagram, max_parallel_sources=1
    ) as connector:
        first = (await connect(bus, instagram, login="anna.shop", external_id="1789")).source_id
        second = (await connect(bus, instagram, login="boris.shop", external_id="1790")).source_id
        bus.submit(resolve(first))
        await eventually(lambda: len(instagram.lookups) == 1)
        waiting = bus.submit(resolve(second, "someone.else"))
        await asyncio.sleep(0.05)

        draining = asyncio.create_task(connector.drain(grace=5.0))
        await asyncio.sleep(0.02)
        held.set()
        await draining
        await asyncio.sleep(0.05)

        assert not bus.is_committed(waiting)
    assert [lookup.value for lookup in instagram.lookups] == ["bob.customer"]


async def test_a_confirm_repeated_after_a_restart_does_not_claim_a_session_the_restore_lost(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        # gone right after `active`: the confirm is not committed and comes again
        source = (await connect(bus, instagram, external_id="1789")).source_id
    instagram.restart()
    instagram.check_failures["1789"] = Failure.NETWORK

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await settled(bus, source)

    assert [(s.status, s.error_code) for s in statuses(bus, source)] == [
        ("active", None),
        ("error", StatusErrorCode.SOURCE_OFFLINE),
    ]
