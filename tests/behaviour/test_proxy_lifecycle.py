"""Proxies at work (contract 8): heartbeats, planned moves, failing over, no proxy = offline."""

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

import pytest

from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.errors import ResultErrorCode, StatusErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Failure
from ig_connector.proxy import Rebalance, TransportState
from ig_connector.proxy.none import NoProxies
from ig_connector.runtime import default_handlers
from ig_connector.runtime.send import send_handler
from ig_connector.store.postgres import PostgresStore
from tests.support.connector import running_connector
from tests.support.crm import command
from tests.support.fake_clock import FakeClock
from tests.support.fake_instagram import FakeInstagram
from tests.support.fake_proxy import FakeProxyService
from tests.support.login import connect, connect_confirm, connect_start, flow_states, statuses
from tests.support.memory_bus import MemoryBus
from tests.support.postgres import SESSION_KEY
from tests.support.tracing import Trace

MINUTE = 60.0
PEER = "5551"


async def eventually(condition: Callable[[], bool], within: float = 2.0) -> None:
    async with asyncio.timeout(within):
        while not condition():  # noqa: ASYNC110  the fakes have no event to wait on
            await asyncio.sleep(0.01)


async def minute(clock: FakeClock) -> None:
    await clock.advance(MINUTE)
    await asyncio.sleep(0.05)  # the work it caused (database, jobs) gets to run


def resolve(source_id: UUID, value: str = "bob.customer") -> dict[str, Any]:
    payload = {"recipient_kind": "username", "value": value}
    return command(CommandType.RESOLVE_RECIPIENT, payload, source_id=source_id)


def send(source_id: UUID) -> dict[str, Any]:
    payload = {"message_id": str(uuid4()), "external_chat_id": PEER, "text": "Hello there"}
    return command(CommandType.SEND, payload, source_id=source_id)


async def ask(bus: MemoryBus, sent: dict[str, Any]) -> ResultPayload:
    delivery = bus.submit(sent)
    await bus.wait_until(lambda: bus.is_committed(delivery))
    return result(bus, sent)


def result(bus: MemoryBus, sent: dict[str, Any]) -> ResultPayload:
    [event] = bus.events(EventType.RESULT, operation_id=UUID(sent["operation_id"]))
    assert isinstance(event.payload, ResultPayload)
    return event.payload


@asynccontextmanager
async def connector(
    bus: MemoryBus,
    clock: FakeClock,
    dsn: str,
    instagram: FakeInstagram,
    proxies: FakeProxyService,
    *,
    sending: bool = False,
) -> AsyncIterator[None]:
    """The service's handlers (platform answers move statuses and proxies); `send` on the bare fake."""
    store = await PostgresStore.open(dsn, session_key=SESSION_KEY)
    handlers = {**default_handlers(instagram), CommandType.SEND: send_handler(instagram, store)}
    try:
        async with running_connector(
            bus, clock, dsn, instagram=instagram, proxies=proxies, handlers=handlers if sending else None
        ):
            yield
    finally:
        await store.close()


@pytest.fixture(autouse=True)
def _users(instagram: FakeInstagram) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")


async def test_every_assignment_is_confirmed_every_minute_with_its_transport(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        source = (await connect(bus, instagram)).source_id
        assignment = proxies.assignments[source].assignment_id
        await minute(clock)
        await eventually(lambda: len(proxies.heartbeats) == 1)
        await minute(clock)
        await eventually(lambda: len(proxies.heartbeats) == 2)

    assert {(h.source_id, h.assignment_id, h.transport) for h in proxies.heartbeats} == {
        (source, assignment, TransportState.CONNECTED)
    }


async def test_a_planned_move_waits_for_the_running_command_then_moves_the_session(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with connector(bus, clock, postgres_dsn, instagram, proxies, sending=True):
        connected = await connect(bus, instagram)
        source = connected.source_id
        old = proxies.assignments[source].assignment_id
        instagram.add_dialog(connected.account.external_id, PEER)
        instagram.hold_send[PEER] = held = asyncio.Event()
        sending = send(source)
        bus.submit(sending)
        await eventually(lambda: len(instagram.sends) == 1)
        move = Rebalance(job_id=uuid4(), target_proxy_id=uuid4())
        proxies.pending[source] = move

        await minute(clock)  # the move is seen
        await minute(clock)  # still waiting for the send: draining
        disconnects_while_sending = list(instagram.disconnects)
        held.set()
        await bus.wait_until(
            lambda: bool(bus.events(EventType.RESULT, operation_id=UUID(sending["operation_id"])))
        )
        await eventually(lambda: len(instagram.restores) == 1)
        after = await ask(bus, resolve(source))

    assert result(bus, sending).ok
    assert disconnects_while_sending == []
    assert [h.transport for h in proxies.heartbeats] == [
        TransportState.CONNECTED,
        TransportState.DRAINING,
        TransportState.CLOSED,
    ]
    [confirmed] = proxies.moves
    assert (confirmed.assignment_id, confirmed.detail) == (old, str(move.job_id))
    new = proxies.assignments[source].assignment_id
    assert new != old
    assert instagram.disconnects == [source]
    assert [r.assignment_id for r in instagram.restores] == [new]
    # the Account never stopped working for CRM
    assert [s.status for s in statuses(bus, source)] == ["active"]
    assert after.ok


async def test_a_confirmed_network_failure_reports_the_proxy_and_takes_the_next_one(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        source = (await connect(bus, instagram)).source_id
        old = proxies.assignments[source].assignment_id
        instagram.lookup_failures["bob.customer"] = Failure.NETWORK
        refused = await ask(bus, resolve(source))
        await eventually(lambda: len(instagram.restores) == 1)
        del instagram.lookup_failures["bob.customer"]
        await bus.wait_until(lambda: len(statuses(bus, source)) == 3)
        after = await ask(bus, resolve(source))

    assert refused.error_code is ResultErrorCode.NETWORK_ERROR
    assert [(f.assignment_id, f.detail) for f in proxies.failures] == [
        (old, "network failure through the proxy")
    ]
    new = proxies.assignments[source].assignment_id
    assert new != old
    assert [r.assignment_id for r in instagram.restores] == [new]
    assert [(s.status, s.error_code) for s in statuses(bus, source)] == [
        ("active", None),
        ("error", StatusErrorCode.SOURCE_OFFLINE),
        ("active", None),
    ]
    assert after.ok


async def test_no_next_proxy_leaves_the_source_offline_and_off_the_network(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        source = (await connect(bus, instagram)).source_id
        instagram.lookup_failures["bob.customer"] = Failure.NETWORK
        proxies.pool_empty = True
        await ask(bus, resolve(source))
        await eventually(lambda: len(proxies.failures) == 1 and len(proxies.reservations) == 3)
        lookups = len(instagram.lookups)
        checks = len(instagram.checks)
        later = await ask(bus, resolve(source))
        await minute(clock)

    assert later.error_code is ResultErrorCode.SOURCE_OFFLINE
    # neither the command nor anything else went to Instagram without a proxy
    assert (len(instagram.lookups), len(instagram.checks), instagram.restores) == (lookups, checks, [])
    assert instagram.disconnects == [source]
    assert source not in proxies.assignments
    assert statuses(bus, source)[-1].error_code is StatusErrorCode.SOURCE_OFFLINE
    # nothing left to confirm: the closed assignment gets no heartbeats
    assert proxies.heartbeats == []


@pytest.mark.parametrize(
    "failure", [Failure.CHALLENGE, Failure.FLOOD, Failure.SESSION_REVOKED, Failure.RATE_LIMITED]
)
async def test_captcha_ban_and_flood_never_change_the_proxy(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    proxies: FakeProxyService,
    failure: Failure,
) -> None:
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        source = (await connect(bus, instagram)).source_id
        assignment = proxies.assignments[source]
        instagram.lookup_failures["bob.customer"] = failure
        await ask(bus, resolve(source))
        await minute(clock)
        await eventually(lambda: len(proxies.heartbeats) == 1)

    assert proxies.failures == []
    assert len(proxies.reservations) == 2  # start and confirm of the login, nothing after
    assert proxies.assignments[source] == assignment
    assert instagram.disconnects == []


async def test_an_assignment_the_service_dropped_is_replaced(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        source = (await connect(bus, instagram)).source_id
        old = proxies.assignments.pop(source).assignment_id  # e.g. heartbeats were missed
        await minute(clock)
        await eventually(lambda: len(instagram.restores) == 1)
        after = await ask(bus, resolve(source))

    new = proxies.assignments[source].assignment_id
    assert new != old
    assert [r.assignment_id for r in instagram.restores] == [new]
    assert proxies.failures == []
    assert [s.status for s in statuses(bus, source)] == ["active"]
    assert after.ok


async def test_a_proxy_service_outage_alone_changes_nothing(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        source = (await connect(bus, instagram)).source_id
        proxies.unreachable = True
        await minute(clock)
        await eventually(lambda: len(proxies.heartbeats) == 1)
        proxies.unreachable = False
        await minute(clock)
        await eventually(lambda: len(proxies.heartbeats) == 2)
        after = await ask(bus, resolve(source))

    assert instagram.disconnects == []
    assert [s.status for s in statuses(bus, source)] == ["active"]
    assert after.ok


class ServiceDownOnMove(FakeProxyService):
    """The service goes away right when the move is to be confirmed."""

    async def rebalance(self, source_id: UUID, assignment_id: int, move: Rebalance) -> None:
        self.unreachable = True
        await super().rebalance(source_id, assignment_id, move)


async def test_a_move_the_service_cannot_confirm_keeps_the_old_proxy(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, trace: Trace
) -> None:
    proxies = ServiceDownOnMove(listener=trace.record)
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        source = (await connect(bus, instagram)).source_id
        old = proxies.assignments[source].assignment_id
        proxies.pending[source] = Rebalance(job_id=uuid4(), target_proxy_id=uuid4())
        await minute(clock)
        await eventually(lambda: len(instagram.restores) == 1)
        after = await ask(bus, resolve(source))

    # back on the proxy that still works, not offline for an outage of the service
    assert [r.assignment_id for r in instagram.restores] == [old]
    assert [s.status for s in statuses(bus, source)] == ["active"]
    assert after.ok


async def test_the_proxy_of_a_login_that_failed_is_released_once_its_flow_is_over(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    instagram.add_account("anna.shop", password="right", external_id="1789")
    start = connect_start(uuid4())
    source = UUID(start["source_id"])
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        bus.submit(start)
        await bus.wait_until(lambda: any(s.state == "confirming" for s in flow_states(bus, start_op(start))))
        bus.submit(connect_confirm(start, password="wrong", totp_secret=None))
        await bus.wait_until(lambda: any(s.state == "failed" for s in flow_states(bus, start_op(start))))
        assignment = proxies.assignments[source].assignment_id
        for _ in range(6):
            await minute(clock)
        await eventually(lambda: len(proxies.releases) == 1)
        beats = len(proxies.heartbeats)
        await minute(clock)

    assert [(r.source_id, r.assignment_id) for r in proxies.releases] == [(source, assignment)]
    # confirmed while the flow could still end, never after its release
    assert len(proxies.heartbeats) == beats
    assert source not in proxies.assignments


def start_op(start: dict[str, Any]) -> UUID:
    return UUID(start["operation_id"])


async def test_without_a_proxy_provider_a_login_fails_source_offline_before_instagram(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    start = connect_start(uuid4())
    started = bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=NoProxies()):
        await bus.wait_until(lambda: bus.is_committed(started))

    [failed] = flow_states(bus, start_op(start))
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.SOURCE_OFFLINE)
    assert instagram.logins == []


async def test_without_a_proxy_provider_connected_sources_go_offline_once(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        source = (await connect(bus, instagram)).source_id
        await ask(bus, resolve(source))  # the login flow is committed
    instagram.restart()

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=NoProxies()):
        await bus.wait_until(lambda: len(statuses(bus, source)) == 2)
        refused = await ask(bus, resolve(source))
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=NoProxies()):
        await ask(bus, resolve(source))

    assert [(s.status, s.error_code) for s in statuses(bus, source)] == [
        ("active", None),
        ("error", StatusErrorCode.SOURCE_OFFLINE),
    ]
    assert refused.error_code is ResultErrorCode.SOURCE_OFFLINE
    assert instagram.restores == []


async def test_a_read_without_an_answer_keeps_the_proxy_and_the_status(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with connector(bus, clock, postgres_dsn, instagram, proxies):
        source = (await connect(bus, instagram)).source_id
        instagram.lookup_failures["bob.customer"] = Failure.NO_ANSWER
        refused = await ask(bus, resolve(source))
        await minute(clock)

    assert refused.error_code is ResultErrorCode.NETWORK_ERROR
    assert proxies.failures == []
    assert instagram.disconnects == []
    assert [s.status for s in statuses(bus, source)] == ["active"]
