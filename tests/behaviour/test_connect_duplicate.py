"""One Account, one Source: the Instagram ID decides, never the username.

A second Source logging into an Account another live Source holds fails with
connect_failed and its new Session is logged out; CRM sets `already_connected` itself.
A dead holder (needs a reconnect, or in error without a live Session) gives the Account
up: it is switched off (`disabled`) and the new Source connects.
Reconnecting (reconnect_source_id) is the same Source: the device is reused, the
Session replaced.
"""

import asyncio
from datetime import datetime
from uuid import UUID, uuid4

from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.errors import ResultErrorCode, StatusErrorCode
from ig_connector.contract.events import ResultPayload, StatusPayload
from ig_connector.instagram import Account, Failure
from ig_connector.runtime import Statuses
from ig_connector.store import LoginFlow, SavedSession, Source, SourceStatus
from ig_connector.store.postgres import PostgresStore
from tests.behaviour.conftest import START
from tests.support.connector import running_connector
from tests.support.crm import command
from tests.support.fake_clock import FakeClock
from tests.support.fake_instagram import FakeInstagram
from tests.support.fake_proxy import FakeProxyService
from tests.support.login import (
    PASSWORD,
    TOTP_SECRET,
    connect,
    connect_confirm,
    connect_start,
    flow_states,
    statuses,
)
from tests.support.memory_bus import MemoryBus
from tests.support.postgres import SESSION_KEY

DEAUTHORIZED = StatusErrorCode.DEAUTHORIZED


async def run_flow(bus: MemoryBus, start: dict[str, object]) -> None:
    bus.submit(start)
    confirmed = bus.submit(connect_confirm(start))
    await bus.wait_until(lambda: bus.is_committed(confirmed))


def flow_of(start: dict[str, object]) -> UUID:
    return UUID(str(start["operation_id"]))


async def resolve_ok(bus: MemoryBus, source_id: UUID) -> bool:
    sent = command(
        CommandType.RESOLVE_RECIPIENT,
        {"recipient_kind": "username", "value": "anna.shop"},
        source_id=source_id,
    )
    delivery = bus.submit(sent)
    await bus.wait_until(lambda: bus.is_committed(delivery))
    [event] = bus.events(EventType.RESULT, operation_id=UUID(str(sent["operation_id"])))
    assert isinstance(event.payload, ResultPayload)
    return event.payload.ok


async def test_second_source_with_the_same_account_fails_and_the_first_keeps_working(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        first = await connect(bus, instagram, login="anna.shop", external_id="1789")
        second = uuid4()
        start = connect_start(second, login="anna.shop")
        await run_flow(bus, start)

        assert await resolve_ok(bus, first.source_id)

    failed = flow_states(bus, flow_of(start))[-1]
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.CONNECT_FAILED)
    assert statuses(bus, second) == []
    assert [s.status for s in statuses(bus, first.source_id)] == ["active"]
    # the second login happened and its Session is gone again
    assert len(instagram.logins) == 2
    [logout] = instagram.logouts
    assert logout.source_id == second
    assert logout.session == instagram.sessions_issued[-1]
    assert second not in instagram.sessions


async def test_reconnect_reuses_the_device_replaces_the_session_and_brings_the_source_back(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        first = await connect(bus, instagram, login="anna.shop", external_id="1789")
    before = await saved_session(postgres_dsn, first.source_id)
    # Instagram revoked the Session meanwhile (the startup check finds that out)
    await save_status(postgres_dsn, first.source_id, SourceStatus("needs_reconnect", DEAUTHORIZED))

    # contract 5.5: a reconnect may come without a login, CRM knows the Source by its id
    start = connect_start(first.source_id, login=None, reconnect_source_id=first.source_id)
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await run_flow(bus, start)

    done = flow_states(bus, flow_of(start))[-1]
    assert (done.state, done.external_account_id) == ("done", "1789")
    reconnect = instagram.logins[-1]
    assert (reconnect.login, reconnect.device) == ("anna.shop", before.device)
    after = await saved_session(postgres_dsn, first.source_id)
    assert after == SavedSession(instagram.sessions_issued[-1], before.device)
    assert after.session != before.session
    assert [s.status for s in statuses(bus, first.source_id)] == ["active", "active"]


async def test_a_renamed_account_is_still_the_same_account(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        first = await connect(bus, instagram, login="anna.shop", external_id="1789")
        instagram.rename("anna.shop", "anna.boutique")

        other = connect_start(uuid4(), login="anna.boutique")
        await run_flow(bus, other)
        again = connect_start(first.source_id, login="anna.boutique", reconnect_source_id=first.source_id)
        await run_flow(bus, again)

    failed = flow_states(bus, flow_of(other))[-1]
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.CONNECT_FAILED)
    done = flow_states(bus, flow_of(again))[-1]
    assert (done.state, done.external_account_id, done.account_username) == (
        "done",
        "1789",
        "anna.boutique",
    )


async def test_reconnect_into_another_account_fails_and_keeps_the_source_as_it_was(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_account("bob.store", password=PASSWORD, totp_secret=TOTP_SECRET, external_id="4242")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        first = await connect(bus, instagram, login="anna.shop", external_id="1789")
        before = await saved_session(postgres_dsn, first.source_id)
        start = connect_start(first.source_id, login="bob.store", reconnect_source_id=first.source_id)
        await run_flow(bus, start)

        assert await resolve_ok(bus, first.source_id)

    failed = flow_states(bus, flow_of(start))[-1]
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.CONNECT_FAILED)
    assert await saved_session(postgres_dsn, first.source_id) == before
    [logout] = instagram.logouts
    assert logout.session == instagram.sessions_issued[-1] != before.session
    # the Source still acts as its own account
    assert instagram.sessions[first.source_id].external_id == "1789"


async def test_reconnect_under_another_source_id_fails_without_a_login(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        first = await connect(bus, instagram, login="anna.shop", external_id="1789")
        start = connect_start(uuid4(), login="anna.shop", reconnect_source_id=first.source_id)
        started = bus.submit(start)
        await bus.wait_until(lambda: bus.is_committed(started))

    [failed] = flow_states(bus, flow_of(start))
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.CONNECT_FAILED)
    assert len(instagram.logins) == 1


async def saved_session(dsn: str, source_id: UUID) -> SavedSession:
    store = await PostgresStore.open(dsn, session_key=SESSION_KEY)
    try:
        saved = await store.session(source_id)
    finally:
        await store.close()
    assert saved is not None
    return saved


async def save_status(dsn: str, source_id: UUID, status: SourceStatus) -> None:
    store = await PostgresStore.open(dsn, session_key=SESSION_KEY)
    try:
        await store.save_status(source_id, status, now=START)
    finally:
        await store.close()


async def test_done_names_the_phone_when_the_account_logged_in_by_phone(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    phone = "+4915112345678"
    instagram.add_account(phone, password=PASSWORD, totp_secret=TOTP_SECRET, external_id="1789")
    by_phone = connect_start(uuid4(), login=None)
    by_phone["payload"]["phone"] = phone
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await run_flow(bus, by_phone)
        by_name = await connect(bus, instagram, login="bob.store", external_id="4242")

    done = flow_states(bus, flow_of(by_phone))[-1]
    # contract 5.5: CRM repeats it as `phone` on a reconnect
    assert (done.state, done.account_phone) == ("done", phone)
    [named] = [s for s in flow_states(bus, by_name.operation_id) if s.state == "done"]
    assert named.account_phone is None


class StoreLosingTheAnswerOfConnect(PostgresStore):
    async def connect_source(
        self,
        flow: LoginFlow,
        account: Account,
        session: SavedSession,
        *,
        now: datetime,
        take_from: Source | None = None,
    ) -> Source:
        await super().connect_source(flow, account, session, now=now, take_from=take_from)
        raise ConnectionError("connection dropped after commit")


async def test_a_session_that_may_be_saved_is_never_logged_out(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_account("anna.shop", password=PASSWORD, totp_secret=TOTP_SECRET, external_id="1789")
    start = connect_start(uuid4())
    async with running_connector(
        bus, clock, postgres_dsn, instagram=instagram, store_type=StoreLosingTheAnswerOfConnect
    ):
        await run_flow(bus, start)

    assert len(instagram.logins) == 1
    assert instagram.logouts == []


async def test_the_device_of_a_failed_first_login_is_kept_for_the_next_attempt(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    # 06.10: a new device on every attempt provokes checks and burns the login budget
    instagram.add_account("anna.shop", password=PASSWORD, totp_secret=TOTP_SECRET, external_id="1789")
    instagram.login_failures["anna.shop"] = Failure.REJECTED
    source_id = uuid4()
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await run_flow(bus, connect_start(source_id))
    del instagram.login_failures["anna.shop"]
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await run_flow(bus, connect_start(source_id))

    first, second = instagram.logins
    assert first.device is not None
    assert second.device == first.device
    assert (await saved_session(postgres_dsn, source_id)).device == first.device


async def test_an_account_held_by_a_source_that_needs_a_reconnect_moves_to_the_new_source(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        old = await connect(bus, instagram, login="anna.shop", external_id="1789")
        await resolve_ok(bus, old.source_id)  # the login flow is committed
    await save_status(postgres_dsn, old.source_id, SourceStatus("needs_reconnect", DEAUTHORIZED))

    new = uuid4()
    start = connect_start(new, login="anna.shop")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await run_flow(bus, start)
        assert await resolve_ok(bus, new)
        # switched off on purpose: nothing CRM should retry
        assert await resolve_code(bus, old.source_id) is ResultErrorCode.CHANNEL_REJECTED

    done = flow_states(bus, flow_of(start))[-1]
    assert (done.state, done.external_account_id) == ("done", "1789")
    assert [s.status for s in statuses(bus, new)] == ["active"]
    assert [s.status for s in statuses(bus, old.source_id)] == ["active", "disabled"]
    assert instagram.logouts == []
    assert await stored_session(postgres_dsn, old.source_id) is None
    # a check of the old Session that was already running cannot bring it back
    store = await PostgresStore.open(postgres_dsn, session_key=SESSION_KEY)
    try:
        late = Statuses(bus=bus, store=store, clock=clock)
        assert not await late.report(old.source_id, StatusPayload(status="active"))
    finally:
        await store.close()
    assert [s.status for s in statuses(bus, old.source_id)] == ["active", "disabled"]


async def test_an_account_held_by_a_source_in_error_without_a_session_moves_to_the_new_source(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, proxies: FakeProxyService
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        old = await connect(bus, instagram, login="anna.shop", external_id="1789")
        await resolve_ok(bus, old.source_id)
    instagram.restart()
    instagram.check_failures["1789"] = Failure.FLOOD  # the restore cannot bring it up

    new = uuid4()
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        await bus.wait_until(lambda: len(statuses(bus, old.source_id)) == 2)
        del instagram.check_failures["1789"]
        start = connect_start(new, login="anna.shop")
        await run_flow(bus, start)
        assert await resolve_ok(bus, new)
        # the old Source's proxy goes back to the service
        for _ in range(7):
            await clock.advance(60)
            await asyncio.sleep(0.05)
        assert old.source_id in [r.source_id for r in proxies.releases]
    # restarted again: the switched-off Source is not brought up, nothing more is said of it
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        assert await resolve_ok(bus, new)

    assert flow_states(bus, flow_of(start))[-1].state == "done"
    assert [s.status for s in statuses(bus, old.source_id)] == ["active", "error", "disabled"]
    assert old.source_id not in [r.source_id for r in instagram.restores[1:]]


async def stored_session(dsn: str, source_id: UUID) -> SavedSession | None:
    store = await PostgresStore.open(dsn, session_key=SESSION_KEY)
    try:
        return await store.session(source_id)
    finally:
        await store.close()


async def resolve_code(bus: MemoryBus, source_id: UUID) -> ResultErrorCode | None:
    sent = command(
        CommandType.RESOLVE_RECIPIENT,
        {"recipient_kind": "username", "value": "anna.shop"},
        source_id=source_id,
    )
    delivery = bus.submit(sent)
    await bus.wait_until(lambda: bus.is_committed(delivery))
    [event] = bus.events(EventType.RESULT, operation_id=UUID(str(sent["operation_id"])))
    assert isinstance(event.payload, ResultPayload)
    return event.payload.error_code
