"""Flow of login: connect.start reserves a proxy and asks for confirmation,
connect.confirm logs in with password and TOTP secret through that proxy.

connect.* never get ack or result: only event.connect.status on the flow's operation_id.
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.errors import ResultErrorCode, StatusErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Failure
from ig_connector.proxy import ProxyAssignment, ProxyRequirement
from ig_connector.store import SourceStatus
from ig_connector.store.postgres import PostgresStore
from tests.support.connector import eventually, running_connector
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
from tests.support.tracing import Trace


def operation(sent: dict[str, Any]) -> UUID:
    return UUID(sent["operation_id"])


def add_anna(instagram: FakeInstagram) -> None:
    instagram.add_account(
        "anna.shop", password=PASSWORD, totp_secret=TOTP_SECRET, external_id="1789", full_name="Anna"
    )


async def test_operator_connects_an_account_with_password_and_totp_secret(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    proxies: FakeProxyService,
) -> None:
    add_anna(instagram)
    source = uuid4()
    start = connect_start(source, login="anna.shop", country="DE", network="residential")
    started = bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        await bus.wait_until(lambda: bus.is_committed(started))
        assert [s.state for s in flow_states(bus, operation(start))] == ["confirming"]
        assert instagram.logins == []

        confirmed = bus.submit(connect_confirm(start))
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    _, done = flow_states(bus, operation(start))
    assert done.state == "done"
    assert (done.external_account_id, done.external_account_name) == ("1789", "anna.shop")
    assert {(r.source_id, r.requirement) for r in proxies.reservations} == {
        (source, ProxyRequirement("DE", "residential"))
    }
    [attempt] = instagram.logins
    assert attempt.login == "anna.shop"
    assert attempt.assignment_id == proxies.assignments[source].assignment_id
    assert [s.status for s in statuses(bus, source)] == ["active"]
    # done first, then the status; and nothing else: no ack, no result
    types = [event.envelope.type for event in bus.events()]
    assert types == [EventType.CONNECT_STATUS, EventType.CONNECT_STATUS, EventType.STATUS]
    for event in bus.events(EventType.CONNECT_STATUS):
        assert event.envelope.source_id == source
        assert event.envelope.operation_id == operation(start)


async def test_connected_source_takes_commands(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        connected = await connect(bus, instagram)
        # the placeholder send handler answers internal_error, an offline Source source_offline
        delivery = bus.submit(
            command(
                CommandType.SEND,
                {"message_id": "m1", "external_chat_id": "42", "text": "hi"},
                source_id=connected.source_id,
            )
        )
        await bus.wait_until(lambda: bus.is_committed(delivery))

    [result] = [
        e.payload for e in bus.events(EventType.RESULT) if e.envelope.source_id == connected.source_id
    ]
    assert isinstance(result, ResultPayload)
    assert result.error_code != ResultErrorCode.SOURCE_OFFLINE


async def test_wrong_password_fails_with_verify_failed_and_source_stays_offline(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    source = uuid4()
    start = connect_start(source)
    bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        confirmed = bus.submit(connect_confirm(start, password="wrong-password"))
        await bus.wait_until(lambda: bus.is_committed(confirmed))
        resolve = bus.submit(
            command(
                CommandType.RESOLVE_RECIPIENT, {"recipient_kind": "username", "value": "x"}, source_id=source
            )
        )
        await bus.wait_until(lambda: bus.is_committed(resolve))

    _, failed = flow_states(bus, operation(start))
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.VERIFY_FAILED)
    assert statuses(bus, source) == []
    [result] = [e.payload for e in bus.events(EventType.RESULT)]
    assert isinstance(result, ResultPayload)
    assert result.error_code == ResultErrorCode.SOURCE_OFFLINE


async def test_wrong_totp_secret_fails_with_verify_failed(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    start = connect_start(uuid4())
    bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        confirmed = bus.submit(connect_confirm(start, totp_secret="AAAABBBBCCCCDDDD"))
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    assert flow_states(bus, operation(start))[-1].error_code == StatusErrorCode.VERIFY_FAILED


async def test_check_the_connector_cannot_pass_fails_with_a_hint_to_use_the_app(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    instagram.login_failures["anna.shop"] = Failure.CHALLENGE
    start = connect_start(uuid4())
    bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        confirmed = bus.submit(connect_confirm(start))
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    failed = flow_states(bus, operation(start))[-1]
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.CONNECT_FAILED)
    assert "Instagram app" in (failed.error_text or "")


@pytest.mark.parametrize("failure", [Failure.NETWORK, Failure.NO_ANSWER])
async def test_network_failure_during_login_fails_with_source_offline(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, failure: Failure
) -> None:
    add_anna(instagram)
    instagram.login_failures["anna.shop"] = failure
    start = connect_start(uuid4())
    bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        confirmed = bus.submit(connect_confirm(start))
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    assert flow_states(bus, operation(start))[-1].error_code == StatusErrorCode.SOURCE_OFFLINE


async def test_a_socks5_proxy_with_credentials_fails_the_login_with_a_clear_text(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    proxies: FakeProxyService,
) -> None:
    # the browser cannot authenticate to SOCKS5; stage 1 asks the customer for http proxies
    add_anna(instagram)
    proxies.scheme = "socks5"
    start = connect_start(uuid4())
    bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        confirmed = bus.submit(connect_confirm(start))
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    failed = flow_states(bus, operation(start))[-1]
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.CONNECT_FAILED)
    assert failed.error_text == "proxy scheme not supported for login"


async def test_no_proxy_in_the_pool_fails_the_start_with_source_offline(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    proxies: FakeProxyService,
) -> None:
    add_anna(instagram)
    proxies.pool_empty = True
    start = connect_start(uuid4())
    started = bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        await bus.wait_until(lambda: bus.is_committed(started))
        # a confirm the operator still sends changes nothing
        confirmed = bus.submit(connect_confirm(start))
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    [failed] = flow_states(bus, operation(start))
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.SOURCE_OFFLINE)
    assert instagram.logins == []


async def test_direct_connection_is_refused_with_source_offline(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    proxies: FakeProxyService,
) -> None:
    start = connect_start(uuid4(), country=None)
    started = bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        await bus.wait_until(lambda: bus.is_committed(started))

    [failed] = flow_states(bus, operation(start))
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.SOURCE_OFFLINE)
    assert proxies.reservations == []


async def test_repeated_start_and_confirm_do_not_log_in_twice(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    proxies: FakeProxyService,
) -> None:
    add_anna(instagram)
    source = uuid4()
    start = connect_start(source)
    confirm = connect_confirm(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram, proxies=proxies):
        bus.submit(start)
        again = bus.submit(start)
        await bus.wait_until(lambda: bus.is_committed(again))
        bus.submit(confirm)
        confirm_again = bus.submit(confirm)
        start_late = bus.submit(start)
        await bus.wait_until(lambda: bus.is_committed(confirm_again) and bus.is_committed(start_late))

    assert len(instagram.logins) == 1
    assert len(proxies.reservations) == 2  # start, and the confirm's own (idempotent) reserve
    # the repeated confirm repeats done (it may have been lost), never the login
    assert [s.state for s in flow_states(bus, operation(start))] == [
        "confirming",
        "confirming",
        "done",
        "done",
    ]
    assert [s.status for s in statuses(bus, source)] == ["active"]


async def test_confirm_without_a_flow_or_for_a_finished_one_does_nothing(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    stray = connect_confirm(connect_start(uuid4()))
    failed_start = connect_start(uuid4(), country=None)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        unknown = bus.submit(stray)
        bus.submit(failed_start)
        late = bus.submit(connect_confirm(failed_start))
        await bus.wait_until(lambda: bus.is_committed(unknown) and bus.is_committed(late))

    assert flow_states(bus, operation(stray)) == []
    assert [s.state for s in flow_states(bus, operation(failed_start))] == ["failed"]
    assert instagram.logins == []


async def test_login_that_hangs_fails_before_the_flow_expires(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    instagram.hold_login["anna.shop"] = asyncio.Event()
    start = connect_start(uuid4())
    begun = clock.now()
    bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        confirmed = bus.submit(connect_confirm(start))
        await eventually(lambda: len(instagram.logins) == 1)
        await asyncio.sleep(0.05)  # the login deadline is set on the clock
        await clock.advance(239)
        assert [s.state for s in flow_states(bus, operation(start))] == ["confirming"]

        await clock.advance(1)
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    failed = flow_states(bus, operation(start))[-1]
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.CONNECT_FAILED)
    assert clock.now() - begun < timedelta(minutes=5)


async def test_confirm_after_the_flow_expired_fails_without_a_login(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    start = connect_start(uuid4())
    started = bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await bus.wait_until(lambda: bus.is_committed(started))
        await clock.advance(5 * 60)
        confirmed = bus.submit(connect_confirm(start))
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    assert flow_states(bus, operation(start))[-1].state == "failed"
    assert instagram.logins == []


async def test_confirm_redelivered_after_a_crash_mid_login_fails_instead_of_logging_in_again(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    instagram.hold_login["anna.shop"] = asyncio.Event()
    start = connect_start(uuid4())
    bus.submit(start)
    confirmed = bus.submit(connect_confirm(start))
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await eventually(lambda: len(instagram.logins) == 1)
    # killed mid-login; the confirm was not committed and comes again
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    assert len(instagram.logins) == 1
    failed = flow_states(bus, operation(start))[-1]
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.CONNECT_FAILED)


async def test_password_and_totp_secret_are_kept_nowhere_and_the_session_is_encrypted(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram, trace: Trace
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        connected = await connect(bus, instagram)

    traced = json.dumps([entry.data for entry in trace.entries], default=str)
    stored = await _dump_tables(postgres_dsn)
    session = f"session-{connected.account.external_id}"
    for secret in (PASSWORD, TOTP_SECRET):
        assert secret not in traced
        assert secret not in stored
    assert session not in stored
    assert "device-" not in stored


async def _dump_tables(dsn: str) -> str:
    db = await asyncpg.connect(dsn)
    try:
        tables = await db.fetch(
            "select table_name from information_schema.tables where table_schema = current_schema()"
        )
        rows = []
        for table in tables:
            for row in await db.fetch(f'select * from "{table["table_name"]}"'):  # noqa: S608
                rows.append({key: _text(value) for key, value in row.items()})
    finally:
        await db.close()
    return json.dumps(rows, default=str)


def _text(value: object) -> object:
    return value.decode("latin-1") if isinstance(value, bytes) else value


async def test_confirm_redelivered_after_done_repeats_done_without_a_new_login(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    source = uuid4()
    start = connect_start(source)
    confirm = connect_confirm(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        bus.submit(start)
        bus.submit(confirm)
        await bus.wait_until(lambda: bool(statuses(bus, source)))
        # done may have been lost with the process before the confirm's offset was committed
        again = bus.submit(confirm)
        await bus.wait_until(lambda: bus.is_committed(again))

    assert len(instagram.logins) == 1
    states = flow_states(bus, operation(start))
    assert [s.state for s in states] == ["confirming", "done", "done"]
    assert states[-1].external_account_id == "1789"
    assert [s.status for s in statuses(bus, source)] == ["active"]


class StoreFailingToSaveStatuses(PostgresStore):
    async def save_status(self, source_id: UUID, status: SourceStatus, *, now: datetime) -> None:
        raise ConnectionError("database went away")


async def test_failure_after_done_never_turns_the_flow_into_failed(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    start = connect_start(uuid4())
    bus.submit(start)

    async with running_connector(
        bus, clock, postgres_dsn, instagram=instagram, store_type=StoreFailingToSaveStatuses
    ):
        confirmed = bus.submit(connect_confirm(start))
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    assert [s.state for s in flow_states(bus, operation(start))] == ["confirming", "done"]


async def test_platform_detail_stays_out_of_error_text(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    instagram.login_failures["anna.shop"] = Failure.REJECTED
    instagram.failure_detail = "response body with sessionid=abc"
    start = connect_start(uuid4())
    bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        confirmed = bus.submit(connect_confirm(start))
        await bus.wait_until(lambda: bus.is_committed(confirmed))

    failed = flow_states(bus, operation(start))[-1]
    assert failed.error_code == StatusErrorCode.CONNECT_FAILED
    assert "sessionid" not in (failed.error_text or "")


class ProxyServiceBrokenOnSecondCall(FakeProxyService):
    async def reserve(self, source_id: UUID, requirement: ProxyRequirement) -> ProxyAssignment:
        if self.reservations:
            raise RuntimeError("bug in the proxy client")
        return await super().reserve(source_id, requirement)


async def test_unexpected_error_mid_confirm_fails_the_flow_once(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    add_anna(instagram)
    start = connect_start(uuid4())
    confirm = connect_confirm(start)
    bus.submit(start)

    async with running_connector(
        bus, clock, postgres_dsn, instagram=instagram, proxies=ProxyServiceBrokenOnSecondCall()
    ):
        bus.submit(confirm)
        again = bus.submit(confirm)
        await bus.wait_until(lambda: bus.is_committed(again))

    assert [s.state for s in flow_states(bus, operation(start))] == ["confirming", "failed"]
    assert instagram.logins == []


async def test_without_a_login_flow_connect_fails_at_once(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str
) -> None:
    start = connect_start(uuid4())
    started = bus.submit(start)

    async with running_connector(bus, clock, postgres_dsn, login_enabled=False):
        await bus.wait_until(lambda: bus.is_committed(started))

    [failed] = flow_states(bus, operation(start))
    assert (failed.state, failed.error_code) == ("failed", StatusErrorCode.CONNECT_FAILED)
