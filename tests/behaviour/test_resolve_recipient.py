"""command.resolve_recipient: CRM asks whether a recipient exists and how to address it.

A private dialog is addressed by the user's Instagram ID: `external_chat_id` = pk.
Nothing is ever sent to the recipient.
"""

import asyncio
from collections.abc import Callable
from uuid import UUID, uuid4

import pytest

from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.errors import ResultErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Failure
from ig_connector.runtime.deadline import DEADLINE_PASSED
from tests.support.connector import running_connector
from tests.support.crm import command
from tests.support.fake_clock import FakeClock
from tests.support.fake_instagram import FakeInstagram
from tests.support.login import connect
from tests.support.memory_bus import MemoryBus


def resolve(source_id: UUID, kind: str, value: str) -> dict[str, object]:
    return command(
        CommandType.RESOLVE_RECIPIENT, {"recipient_kind": kind, "value": value}, source_id=source_id
    )


def result_of(bus: MemoryBus, sent: dict[str, object]) -> ResultPayload:
    [event] = bus.events(EventType.RESULT, operation_id=UUID(str(sent["operation_id"])))
    assert isinstance(event.payload, ResultPayload)
    return event.payload


async def eventually(condition: Callable[[], bool], within: float = 2.0) -> None:
    async with asyncio.timeout(within):
        while not condition():  # noqa: ASYNC110  the fake has no event to wait on
            await asyncio.sleep(0.01)


async def ask(bus: MemoryBus, sent: dict[str, object]) -> ResultPayload:
    delivery = bus.submit(sent)
    await bus.wait_until(lambda: bus.is_committed(delivery))
    return result_of(bus, sent)


async def test_finds_a_user_by_username(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer", full_name="Bob Smith")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram)).source_id
        result = await ask(bus, resolve(source, "username", "bob.customer"))

    assert result == ResultPayload(ok=True, external_chat_id="4242", display_name="Bob Smith")


async def test_finds_a_user_by_instagram_id_named_by_username_without_full_name(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram)).source_id
        result = await ask(bus, resolve(source, "external_id", "4242"))

    assert result == ResultPayload(ok=True, external_chat_id="4242", display_name="bob.customer")
    assert [(lookup.source_id, lookup.by) for lookup in instagram.lookups] == [(source, "external_id")]


async def test_unknown_user_is_recipient_not_found(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram)).source_id
        by_name = await ask(bus, resolve(source, "username", "nobody.here"))
        by_id = await ask(bus, resolve(source, "external_id", "999"))

    for result in (by_name, by_id):
        assert (result.ok, result.error_code) == (False, ResultErrorCode.RECIPIENT_NOT_FOUND)
    assert len(instagram.lookups) == 2


async def test_phone_and_non_numeric_id_are_not_found_without_asking_instagram(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram)).source_id
        by_phone = await ask(bus, resolve(source, "phone", "+79991234567"))
        by_bad_id = await ask(bus, resolve(source, "external_id", "bob.customer"))

    for result in (by_phone, by_bad_id):
        assert (result.ok, result.error_code) == (False, ResultErrorCode.RECIPIENT_NOT_FOUND)
    assert instagram.lookups == []


async def test_lookup_without_answer_within_the_deadline_is_network_error(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    instagram.hold_lookup["bob.customer"] = asyncio.Event()
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram)).source_id
        sent = resolve(source, "username", "bob.customer")
        delivery = bus.submit(sent)
        await eventually(lambda: bool(instagram.lookups))
        await clock.advance(7)
        assert not bus.is_committed(delivery)

        # CRM gives up at 10 s: the answer comes before that
        await clock.advance(2)
        await bus.wait_until(lambda: bus.is_committed(delivery))

    result = result_of(bus, sent)
    assert (result.ok, result.error_code) == (False, ResultErrorCode.NETWORK_ERROR)


async def test_lookup_whose_time_ran_out_in_the_queue_is_network_error_without_asking_instagram(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    instagram.hold_lookup["slow.one"] = asyncio.Event()
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram)).source_id
        bus.submit(resolve(source, "username", "slow.one"))
        queued = resolve(source, "username", "bob.customer")
        delivery = bus.submit(queued)
        await eventually(lambda: bool(instagram.lookups))
        # the first lookup uses up the time CRM gives the queued one, counted from its ack
        await clock.advance(9)
        await bus.wait_until(lambda: bus.is_committed(delivery))

    assert result_of(bus, queued) == ResultPayload(
        ok=False, error_code=ResultErrorCode.NETWORK_ERROR, error_text=DEADLINE_PASSED
    )
    assert [lookup.value for lookup in instagram.lookups] == ["slow.one"]


@pytest.mark.parametrize(
    ("failure", "code"),
    [
        (Failure.FLOOD, ResultErrorCode.PEER_FLOOD),
        (Failure.RATE_LIMITED, ResultErrorCode.RATE_LIMITED),
        (Failure.NETWORK, ResultErrorCode.NETWORK_ERROR),
        (Failure.SESSION_REVOKED, ResultErrorCode.SOURCE_OFFLINE),
        (Failure.CHALLENGE, ResultErrorCode.SOURCE_OFFLINE),
        (Failure.BAD_CREDENTIALS, ResultErrorCode.SOURCE_OFFLINE),
        (Failure.REJECTED, ResultErrorCode.CHANNEL_REJECTED),
    ],
)
async def test_platform_refusal_maps_to_its_error_code(
    bus: MemoryBus,
    clock: FakeClock,
    postgres_dsn: str,
    instagram: FakeInstagram,
    failure: Failure,
    code: ResultErrorCode,
) -> None:
    instagram.lookup_failures["bob.customer"] = failure
    instagram.failure_detail = "sessionid=abc123 leaked by the adapter"
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        source = (await connect(bus, instagram)).source_id
        result = await ask(bus, resolve(source, "username", "bob.customer"))

    assert (result.ok, result.error_code) == (False, code)
    assert "abc123" not in (result.error_text or "")


async def test_source_not_connected_is_source_offline_without_lookup(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, instagram: FakeInstagram
) -> None:
    instagram.add_user(external_id="4242", username="bob.customer")
    async with running_connector(bus, clock, postgres_dsn, instagram=instagram):
        result = await ask(bus, resolve(uuid4(), "username", "bob.customer"))

    assert (result.ok, result.error_code) == (False, ResultErrorCode.SOURCE_OFFLINE)
    assert instagram.lookups == []
