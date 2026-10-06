"""The Flow of login as CRM drives it, and a connected Source for tests that need one."""

from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.events import ConnectStatusPayload, StatusPayload
from ig_connector.instagram import Account
from tests.support.crm import command
from tests.support.fake_instagram import FakeInstagram
from tests.support.memory_bus import MemoryBus, Published

PASSWORD = "correct-horse-battery-staple"
TOTP_SECRET = "JBSWY3DPEHPK3PXPJBSWY3DP"


def connect_start(
    source_id: UUID,
    *,
    login: str | None = "anna.shop",
    operation_id: UUID | None = None,
    country: str | None = "DE",
    network: str | None = "residential",
    reconnect_source_id: UUID | None = None,
) -> dict[str, Any]:
    operation_id = operation_id or uuid4()
    payload: dict[str, Any] = {
        # first connection: flow_id = source_id (contract 5.5); a reconnect has its own
        "flow_id": str(operation_id if reconnect_source_id else source_id),
        "role": "operator_channel",
        "method": "pairing_code",
        "phone": None,
        "login": login,
        "reconnect_source_id": None if reconnect_source_id is None else str(reconnect_source_id),
        "proxy_country_code": country,
        "proxy_network_type": network,
    }
    return command(CommandType.CONNECT_START, payload, source_id=source_id, operation_id=operation_id)


def connect_confirm(
    start: dict[str, Any], *, password: str = PASSWORD, totp_secret: str | None = TOTP_SECRET
) -> dict[str, Any]:
    """The confirm of the flow `start` began: same operation_id and source_id."""
    payload = {"flow_id": start["payload"]["flow_id"], "code": None, "password": password}
    if totp_secret is not None:
        payload["totp_secret"] = totp_secret
    return command(
        CommandType.CONNECT_CONFIRM,
        payload,
        source_id=UUID(start["source_id"]),
        operation_id=UUID(start["operation_id"]),
    )


def flow_states(bus: MemoryBus, operation_id: UUID) -> list[ConnectStatusPayload]:
    out = []
    for event in bus.events(EventType.CONNECT_STATUS, operation_id=operation_id):
        assert isinstance(event.payload, ConnectStatusPayload)
        out.append(event.payload)
    return out


def statuses(bus: MemoryBus, source_id: UUID) -> list[StatusPayload]:
    out = []
    for event in bus.events(EventType.STATUS):
        if event.envelope.source_id == source_id:
            assert isinstance(event.payload, StatusPayload)
            out.append(event.payload)
    return out


def published_for(bus: MemoryBus, source_id: UUID) -> list[Published]:
    return [event for event in bus.events() if event.envelope.source_id == source_id]


@dataclass(frozen=True, slots=True)
class Connected:
    source_id: UUID
    # the flow's operation_id
    operation_id: UUID
    account: Account


async def connect(
    bus: MemoryBus,
    instagram: FakeInstagram,
    *,
    source_id: UUID | None = None,
    login: str = "anna.shop",
    external_id: str = "1789",
    username: str | None = None,
) -> Connected:
    """Connect a Source through the real Flow of login; call inside running_connector.

    Registers the account in the fake Instagram (if new), runs connect.start and
    connect.confirm, and returns once the Source is `active`.
    """
    source_id = source_id or uuid4()
    if login not in instagram.accounts:
        instagram.add_account(
            login, password=PASSWORD, totp_secret=TOTP_SECRET, external_id=external_id, username=username
        )
    start = connect_start(source_id, login=login)
    operation_id = UUID(start["operation_id"])
    bus.submit(start)
    await bus.wait_until(lambda: any(s.state == "confirming" for s in flow_states(bus, operation_id)))
    bus.submit(connect_confirm(start))
    await bus.wait_until(lambda: any(s.status == "active" for s in statuses(bus, source_id)))
    [done] = [s for s in flow_states(bus, operation_id) if s.state == "done"]
    account = instagram.accounts[login].account
    assert done.external_account_id == account.external_id
    return Connected(source_id, operation_id, account)
