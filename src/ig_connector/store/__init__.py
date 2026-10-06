"""Store port: what the runtime keeps between deliveries and across restarts."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from ig_connector.contract.envelope import CommandEnvelope
from ig_connector.contract.errors import StatusErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Account, Device, InboxPositions, SessionData, ThreadPosition
from ig_connector.proxy import ProxyRequirement

__all__ = [
    "AccountMismatch",
    "AccountTaken",
    "FlowState",
    "LoginFlow",
    "Operation",
    "OperationState",
    "SavedSession",
    "Source",
    "SourceStatus",
    "Store",
]


class OperationState(StrEnum):
    RECEIVED = "received"
    EXECUTING = "executing"
    SENDING = "sending"
    DONE = "done"


@dataclass(frozen=True, slots=True)
class Operation:
    """A CRM command with its saved outcome, keyed by (operation_id, type)."""

    operation_id: UUID
    type: str
    source_id: UUID
    state: OperationState
    result: ResultPayload | None
    received_at: datetime
    updated_at: datetime
    # send only: our label of the message, set when it moved to sending
    client_context: str | None = None


class FlowState(StrEnum):
    CONFIRMING = "confirming"
    # a login attempt started: a redelivery must not start a second one
    LOGGING_IN = "logging_in"
    DONE = "done"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class LoginFlow:
    """A Flow of login, keyed by the operation_id of its connect.start and connect.confirm.

    The password and the TOTP secret are never part of it: they live only in the
    connect.confirm being executed.
    """

    operation_id: UUID
    source_id: UUID
    channel_type: str
    state: FlowState
    # None when the flow failed before it knew them
    login: str | None
    proxy: ProxyRequirement | None
    started_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class SourceStatus:
    """The last event.status published for a Source."""

    status: str
    error_code: StatusErrorCode | None = None


@dataclass(frozen=True, slots=True)
class Source:
    source_id: UUID
    channel_type: str
    account: Account
    proxy: ProxyRequirement
    # None until the first event.status is out
    status: SourceStatus | None
    # the login of the last successful Flow of login; a reconnect may come without one
    login: str | None = None
    # its Account moved to another Source: no Session, never brought up again
    disabled: bool = False


class AccountTaken(Exception):
    """The Account (Instagram ID) already belongs to another Source."""

    def __init__(self, external_id: str, holder: UUID | None = None) -> None:
        super().__init__(f"account {external_id} belongs to another Source")
        self.external_id = external_id
        self.holder = holder


class AccountMismatch(Exception):
    """The Source is bound to another Account than the one just logged into."""

    def __init__(self, source_id: UUID) -> None:
        super().__init__(f"Source {source_id} belongs to another account")
        self.source_id = source_id


@dataclass(frozen=True, slots=True)
class SavedSession:
    session: SessionData
    device: Device


class Store(Protocol):
    async def record_operation(self, envelope: CommandEnvelope, *, now: datetime) -> Operation:
        """Save the command if it is new; return the saved Operation either way (dedup)."""
        ...

    async def finish_operation(
        self, operation: Operation, result: ResultPayload, *, now: datetime
    ) -> Operation:
        """Save the result and move the Operation to done; a later finish overwrites it."""
        ...

    async def start_sending(self, operation: Operation, client_context: str, *, now: datetime) -> Operation:
        """Move the Operation to sending with the message's label, before the platform call.

        From here on the message may be out: a redelivery must reconcile, never send again.
        """
        ...

    async def start_flow(self, flow: LoginFlow) -> LoginFlow:
        """Save the flow if its operation_id is new; return the saved one either way (dedup)."""
        ...

    async def flow(self, operation_id: UUID) -> LoginFlow | None: ...

    async def move_flow(
        self, operation_id: UUID, *, expected: FlowState, to: FlowState, now: datetime
    ) -> bool:
        """Move the flow only from `expected`; False when it is elsewhere (or unknown)."""
        ...

    async def connect_source(
        self,
        flow: LoginFlow,
        account: Account,
        session: SavedSession,
        *,
        now: datetime,
        take_from: Source | None = None,
    ) -> Source:
        """In one transaction: the Source with its Session and device saved, the flow done.

        A Source connected again (reconnect) keeps its id; its Session and device are
        replaced, its last status is kept. Nothing is saved, and AccountTaken raised (naming
        the holder), when another Source holds the Account; AccountMismatch when this Source
        holds another one. Its inbox starts afresh at `now` (thread positions dropped): no
        history, no catching up.

        `take_from`, the holder as last read: the Account moves from it, which is switched
        off and loses its Session and device. AccountTaken if its status changed meanwhile.
        """
        ...

    async def source(self, source_id: UUID) -> Source | None: ...

    async def session(self, source_id: UUID) -> SavedSession | None:
        """The Source's Session and device, decrypted."""
        ...

    async def device(self, source_id: UUID) -> Device | None:
        """The Source's device: its Session's, or the one saved before its first login."""
        ...

    async def save_device(self, source_id: UUID, device: Device, *, now: datetime) -> None:
        """Keep a device made for a Source not connected yet: one device per Source, always."""
        ...

    async def save_status(self, source_id: UUID, status: SourceStatus, *, now: datetime) -> None: ...

    async def connected_sources(self) -> list[UUID]:
        """Every Source connected and not switched off, whatever its status."""
        ...

    async def inbox_positions(self, source_id: UUID) -> InboxPositions | None:
        """Where the connector is in the Source's inbox; None for a Source never connected."""
        ...

    async def move_inbox_position(
        self, source_id: UUID, thread_id: str, position: ThreadPosition, *, now: datetime
    ) -> None: ...
