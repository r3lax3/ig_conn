"""What the runtime needs to know about a Source before running its command."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from ig_connector.contract.errors import StatusErrorCode
from ig_connector.store import Store


class SourceCondition(StrEnum):
    ACTIVE = "active"
    # no Session, needs_reconnect, no proxy, network down
    OFFLINE = "offline"
    # Instagram restricted the Account (flood)
    LIMITED = "limited"
    # switched off: its Account moved to another Source
    DISABLED = "disabled"


@dataclass(frozen=True, slots=True)
class SourceState:
    source_id: UUID
    condition: SourceCondition


class SourceStates(Protocol):
    async def state(self, source_id: UUID) -> SourceState: ...


class NoActiveSources:
    """Every Source is offline: there is no way to bring up a Session yet."""

    async def state(self, source_id: UUID) -> SourceState:
        return SourceState(source_id, SourceCondition.OFFLINE)


class StoredSourceStates:
    """Source states from the store: the last published status of a connected Source.

    No Source (never connected) or no status yet is offline; `active` is active; `error`
    with `peer_flood` is limited; anything else is offline.
    """

    def __init__(self, store: Store) -> None:
        self._store = store

    async def state(self, source_id: UUID) -> SourceState:
        source = await self._store.source(source_id)
        status = None if source is None else source.status
        if status is None:
            return SourceState(source_id, SourceCondition.OFFLINE)
        if status.status == "active":
            return SourceState(source_id, SourceCondition.ACTIVE)
        if status.status == "error" and status.error_code is StatusErrorCode.PEER_FLOOD:
            return SourceState(source_id, SourceCondition.LIMITED)
        return SourceState(source_id, SourceCondition.OFFLINE)
