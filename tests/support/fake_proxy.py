"""A proxy service in memory, for behaviour tests."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import SecretStr

from ig_connector.proxy import (
    AssignmentLost,
    Heartbeat,
    ProxyAssignment,
    ProxyRequirement,
    ProxyUnavailable,
    Rebalance,
    TransportState,
)

Listener = Callable[[str, Mapping[str, Any]], None]


@dataclass(frozen=True, slots=True)
class Reservation:
    source_id: UUID
    requirement: ProxyRequirement


@dataclass(frozen=True, slots=True)
class HeartbeatCall:
    source_id: UUID
    assignment_id: int
    transport: TransportState


@dataclass(frozen=True, slots=True)
class AssignmentCall:
    """connection-failed, rebalance or release: what it named."""

    source_id: UUID
    assignment_id: int
    detail: str = ""


class FakeProxyService:
    """Hands out one assignment per Source, like the service does.

    Scenario knobs: `pool_empty` (every new reservation is a 409), `unreachable` (the
    service does not answer at all), `pending[source_id] = Rebalance` (a planned move,
    shown in heartbeats until confirmed). Observations: `reservations`, `heartbeats`,
    `failures` (connection-failed), `moves` (rebalance), `releases`, all in call order.
    Calls naming an assignment that is not the Source's raise AssignmentLost (409). A
    move is accepted only after a heartbeat said the transport is closed, like the service.
    """

    def __init__(self, *, listener: Listener | None = None) -> None:
        self.pool_empty = False
        # of the proxies handed out ("socks5": the service's pool imports those too)
        self.scheme = "http"
        self.unreachable = False
        self.reservations: list[Reservation] = []
        self.assignments: dict[UUID, ProxyAssignment] = {}
        self.pending: dict[UUID, Rebalance] = {}
        self.heartbeats: list[HeartbeatCall] = []
        self.failures: list[AssignmentCall] = []
        self.moves: list[AssignmentCall] = []
        self.releases: list[AssignmentCall] = []
        self._transport: dict[UUID, TransportState] = {}
        self._listener = listener
        self._next_id = 1

    async def reserve(self, source_id: UUID, requirement: ProxyRequirement) -> ProxyAssignment:
        self.reservations.append(Reservation(source_id, requirement))
        self._emit(
            "proxy.reserve",
            {
                "source_id": str(source_id),
                "country_code": requirement.country_code,
                "network_type": requirement.network_type,
            },
        )
        if self.unreachable:
            raise ProxyUnavailable("proxy service unreachable")
        assignment = self.assignments.get(source_id)
        if assignment is not None:
            return assignment
        if self.pool_empty:
            raise ProxyUnavailable("no working proxy available")
        return self._assign(source_id)

    async def heartbeat(self, source_id: UUID, assignment_id: int, transport: TransportState) -> Heartbeat:
        self.heartbeats.append(HeartbeatCall(source_id, assignment_id, transport))
        self._emit(
            "proxy.heartbeat",
            {"source_id": str(source_id), "assignment_id": assignment_id, "transport": transport.value},
        )
        self._held(source_id, assignment_id)
        self._transport[source_id] = transport
        return Heartbeat(pending_rebalance=self.pending.get(source_id))

    async def connection_failed(self, source_id: UUID, assignment_id: int, reason: str) -> None:
        self.failures.append(AssignmentCall(source_id, assignment_id, reason))
        self._emit("proxy.connection_failed", {"source_id": str(source_id), "assignment_id": assignment_id})
        self._held(source_id, assignment_id)
        del self.assignments[source_id]

    async def rebalance(self, source_id: UUID, assignment_id: int, move: Rebalance) -> None:
        self.moves.append(AssignmentCall(source_id, assignment_id, str(move.job_id)))
        self._emit("proxy.rebalance", {"source_id": str(source_id), "assignment_id": assignment_id})
        self._held(source_id, assignment_id)
        if self.pending.get(source_id) != move or self._transport.get(source_id) is not TransportState.CLOSED:
            raise AssignmentLost("no such rebalance job or transport not closed")
        del self.pending[source_id]
        self._assign(source_id)

    async def release(self, source_id: UUID, assignment_id: int) -> None:
        self.releases.append(AssignmentCall(source_id, assignment_id))
        self._emit("proxy.release", {"source_id": str(source_id), "assignment_id": assignment_id})
        self._held(source_id, assignment_id)
        del self.assignments[source_id]

    def _held(self, source_id: UUID, assignment_id: int) -> None:
        if self.unreachable:
            raise ProxyUnavailable("proxy service unreachable")
        held = self.assignments.get(source_id)
        if held is None or held.assignment_id != assignment_id:
            raise AssignmentLost("assignment changed")

    def _assign(self, source_id: UUID) -> ProxyAssignment:
        assignment = ProxyAssignment(
            assignment_id=self._next_id,
            url=SecretStr(f"{self.scheme}://user{self._next_id}:proxy-secret@proxy{self._next_id}.test:8080"),
        )
        self._next_id += 1
        self.assignments[source_id] = assignment
        self._transport.pop(source_id, None)
        return assignment

    def _emit(self, kind: str, data: Mapping[str, Any]) -> None:
        if self._listener is not None:
            self._listener(kind, data)
