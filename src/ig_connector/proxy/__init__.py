"""Proxy port: the only way out to the network for a Source (contract 8).

account_id at the proxy service is the source_id. Without an assignment a Source does not
go to the network at all: there is no direct fallback.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from pydantic import SecretStr

__all__ = [
    "AssignmentLost",
    "Heartbeat",
    "ProxyAssignment",
    "ProxyProvider",
    "ProxyRequirement",
    "ProxyUnavailable",
    "Rebalance",
    "TransportState",
]


@dataclass(frozen=True, slots=True)
class ProxyRequirement:
    """What the operator asked for in connect.start; fixed for the Source after login."""

    country_code: str
    # None: the service's default
    network_type: str | None = None


@dataclass(frozen=True, slots=True)
class ProxyAssignment:
    assignment_id: int
    # scheme://user:password@host:port, credentials included: never log it
    url: SecretStr = field(repr=False)

    @property
    def scheme(self) -> str:
        """`http`, `socks5`, ...: safe to log. Chromium cannot use SOCKS5 with credentials."""
        return self.url.get_secret_value().partition("://")[0].lower()


class TransportState(StrEnum):
    """The Source's connections through its proxy, as the service is told in heartbeats."""

    UNKNOWN = "unknown"
    CONNECTED = "connected"
    # waiting for the running command before a move
    DRAINING = "draining"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class Rebalance:
    """A planned move the service asks for (`pending_rebalance`)."""

    job_id: UUID
    target_proxy_id: UUID


@dataclass(frozen=True, slots=True)
class Heartbeat:
    pending_rebalance: Rebalance | None = None


class ProxyUnavailable(Exception):
    """No proxy for the Source: 409 from the service, or the service cannot be reached.

    The message is safe to log and to send to CRM: no credentials, no URLs.
    """


class AssignmentLost(ProxyUnavailable):
    """The assignment named in the call is no longer the Source's (closed or replaced)."""


class ProxyProvider(Protocol):
    """Every call but reserve names the assignment it is about (expected_assignment_id)."""

    async def reserve(self, source_id: UUID, requirement: ProxyRequirement) -> ProxyAssignment:
        """The Source's assignment; the same one again for the same source_id (idempotent)."""
        ...

    async def heartbeat(self, source_id: UUID, assignment_id: int, transport: TransportState) -> Heartbeat:
        """The assignment is alive, its transport in this state; says if a move is asked for."""
        ...

    async def connection_failed(self, source_id: UUID, assignment_id: int, reason: str) -> None:
        """A confirmed network failure through this proxy: the next reserve gives another one."""
        ...

    async def rebalance(self, source_id: UUID, assignment_id: int, move: Rebalance) -> None:
        """Confirm the planned move; the transport is closed already. Reserve gives the new one."""
        ...

    async def release(self, source_id: UUID, assignment_id: int) -> None:
        """Give the assignment back: the Source stops working."""
        ...
