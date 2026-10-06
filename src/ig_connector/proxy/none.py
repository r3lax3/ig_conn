"""No proxy configured: no Source may go to the network, so every reserve is refused.

Logins then fail with source_offline and connected Sources report `error` +
`source_offline` instead of staying silent.
"""

from uuid import UUID

from ig_connector.proxy import (
    Heartbeat,
    ProxyAssignment,
    ProxyRequirement,
    ProxyUnavailable,
    Rebalance,
    TransportState,
)

__all__ = ["NoProxies"]


class NoProxies:
    async def reserve(self, source_id: UUID, requirement: ProxyRequirement) -> ProxyAssignment:
        raise ProxyUnavailable("no proxy configured")

    # nothing is ever assigned, so nothing below is ever asked about an assignment

    async def heartbeat(self, source_id: UUID, assignment_id: int, transport: TransportState) -> Heartbeat:
        return Heartbeat()

    async def connection_failed(self, source_id: UUID, assignment_id: int, reason: str) -> None:
        return None

    async def rebalance(self, source_id: UUID, assignment_id: int, move: Rebalance) -> None:
        return None

    async def release(self, source_id: UUID, assignment_id: int) -> None:
        return None
