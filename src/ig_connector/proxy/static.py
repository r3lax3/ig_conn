"""One proxy from the config for every Source: no service, nothing to heartbeat or move.

For local runs and a stand without the proxy service. The requirement from connect.start
is not checked against it: the operator chose this proxy by configuring it.
"""

import logging
from uuid import UUID

from pydantic import SecretStr

from ig_connector.proxy import Heartbeat, ProxyAssignment, ProxyRequirement, Rebalance, TransportState

__all__ = ["StaticProxy"]

log = logging.getLogger(__name__)

STATIC_ASSIGNMENT_ID = 1


class StaticProxy:
    def __init__(self, url: SecretStr) -> None:
        self._assignment = ProxyAssignment(assignment_id=STATIC_ASSIGNMENT_ID, url=url)

    async def reserve(self, source_id: UUID, requirement: ProxyRequirement) -> ProxyAssignment:
        return self._assignment

    async def heartbeat(self, source_id: UUID, assignment_id: int, transport: TransportState) -> Heartbeat:
        return Heartbeat()

    async def connection_failed(self, source_id: UUID, assignment_id: int, reason: str) -> None:
        # there is no other proxy to switch to: the next reserve gives the same one
        log.warning("network failure through the static proxy", extra={"source_id": str(source_id)})

    async def rebalance(self, source_id: UUID, assignment_id: int, move: Rebalance) -> None:
        # never asked for: heartbeats never carry a move
        return None

    async def release(self, source_id: UUID, assignment_id: int) -> None:
        return None
