"""Proxies at work (contract 8): heartbeats, planned moves, failing over a dead proxy.

The lifecycle is the ProxyProvider everyone else uses (login, restore): it remembers
every assignment it handed out and confirms each one to the service every 60 s with the
state of the Source's transport. What changes a Source's proxy runs in that Source's
executor between its commands, never next to one:

- a planned move (`pending_rebalance`): close the transport, tell the service it is
  closed, confirm the move with its job_id, bring the Session up on the new proxy;
- a confirmed network failure (the adapter's NETWORK refusal): connection-failed, then
  the Session up on the next proxy the service gives (it limits the attempts);
- an assignment the service no longer has for the Source: the Session up on a new one.

Bringing the Session up is SessionRestore's: it reserves first, and without a proxy
(409, service down) the Source is `error` + `source_offline` and does not touch the
network. Captcha, ban, flood and a revoked Session never change the proxy.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from uuid import UUID

from ig_connector.clock import Clock, within
from ig_connector.instagram import Failure, Platform
from ig_connector.masking import mask_text
from ig_connector.proxy import (
    AssignmentLost,
    Heartbeat,
    ProxyAssignment,
    ProxyProvider,
    ProxyRequirement,
    ProxyUnavailable,
    Rebalance,
    TransportState,
)
from ig_connector.runtime.login import FLOW_LIFETIME
from ig_connector.runtime.restore import SessionRestore
from ig_connector.runtime.statuses import Statuses
from ig_connector.store import Store

__all__ = ["HEARTBEAT_INTERVAL", "ProxyLifecycle", "Schedule"]

log = logging.getLogger(__name__)

# contract 8: stable after 60 s, alive while the pauses stay under 180 s
HEARTBEAT_INTERVAL = 60.0
# a call to the service; a hung one must not hold the round or its Source
CALL_TIMEOUT = 10.0

Job = Callable[[], Awaitable[None]]
# runs the job in the Source's executor after its running command; False: not running
Schedule = Callable[[UUID, Job], bool]


class ProxyLifecycle:
    def __init__(
        self,
        *,
        provider: ProxyProvider,
        platform: Platform,
        statuses: Statuses,
        store: Store,
        clock: Clock,
        interval: float = HEARTBEAT_INTERVAL,
        timeout: float = CALL_TIMEOUT,
    ) -> None:
        self._provider = provider
        # the bare adapter: closing a transport is not a platform answer about the Source
        self._platform = platform
        self._statuses = statuses
        self._store = store
        self._clock = clock
        self._interval = interval
        self._timeout = timeout
        self._held: dict[UUID, ProxyAssignment] = {}
        self._reserved_at: dict[UUID, datetime] = {}
        # set while a change of proxy is pending or running for the Source
        self._transport: dict[UUID, TransportState] = {}
        self._pending: set[UUID] = set()
        # one call to the service per Source at a time: a heartbeat in flight must not
        # reach it after the `closed` a move sent (the move would be refused)
        self._locks: dict[UUID, asyncio.Lock] = {}
        # the proxy a move could not be confirmed away from: still the Source's, and its
        # way back while the service cannot be asked for one
        self._fallback: dict[UUID, ProxyAssignment] = {}
        self._schedule: Schedule | None = None
        self._restore: SessionRestore | None = None
        self._failed_over: dict[UUID, datetime] = {}

    # -- ProxyProvider, for the login flow and the restore -------------------------------

    async def reserve(self, source_id: UUID, requirement: ProxyRequirement) -> ProxyAssignment:
        fallback = self._fallback.pop(source_id, None)
        try:
            async with self._lock(source_id):
                assignment = await self._provider.reserve(source_id, requirement)
        except ProxyUnavailable as exc:
            if fallback is None or isinstance(exc, AssignmentLost):
                raise
            log.warning(
                "proxy service unavailable: back on the proxy kept", extra={"source_id": str(source_id)}
            )
            assignment = fallback
        self._held[source_id] = assignment
        self._reserved_at[source_id] = self._clock.now()
        return assignment

    async def heartbeat(self, source_id: UUID, assignment_id: int, transport: TransportState) -> Heartbeat:
        return await self._provider.heartbeat(source_id, assignment_id, transport)

    async def connection_failed(self, source_id: UUID, assignment_id: int, reason: str) -> None:
        await self._provider.connection_failed(source_id, assignment_id, reason)

    async def rebalance(self, source_id: UUID, assignment_id: int, move: Rebalance) -> None:
        await self._provider.rebalance(source_id, assignment_id, move)

    async def release(self, source_id: UUID, assignment_id: int) -> None:
        await self._provider.release(source_id, assignment_id)
        self._forget(source_id, assignment_id)

    # -- the loop ------------------------------------------------------------------------

    async def run(self, schedule: Schedule, restore: SessionRestore | None) -> None:
        """Heartbeat every assignment every interval, until cancelled.

        `schedule` puts a job into a Source's executor; `restore` brings a Session up on
        the Source's (new) proxy. Without it a Source that loses its proxy stays offline.
        """
        self._schedule = schedule
        self._restore = restore
        while True:
            await self._clock.sleep(self._interval)
            held = list(self._held.items())
            await asyncio.gather(*(self._round(source_id, assignment) for source_id, assignment in held))

    def network_failed(self, source_id: UUID) -> None:
        """The adapter confirmed a network failure for the Source: fail its proxy over.

        Called from wherever the platform answered NETWORK; runs after the current command.
        """
        assignment = self._held.get(source_id)
        if assignment is None:
            return
        # at most once per interval: a static proxy (or a broken network on our side) gives
        # the same failure again at once, and every fail-over costs the service an attempt
        last = self._failed_over.get(source_id)
        now = self._clock.now()
        if last is not None and now - last < timedelta(seconds=self._interval):
            log.info("proxy not failed over again so soon", extra={"source_id": str(source_id)})
            return
        if self._later(source_id, lambda: self._fail_over(source_id, assignment)):
            self._failed_over[source_id] = now

    async def _round(self, source_id: UUID, assignment: ProxyAssignment) -> None:
        try:
            if await self._abandoned(source_id, assignment):
                return
            await self._beat(source_id, assignment)
        except Exception:
            log.exception("proxy heartbeat failed", extra={"source_id": str(source_id)})

    async def _abandoned(self, source_id: UUID, assignment: ProxyAssignment) -> bool:
        """Released if no working Source uses it: its login flow is over, failed or expired,
        or its Source was switched off (its Account moved to another Source)."""
        reserved_at = self._reserved_at.get(source_id)
        if reserved_at is None or self._clock.now() - reserved_at <= FLOW_LIFETIME:
            return False
        source = await self._store.source(source_id)
        if source is not None and not source.disabled:
            return False
        ids = {"source_id": str(source_id), "assignment_id": assignment.assignment_id}
        try:
            async with self._lock(source_id):
                await self._call(self._provider.release(source_id, assignment.assignment_id))
            log.info("proxy released: no working Source uses it", extra=ids)
        except (ProxyUnavailable, TimeoutError) as exc:
            if not isinstance(exc, AssignmentLost):
                # tried again next round
                log.warning("proxy not released (%s)", type(exc).__name__, extra=ids)
                return True
        self._forget(source_id, assignment.assignment_id)
        return True

    async def _beat(self, source_id: UUID, assignment: ProxyAssignment) -> None:
        ids = {"source_id": str(source_id), "assignment_id": assignment.assignment_id}
        try:
            async with self._lock(source_id):
                if self._held.get(source_id) != assignment:
                    return
                transport = self._transport.get(source_id) or (
                    TransportState.CONNECTED
                    if self._statuses.has_session(source_id)
                    else TransportState.UNKNOWN
                )
                beat = await self._call(
                    self._provider.heartbeat(source_id, assignment.assignment_id, transport)
                )
        except AssignmentLost as exc:
            log.warning("proxy assignment lost (%s)", mask_text(str(exc)), extra=ids)
            if self._held.get(source_id) == assignment:
                self._later(source_id, lambda: self._replace(source_id, assignment))
            return
        except (ProxyUnavailable, TimeoutError) as exc:
            # the proxy itself may work on: a service outage alone changes nothing for the Source
            log.warning("proxy heartbeat failed (%s)", type(exc).__name__, extra=ids)
            return
        move = beat.pending_rebalance
        if move is not None and source_id not in self._pending and self._held.get(source_id) == assignment:
            log.info("proxy move asked for", extra={**ids, "job_id": str(move.job_id)})
            if self._later(source_id, lambda: self._move(source_id, assignment, move)):
                # the running command finishes first
                self._transport[source_id] = TransportState.DRAINING

    def _later(self, source_id: UUID, job: Job) -> bool:
        if source_id in self._pending:
            return False
        if self._schedule is None:
            log.warning("proxy change not scheduled: not running", extra={"source_id": str(source_id)})
            return False

        async def once() -> None:
            try:
                await job()
            finally:
                self._pending.discard(source_id)
                self._transport.pop(source_id, None)

        self._pending.add(source_id)
        if not self._schedule(source_id, once):
            self._pending.discard(source_id)
            return False
        return True

    async def _move(self, source_id: UUID, assignment: ProxyAssignment, move: Rebalance) -> None:
        ids = {
            "source_id": str(source_id),
            "assignment_id": assignment.assignment_id,
            "job_id": str(move.job_id),
        }
        if self._held.get(source_id) != assignment:
            return

        async def confirm() -> None:
            self._transport[source_id] = TransportState.CLOSED
            try:
                async with self._lock(source_id):
                    # the service accepts the move only once it was told the transport is closed
                    closed = TransportState.CLOSED
                    await self._call(self._provider.heartbeat(source_id, assignment.assignment_id, closed))
                    await self._call(self._provider.rebalance(source_id, assignment.assignment_id, move))
                log.info("proxy move confirmed", extra=ids)
            except AssignmentLost as exc:
                # refused: reserve says which proxy the Source has now
                log.warning("proxy move refused (%s)", mask_text(str(exc)), extra=ids)
            except (ProxyUnavailable, TimeoutError) as exc:
                # not moved as far as we know: the old proxy still works for it
                log.warning("proxy move not confirmed (%s)", type(exc).__name__, extra=ids)
                self._fallback[source_id] = assignment

        await self._change(source_id, assignment, confirm)

    async def _fail_over(self, source_id: UUID, assignment: ProxyAssignment) -> None:
        ids = {"source_id": str(source_id), "assignment_id": assignment.assignment_id}
        if self._held.get(source_id) != assignment:
            return

        async def report() -> None:
            try:
                reason = "network failure through the proxy"
                async with self._lock(source_id):
                    await self._call(
                        self._provider.connection_failed(source_id, assignment.assignment_id, reason)
                    )
                log.info("proxy connection failure reported", extra=ids)
            except (ProxyUnavailable, TimeoutError) as exc:
                log.warning("proxy connection failure not reported (%s)", type(exc).__name__, extra=ids)

        await self._change(source_id, assignment, report)

    async def _replace(self, source_id: UUID, assignment: ProxyAssignment) -> None:
        if self._held.get(source_id) != assignment:
            return

        async def nothing() -> None:
            return None

        await self._change(source_id, assignment, nothing)

    async def _change(
        self, source_id: UUID, assignment: ProxyAssignment, between: Callable[[], Awaitable[None]]
    ) -> None:
        """Close the transport, tell the service, and always bring the Session up again."""
        self._statuses.session_closed(source_id)
        try:
            await self._platform.disconnect(source_id)
            await between()
        except Exception:
            log.exception("proxy change failed midway", extra={"source_id": str(source_id)})
        finally:
            self._forget(source_id, assignment.assignment_id)
            await self._reopen(source_id)

    async def _reopen(self, source_id: UUID) -> None:
        if self._restore is None:
            await self._statuses.failed(source_id, Failure.NETWORK)
            return
        # reserves (the new proxy, or 409 = offline without touching the network), restores, checks
        await self._restore.restore(source_id)

    async def _call[T](self, call: Awaitable[T]) -> T:
        return await within(self._clock, self._timeout, call)

    def _lock(self, source_id: UUID) -> asyncio.Lock:
        return self._locks.setdefault(source_id, asyncio.Lock())

    def _forget(self, source_id: UUID, assignment_id: int) -> None:
        held = self._held.get(source_id)
        if held is not None and held.assignment_id == assignment_id:
            del self._held[source_id]
            self._reserved_at.pop(source_id, None)
