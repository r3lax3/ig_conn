"""event.status: published only on a real change, the last one kept with the Source.

The saved status is what CRM was told; it survives a restart. What this process knows
for itself lives in memory: a Source counts as active only once this process has a live
Session for it, confirmed by a login or by the liveness check after a restart, never
because the saved status says so (contract 6.2: `active` is a claim about a live account).
"""

import asyncio
import logging
from datetime import datetime
from uuid import UUID, uuid4

from ig_connector.bus import Bus
from ig_connector.clock import Clock
from ig_connector.contract.codec import build_event
from ig_connector.contract.envelope import CONTRACT_VERSION
from ig_connector.contract.errors import StatusErrorCode
from ig_connector.contract.events import StatusPayload
from ig_connector.instagram import Failure
from ig_connector.runtime.refusals import status_for
from ig_connector.runtime.sources import SourceCondition, SourceState
from ig_connector.store import Source, SourceStatus, Store

log = logging.getLogger(__name__)

ACTIVE = StatusPayload(status="active")


class Statuses:
    """Reports statuses, and is the SourceStates the runtime gates commands and polls by."""

    def __init__(self, *, bus: Bus, store: Store, clock: Clock) -> None:
        self._bus = bus
        self._store = store
        self._clock = clock
        # confirmed by this process; a Source missing here has no live Session yet
        self._known: dict[UUID, SourceStatus] = {}
        self._since: dict[UUID, datetime] = {}
        self._locks: dict[UUID, asyncio.Lock] = {}
        # confirmed active at some point: the adapter holds a live Session for them
        self._live: set[UUID] = set()
        # published by this process; ahead of the saved one when a save failed
        self._published: dict[UUID, SourceStatus] = {}

    async def report(self, source_id: UUID, payload: StatusPayload) -> bool:
        """Publish the status if it differs from the last one published; True if it went out.

        Saved after publishing: a crash in between publishes it once more after a restart,
        never leaves CRM without it. The last one published is also kept in memory: a save
        that failed must not make the next change look like no change (it is saved again
        then). A status that did not get out is not taken as known, so it is reported
        again on the next confirmation instead of being lost.
        """
        async with self._locks.setdefault(source_id, asyncio.Lock()):
            source = await self._store.source(source_id)
            if source is None:
                raise LookupError(f"no Source {source_id}")
            if source.disabled and payload.status != "disabled":
                # a check that was already running when its Account moved away
                log.info("status of a switched-off source not reported", extra={"source_id": str(source_id)})
                return False
            status = SourceStatus(payload.status, payload.error_code)
            publish = self._published.get(source_id, source.status) != status
            # shielded: a caller cancelled mid-way (a poll interrupted by a command) still
            # leaves the status out, known and saved
            applying = self._apply(source, payload, status, publish=publish, save=source.status != status)
            await asyncio.shield(applying)
            return publish

    async def _apply(
        self, source: Source, payload: StatusPayload, status: SourceStatus, *, publish: bool, save: bool
    ) -> None:
        source_id = source.source_id
        if publish:
            # not a reply to a command: its own operation_id and our contract version (contract 4)
            await self._bus.publish(
                build_event(
                    payload=payload,
                    operation_id=uuid4(),
                    source_id=source_id,
                    channel_type=source.channel_type,
                    contract_version=CONTRACT_VERSION,
                    occurred_at=self._clock.now(),
                )
            )
            self._published[source_id] = status
            log.info(
                "source status published",
                extra={"source_id": str(source_id), "status": status.status, "error_code": status.error_code},
            )
        if self._known.get(source_id) != status:
            self._known[source_id] = status
            self._since[source_id] = self._clock.now()
        if status.status == "active":
            self._live.add(source_id)
        if save:
            await self._store.save_status(source_id, status, now=self._clock.now())

    async def failed(self, source_id: UUID, failure: Failure) -> None:
        """A platform failure happened for the Source: change its status if the failure says so.

        The one entry point for every part that talks to the platform or the proxy for a
        Source (polls, commands, liveness checks, the proxy lifecycle). Never raises: a
        status that could not be reported is logged.
        """
        payload = status_for(failure)
        if payload is None:
            return
        try:
            await self.report(source_id, payload)
        except Exception:
            log.exception("source status not reported", extra={"source_id": str(source_id)})

    async def succeeded(self, source_id: UUID) -> None:
        """The platform answered for the Source: an `error` it was in is over (back to active).

        `needs_reconnect` is left as it is: only a new login ends it.
        """
        known = self._known.get(source_id)
        if known is None or known.status != "error":
            return
        try:
            await self.report(source_id, ACTIVE)
        except Exception:
            log.exception("source status not reported", extra={"source_id": str(source_id)})

    def has_session(self, source_id: UUID) -> bool:
        """This process brought the Source's Session up (login or restore) and was not told it is gone."""
        known = self._known.get(source_id)
        return source_id in self._live and not (known is not None and known.status == "needs_reconnect")

    def session_closed(self, source_id: UUID) -> None:
        """The adapter dropped the Source's live Session (its proxy changes).

        Until a restore brings it up again, a recheck must restore it (reserving a proxy
        first), never just check a Session that is not there.
        """
        self._live.discard(source_id)

    def known(self, source_id: UUID) -> tuple[SourceStatus, datetime] | None:
        """The status this process confirmed for the Source and since when; None before it did."""
        status = self._known.get(source_id)
        return None if status is None else (status, self._since[source_id])

    async def state(self, source_id: UUID) -> SourceState:
        status = self._known.get(source_id)
        if status is None:
            # not confirmed by this process yet; a switched-off Source never will be
            source = await self._store.source(source_id)
            if source is not None and source.disabled:
                return SourceState(source_id, SourceCondition.DISABLED)
        elif status.status == "disabled":
            return SourceState(source_id, SourceCondition.DISABLED)
        # a Session closed for a proxy change is not active until it is up again
        if status is not None and status.status == "active" and source_id in self._live:
            return SourceState(source_id, SourceCondition.ACTIVE)
        if status is not None and status.error_code is StatusErrorCode.PEER_FLOOD:
            return SourceState(source_id, SourceCondition.LIMITED)
        return SourceState(source_id, SourceCondition.OFFLINE)
