"""Bringing Sources up without a login: after a restart, and again while they are in error.

Per Source, in its own executor before any of its commands: the proxy assignment
(the same one, reserve is idempotent), the saved Session made live again, then one real
request to prove it works. Only that answer makes the Source active; a refusal
sets the status it means. A Source that needs a reconnect is left alone: its Session is
gone, only a new login helps.
"""

import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from uuid import UUID

from ig_connector.clock import Clock, within
from ig_connector.contract.errors import StatusErrorCode
from ig_connector.instagram import Failure, Platform, PlatformError
from ig_connector.masking import mask_text
from ig_connector.proxy import ProxyProvider, ProxyUnavailable
from ig_connector.runtime.statuses import ACTIVE, Statuses
from ig_connector.store import Store

log = logging.getLogger(__name__)

# each of restoring and checking; a hung Session must not hold its Source's commands long
RESTORE_TIMEOUT = 30.0
# a restricted Account is not asked again sooner: restrictions last hours, and reads may
# work all along while sending does not (the status would flap)
FLOOD_RECHECK = timedelta(minutes=15)


class SessionRestore:
    def __init__(
        self,
        *,
        store: Store,
        clock: Clock,
        platform: Platform,
        proxies: ProxyProvider,
        statuses: Statuses,
        timeout: float = RESTORE_TIMEOUT,
        # a NETWORK refusal through the proxy: its lifecycle fails the proxy over
        on_network_failure: Callable[[UUID], None] | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._platform = platform
        self._proxies = proxies
        self._statuses = statuses
        self._timeout = timeout
        self._on_network_failure = on_network_failure
        self._rechecked: dict[UUID, datetime] = {}

    async def restore(self, source_id: UUID) -> None:
        """Make the Source's saved Session live and confirm it; its status follows the answer.

        Anything unexpected (store, decryption, an unmapped adapter error) leaves it offline
        with `error` + `source_offline`: CRM may still show the `active` of the last process.
        """
        ids = {"source_id": str(source_id)}
        try:
            confirmed = await self._restore(source_id, live=False)
        except Exception:
            log.exception("source not restored: unexpected error", extra=ids)
            await self._statuses.failed(source_id, Failure.NETWORK)
            return
        if confirmed:
            await self._confirmed(source_id)

    async def recheck(self, source_id: UUID) -> None:
        """For a Source that is not active: try to bring it back, unless that is pointless now.

        A Session this process already has is only checked, never reloaded over itself.
        """
        now = self._clock.now()
        known = self._statuses.known(source_id)
        if known is not None:
            status, since = known
            if status.status == "needs_reconnect":
                return
            if status.error_code is StatusErrorCode.PEER_FLOOD:
                last = max(since, self._rechecked.get(source_id, since))
                if now - last < FLOOD_RECHECK:
                    return
        self._rechecked[source_id] = now
        try:
            confirmed = await self._restore(source_id, live=self._statuses.has_session(source_id))
        except Exception:
            log.exception("source not rechecked: unexpected error", extra={"source_id": str(source_id)})
            await self._statuses.failed(source_id, Failure.NETWORK)
            return
        if confirmed:
            await self._confirmed(source_id)

    async def _restore(self, source_id: UUID, *, live: bool) -> bool:
        """True once the Session answered; a refusal is reported as the status it means."""
        ids = {"source_id": str(source_id)}
        source = await self._store.source(source_id)
        if source is not None and source.disabled:
            log.debug("source not restored: switched off", extra=ids)
            return False
        saved = await self._store.session(source_id)
        if source is None or saved is None:
            raise LookupError("a connected Source without its Session")
        if source.status is not None and source.status.status == "needs_reconnect":
            log.info("source not restored: it needs a reconnect", extra=ids)
            return False
        try:
            if not live:
                proxy = await self._proxies.reserve(source_id, source.proxy)
                restoring = self._platform.restore(source_id, saved.session, saved.device, proxy)
                await within(self._clock, self._timeout, restoring)
            await within(self._clock, self._timeout, self._platform.check(source_id))
        except ProxyUnavailable as exc:
            # never directly: without a proxy the Source stays offline
            log.warning("source not restored: no proxy (%s)", mask_text(str(exc)), extra=ids)
            await self._statuses.failed(source_id, Failure.NETWORK)
            return False
        except TimeoutError:
            log.warning("source not restored: timed out", extra=ids)
            await self._statuses.failed(source_id, Failure.NETWORK)
            return False
        except PlatformError as exc:
            if await self._moved_away(source_id):
                return False
            log.warning(
                "source not restored: %s (%s)",
                exc.failure,
                mask_text(exc.detail),
                extra={**ids, "error_code": exc.failure.value},
            )
            # no answer leaves the Source as offline as a timeout does, without blaming the proxy
            failure = Failure.NETWORK if exc.failure is Failure.NO_ANSWER else exc.failure
            await self._statuses.failed(source_id, failure)
            if exc.failure is Failure.NETWORK and self._on_network_failure is not None:
                self._on_network_failure(source_id)
            return False
        return not await self._moved_away(source_id)

    async def _moved_away(self, source_id: UUID) -> bool:
        """Whether the Account moved to another Source while this one was being brought up.

        The move drops the Session it closes; one restored meanwhile is dropped here.
        """
        source = await self._store.source(source_id)
        if source is None or not source.disabled:
            return False
        log.info("source switched off while being restored", extra={"source_id": str(source_id)})
        await self._platform.disconnect(source_id)
        return True

    async def _confirmed(self, source_id: UUID) -> None:
        ids = {"source_id": str(source_id)}
        log.info("source confirmed live", extra=ids)
        try:
            await self._statuses.report(source_id, ACTIVE)
        except Exception:
            # not taken as active then: the next recheck reports it again
            log.exception("active status not reported", extra=ids)
