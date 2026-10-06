"""Healthcheck: the service touches a file while it works; a separate process checks its age.

No port to open and nothing to install in the image: docker compose runs
`ig-connector health`, which exits 0 when the file is fresh and 1 otherwise. The file is
touched only while the event loop runs and every probe passes (the database answers, the
Kafka consumer holds its partitions), so a hung loop, a lost database or a consumer
without partitions all turn the service unhealthy within STALE_AFTER seconds.
"""

import asyncio
import logging
import tempfile
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from pathlib import Path

from ig_connector.clock import Clock

log = logging.getLogger(__name__)

DEFAULT_FILE = Path(tempfile.gettempdir()) / "ig-connector.health"
BEAT_EVERY = 10.0
# while unhealthy (starting up, broker away) look again sooner
RETRY_EVERY = 1.0
STALE_AFTER = 30.0
_PROBE_TIMEOUT = 5.0

Probe = Callable[[], Awaitable[bool]]


def is_healthy(path: Path, *, now: float | None = None) -> bool:
    try:
        touched = path.stat().st_mtime
    except OSError:
        return False
    return (time.time() if now is None else now) - touched < STALE_AFTER


async def beat(path: Path, probes: Sequence[Probe], clock: Clock) -> None:
    """Touch path every BEAT_EVERY seconds while all probes pass; runs until cancelled."""
    healthy: bool | None = None
    try:
        while True:
            ok = await _all_pass(probes)
            if ok:
                path.touch()  # noqa: ASYNC240 - a tiny local file, not worth a thread
            if ok != healthy:
                (log.info if ok else log.warning)("service healthy" if ok else "service unhealthy")
                healthy = ok
            await clock.sleep(BEAT_EVERY if ok else RETRY_EVERY)
    finally:
        with suppress(OSError):
            path.unlink()  # noqa: ASYNC240


async def _all_pass(probes: Sequence[Probe]) -> bool:
    for probe in probes:
        try:
            async with asyncio.timeout(_PROBE_TIMEOUT):
                if not await probe():
                    return False
        except Exception as exc:
            # the type only: messages of driver errors may carry a DSN
            log.warning(
                "health probe failed",
                extra={"probe": getattr(probe, "__name__", ""), "error": type(exc).__name__},
            )
            return False
    return True
