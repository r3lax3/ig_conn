"""A Clock whose time moves only when the test says so."""

import asyncio
import heapq
import itertools
from datetime import datetime, timedelta


class FakeClock:
    """Time moves only on advance(); sleepers wake in deadline order as it passes them."""

    def __init__(self, start: datetime) -> None:
        self._now = start
        self._sleepers: list[tuple[datetime, int, asyncio.Future[None]]] = []
        self._order = itertools.count()

    def now(self) -> datetime:
        return self._now

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:  # like asyncio.sleep: yield, never wait for advance()
            await asyncio.sleep(0)
            return
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        deadline = self._now + timedelta(seconds=seconds)
        heapq.heappush(self._sleepers, (deadline, next(self._order), future))
        await future

    async def advance(self, seconds: float) -> None:
        """Move time forward, letting each woken task run before the next deadline.

        A woken task gets a few loop turns to go back to sleep. One that awaits real I/O
        first (a database, say) may miss deadlines inside this window: advance in steps.
        """
        target = self._now + timedelta(seconds=seconds)
        while self._sleepers and self._sleepers[0][0] <= target:
            deadline, _, future = heapq.heappop(self._sleepers)
            if future.done():  # the sleeper was cancelled
                continue
            self._now = deadline
            future.set_result(None)
            await _settle()
        self._now = target
        await _settle()


async def _settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)
