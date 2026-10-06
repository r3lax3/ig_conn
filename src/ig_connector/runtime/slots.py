"""The one limit on Sources talking to Instagram at once, with commands served first."""

import asyncio
import heapq
import itertools
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class Slots:
    """At most `size` holders; a freed slot goes to a waiting command before other work.

    Commands have CRM's timers running (resolve gets 10 s), polls and restores do not.
    Among equals the first to wait is served first.
    """

    def __init__(self, size: int) -> None:
        if size < 1:
            raise ValueError("at least one slot")
        self._free = size
        self._waiting: list[tuple[int, int, asyncio.Future[None]]] = []
        self._order = itertools.count()

    @asynccontextmanager
    async def hold(self, *, command: bool) -> AsyncIterator[None]:
        await self._acquire(command=command)
        try:
            yield
        finally:
            self._release()

    async def _acquire(self, *, command: bool) -> None:
        if self._free > 0 and not self._waiting:
            self._free -= 1
            return
        turn: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiting, (0 if command else 1, next(self._order), turn))
        try:
            await turn
        except asyncio.CancelledError:
            if turn.done() and not turn.cancelled():
                # handed the slot just as the waiter was cancelled: pass it on
                self._release()
            raise

    def _release(self) -> None:
        while self._waiting:
            _, _, turn = heapq.heappop(self._waiting)
            if not turn.done():
                turn.set_result(None)
                return
        self._free += 1
