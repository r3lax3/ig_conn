import asyncio
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


async def within[T](clock: Clock, seconds: float, awaitable: Awaitable[T]) -> T:
    """The awaitable's result, or TimeoutError once `seconds` of clock time pass (it is cancelled).

    Like asyncio.timeout, but on the injected Clock, so tests drive deadlines with FakeClock.
    """
    work = asyncio.ensure_future(awaitable)
    timer = asyncio.ensure_future(clock.sleep(seconds))
    try:
        done, _ = await asyncio.wait((work, timer), return_when=asyncio.FIRST_COMPLETED)
    finally:
        timer.cancel()
        if not work.done():
            work.cancel()
            await asyncio.wait((work,))
    if work not in done:
        raise TimeoutError(f"no answer within {seconds} s")
    return work.result()
