import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from ig_connector.clock import Clock, SystemClock, within
from tests.support.fake_clock import FakeClock

START = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


async def test_fake_clock_stands_still_until_advanced() -> None:
    clock = FakeClock(START)
    assert clock.now() == START
    await clock.advance(90)
    assert clock.now() == START + timedelta(seconds=90)


async def test_sleepers_wake_in_deadline_order_when_time_reaches_them() -> None:
    clock = FakeClock(START)
    woke: list[str] = []

    async def sleeper(name: str, seconds: float) -> None:
        await clock.sleep(seconds)
        woke.append(name)

    tasks = [asyncio.create_task(sleeper("heartbeat", 60)), asyncio.create_task(sleeper("poll", 5))]
    await asyncio.sleep(0)

    await clock.advance(4)
    assert woke == []
    await clock.advance(1)
    assert woke == ["poll"]
    await clock.advance(100)
    assert woke == ["poll", "heartbeat"]
    await asyncio.gather(*tasks)


async def test_a_sleeper_sees_the_time_it_woke_at() -> None:
    clock = FakeClock(START)
    seen: list[datetime] = []

    async def poll_twice() -> None:
        for _ in range(2):
            await clock.sleep(10)
            seen.append(clock.now())

    task = asyncio.create_task(poll_twice())
    await asyncio.sleep(0)
    await clock.advance(10)
    await clock.advance(10)
    await task
    assert seen == [START + timedelta(seconds=10), START + timedelta(seconds=20)]


async def test_cancelled_sleeper_does_not_break_the_clock() -> None:
    clock = FakeClock(START)
    task = asyncio.create_task(clock.sleep(5))
    await asyncio.sleep(0)
    task.cancel()
    await clock.advance(10)
    assert task.cancelled()


async def test_system_clock_is_aware_utc() -> None:
    clock: Clock = SystemClock()
    assert clock.now().tzinfo is UTC
    await clock.sleep(0)


async def test_zero_or_negative_sleep_does_not_wait_for_advance() -> None:
    clock = FakeClock(START)
    async with asyncio.timeout(1):
        await clock.sleep(0)
        await clock.sleep(-5)
    assert clock.now() == START


async def test_within_gives_up_when_clock_time_runs_out() -> None:
    clock = FakeClock(START)
    never = asyncio.Event()
    attempt = asyncio.create_task(within(clock, 10, never.wait()))
    await _settle()
    await clock.advance(9)
    assert not attempt.done()

    await clock.advance(1)

    with pytest.raises(TimeoutError):
        await attempt


async def test_within_returns_the_result_in_time() -> None:
    clock = FakeClock(START)

    async def answer() -> int:
        await clock.sleep(3)
        return 42

    attempt = asyncio.create_task(within(clock, 10, answer()))
    await _settle()
    await clock.advance(3)

    assert await attempt == 42
    assert clock.now() == START + timedelta(seconds=3)


async def test_within_passes_errors_through() -> None:
    async def broken() -> None:
        raise ValueError("bad")

    with pytest.raises(ValueError, match="bad"):
        await within(FakeClock(START), 10, broken())


async def _settle() -> None:
    # within() starts its timer and the work as tasks: give them turns to reach the clock
    for _ in range(5):
        await asyncio.sleep(0)
