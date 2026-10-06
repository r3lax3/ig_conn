import asyncio

from ig_connector.runtime.slots import Slots


async def _hold(slots: Slots, name: str, order: list[str], *, command: bool, release: asyncio.Event) -> None:
    async with slots.hold(command=command):
        order.append(name)
        await release.wait()


async def test_a_freed_slot_goes_to_a_command_before_earlier_other_work() -> None:
    slots = Slots(1)
    order: list[str] = []
    go = asyncio.Event()
    first = asyncio.create_task(_hold(slots, "first", order, command=True, release=go))
    await asyncio.sleep(0)
    poll = asyncio.create_task(_hold(slots, "poll", order, command=False, release=go))
    await asyncio.sleep(0)
    cmd = asyncio.create_task(_hold(slots, "command", order, command=True, release=go))
    await asyncio.sleep(0)

    go.set()
    await asyncio.gather(first, poll, cmd)

    assert order == ["first", "command", "poll"]


async def test_a_waiter_cancelled_while_handed_the_slot_passes_it_on() -> None:
    slots = Slots(1)
    order: list[str] = []
    go = asyncio.Event()
    holding = slots.hold(command=True)
    await holding.__aenter__()
    cancelled = asyncio.create_task(_hold(slots, "cancelled", order, command=True, release=go))
    later = asyncio.create_task(_hold(slots, "later", order, command=False, release=go))
    await asyncio.sleep(0)

    await holding.__aexit__(None, None, None)  # hands the slot to `cancelled`, not run yet
    cancelled.cancel()
    go.set()
    await asyncio.wait_for(later, timeout=1)

    assert order == ["later"]
