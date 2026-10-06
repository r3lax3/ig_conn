import asyncio
from collections import deque
from dataclasses import dataclass

from ig_connector.bus import Bus, Delivery


@dataclass(slots=True)
class _Slot:
    delivery: Delivery
    done: bool = False


class PartitionOffsets:
    """Commits each partition up to its contiguous tail of finished deliveries.

    Sources run in parallel, so a later record of a partition may finish first; its
    offset waits until every record before it has finished too.
    """

    def __init__(self, bus: Bus) -> None:
        self._bus = bus
        self._partitions: dict[int, deque[_Slot]] = {}
        self._lock = asyncio.Lock()

    def track(self, delivery: Delivery) -> None:
        """Register a delivery in read order, before it can finish."""
        self._partitions.setdefault(delivery.partition, deque()).append(_Slot(delivery))

    async def finish(self, delivery: Delivery) -> None:
        async with self._lock:
            slots = self._partitions[delivery.partition]
            for slot in slots:
                if slot.delivery is delivery:
                    slot.done = True
                    break
            else:
                raise ValueError(f"p{delivery.partition}@{delivery.offset} was never tracked")
            tail = None
            while slots and slots[0].done:
                tail = slots.popleft().delivery
            if tail is not None:
                await self._bus.commit(tail)
