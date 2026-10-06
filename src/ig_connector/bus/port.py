"""The bus as the runtime sees it, and the topic names a channel_type gives (contract 2)."""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol

from ig_connector.contract.envelope import EventEnvelope


def commands_topic(channel_type: str) -> str:
    return f"crm.connector.commands.{channel_type}"


def events_topic(channel_type: str) -> str:
    return f"crm.connector.events.{channel_type}"


@dataclass(frozen=True, slots=True)
class Delivery:
    """One command record as read from the commands topic, not yet parsed."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes


class Bus(Protocol):
    def deliveries(self) -> AsyncIterator[Delivery]:
        """Records in partition order, starting after the last committed offset.

        A new iterator starts a new consumer session on its first step (like a restart):
        the previous iterator ends and uncommitted records are delivered again.
        """
        ...

    async def publish(self, event: EventEnvelope) -> None:
        """Publish to the events topic keyed by source_id; returns once the bus has it."""
        ...

    async def commit(self, delivery: Delivery) -> None:
        """Mark this delivery and everything before it in its partition as done.

        Committing past a record that has no terminal answer yet is the caller's bug:
        the bus does not track gaps.
        """
        ...
