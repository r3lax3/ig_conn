"""In-memory bus: the CRM side for behaviour tests and the port side for the connector."""

import asyncio
import json
import zlib
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, replace
from functools import partial
from typing import Any
from uuid import UUID

from ig_connector.bus import Delivery, commands_topic, events_topic
from ig_connector.contract.codec import ContractViolationError, parse_event, serialize
from ig_connector.contract.envelope import EventEnvelope, EventType
from ig_connector.contract.events import EventPayload

Listener = Callable[[str, dict[str, Any]], None]


@dataclass(frozen=True, slots=True)
class Published:
    topic: str
    key: bytes
    envelope: EventEnvelope
    payload: EventPayload


class MemoryBus:
    def __init__(self, channel_type: str, *, partitions: int = 3, listener: Listener | None = None) -> None:
        self.channel_type = channel_type
        self.commands_topic = commands_topic(channel_type)
        self.events_topic = events_topic(channel_type)
        self._partitions = partitions
        self._listener = listener
        self._log: list[Delivery] = []
        self._next_offset = [0] * partitions
        self._committed: dict[int, int] = {}
        self._published: list[Published] = []
        self._session = 0
        # by identity: a redelivered record is equal to the stale one but must not commit as it
        self._in_session: dict[int, Delivery] = {}
        self._changed = asyncio.Event()

    # CRM side

    def submit(self, command: Mapping[str, Any] | bytes, *, key: str | None = None) -> Delivery:
        """Put a command on the commands topic; the key defaults to its source_id."""
        if isinstance(command, bytes):
            value = command
        else:
            value = json.dumps(command, ensure_ascii=False).encode()
            key = key or str(command["source_id"])
        raw_key = key.encode() if key is not None else None
        partition = zlib.crc32(raw_key) % self._partitions if raw_key else 0
        delivery = Delivery(self.commands_topic, partition, self._next_offset[partition], raw_key, value)
        self._next_offset[partition] += 1
        self._log.append(delivery)
        self._emit("bus.command", {**_coordinates(delivery), "command": _readable(value)})
        self._notify()
        return delivery

    def events(
        self, event_type: EventType | None = None, *, operation_id: UUID | None = None
    ) -> list[Published]:
        return [
            event
            for event in self._published
            if (event_type is None or event.envelope.type == event_type)
            and (operation_id is None or event.envelope.operation_id == operation_id)
        ]

    def is_committed(self, delivery: Delivery) -> bool:
        return self._committed.get(delivery.partition, -1) >= delivery.offset

    async def wait_until(self, condition: Callable[[], bool], *, within: float = 2.0) -> None:
        """Wait in real time until condition() holds; re-checked on every bus change."""
        try:
            async with asyncio.timeout(within):
                await self._until(condition)
        except TimeoutError:
            seen = [f"{e.envelope.type}@{e.envelope.operation_id}" for e in self._published]
            raise AssertionError(
                f"condition not met in {within}s; published: {seen}; committed: {self._committed}"
            ) from None

    # port side

    async def deliveries(self) -> AsyncIterator[Delivery]:
        self._session += 1
        session = self._session
        start = dict(self._committed)
        self._in_session = {}
        self._notify()  # ends the previous session's iterator
        position = 0
        while True:
            await self._until(partial(self._session_news, session, position))
            if session != self._session:
                return
            record = self._log[position]
            position += 1
            if record.offset <= start.get(record.partition, -1):
                continue
            delivery = replace(record)
            self._in_session[id(delivery)] = delivery
            self._emit("bus.deliver", _coordinates(delivery))
            yield delivery

    async def publish(self, event: EventEnvelope) -> None:
        value = serialize(event)
        envelope, payload = parse_event(value)  # refuse what the CRM could not read
        if envelope.channel_type != self.channel_type:
            raise ContractViolationError(
                f"channel_type {envelope.channel_type!r} does not match topic {self.events_topic}"
            )
        published = Published(self.events_topic, str(envelope.source_id).encode(), envelope, payload)
        self._published.append(published)
        self._emit("bus.publish", {"key": published.key.decode(), "event": json.loads(value)})
        self._notify()

    async def commit(self, delivery: Delivery) -> None:
        # Kafka refuses commits from a consumer that lost its partitions; so do we
        if self._in_session.get(id(delivery)) is not delivery:
            raise ValueError(f"p{delivery.partition}@{delivery.offset} not delivered in the current session")
        if delivery.offset < self._committed.get(delivery.partition, -1):
            raise ValueError(
                f"commit going backwards in partition {delivery.partition}: "
                f"{delivery.offset} < {self._committed[delivery.partition]}"
            )
        self._committed[delivery.partition] = delivery.offset
        self._emit("bus.commit", _coordinates(delivery))
        self._notify()

    def _session_news(self, session: int, position: int) -> bool:
        return session != self._session or position < len(self._log)

    def _emit(self, kind: str, fields: dict[str, Any]) -> None:
        if self._listener is not None:
            self._listener(kind, fields)

    def _notify(self) -> None:
        self._changed.set()
        self._changed = asyncio.Event()

    async def _until(self, condition: Callable[[], bool]) -> None:
        while not condition():
            await self._changed.wait()


def _coordinates(delivery: Delivery) -> dict[str, Any]:
    return {"partition": delivery.partition, "offset": delivery.offset}


def _readable(value: bytes) -> Any:
    try:
        return json.loads(value)
    except ValueError:
        return value.decode("utf-8", errors="replace")
