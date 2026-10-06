"""Inbound messages: a Source's inbox is read between its commands and published.

The Source's executor calls `poll` when the interval has passed (never at the same
time as a command of that Source; a command arriving interrupts the poll). A message is
published, then its thread's position moves past it: a crash or an interruption in
between publishes it once more, as the same event (CRM deduplicates), never loses it.

A personal dialog is addressed by the Instagram id of the other person: external_chat_id
= external_user_id (contract 4, platform identifiers). The thread id stays inside. Group threads and
pending requests are not supported in stage 1 and are skipped.
"""

import logging
import random
from datetime import UTC
from uuid import UUID, uuid5

from ig_connector.bus import Bus
from ig_connector.clock import Clock, within
from ig_connector.contract.codec import build_event
from ig_connector.contract.envelope import CONTRACT_VERSION, EventEnvelope
from ig_connector.contract.events import InboundMessagePayload
from ig_connector.instagram import InboundSource, InboxMessage, InboxThread, PlatformError, ThreadPosition
from ig_connector.masking import mask_text
from ig_connector.store import Source, Store

log = logging.getLogger(__name__)

# a hung inbox must not stop the Source's polling for long (a command interrupts a poll anyway)
POLL_TIMEOUT = 30.0

# event operation_ids derived from the message: a repeat after a crash is the same event
_EVENT_IDS = UUID("5b0f2a7e-3c1d-4e8a-9f6b-1a2b3c4d5e6f")


class InboundPolling:
    def __init__(
        self,
        *,
        bus: Bus,
        store: Store,
        clock: Clock,
        source: InboundSource,
        interval: float,
        timeout: float = POLL_TIMEOUT,
        stagger: bool = True,
    ) -> None:
        if interval <= 0:
            raise ValueError("the poll interval must be positive")
        self.interval = interval
        self._bus = bus
        self._store = store
        self._clock = clock
        self._source = source
        self._timeout = timeout
        self._stagger = stagger

    def first_delay(self) -> float:
        """Seconds before a Source's first poll: spread, so that Sources do not poll in step."""
        return random.uniform(0, self.interval) if self._stagger else 0.0  # noqa: S311  not crypto

    async def poll(self, source_id: UUID) -> None:
        """Publish the Source's new inbound messages; refusals are logged, tried next interval."""
        ids = {"source_id": str(source_id)}
        source = await self._store.source(source_id)
        positions = await self._store.inbox_positions(source_id)
        if source is None or positions is None:
            return
        try:
            fetching = self._source.new_messages(source_id, positions)
            threads = await within(self._clock, self._timeout, fetching)
        except TimeoutError:
            log.warning("inbox poll timed out", extra=ids)
            return
        except PlatformError as exc:
            # the platform wrapper turns refusals into event.status; here the poll is just tried again
            log.info(
                "inbox poll refused: %s (%s)",
                exc.failure,
                mask_text(exc.detail),
                extra={**ids, "error_code": exc.failure.value},
            )
            return
        for thread in threads:
            await self._thread(source, thread)

    async def _thread(self, source: Source, thread: InboxThread) -> None:
        if thread.pending:
            # a message request: accepting it is stage 2; it comes once it is in the main inbox
            return
        for message in thread.messages:
            payload = None if thread.is_group else _payload(source, thread, message)
            if payload is not None:
                event = _event(source, message, payload)
                if event is not None:
                    await self._bus.publish(event)
                    log.info(
                        "inbound message published",
                        extra={
                            "source_id": str(source.source_id),
                            "external_message_id": payload.external_message_id,
                            "kind": payload.kind,
                            "text": payload.text,
                        },
                    )
            # skipped ones move the position too: they are never asked for again
            sent_at = message.sent_at if message.sent_at.tzinfo else message.sent_at.replace(tzinfo=UTC)
            await self._store.move_inbox_position(
                source.source_id,
                thread.thread_id,
                ThreadPosition(message.message_id, sent_at),
                now=self._clock.now(),
            )


def _event(source: Source, message: InboxMessage, payload: InboundMessagePayload) -> EventEnvelope | None:
    try:
        return build_event(
            payload=payload,
            operation_id=uuid5(_EVENT_IDS, f"{source.source_id}/{message.message_id}"),
            source_id=source.source_id,
            channel_type=source.channel_type,
            contract_version=CONTRACT_VERSION,
            occurred_at=message.sent_at,
        )
    except ValueError:
        # pydantic's ValidationError is a ValueError. Waiting for a message CRM cannot be told
        # about would hold back everything after it, forever: skip it, loudly
        log.error(
            "inbound message skipped: it cannot be published",
            extra={"source_id": str(source.source_id), "external_message_id": message.message_id},
        )
        return None


def _payload(source: Source, thread: InboxThread, message: InboxMessage) -> InboundMessagePayload | None:
    """The event payload for a message of a personal thread; None for the Account's own ones."""
    if message.is_sent_by_viewer or message.sender_id == source.account.external_id:
        return None
    if len(thread.users) != 1:
        return None
    [peer] = thread.users
    # TODO(contract 2.2): direction, sent_at, and a status per media item
    # a link is a text with a URL in it
    is_text = message.item_type in ("text", "link") and bool(message.text)
    return InboundMessagePayload(
        external_chat_id=peer.user_id,
        external_message_id=message.message_id,
        external_user_id=message.sender_id or peer.user_id,
        user_display_name=peer.full_name or peer.username,
        text=(message.text or "") if is_text else "",
        kind="text" if is_text else "unsupported",
        reply_to_external_id=message.reply_to_message_id,
    )
