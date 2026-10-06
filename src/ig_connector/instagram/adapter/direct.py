"""Instagram's Direct answers (inbox, thread pages) read into the port's types.

Read from the raw JSON, not through aiograpi's extractors: those cut message times to
whole seconds in local time (inbox positions need Instagram's microseconds, in UTC), and
one item of a shape they do not know fails the whole page. Here an unreadable thread or
item is skipped and logged. The requests are aiograpi 2.0.15's own (direct_threads_chunk,
direct_thread_chunk), with `visual_message_return_type=unseen`: reading marks nothing seen.
"""

import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from ig_connector.instagram import InboxMessage, InboxUser, ThreadPosition

__all__ = [
    "RawThread",
    "inbox_params",
    "is_after",
    "message_key",
    "parse_thread",
    "thread_params",
]

log = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def inbox_params(cursor: str | None, tracking_id: str) -> dict[str, str]:
    """aiograpi direct_threads_chunk's query: the main inbox, never the pending one."""
    params = {
        "eb_device_id": "0",
        "igd_request_log_tracking_id": tracking_id,
        "visual_message_return_type": "unseen",
        "thread_message_limit": "10",
        "persistentBadging": "true",
        "limit": "20",
        "is_prefetching": "false",
        "fetch_reason": "initial_snapshot",
        "include_old_mrs": "false",
        "no_pending_badge": "true",
        "push_disabled": "true",
    }
    if cursor:
        params.update({"cursor": cursor, "direction": "older", "fetch_reason": "page_scroll"})
    return params


def thread_params(cursor: str | None, limit: int = 20) -> dict[str, str]:
    """aiograpi direct_thread_chunk's query: one page of a thread, older than `cursor`."""
    params = {
        "visual_message_return_type": "unseen",
        "direction": "older",
        "seq_id": "40065",
        "limit": str(limit),
    }
    if cursor:
        params["cursor"] = cursor
    return params


@dataclass(frozen=True, slots=True)
class RawThread:
    thread_id: str
    users: Sequence[InboxUser]
    is_group: bool
    pending: bool
    # newest first, as Instagram sends them
    messages: Sequence[InboxMessage] = field(repr=False)
    # every message's label, by message id (history reads)
    labels: Mapping[str, str | None] = field(repr=False)
    has_older: bool = False
    oldest_cursor: str | None = None


def parse_thread(data: Any, viewer_id: str) -> RawThread | None:
    """One thread of an inbox page or a thread page; None if it cannot be read."""
    if not isinstance(data, Mapping) or not data.get("thread_id"):
        log.warning("unreadable thread skipped")
        return None
    messages: list[InboxMessage] = []
    labels: dict[str, str | None] = {}
    for raw in data.get("items") or ():
        parsed = _message(raw, viewer_id)
        if parsed is None:
            log.warning("unreadable message skipped", extra={"thread_id": str(data["thread_id"])})
            continue
        messages.append(parsed)
        # a text with a URL comes back as a `link` item, our label inside the link
        label = raw.get("client_context") or _link(raw).get("client_context")
        labels[parsed.message_id] = str(label) if label else None
    return RawThread(
        thread_id=str(data["thread_id"]),
        users=list(_users(data.get("users") or (), viewer_id)),
        is_group=bool(data.get("is_group")),
        pending=bool(data.get("pending")),
        messages=sorted(messages, key=message_key, reverse=True),
        labels=labels,
        has_older=bool(data.get("has_older")),
        oldest_cursor=str(data["oldest_cursor"]) if data.get("oldest_cursor") else None,
    )


def _users(raw: Iterable[Any], viewer_id: str) -> Iterable[InboxUser]:
    for user in raw:
        if not isinstance(user, Mapping):
            continue
        pk = user.get("pk", user.get("id"))
        if pk is None or str(pk) == viewer_id:
            continue
        yield InboxUser(str(pk), user.get("username") or None, user.get("full_name") or None)


def _message(raw: Any, viewer_id: str) -> InboxMessage | None:
    if not isinstance(raw, Mapping):
        return None
    try:
        message_id = str(raw["item_id"])
        sent_at = _EPOCH + timedelta(microseconds=int(raw["timestamp"]))
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    sender = raw.get("user_id")
    sender_id = None if sender is None or sender == "" else str(sender)
    reply = raw.get("replied_to_message")
    text = raw.get("text")
    if not isinstance(text, str):
        text = _link(raw).get("text")
    return InboxMessage(
        message_id=message_id,
        sender_id=sender_id,
        sent_at=sent_at,
        item_type=str(raw["item_type"]) if raw.get("item_type") else None,
        text=text if isinstance(text, str) else None,
        is_sent_by_viewer=bool(raw.get("is_sent_by_viewer")) or sender_id == viewer_id,
        reply_to_message_id=str(reply["item_id"])
        if isinstance(reply, Mapping) and reply.get("item_id")
        else None,
    )


def _link(raw: Mapping[str, Any]) -> Mapping[str, Any]:
    """The `link` part of a link item (aiograpi MessageLink: text, client_context); empty if none."""
    link = raw.get("link")
    return link if isinstance(link, Mapping) else {}


def message_key(message: InboxMessage | ThreadPosition) -> tuple[datetime, int, str]:
    """Order of messages in a thread: time, then the id (numeric strings: by length first)."""
    return (message.sent_at, len(message.message_id), message.message_id)


def is_after(message: InboxMessage, position: ThreadPosition | None, since: datetime) -> bool:
    if position is None:
        return message.sent_at > since
    return message_key(message) > message_key(position)
