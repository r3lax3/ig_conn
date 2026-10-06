"""Platform port: everything the connector does with Instagram.

Adapters translate every aiograpi and browser exception at the boundary into
PlatformError with a Failure from the closed list below; nothing else leaves them except
bugs. This module holds login, sending text and the inbound source.
"""

import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from pydantic import SecretStr

from ig_connector.proxy import ProxyAssignment

__all__ = [
    "Account",
    "Device",
    "DialogMessage",
    "Failure",
    "InboundSource",
    "InboxMessage",
    "InboxPositions",
    "InboxThread",
    "InboxUser",
    "LoggedIn",
    "LoginRequest",
    "OutgoingText",
    "Platform",
    "PlatformError",
    "SentMessage",
    "SessionData",
    "ThreadPosition",
    "UnsupportedProxy",
    "User",
    "new_client_context",
]


class Failure(StrEnum):
    # wrong login, password or 2FA code
    BAD_CREDENTIALS = "bad_credentials"
    SESSION_REVOKED = "session_revoked"
    # Instagram wants a check we cannot pass: SMS, email, checkpoint, captcha, 2FA without a secret
    CHALLENGE = "challenge"
    FLOOD = "flood"
    RATE_LIMITED = "rate_limited"
    REJECTED = "rejected"
    NOT_FOUND = "not_found"
    # the proxy or the connection to Instagram failed, before anything reached Instagram
    NETWORK = "network"
    # a read asked and got no answer (timed out, broken off): not proven to be the proxy
    NO_ANSWER = "no_answer"
    UNKNOWN_AFTER_SEND = "unknown_after_send"


class PlatformError(Exception):
    """A refusal of the platform, typed. `detail` is safe to log and to send to CRM."""

    def __init__(self, failure: Failure, detail: str = "") -> None:
        super().__init__(f"{failure}: {detail}" if detail else str(failure))
        self.failure = failure
        self.detail = detail


class UnsupportedProxy(PlatformError):
    """The login cannot go through this kind of proxy (Chromium: no SOCKS5 with credentials).

    Nothing reached Instagram; another proxy of the same kind would not help either.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(Failure.REJECTED, detail)


@dataclass(frozen=True, slots=True)
class Device:
    """The Source's "phone": browser profile and aiograpi device settings, opaque outside the adapter.

    Created once on the first login and reused on every later one: a new device on each
    attempt provokes checks. Stored encrypted with the Session.
    """

    data: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class SessionData:
    """A logged-in aiograpi Session, opaque outside the adapter; a credential, stored encrypted."""

    data: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class Account:
    # Instagram user id (pk): stable, unlike the username
    external_id: str
    username: str
    full_name: str | None = None


@dataclass(frozen=True, slots=True)
class User:
    """Someone on Instagram, as a user lookup sees them."""

    # Instagram user id (pk): addresses the private dialog with them
    external_id: str
    username: str
    full_name: str | None = None


@dataclass(frozen=True, slots=True)
class LoginRequest:
    source_id: UUID
    # username, phone or email
    login: str
    password: SecretStr = field(repr=False)
    # the connector generates 2FA codes from it; None for an account without 2FA
    totp_secret: SecretStr | None = field(repr=False)
    # always set: there is no login without a proxy
    proxy: ProxyAssignment
    # the Source's device; None only for callers that let the adapter make one
    device: Device | None


@dataclass(frozen=True, slots=True)
class LoggedIn:
    account: Account
    session: SessionData
    # the device used: the one given, or a new one
    device: Device


# the range aiograpi's generate_mutation_token draws from: our label looks like its own
_CLIENT_CONTEXT_MIN = 6800011111111111111
_CLIENT_CONTEXT_MAX = 6800099999999999999


def new_client_context() -> str:
    """A fresh label for an outgoing message: 19 digits, as Instagram's apps send."""
    return str(_CLIENT_CONTEXT_MIN + secrets.randbelow(_CLIENT_CONTEXT_MAX - _CLIENT_CONTEXT_MIN + 1))


@dataclass(frozen=True, slots=True)
class OutgoingText:
    source_id: UUID
    # Instagram user id of the person on the other side of the personal dialog
    peer_id: str
    text: str = field(repr=False)
    # our label, saved before the call: the message is found by it in the dialog history
    client_context: str
    # Instagram message id to reply to; the adapter may send without the quote if it cannot
    reply_to: str | None = None


@dataclass(frozen=True, slots=True)
class SentMessage:
    message_id: str
    # Instagram thread id, internal: never leaves the connector
    thread_id: str


@dataclass(frozen=True, slots=True)
class DialogMessage:
    """A message in a dialog's history, as much as reconciling a send needs."""

    message_id: str
    # the sender's label: ours on what the connector sent, echoed back by Instagram
    client_context: str | None


class Platform(Protocol):
    """Every call but login acts as a Source, by source_id: the adapter holds its live Session."""

    def new_device(self) -> Device:
        """A fresh device for a Source's first login; local, nothing reaches Instagram.

        The login flow stores it before the first attempt, so that a failed attempt and
        the next one look like the same phone.
        """
        ...

    async def login(self, request: LoginRequest) -> LoggedIn:
        """One login attempt, never retried inside. Raises PlatformError on a refusal."""
        ...

    async def logout(self, source_id: UUID, session: SessionData, proxy: ProxyAssignment) -> None:
        """End this Session on Instagram through the proxy. Raises PlatformError.

        For a Session a login just gave and the connector rejects. If that login
        replaced the Source's live Session, the previous one is the live one again.
        """
        ...

    async def restore(
        self, source_id: UUID, session: SessionData, device: Device, proxy: ProxyAssignment
    ) -> None:
        """Make the saved Session the Source's live one again, through the proxy, without a login.

        After a restart. Local only (aiograpi set_settings + set_proxy): whether the
        Session still works is `check`'s job. Raises PlatformError if it cannot be loaded.
        """
        ...

    async def disconnect(self, source_id: UUID) -> None:
        """Close the Source's transport: its live Session is dropped with its connections.

        Local only, never raises; nothing goes to Instagram for the Source until `restore`
        brings the Session up again (on another proxy after a move or a network failure).
        """
        ...

    async def check(self, source_id: UUID) -> None:
        """Prove the Source's live Session works with one real read request (account_info).

        Raises PlatformError: SESSION_REVOKED / CHALLENGE when Instagram no longer accepts
        it, FLOOD, NETWORK, ... like any other call.
        """
        ...

    async def has_dialog(self, source_id: UUID, peer_id: str) -> bool:
        """Whether the Source's inbox has a personal dialog with this user. Reads only."""
        ...

    async def send_text(self, message: OutgoingText) -> SentMessage:
        """Send into the existing personal dialog with message.peer_id, once, never retried.

        Raises PlatformError: NETWORK only when nothing reached Instagram, UNKNOWN_AFTER_SEND
        when the request may have gone out.
        """
        ...

    async def recent_messages(self, source_id: UUID, peer_id: str) -> Sequence[DialogMessage]:
        """The latest messages (about 20, any order) of the personal dialog with peer_id. Reads only.

        Used to find a send of unknown outcome by its client_context. Raises PlatformError.
        """
        ...

    async def user_by_username(self, source_id: UUID, username: str) -> User:
        """Look a user up through the Source's Session. Raises PlatformError, NOT_FOUND if none."""
        ...

    async def user_by_id(self, source_id: UUID, external_id: str) -> User:
        """Look a user up by Instagram ID through the Source's Session, like user_by_username."""
        ...


@dataclass(frozen=True, slots=True)
class InboxUser:
    """aiograpi UserShort."""

    # Instagram user id (pk)
    user_id: str
    username: str | None = None
    full_name: str | None = None


@dataclass(frozen=True, slots=True)
class InboxMessage:
    """aiograpi DirectMessage, the fields the connector reads."""

    # DirectMessage.id (the item_id): what send returns as external_message_id
    message_id: str
    # DirectMessage.user_id: the author
    sender_id: str | None
    # DirectMessage.timestamp: the time on the platform
    sent_at: datetime
    # DirectMessage.item_type: "text", "media", "voice_media", "link", "clip", ...
    item_type: str | None
    text: str | None = field(default=None, repr=False)
    # DirectMessage.is_sent_by_viewer: written by the Account itself (app, web, or us)
    is_sent_by_viewer: bool = False
    # DirectMessage.reply.id
    reply_to_message_id: str | None = None


@dataclass(frozen=True, slots=True)
class InboxThread:
    """aiograpi DirectThread with only its messages after the given position."""

    # DirectThread.id: internal, never leaves the connector (contract 4, platform identifiers)
    thread_id: str
    # DirectThread.users: the other participants, without the Account itself
    users: Sequence[InboxUser]
    is_group: bool
    pending: bool
    # oldest first; possibly not all of them (the source pages), the rest comes next time
    messages: Sequence[InboxMessage]


@dataclass(frozen=True, slots=True)
class ThreadPosition:
    """The last message of a thread the connector is done with."""

    message_id: str
    sent_at: datetime


@dataclass(frozen=True, slots=True)
class InboxPositions:
    """Where the connector is in the Account's inbox."""

    # a thread without a position: only messages after this moment (the Source's connect)
    since: datetime
    # by thread_id
    threads: Mapping[str, ThreadPosition]


class InboundSource(Protocol):
    """New messages of a Source's main inbox: polled over HTTP now, Realtime MQTT later.

    The connector asks between commands of the Source and moves the positions only after
    the events are out, so an implementation keeps no position of its own and may return
    the same message again until the position passes it.
    """

    async def new_messages(self, source_id: UUID, positions: InboxPositions) -> Sequence[InboxThread]:
        """Threads of the main inbox (not pending requests) with messages after their position.

        A thread's messages are a contiguous run starting right after its position (or
        after `since` for a thread without one), oldest first, with no gap: the connector
        moves the position to each message it handles, so a skipped one is lost. An
        implementation that reads the newest N (aiograpi's thread_message_limit) must page
        back to the position before returning, or return only the oldest part.
        Never marks anything seen. Raises PlatformError on a refusal.
        """
        ...
