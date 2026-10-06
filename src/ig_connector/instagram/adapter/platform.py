"""The platform port and the inbound source on aiograpi 2.0.15, login through the web.

The adapter holds one live aiograpi Client per Source (one replica per channel, contract 3), always
behind the Source's proxy. Every aiograpi error becomes a PlatformError at this boundary
(errors.py); a send is told apart from a read there. Nothing here ever marks a message
seen, and nothing here repeats a request: CRM retries by the result code.
"""

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID, uuid4

from aiograpi.utils.serialization import dumps

from ig_connector.instagram import (
    Device,
    DialogMessage,
    Failure,
    InboxMessage,
    InboxPositions,
    InboxThread,
    LoggedIn,
    LoginRequest,
    OutgoingText,
    PlatformError,
    SentMessage,
    SessionData,
    User,
)
from ig_connector.instagram.adapter.client import labelled, new_client
from ig_connector.instagram.adapter.direct import (
    RawThread,
    inbox_params,
    is_after,
    message_key,
    parse_thread,
    thread_params,
)
from ig_connector.instagram.adapter.errors import read_refusal, send_refusal
from ig_connector.instagram.weblogin import WebSession
from ig_connector.instagram.weblogin.device import BrowserProfile, WebDevice, encode_device
from ig_connector.instagram.weblogin.session import close_client
from ig_connector.proxy import ProxyAssignment

__all__ = ["AiograpiPlatform", "Clients", "Login"]

log = logging.getLogger(__name__)

# inbox pages of 20 threads per poll: further only while the oldest thread still has news
INBOX_PAGES = 5
# pages of 20 messages back into one thread per poll, to reach the connector's position
THREAD_PAGES = 5
# messages read to find a send by its label
HISTORY = 20

Clients = Callable[[Mapping[str, Any] | None, str], Any]


class Login(Protocol):
    """WebLogin: one browser login, the Session opened in aiograpi."""

    async def login(self, request: LoginRequest) -> WebSession: ...


@dataclass(slots=True)
class _Live:
    client: Any = field(repr=False)
    session: SessionData
    # peer Instagram ID -> thread id of the personal dialog, as found
    dialogs: dict[str, str] = field(default_factory=dict)


class AiograpiPlatform:
    def __init__(self, *, web: Login, clients: Clients = new_client) -> None:
        self._web = web
        self._clients = clients
        self._live: dict[UUID, _Live] = {}
        # the Session a login replaced: live again if the flow refuses the new one
        self._replaced: dict[UUID, _Live] = {}

    async def close(self) -> None:
        """Close every Client's HTTP sessions (service stop)."""
        for live in [*self._live.values(), *self._replaced.values()]:
            await close_client(live.client)
        self._live.clear()
        self._replaced.clear()

    # login and Sessions

    def new_device(self) -> Device:
        return encode_device(WebDevice(BrowserProfile.new()))

    async def login(self, request: LoginRequest) -> LoggedIn:
        result = await self._web.login(request)
        source_id = request.source_id
        # kept before any await: a timeout cancelling the close below must not lose the new one
        older = self._replaced.pop(source_id, None)
        previous = self._live.get(source_id)
        if previous is not None:
            self._replaced[source_id] = previous
        self._live[source_id] = _Live(result.client, result.logged_in.session)
        if older is not None:
            await close_client(older.client)
        return result.logged_in

    async def logout(self, source_id: UUID, session: SessionData, proxy: ProxyAssignment) -> None:
        live = self._live.get(source_id)
        if live is not None and live.session == session:
            client = live.client
            previous = self._replaced.pop(source_id, None)
            if previous is None:
                del self._live[source_id]
            else:
                self._live[source_id] = previous
        else:
            client = self._client_for(session, proxy)
        try:
            await client.logout()
        except Exception as exc:
            raise read_refusal(exc, curl=client.private_transport == "curl") from None
        finally:
            await close_client(client)

    async def restore(
        self, source_id: UUID, session: SessionData, device: Device, proxy: ProxyAssignment
    ) -> None:
        client = self._client_for(session, proxy)
        old = [self._live.get(source_id), self._replaced.pop(source_id, None)]
        self._live[source_id] = _Live(client, session)
        for live in old:
            if live is not None:
                await close_client(live.client)

    async def check(self, source_id: UUID) -> None:
        # aiograpi account_info's request; its parser wants profile fields that prove nothing
        try:
            answer = await self._request(
                await self._working(source_id), "accounts/current_user/", {"edit": "true"}
            )
        except PlatformError as exc:
            if exc.failure in (Failure.SESSION_REVOKED, Failure.CHALLENGE):
                # Instagram ended it: only a new login helps, and that brings its own Client
                await self._drop(source_id)
            raise
        if not isinstance(answer.get("user"), Mapping):
            raise PlatformError(Failure.REJECTED, "Instagram answered the check without the account")

    def _client_for(self, session: SessionData, proxy: ProxyAssignment) -> Any:
        url = proxy.url.get_secret_value()
        if not url:
            # aiograpi takes no proxy as "go direct": never
            raise PlatformError(Failure.NETWORK, "no proxy address")
        try:
            settings = json.loads(session.data)
            if not isinstance(settings, dict):
                raise ValueError("not aiograpi settings")
            return self._clients(settings, url)
        except (ValueError, TypeError, KeyError) as exc:
            raise PlatformError(
                Failure.SESSION_REVOKED, f"saved Session unreadable ({type(exc).__name__})"
            ) from None

    async def _working(self, source_id: UUID) -> Any:
        """The live Client for a call of the Source; the one its last login replaced is closed.

        A Source's calls are serial (its executor): any call after a login comes after
        its flow, which has kept the new Session by then or logged it out.
        """
        client = self._client(source_id)
        replaced = self._replaced.pop(source_id, None)
        if replaced is not None:
            await close_client(replaced.client)
        return client

    async def disconnect(self, source_id: UUID) -> None:
        await self._drop(source_id)

    async def _drop(self, source_id: UUID) -> None:
        """Forget the Source's Clients and close their connections (close_client never raises)."""
        for live in (self._live.pop(source_id, None), self._replaced.pop(source_id, None)):
            if live is not None:
                await close_client(live.client)

    def _client(self, source_id: UUID) -> Any:
        live = self._live.get(source_id)
        if live is None:
            raise PlatformError(Failure.SESSION_REVOKED, "no live Session")
        return live.client

    # users

    async def user_by_username(self, source_id: UUID, username: str) -> User:
        client = await self._working(source_id)
        try:
            found = await client.user_info_by_username_v1(username)
        except Exception as exc:
            raise read_refusal(exc, curl=client.private_transport == "curl") from None
        return User(str(found.pk), str(found.username), found.full_name or None)

    async def user_by_id(self, source_id: UUID, external_id: str) -> User:
        client = await self._working(source_id)
        try:
            found = await client.user_info_v1(external_id)
        except Exception as exc:
            raise read_refusal(exc, curl=client.private_transport == "curl") from None
        return User(str(found.pk), str(found.username), found.full_name or None)

    # dialogs and sending

    async def has_dialog(self, source_id: UUID, peer_id: str) -> bool:
        return await self._dialog(source_id, peer_id) is not None

    async def send_text(self, message: OutgoingText) -> SentMessage:
        client = await self._working(message.source_id)
        thread_id = await self._dialog(message.source_id, message.peer_id)
        if thread_id is None:
            raise PlatformError(Failure.REJECTED, "no personal dialog with this user")
        # a quote needs the label of the quoted message; stage 1 sends without it
        client.last_response = None
        try:
            with labelled(message.client_context):
                sent = await client.direct_send(message.text, thread_ids=[int(thread_id)])
        except Exception as exc:
            raise send_refusal(exc, client.last_response, curl=client.private_transport == "curl") from None
        if not sent.id:
            raise PlatformError(Failure.UNKNOWN_AFTER_SEND, "Instagram answered without a message id")
        if sent.client_context and sent.client_context != message.client_context:
            log.warning("Instagram answered with another label", extra={"source_id": str(message.source_id)})
        return SentMessage(message_id=str(sent.id), thread_id=thread_id)

    async def recent_messages(self, source_id: UUID, peer_id: str) -> Sequence[DialogMessage]:
        thread_id = await self._dialog(source_id, peer_id)
        if thread_id is None:
            raise PlatformError(Failure.NOT_FOUND, "no personal dialog with this user")
        page = await self._thread_page(source_id, thread_id, None, HISTORY)
        if page is None:
            raise PlatformError(Failure.REJECTED, "unreadable thread")
        return [DialogMessage(m.message_id, page.labels.get(m.message_id)) for m in page.messages]

    async def _dialog(self, source_id: UUID, peer_id: str) -> str | None:
        """The thread id of the personal dialog with peer_id in the main inbox, or None."""
        live = self._live.get(source_id)
        if live is None:
            raise PlatformError(Failure.SESSION_REVOKED, "no live Session")
        if peer_id in live.dialogs:
            return live.dialogs[peer_id]
        if not (peer_id.isascii() and peer_id.isdigit()):
            raise PlatformError(Failure.NOT_FOUND, "an Instagram ID is a number")
        viewer = str(live.client.user_id)
        # recent dialogs first: the usual case, and the inbox read is the one proven live
        raw = await self._request(live.client, "direct_v2/inbox/", inbox_params(None, str(uuid4())))
        candidates = [parse_thread(t, viewer) for t in (raw.get("inbox") or {}).get("threads") or ()]
        found = next((t for t in candidates if t is not None and _is_dialog_with(t, peer_id)), None)
        if found is None:
            raw = await self._request(
                live.client,
                "direct_v2/threads/get_by_participants/",
                {"recipient_users": dumps([int(peer_id)]), "seq_id": "2580572", "limit": "20"},
            )
            thread = parse_thread(raw.get("thread"), viewer) if raw.get("thread") else None
            found = thread if thread is not None and _is_dialog_with(thread, peer_id) else None
        if found is None:
            return None
        live.dialogs[peer_id] = found.thread_id
        return found.thread_id

    # inbound

    async def new_messages(self, source_id: UUID, positions: InboxPositions) -> Sequence[InboxThread]:
        client = await self._working(source_id)
        viewer = str(client.user_id)
        found: list[InboxThread] = []
        cursor: str | None = None
        for _ in range(INBOX_PAGES):
            raw = await self._request(client, "direct_v2/inbox/", inbox_params(cursor, str(uuid4())))
            inbox = raw.get("inbox") or {}
            oldest_has_news = False
            for data in inbox.get("threads") or ():
                thread = parse_thread(data, viewer)
                if thread is None or thread.pending:
                    continue
                messages = await self._news(source_id, thread, positions)
                oldest_has_news = bool(messages)
                if messages:
                    found.append(
                        InboxThread(thread.thread_id, thread.users, thread.is_group, thread.pending, messages)
                    )
            cursor = inbox.get("oldest_cursor") or None
            # threads come by last activity: past one with nothing new, the rest are older
            if not (inbox.get("has_older") and cursor and oldest_has_news):
                break
        # oldest activity first: a poll cut short leaves the newest threads, which come again
        return sorted(found, key=lambda t: message_key(t.messages[-1]))

    async def _news(
        self, source_id: UUID, thread: RawThread, positions: InboxPositions
    ) -> list[InboxMessage]:
        """The thread's messages after its position, oldest first, paging back if needed."""
        position = positions.threads.get(thread.thread_id)
        news = [m for m in thread.messages if is_after(m, position, positions.since)]
        reached = len(news) < len(thread.messages)
        has_older, cursor = thread.has_older, thread.oldest_cursor
        pages = 0
        while not reached and has_older and cursor and pages < THREAD_PAGES:
            pages += 1
            page = await self._thread_page(source_id, thread.thread_id, cursor, 20)
            if page is None:
                break
            known = {m.message_id for m in news}
            older = [m for m in page.messages if m.message_id not in known]
            older_news = [m for m in older if is_after(m, position, positions.since)]
            news.extend(older_news)
            reached = len(older_news) < len(older) or not older
            has_older, cursor = page.has_older, page.oldest_cursor
        if not reached and has_older:
            # more than THREAD_PAGES pages since the last poll: the oldest of them are lost
            log.warning(
                "thread history too long to page back, older messages skipped",
                extra={"source_id": str(source_id), "thread_id": thread.thread_id},
            )
        return sorted(news, key=message_key)

    async def _thread_page(
        self, source_id: UUID, thread_id: str, cursor: str | None, limit: int
    ) -> RawThread | None:
        client = await self._working(source_id)
        raw = await self._request(client, f"direct_v2/threads/{thread_id}/", thread_params(cursor, limit))
        return parse_thread(raw.get("thread"), str(client.user_id))

    @staticmethod
    async def _request(client: Any, endpoint: str, params: Mapping[str, str]) -> Mapping[str, Any]:
        try:
            answer = await client.private_request(endpoint, params=dict(params))
        except Exception as exc:
            raise read_refusal(exc, curl=client.private_transport == "curl") from None
        return answer if isinstance(answer, Mapping) else {}


def _is_dialog_with(thread: RawThread, peer_id: str) -> bool:
    """A personal dialog in the main inbox with someone in it: what send may write into."""
    return (
        not thread.is_group
        and not thread.pending
        and [u.user_id for u in thread.users] == [peer_id]
        and bool(thread.messages)
    )
