"""Instagram in memory, with scenarios, for behaviour tests."""

import asyncio
import itertools
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from ig_connector.instagram import (
    Account,
    Device,
    DialogMessage,
    Failure,
    InboxMessage,
    InboxPositions,
    InboxThread,
    InboxUser,
    LoggedIn,
    LoginRequest,
    OutgoingText,
    PlatformError,
    SentMessage,
    SessionData,
    UnsupportedProxy,
    User,
)
from ig_connector.proxy import ProxyAssignment

Listener = Callable[[str, Mapping[str, Any]], None]


@dataclass(frozen=True, slots=True)
class FakeAccount:
    login: str
    password: str
    # None: the account has no 2FA
    totp_secret: str | None
    account: Account


@dataclass(frozen=True, slots=True)
class LoginAttempt:
    login: str
    assignment_id: int
    device: Device | None


@dataclass(slots=True)
class FakeThread:
    thread_id: str
    users: tuple[InboxUser, ...]
    is_group: bool
    pending: bool
    messages: list[InboxMessage] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class Restore:
    source_id: UUID
    assignment_id: int
    device: Device


@dataclass(frozen=True, slots=True)
class Logout:
    source_id: UUID
    session: SessionData
    assignment_id: int


@dataclass(frozen=True, slots=True)
class SentText:
    """A message that reached the fake Instagram (also when the sender was told it is unknown)."""

    # Instagram id of the sending account
    account_id: str
    peer_id: str
    thread_id: str
    message_id: str
    text: str
    client_context: str
    reply_to: str | None


@dataclass(frozen=True, slots=True)
class Lookup:
    source_id: UUID
    # "username" or "external_id"
    by: str
    value: str


class FakeInstagram:
    """Accounts the test registers, and what Instagram does on login and in the inbox.

    Scenario knobs, by login: `login_failures[login] = Failure.X` (Instagram refuses with
    it), `hold_login[login] = asyncio.Event()` (the login hangs until the event is set).
    `logins` lists every attempt; password and TOTP secret are never kept or traced.
    `sessions_issued` lists every Session a login gave, `logouts` every logout.
    `rename(login, username)`: the account changes its username (= its login), the
    Instagram ID stays.

    Sending, by the peer's Instagram id: `add_dialog(account_id, peer_id)` puts a personal
    dialog into the account's inbox; `dialog_failures[peer]` / `send_failures[peer]` refuse
    the inbox check / the send (UNKNOWN_AFTER_SEND: the message does go out, then the error);
    `hold_dialog[peer]` / `hold_send[peer]` hang the call until the event is set (a held send
    cancelled meanwhile never goes out), `hang_after_send[peer]` lets the message out and then
    hangs (a crash right after the send). `dialog_checks` and `sends` list every call, `sent`
    what went out. Dialog history (`recent_messages`) is served from `sent`;
    `history_failures[peer]` / `hold_history[peer]` refuse / hang it, `history_reads` lists
    every read. A Source is known to the fake from its successful login on; a send or
    dialog check for an unknown one is SESSION_REVOKED.

    User lookup: `add_user(...)` (accounts are users too); by looked-up value,
    `lookup_failures[value] = Failure.X`, `hold_lookup[value] = asyncio.Event()`;
    `lookups` lists every lookup.

    Inbox, by the Account's external_id: `message(...)` puts a message into a thread,
    `inbox_failures[external_id] = Failure.X` (polling is refused), `hold_inbox[external_id]
    = asyncio.Event()` (a poll hangs until set), `inbox_page` (new messages per thread per
    poll, like aiograpi's thread_message_limit). `inbox_polls` lists the polled source_ids.
    A Source is known to the fake once it logged in (its Session lives on).

    Restart: `restart()` forgets every live Session (the process is gone); `restore(...)`
    brings one back from a Session this fake issued (an unknown one is SESSION_REVOKED),
    `restores` lists them; `disconnect(source_id)` drops a live Session (`disconnects`).
    `check(source_id)` is the liveness request: by the Account's external_id,
    `check_failures[external_id] = Failure.X`, `hold_check[external_id] = asyncio.Event()`;
    `checks` lists the checked source_ids.
    """

    def __init__(self, *, listener: Listener | None = None) -> None:
        self.accounts: dict[str, FakeAccount] = {}
        self.login_failures: dict[str, Failure] = {}
        self.hold_login: dict[str, asyncio.Event] = {}
        # the adapter's detail text on a scripted refusal
        self.failure_detail = "scripted by the test"
        self.logins: list[LoginAttempt] = []
        self.sessions_issued: list[SessionData] = []
        self.logouts: list[Logout] = []
        # the account each Source logged in as: the adapter's live Sessions
        self.sessions: dict[UUID, Account] = {}
        # account_id -> peer_id -> thread_id
        self.dialogs: dict[str, dict[str, str]] = {}
        self.dialog_failures: dict[str, Failure] = {}
        self.send_failures: dict[str, Failure] = {}
        self.hold_dialog: dict[str, asyncio.Event] = {}
        self.hold_send: dict[str, asyncio.Event] = {}
        # every call, also refused or held ones: (source_id, peer_id) / the message
        self.dialog_checks: list[tuple[UUID, str]] = []
        self.sends: list[OutgoingText] = []
        self.sent: list[SentText] = []
        self.hang_after_send: dict[str, asyncio.Event] = {}
        self.history_failures: dict[str, Failure] = {}
        self.hold_history: dict[str, asyncio.Event] = {}
        self.history_reads: list[tuple[UUID, str]] = []
        self.users: dict[str, User] = {}
        self.lookup_failures: dict[str, Failure] = {}
        self.hold_lookup: dict[str, asyncio.Event] = {}
        self.lookups: list[Lookup] = []
        self._listener = listener
        self._devices = 0
        self._sessions = 0
        self.threads: dict[str, dict[str, FakeThread]] = {}
        self.inbox_failures: dict[str, Failure] = {}
        self.hold_inbox: dict[str, asyncio.Event] = {}
        self.inbox_page = 10
        self.inbox_polls: list[UUID] = []
        # Instagram item ids are big increasing numbers
        self._item_ids = itertools.count(28597946203914980615241927545176064)
        self._thread_ids = itertools.count(340282366841710300949128531777654287254)
        self._messages = 0
        self._replaced: dict[bytes, tuple[UUID, Account | None]] = {}
        self._issued: dict[bytes, Account] = {}
        self.restores: list[Restore] = []
        self.disconnects: list[UUID] = []
        self.checks: list[UUID] = []
        self.check_failures: dict[str, Failure] = {}
        self.hold_check: dict[str, asyncio.Event] = {}

    def add_account(
        self,
        login: str,
        *,
        password: str,
        totp_secret: str | None = None,
        external_id: str,
        username: str | None = None,
        full_name: str | None = None,
    ) -> Account:
        account = Account(external_id=external_id, username=username or login, full_name=full_name)
        self.accounts[login] = FakeAccount(login, password, totp_secret, account)
        self.add_user(external_id=external_id, username=account.username, full_name=full_name)
        return account

    def rename(self, login: str, username: str) -> Account:
        known = self.accounts.pop(login)
        account = Account(known.account.external_id, username, known.account.full_name)
        self.accounts[username] = FakeAccount(username, known.password, known.totp_secret, account)
        self.add_user(external_id=account.external_id, username=username, full_name=account.full_name)
        return account

    def add_user(self, *, external_id: str, username: str, full_name: str | None = None) -> User:
        user = User(external_id=external_id, username=username, full_name=full_name)
        self.users[external_id] = user
        return user

    async def user_by_username(self, source_id: UUID, username: str) -> User:
        await self._lookup(Lookup(source_id, "username", username))
        for user in self.users.values():
            if user.username == username:
                return user
        raise PlatformError(Failure.NOT_FOUND, "no such user")

    async def user_by_id(self, source_id: UUID, external_id: str) -> User:
        await self._lookup(Lookup(source_id, "external_id", external_id))
        user = self.users.get(external_id)
        if user is None:
            raise PlatformError(Failure.NOT_FOUND, "no such user")
        return user

    async def _lookup(self, lookup: Lookup) -> None:
        self.lookups.append(lookup)
        self._emit("instagram.user_lookup", {"source_id": str(lookup.source_id), "by": lookup.by})
        held = self.hold_lookup.get(lookup.value)
        if held is not None:
            await held.wait()
        failure = self.lookup_failures.get(lookup.value)
        if failure is not None:
            raise PlatformError(failure, self.failure_detail)

    async def login(self, request: LoginRequest) -> LoggedIn:
        self.logins.append(LoginAttempt(request.login, request.proxy.assignment_id, request.device))
        self._emit("instagram.login", {"login": request.login, "proxy": request.proxy.assignment_id})
        held = self.hold_login.get(request.login)
        if held is not None:
            await held.wait()
        if request.proxy.scheme == "socks5":
            # as the real browser login: Chromium cannot authenticate to a SOCKS5 proxy
            raise UnsupportedProxy("the browser cannot use a SOCKS5 proxy with credentials")
        failure = self.login_failures.get(request.login)
        if failure is not None:
            raise PlatformError(failure, self.failure_detail)
        known = self.accounts.get(request.login)
        if known is None or request.password.get_secret_value() != known.password:
            raise PlatformError(Failure.BAD_CREDENTIALS, "wrong login or password")
        if known.totp_secret is not None:
            if request.totp_secret is None:
                raise PlatformError(Failure.CHALLENGE, "2FA code needed, no secret given")
            if request.totp_secret.get_secret_value() != known.totp_secret:
                raise PlatformError(Failure.BAD_CREDENTIALS, "wrong 2FA code")
        device = request.device or self._new_device()
        self._sessions += 1
        session = SessionData(f"session-{known.account.external_id}-{self._sessions}".encode())
        self.sessions_issued.append(session)
        self._issued[session.data] = known.account
        # a logout of this Session brings the one it replaced back
        self._replaced[session.data] = (request.source_id, self.sessions.get(request.source_id))
        self.sessions[request.source_id] = known.account
        self._emit("instagram.logged_in", {"external_id": known.account.external_id})
        return LoggedIn(account=known.account, session=session, device=device)

    async def logout(self, source_id: UUID, session: SessionData, proxy: ProxyAssignment) -> None:
        self.logouts.append(Logout(source_id, session, proxy.assignment_id))
        self._emit("instagram.logout", {"source_id": str(source_id), "proxy": proxy.assignment_id})
        replaced = self._replaced.pop(session.data, None)
        if replaced is not None:
            owner, previous = replaced
            if previous is None:
                self.sessions.pop(owner, None)
            else:
                self.sessions[owner] = previous

    def restart(self) -> None:
        """The connector process is gone: no Source has a live Session until it is restored."""
        self.sessions.clear()

    async def restore(
        self, source_id: UUID, session: SessionData, device: Device, proxy: ProxyAssignment
    ) -> None:
        self.restores.append(Restore(source_id, proxy.assignment_id, device))
        self._emit("instagram.restore", {"source_id": str(source_id), "proxy": proxy.assignment_id})
        account = self._issued.get(session.data)
        if account is None:
            raise PlatformError(Failure.SESSION_REVOKED, "unknown session")
        self.sessions[source_id] = account

    async def disconnect(self, source_id: UUID) -> None:
        self.disconnects.append(source_id)
        self._emit("instagram.disconnect", {"source_id": str(source_id)})
        self.sessions.pop(source_id, None)

    async def check(self, source_id: UUID) -> None:
        self.checks.append(source_id)
        account = self._session(source_id)
        self._emit("instagram.check", {"source_id": str(source_id)})
        held = self.hold_check.get(account.external_id)
        if held is not None:
            await held.wait()
        failure = self.check_failures.get(account.external_id)
        if failure is not None:
            raise PlatformError(failure, self.failure_detail)

    def add_dialog(self, account_id: str, peer_id: str) -> str:
        """A personal dialog of the account with the peer, in the account's inbox; its thread id."""
        threads = self.dialogs.setdefault(account_id, {})
        return threads.setdefault(peer_id, f"thread-{account_id}-{peer_id}")

    async def has_dialog(self, source_id: UUID, peer_id: str) -> bool:
        self.dialog_checks.append((source_id, peer_id))
        account = self._session(source_id)
        self._emit("instagram.has_dialog", {"account": account.external_id, "peer": peer_id})
        held = self.hold_dialog.get(peer_id)
        if held is not None:
            await held.wait()
        failure = self.dialog_failures.get(peer_id)
        if failure is not None:
            raise PlatformError(failure, self.failure_detail)
        return peer_id in self.dialogs.get(account.external_id, {})

    async def send_text(self, message: OutgoingText) -> SentMessage:
        self.sends.append(message)
        account = self._session(message.source_id)
        self._emit(
            "instagram.send",
            {"account": account.external_id, "peer": message.peer_id, "text_length": len(message.text)},
        )
        held = self.hold_send.get(message.peer_id)
        if held is not None:
            await held.wait()
        failure = self.send_failures.get(message.peer_id)
        if failure is not None and failure is not Failure.UNKNOWN_AFTER_SEND:
            raise PlatformError(failure, self.failure_detail)
        thread_id = self.dialogs.get(account.external_id, {}).get(message.peer_id)
        if thread_id is None:
            # Instagram would open a new dialog here: the connector must never get this far
            raise AssertionError(f"send to {message.peer_id} without a dialog")
        self._messages += 1
        sent = SentText(
            account_id=account.external_id,
            peer_id=message.peer_id,
            thread_id=thread_id,
            message_id=f"item-{self._messages}",
            text=message.text,
            client_context=message.client_context,
            reply_to=message.reply_to,
        )
        self.sent.append(sent)
        self._emit("instagram.sent", {"message_id": sent.message_id})
        hang = self.hang_after_send.get(message.peer_id)
        if hang is not None:
            await hang.wait()
        if failure is not None:
            raise PlatformError(failure, self.failure_detail)
        return SentMessage(message_id=sent.message_id, thread_id=thread_id)

    async def recent_messages(self, source_id: UUID, peer_id: str) -> list[DialogMessage]:
        self.history_reads.append((source_id, peer_id))
        account = self._session(source_id)
        self._emit("instagram.history", {"account": account.external_id, "peer": peer_id})
        held = self.hold_history.get(peer_id)
        if held is not None:
            await held.wait()
        failure = self.history_failures.get(peer_id)
        if failure is not None:
            raise PlatformError(failure, self.failure_detail)
        dialog = [m for m in self.sent if (m.account_id, m.peer_id) == (account.external_id, peer_id)]
        return [DialogMessage(m.message_id, m.client_context) for m in reversed(dialog[-20:])]

    def _session(self, source_id: UUID) -> Account:
        account = self.sessions.get(source_id)
        if account is None:
            raise PlatformError(Failure.SESSION_REVOKED, "no live session for the source")
        return account

    def message(
        self,
        account: str,
        *,
        peer: InboxUser,
        at: datetime,
        text: str | None = None,
        item_type: str = "text",
        by_viewer: bool = False,
        pending: bool = False,
        reply_to: str | None = None,
    ) -> InboxMessage:
        """A message in the one-to-one thread of `account` (external_id) with `peer`.

        By the peer unless `by_viewer` (the Account wrote it in the app). `pending`: the
        thread is a message request (a new thread only).
        """
        thread = self._thread(account, (peer,), is_group=False, pending=pending)
        sender = account if by_viewer else peer.user_id
        return self._add(thread, sender, by_viewer, at=at, text=text, item_type=item_type, reply_to=reply_to)

    def group_message(
        self, account: str, *, members: Sequence[InboxUser], sender: InboxUser, at: datetime, text: str
    ) -> InboxMessage:
        thread = self._thread(account, tuple(members), is_group=True, pending=False)
        return self._add(thread, sender.user_id, False, at=at, text=text, item_type="text", reply_to=None)

    async def new_messages(self, source_id: UUID, positions: InboxPositions) -> Sequence[InboxThread]:
        self.inbox_polls.append(source_id)
        self._emit("instagram.inbox", {"source_id": str(source_id), "threads": len(positions.threads)})
        account = self._session(source_id).external_id
        held = self.hold_inbox.get(account)
        if held is not None:
            await held.wait()
        failure = self.inbox_failures.get(account)
        if failure is not None:
            raise PlatformError(failure, self.failure_detail)
        out = []
        for thread in self.threads.get(account, {}).values():
            if thread.pending:  # the pending inbox is another endpoint
                continue
            position = positions.threads.get(thread.thread_id)
            if position is None:
                new = [m for m in thread.messages if m.sent_at > positions.since]
            else:
                after = (position.sent_at, int(position.message_id))
                new = [m for m in thread.messages if (m.sent_at, int(m.message_id)) > after]
            if new:
                page = tuple(new[: self.inbox_page])
                out.append(InboxThread(thread.thread_id, thread.users, thread.is_group, thread.pending, page))
        return out

    def _thread(
        self, account: str, users: tuple[InboxUser, ...], *, is_group: bool, pending: bool
    ) -> FakeThread:
        threads = self.threads.setdefault(account, {})
        for thread in threads.values():
            if thread.users == users and thread.is_group == is_group:
                return thread
        thread = FakeThread(str(next(self._thread_ids)), users, is_group, pending)
        threads[thread.thread_id] = thread
        return thread

    def _add(
        self,
        thread: FakeThread,
        sender_id: str,
        by_viewer: bool,
        *,
        at: datetime,
        text: str | None,
        item_type: str,
        reply_to: str | None,
    ) -> InboxMessage:
        message = InboxMessage(
            message_id=str(next(self._item_ids)),
            sender_id=sender_id,
            sent_at=at,
            item_type=item_type,
            text=text,
            is_sent_by_viewer=by_viewer,
            reply_to_message_id=reply_to,
        )
        thread.messages.append(message)
        thread.messages.sort(key=lambda m: (m.sent_at, int(m.message_id)))
        return message

    def new_device(self) -> Device:
        return self._new_device()

    def _new_device(self) -> Device:
        self._devices += 1
        return Device(f"device-{self._devices}".encode())

    def _emit(self, kind: str, data: Mapping[str, Any]) -> None:
        if self._listener is not None:
            self._listener(kind, data)
