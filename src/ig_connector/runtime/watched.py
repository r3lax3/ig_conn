"""The platform as the runtime uses it: every answer for a Source moves its status.

Wraps the adapter so that handlers and polls need not know about statuses: a refusal
that says something about the Source (revoked Session, check, flood, network) changes
its status, a successful call ends an `error` (recovery). Login and logout are the
login flow's business and pass through untouched.
"""

from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol
from uuid import UUID

from ig_connector.instagram import (
    Device,
    DialogMessage,
    Failure,
    InboundSource,
    InboxPositions,
    InboxThread,
    LoggedIn,
    LoginRequest,
    OutgoingText,
    Platform,
    PlatformError,
    SentMessage,
    SessionData,
    User,
)
from ig_connector.proxy import ProxyAssignment
from ig_connector.runtime.statuses import Statuses


class PlatformWithInbox(Platform, InboundSource, Protocol):
    """The adapter: the platform port and the inbound source in one (as the fake is)."""


class WatchedPlatform:
    def __init__(
        self,
        platform: PlatformWithInbox,
        statuses: Statuses,
        *,
        on_network_failure: Callable[[UUID], None] | None = None,
    ) -> None:
        self._platform = platform
        self._statuses = statuses
        # a NETWORK refusal is a confirmed failure through the proxy: its lifecycle fails it over
        self._on_network_failure = on_network_failure

    async def _watch[T](self, source_id: UUID, call: Awaitable[T]) -> T:
        try:
            result = await call
        except PlatformError as exc:
            await self._statuses.failed(source_id, exc.failure)
            if exc.failure is Failure.NETWORK and self._on_network_failure is not None:
                self._on_network_failure(source_id)
            raise
        await self._statuses.succeeded(source_id)
        return result

    def new_device(self) -> Device:
        return self._platform.new_device()

    async def login(self, request: LoginRequest) -> LoggedIn:
        return await self._platform.login(request)

    async def logout(self, source_id: UUID, session: SessionData, proxy: ProxyAssignment) -> None:
        await self._platform.logout(source_id, session, proxy)

    async def restore(
        self, source_id: UUID, session: SessionData, device: Device, proxy: ProxyAssignment
    ) -> None:
        await self._platform.restore(source_id, session, device, proxy)

    async def disconnect(self, source_id: UUID) -> None:
        await self._platform.disconnect(source_id)

    async def check(self, source_id: UUID) -> None:
        await self._watch(source_id, self._platform.check(source_id))

    async def has_dialog(self, source_id: UUID, peer_id: str) -> bool:
        return await self._watch(source_id, self._platform.has_dialog(source_id, peer_id))

    async def send_text(self, message: OutgoingText) -> SentMessage:
        return await self._watch(message.source_id, self._platform.send_text(message))

    async def recent_messages(self, source_id: UUID, peer_id: str) -> Sequence[DialogMessage]:
        return await self._watch(source_id, self._platform.recent_messages(source_id, peer_id))

    async def user_by_username(self, source_id: UUID, username: str) -> User:
        return await self._watch(source_id, self._platform.user_by_username(source_id, username))

    async def user_by_id(self, source_id: UUID, external_id: str) -> User:
        return await self._watch(source_id, self._platform.user_by_id(source_id, external_id))

    async def new_messages(self, source_id: UUID, positions: InboxPositions) -> Sequence[InboxThread]:
        return await self._watch(source_id, self._platform.new_messages(source_id, positions))
