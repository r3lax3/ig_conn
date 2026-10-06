"""From the browser's `sessionid` to an aiograpi Session: `login_by_sessionid`."""

import asyncio
import json
import logging
import re
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

from aiograpi.exceptions import (
    BadCredentials,
    BadPassword,
    ChallengeError,
    ClientConnectionError,
    ClientError,
    ClientLoginRequired,
    ClientThrottledError,
    ClientUnauthorizedError,
    ConsentRequired,
    FeedbackRequired,
    LoginRequired,
    PleaseWaitFewMinutes,
    ProxyAddressIsBlocked,
    ProxyError,
    RateLimitError,
    TwoFactorRequired,
)
from pydantic import SecretStr

from ig_connector.instagram import Account, Failure, PlatformError, SessionData
from ig_connector.instagram.adapter.client import new_client
from ig_connector.proxy import ProxyAssignment

__all__ = ["AiograpiSessions", "OpenedSession", "SessionOpener", "aiograpi_failure", "close_client"]

log = logging.getLogger(__name__)

# what login_by_sessionid accepts: the user id first, longer than 30 characters
_SESSIONID = re.compile(r"\d+\S{30,}")
# aiograpi settings that describe the phone, not the login: kept as the device
_DEVICE_KEYS = (
    "uuids",
    "device_settings",
    "user_agent",
    "country",
    "country_code",
    "locale",
    "timezone_offset",
    "timezone_name",
)


@dataclass(frozen=True, slots=True)
class OpenedSession:
    account: Account
    # aiograpi get_settings() as JSON: what Client(settings=...) restores
    session: SessionData
    # aiograpi device settings to reuse on the next login
    app_device: dict[str, Any] = field(repr=False)
    # the live aiograpi Client, logged in, proxy set: the platform adapter keeps it
    client: Any = field(repr=False)


class SessionOpener(Protocol):
    async def open(
        self,
        source_id: UUID,
        sessionid: SecretStr,
        proxy: ProxyAssignment,
        app_device: Mapping[str, Any] | None,
    ) -> OpenedSession:
        """An aiograpi Session for this sessionid through the proxy. Raises PlatformError."""
        ...


# a web session aiograpi could not take over is logged out within this, best effort
WEB_LOGOUT_TIMEOUT = 10.0


class AiograpiSessions:
    def __init__(self, clients: Callable[[Mapping[str, Any] | None, str], Any] | None = None) -> None:
        self._clients = clients or new_client

    async def open(
        self,
        source_id: UUID,
        sessionid: SecretStr,
        proxy: ProxyAssignment,
        app_device: Mapping[str, Any] | None,
    ) -> OpenedSession:
        url = proxy.url.get_secret_value()
        if not url:
            # aiograpi takes no proxy as "go direct": never
            raise PlatformError(Failure.NETWORK, "no proxy address")
        if not _SESSIONID.fullmatch(sessionid.get_secret_value()):
            # aiograpi checks this with assert, which python -O drops
            raise PlatformError(Failure.REJECTED, "Instagram gave a session id aiograpi cannot use")
        client = self._clients(app_device, url)
        try:
            await client.login_by_sessionid(sessionid.get_secret_value())
            settings = client.get_settings()
        except BaseException as exc:
            if isinstance(exc, Exception | asyncio.CancelledError):
                # a cancel is the login flow's deadline: the session is still left behind.
                # Shielded, so the logout and the close run to the end either way
                await asyncio.shield(_give_up(client))
            else:
                await close_client(client)
            if isinstance(exc, ClientError | OSError | TimeoutError):
                raise aiograpi_failure(exc) from None
            if isinstance(exc, Exception):
                # aiograpi lets parsing errors of Instagram's answers through (pydantic, KeyError)
                raise PlatformError(Failure.REJECTED, f"aiograpi {type(exc).__name__}") from None
            raise
        return OpenedSession(
            account=Account(external_id=str(client.user_id), username=str(client.username)),
            session=SessionData(json.dumps(settings).encode()),
            app_device={k: settings[k] for k in _DEVICE_KEYS if k in settings},
            client=client,
        )


async def _give_up(client: Any) -> None:
    await _logout_web_session(client)
    await close_client(client)


async def _logout_web_session(client: Any) -> None:
    """End the browser's session that aiograpi could not take over, instead of leaving it live."""
    if not client.user_id:
        return
    try:
        async with asyncio.timeout(WEB_LOGOUT_TIMEOUT):
            await client.logout()
    except Exception as exc:
        log.warning("web session not logged out", extra={"error_type": type(exc).__name__})
    else:
        log.info("web session logged out after aiograpi refused it")


async def close_client(client: Any) -> None:
    """Close the aiograpi Client's HTTP sessions (private, public, graphql) to the proxy."""
    for name in ("private", "public", "graphql"):
        session = getattr(client, name, None)
        close = getattr(session, "_close", None)
        if close is not None:
            with suppress(Exception):
                await close()


def aiograpi_failure(exc: BaseException) -> PlatformError:
    """The port's refusal for an aiograpi error; the detail names the type, never the message.

    aiograpi messages quote requests and responses, which carry the session.
    """
    detail = f"aiograpi {type(exc).__name__}"
    match exc:
        case FeedbackRequired():
            failure = Failure.FLOOD
        case ChallengeError() | ConsentRequired() | TwoFactorRequired():
            failure = Failure.CHALLENGE
        case LoginRequired() | ClientLoginRequired() | ClientUnauthorizedError():
            failure = Failure.SESSION_REVOKED
        case BadPassword() | BadCredentials():
            failure = Failure.BAD_CREDENTIALS
        case PleaseWaitFewMinutes() | RateLimitError() | ClientThrottledError():
            failure = Failure.RATE_LIMITED
        case ProxyAddressIsBlocked() | ProxyError() | ClientConnectionError() | OSError() | TimeoutError():
            # NB "nothing reached Instagram" is not certain here: callers that sent something
            # already (a login, a message) must not pass this on as NETWORK
            failure = Failure.NETWORK
        case _:
            failure = Failure.REJECTED
    return PlatformError(failure, detail)
