"""The real login of the platform port: browser -> `sessionid` -> aiograpi.

Headless Chromium logs in on Instagram's web with the login, password and a TOTP code
generated from the secret, through the Source's proxy. The `sessionid` cookie goes to
aiograpi `login_by_sessionid` over the same proxy, which names the account. The device
(browser profile + aiograpi device settings) comes back for storing and is reused.

The platform adapter calls `WebLogin.login` from its `Platform.login`, keeps
`WebSession.client` as the Source's live Session and returns `WebSession.logged_in`.
"""

from dataclasses import dataclass, field, replace
from typing import Any

from ig_connector.clock import Clock
from ig_connector.instagram import Failure, LoggedIn, LoginRequest, PlatformError
from ig_connector.instagram.weblogin.browser import INSTAGRAM, BrowserLogin, browser_proxy
from ig_connector.instagram.weblogin.device import decode_device, encode_device
from ig_connector.instagram.weblogin.session import (
    AiograpiSessions,
    OpenedSession,
    SessionOpener,
    aiograpi_failure,
)

__all__ = [
    "AiograpiSessions",
    "OpenedSession",
    "SessionOpener",
    "WebLogin",
    "WebSession",
    "aiograpi_failure",
]


@dataclass(frozen=True, slots=True)
class WebSession:
    logged_in: LoggedIn
    # the logged-in aiograpi Client with the Source's proxy, ready for use
    client: Any = field(repr=False)


class WebLogin:
    """One login attempt per call, never retried inside; raises PlatformError on a refusal.

    `deadline` bounds the browser part (real time), keep it under the flow's login timeout.
    """

    def __init__(
        self,
        *,
        clock: Clock,
        sessions: SessionOpener | None = None,
        base_url: str = INSTAGRAM,
        deadline: float = 120.0,
        poll_interval: float = 1.0,
    ) -> None:
        self._browser = BrowserLogin(
            clock=clock, base_url=base_url, deadline=deadline, poll_interval=poll_interval
        )
        self._sessions = sessions or AiograpiSessions()

    async def login(self, request: LoginRequest) -> WebSession:
        # checked before anything starts: no proxy, no browser
        browser_proxy(request.proxy)
        device = decode_device(request.device)
        browser = await self._browser.login(
            request.source_id,
            request.login,
            request.password,
            request.totp_secret,
            request.proxy,
            device.browser,
        )
        try:
            opened: OpenedSession = await self._sessions.open(
                request.source_id, browser.sessionid, request.proxy, device.app
            )
        except PlatformError as exc:
            if exc.failure is not Failure.NETWORK:
                raise
            # the password went out already: a quick retry as "offline" would be a second login
            raise PlatformError(Failure.REJECTED, f"after the web login: {exc.detail}") from None
        used = replace(device, browser=browser.profile, app=opened.app_device)
        logged_in = LoggedIn(account=opened.account, session=opened.session, device=encode_device(used))
        return WebSession(logged_in=logged_in, client=opened.client)
