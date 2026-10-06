"""Instagram's web login in headless Chromium: login, password, TOTP code -> `sessionid`.

One attempt, never retried inside. The browser starts only with the Source's proxy and
lives only for the attempt. Pages are told apart by URL and text (`pages.recognise`); the
markup is used only to find the fields, with a fallback for the code field, which on the
new 2FA page carries none of the old names (seen live in October 2026).
"""

import asyncio
import binascii
import logging
import re
from contextlib import suppress
from dataclasses import dataclass, replace
from typing import Any, cast
from urllib.parse import unquote, urlsplit
from uuid import UUID

from aiograpi.mixins.totp import TOTP
from playwright.async_api import BrowserContext, ProxySettings, StorageState, async_playwright
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page as BrowserPage
from pydantic import SecretStr

from ig_connector.clock import Clock
from ig_connector.instagram import Failure, PlatformError, UnsupportedProxy
from ig_connector.instagram.weblogin.device import BrowserProfile
from ig_connector.instagram.weblogin.pages import Page, recognise
from ig_connector.masking import MASK, mask_text
from ig_connector.proxy import ProxyAssignment

__all__ = ["BrowserLoggedIn", "BrowserLogin", "browser_proxy"]

log = logging.getLogger(__name__)

INSTAGRAM = "https://www.instagram.com"

_USERNAME_FIELD = 'input[name="username"], input[name="email"], input[autocomplete="username"]'
_PASSWORD_FIELD = 'input[name="password"], input[name="pass"], input[type="password"]'  # noqa: S105
_CODE_FIELD = (
    'input[name="verificationCode"], input[autocomplete="one-time-code"], input[name="approvals_code"]'
)
# the new 2FA page: the only visible text input on a page that asks for the code
_ANY_TEXT_INPUT = (
    'input:visible:not([type="password"]):not([type="hidden"]):not([type="checkbox"]):not([type="submit"])'
)
_CONSENT = re.compile(r"allow all|accept all|only allow essential|decline optional", re.I)
_PROXY_SCHEMES = frozenset({"http", "https", "socks5"})
# the address the browser shows when it could not load a page at all
_BROWSER_ERROR_PAGE = "chrome-error://"
_EXCERPT = 300
# a field that is there fills at once; waiting longer only hides a page we do not know
_ACTION_TIMEOUT_MS = 10_000
# WebRTC talks UDP past an HTTP proxy and would show the server's own address
_CHROMIUM_ARGS = ["--force-webrtc-ip-handling-policy", "--webrtc-ip-handling-policy=disable_non_proxied_udp"]


@dataclass(frozen=True, slots=True)
class BrowserLoggedIn:
    sessionid: SecretStr
    # the profile after the login, without the session cookie
    profile: BrowserProfile


def browser_proxy(proxy: ProxyAssignment) -> ProxySettings:
    """Playwright's proxy settings from the assignment; refuses anything it cannot use.

    There is no browser without a proxy: an empty or broken address is a refusal here,
    before Chromium starts, never a direct connection.
    """
    try:
        url = urlsplit(proxy.url.get_secret_value())
        host, port = url.hostname, url.port
    except ValueError:
        raise PlatformError(Failure.NETWORK, "proxy address unusable") from None
    if url.scheme not in _PROXY_SCHEMES or not host or port is None:
        raise PlatformError(Failure.NETWORK, "proxy address unusable")
    if url.scheme == "socks5" and url.username:
        raise UnsupportedProxy("the browser cannot use a SOCKS5 proxy with credentials")
    server_host = f"[{host}]" if ":" in host else host
    settings = ProxySettings(server=f"{url.scheme}://{server_host}:{port}")
    if url.username:
        settings["username"] = unquote(url.username)
        settings["password"] = unquote(url.password or "")
    return settings


class BrowserLogin:
    """The browser half of the web login; `clock` is for TOTP codes only.

    Waiting for pages is real time, like the browser itself: `deadline` bounds the whole
    attempt below the flow's own timeout, `poll_interval` is how often the page is read.
    """

    def __init__(
        self,
        *,
        clock: Clock,
        base_url: str = INSTAGRAM,
        deadline: float = 120.0,
        poll_interval: float = 1.0,
        headless: bool = True,
    ) -> None:
        self._clock = clock
        self._base_url = base_url.rstrip("/")
        self._deadline = deadline
        self._poll = poll_interval
        self._headless = headless

    async def login(
        self,
        source_id: UUID,
        login: str,
        password: SecretStr,
        totp_secret: SecretStr | None,
        proxy: ProxyAssignment,
        profile: BrowserProfile,
    ) -> BrowserLoggedIn:
        settings = browser_proxy(proxy)
        totp = _totp(totp_secret)
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=self._headless, proxy=settings, args=_CHROMIUM_ARGS
            )
            try:
                if profile.user_agent is None:
                    profile = replace(profile, user_agent=_user_agent(browser.version))
                state = cast(StorageState, profile.storage_state) if profile.storage_state else None
                context = await browser.new_context(
                    user_agent=profile.user_agent,
                    viewport={"width": profile.viewport_width, "height": profile.viewport_height},
                    locale=profile.locale,
                    timezone_id=profile.timezone_id,
                    storage_state=state,
                )
                context.set_default_timeout(_ACTION_TIMEOUT_MS)
                # an old session cookie in the profile would skip the login it is meant to redo
                await context.clear_cookies(name="sessionid")
                attempt = _Attempt(self, source_id, context, await context.new_page(), login, password, totp)
                sessionid = await attempt.run()
                after = cast(dict[str, Any], await context.storage_state())
            finally:
                # a crashed browser must not hide the attempt's own outcome
                with suppress(PlaywrightError):
                    await browser.close()
        return BrowserLoggedIn(sessionid=sessionid, profile=profile.with_state(after))


class _Attempt:
    def __init__(
        self,
        owner: BrowserLogin,
        source_id: UUID,
        context: BrowserContext,
        page: BrowserPage,
        login: str,
        password: SecretStr,
        totp: TOTP | None,
    ) -> None:
        self._owner = owner
        self._source_id = source_id
        self._context = context
        self._page = page
        self._login = login
        self._password = password
        self._totp = totp
        self._submitted = False
        self._code_sent: str | None = None
        self._seen: Page | None = None

    async def run(self) -> SecretStr:
        owner = self._owner
        loop = asyncio.get_running_loop()
        until = loop.time() + owner._deadline
        try:
            await self._page.goto(f"{owner._base_url}/accounts/login/", wait_until="domcontentloaded")
        except PlaywrightError:
            await self._snapshot("not_loaded", "")
            raise PlatformError(Failure.NETWORK, "the login page did not load") from None
        while True:
            if sessionid := await self._session_cookie():
                log.info("web login got a session", extra=self._ids())
                return sessionid
            text = await self._text()
            try:
                await self._step(text)
            except PlaywrightError:
                # a field gone or a click that timed out: the page is not what we took it for
                await self._snapshot("browser_step_failed", text)
                raise PlatformError(Failure.REJECTED, "the login page did not behave as expected") from None
            if loop.time() >= until:
                await self._snapshot("no_outcome", text)
                raise PlatformError(Failure.REJECTED, "no known outcome of the web login in time")
            await asyncio.sleep(owner._poll)

    async def _step(self, text: str) -> None:
        url = self._page.url
        if url.startswith(_BROWSER_ERROR_PAGE):
            await self._snapshot("browser_error", text)
            if not self._submitted:
                raise PlatformError(Failure.NETWORK, "the login page did not load")
            # the password may have reached Instagram: not a retry-safe network failure
            raise PlatformError(Failure.REJECTED, "the connection broke after the password was sent")
        page = recognise(url, text)
        if page is not self._seen:
            log.debug("web login page", extra={**self._ids(), "page": page, "page_path": _path(url)})
            self._seen = page
        match page:
            case Page.CHALLENGE:
                await self._refuse(Failure.CHALLENGE, "Instagram asks for a check", text)
            case Page.RATE_LIMITED:
                await self._refuse(Failure.RATE_LIMITED, "Instagram asks to wait before logging in", text)
            case Page.WRONG_CREDENTIALS:
                await self._refuse(Failure.BAD_CREDENTIALS, "wrong login, password or 2FA code", text)
            case Page.TWO_FACTOR_OTHER:
                await self._refuse(Failure.CHALLENGE, "2FA code by SMS, WhatsApp or email", text)
            case Page.TWO_FACTOR_APP:
                await self._enter_code(text)
            case _ if not self._submitted:
                await self._submit_password()

    async def _submit_password(self) -> None:
        page = self._page
        consent = page.get_by_role("button", name=_CONSENT).first
        if await _visible(consent):
            await consent.click()
        username = page.locator(_USERNAME_FIELD).first
        password = page.locator(_PASSWORD_FIELD).first
        # a form that asks for the username alone is not one we know: wait, then refuse
        if not (await _visible(username) and await _visible(password)):
            return
        await username.fill(self._login)
        await password.fill(self._password.get_secret_value())
        # from here on the password may have reached Instagram
        self._submitted = True
        await password.press("Enter")
        log.info("web login password sent", extra=self._ids())

    async def _enter_code(self, text: str) -> None:
        if self._code_sent is not None:
            return
        if self._totp is None:
            await self._refuse(Failure.CHALLENGE, "2FA is on and no TOTP secret was given", text)
            return
        field = self._page.locator(_CODE_FIELD).first
        if not await _visible(field):
            candidates = self._page.locator(_ANY_TEXT_INPUT)
            if await candidates.count() != 1:
                return
            field = candidates.first
        timecode = int(self._clock_now()) // self._totp.interval
        code = cast(str, self._totp.generate_otp(timecode))
        self._code_sent = code
        await field.fill(code)
        await field.press("Enter")
        log.info("web login 2FA code sent", extra=self._ids())

    def _clock_now(self) -> float:
        return self._owner._clock.now().timestamp()

    async def _refuse(self, failure: Failure, detail: str, text: str) -> None:
        await self._snapshot(str(failure), text)
        raise PlatformError(failure, detail)

    async def _snapshot(self, outcome: str, text: str) -> None:
        """The page as the attempt ended, for the log: path and a short excerpt, no secrets."""
        excerpt = " ".join(text.split())
        for secret in (self._password.get_secret_value(), self._code_sent):
            if secret:
                excerpt = excerpt.replace(secret, MASK)
        log.info(
            "web login ended",
            extra={
                **self._ids(),
                "outcome": outcome,
                "page_path": _path(self._page.url),
                "page_excerpt": mask_text(excerpt)[:_EXCERPT],
                "password_sent": self._submitted,
                "code_sent": self._code_sent is not None,
            },
        )

    async def _session_cookie(self) -> SecretStr | None:
        for cookie in await self._context.cookies(self._owner._base_url):
            if cookie.get("name") == "sessionid" and cookie.get("value"):
                return SecretStr(cookie["value"])
        return None

    async def _text(self) -> str:
        try:
            return await self._page.inner_text("body", timeout=2000)
        except PlaywrightError:
            # mid-navigation: the next poll reads the new page
            return ""

    def _ids(self) -> dict[str, Any]:
        return {"source_id": str(self._source_id)}


async def _visible(locator: Any) -> bool:
    try:
        return bool(await locator.is_visible())
    except PlaywrightError:
        return False


def _totp(secret: SecretStr | None) -> TOTP | None:
    if secret is None:
        return None
    # Instagram shows the key in groups of four, people paste it as shown
    seed = "".join(secret.get_secret_value().split()).upper()
    totp = TOTP(seed)
    try:
        if not seed or not totp.byte_secret():
            raise ValueError
    except (binascii.Error, ValueError):
        raise PlatformError(Failure.BAD_CREDENTIALS, "the TOTP secret is not a base32 key") from None
    return totp


def _user_agent(browser_version: str) -> str:
    # the headless build calls itself HeadlessChrome: present it as the desktop browser it is
    major = browser_version.split(".", 1)[0]
    return (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{major}.0.0.0 Safari/537.36"
    )


def _path(url: str) -> str:
    try:
        return urlsplit(url).path[:120]
    except ValueError:
        return ""
