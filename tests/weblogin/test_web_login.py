"""The browser login against a pretend Instagram behind a local proxy.

Real headless Chromium, no network: the pretend Instagram is reachable only through the
proxy. The aiograpi side (login_by_sessionid) is replaced by a recording fake.
"""

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr

from ig_connector.instagram import Account, Device, Failure, LoginRequest, PlatformError, SessionData
from ig_connector.instagram.weblogin import OpenedSession, WebLogin
from ig_connector.proxy import ProxyAssignment
from tests.support.fake_clock import FakeClock
from tests.support.instagram_web import BASE_URL, ProxyServer, WebInstagram, proxy_assignment, running_proxy

# RFC 6238 appendix B: this secret at 59 s after the epoch gives ...287082
TOTP_SECRET = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
TOTP_CODE = "287082"
AT_59_SECONDS = datetime(1970, 1, 1, 0, 0, 59, tzinfo=UTC)


def _chromium_installed() -> bool:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        return Path(p.chromium.executable_path).exists()


pytestmark = pytest.mark.skipif(
    not _chromium_installed(),
    reason="Chromium for Playwright is not installed: uv run playwright install chromium",
)


@dataclass
class Opened:
    source_id: UUID
    sessionid: str
    proxy: ProxyAssignment
    app_device: Mapping[str, Any] | None


@dataclass
class FakeSessions:
    """aiograpi's login_by_sessionid, recorded: the session is the sessionid it got."""

    opened: list[Opened] = field(default_factory=list)
    username: str = "anna.shop"
    # the refusal aiograpi answers with
    failure: Failure | None = None

    async def open(
        self,
        source_id: UUID,
        sessionid: SecretStr,
        proxy: ProxyAssignment,
        app_device: Mapping[str, Any] | None,
    ) -> OpenedSession:
        self.opened.append(Opened(source_id, sessionid.get_secret_value(), proxy, app_device))
        if self.failure is not None:
            raise PlatformError(self.failure, "scripted")
        user_id = sessionid.get_secret_value().split("%3A")[0]
        return OpenedSession(
            account=Account(external_id=user_id, username=self.username),
            session=SessionData(f"session-{user_id}".encode()),
            app_device={"uuids": {"phone_id": "p-1"}, "device_settings": {"model": "x"}},
            client=None,
        )


@pytest.fixture
def instagram() -> WebInstagram:
    return WebInstagram()


@pytest.fixture
async def proxy(instagram: WebInstagram) -> AsyncIterator[ProxyServer]:
    async with running_proxy(instagram) as server:
        yield server


@pytest.fixture
def sessions() -> FakeSessions:
    return FakeSessions()


@pytest.fixture
def web(sessions: FakeSessions) -> WebLogin:
    return WebLogin(
        clock=FakeClock(AT_59_SECONDS), sessions=sessions, base_url=BASE_URL, poll_interval=0.1, deadline=10
    )


def request(
    proxy: ProxyAssignment,
    *,
    login: str = "anna.shop",
    password: str = "correct horse",  # noqa: S107
    totp_secret: str | None = TOTP_SECRET,
    device: Device | None = None,
    source_id: UUID | None = None,
) -> LoginRequest:
    return LoginRequest(
        source_id=source_id or uuid4(),
        login=login,
        password=SecretStr(password),
        totp_secret=None if totp_secret is None else SecretStr(totp_secret),
        proxy=proxy,
        device=device,
    )


async def test_password_and_totp_give_the_account_and_its_session(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram, sessions: FakeSessions
) -> None:
    source_id = uuid4()

    result = await web.login(request(proxy.assignment, source_id=source_id))

    assert instagram.codes == [TOTP_CODE]
    assert result.logged_in.account == Account(external_id="1789", username="anna.shop")
    assert result.logged_in.session == SessionData(b"session-1789")
    assert sessions.opened == [Opened(source_id, instagram.sessionid, proxy.assignment, None)]
    assert proxy.foreign == []


async def test_a_secret_with_spaces_and_lower_case_works(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram
) -> None:
    secret = " ".join(TOTP_SECRET[i : i + 4] for i in range(0, len(TOTP_SECRET), 4)).lower()

    await web.login(request(proxy.assignment, totp_secret=secret))

    assert instagram.codes == [TOTP_CODE]


async def test_account_without_2fa_logs_in_with_the_password_alone(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram
) -> None:
    instagram.totp_code = None

    result = await web.login(request(proxy.assignment, totp_secret=None))

    assert result.logged_in.account.external_id == "1789"
    assert instagram.codes == []


async def test_cookie_banner_is_accepted(web: WebLogin, proxy: ProxyServer, instagram: WebInstagram) -> None:
    instagram.cookie_banner = True

    result = await web.login(request(proxy.assignment))

    assert result.logged_in.account.external_id == "1789"
    [sent] = [s for s in instagram.seen if s.method == "POST" and s.path == "/accounts/login/ajax/"]
    assert sent.cookies.get("consent") == "1"


async def _refusal(web: WebLogin, login: LoginRequest) -> PlatformError:
    with pytest.raises(PlatformError) as caught:
        await web.login(login)
    return caught.value


async def test_wrong_password_is_bad_credentials(
    web: WebLogin, proxy: ProxyServer, sessions: FakeSessions
) -> None:
    error = await _refusal(web, request(proxy.assignment, password="wrong"))

    assert error.failure is Failure.BAD_CREDENTIALS
    assert sessions.opened == []


async def test_wrong_totp_secret_is_bad_credentials_after_one_code(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram
) -> None:
    error = await _refusal(web, request(proxy.assignment, totp_secret="JBSWY3DPEHPK3PXP"))

    assert error.failure is Failure.BAD_CREDENTIALS
    assert len(instagram.codes) == 1


async def test_a_secret_that_is_not_base32_is_refused_before_the_browser(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram
) -> None:
    error = await _refusal(web, request(proxy.assignment, totp_secret="not a secret!"))

    assert error.failure is Failure.BAD_CREDENTIALS
    assert instagram.seen == []


async def test_2fa_without_a_secret_is_a_challenge(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram
) -> None:
    error = await _refusal(web, request(proxy.assignment, totp_secret=None))

    assert error.failure is Failure.CHALLENGE
    assert instagram.codes == []


async def test_2fa_by_sms_is_a_challenge(web: WebLogin, proxy: ProxyServer, instagram: WebInstagram) -> None:
    instagram.after_password = "/accounts/login/two_step_verification/?sms=1"

    error = await _refusal(web, request(proxy.assignment))

    assert error.failure is Failure.CHALLENGE
    assert instagram.codes == []


async def test_checkpoint_is_a_challenge(web: WebLogin, proxy: ProxyServer, instagram: WebInstagram) -> None:
    instagram.after_password = "/challenge/action/AXH?next=%2F"

    error = await _refusal(web, request(proxy.assignment))

    assert error.failure is Failure.CHALLENGE


async def test_an_unknown_page_is_refused_with_a_snapshot_in_the_log(
    sessions: FakeSessions, proxy: ProxyServer, instagram: WebInstagram, caplog: pytest.LogCaptureFixture
) -> None:
    web = WebLogin(
        clock=FakeClock(AT_59_SECONDS), sessions=sessions, base_url=BASE_URL, poll_interval=0.1, deadline=3
    )
    instagram.after_password = "/brand/new/screen/"
    caplog.set_level(logging.INFO, logger="ig_connector")

    error = await _refusal(web, request(proxy.assignment))

    assert error.failure is Failure.REJECTED
    snapshots = [r for r in caplog.records if getattr(r, "page_path", None) == "/brand/new/screen/"]
    assert snapshots
    assert "Something entirely new" in snapshots[-1].page_excerpt  # type: ignore[attr-defined]


async def test_unreachable_proxy_is_a_network_failure(web: WebLogin, proxy: ProxyServer) -> None:
    dead = proxy_assignment(port=1)

    error = await _refusal(web, request(dead))

    assert error.failure is Failure.NETWORK


@pytest.mark.parametrize(
    "url",
    ["", "not a url", "http://user:pass@:8080", "ftp://127.0.0.1:21"],
)
async def test_the_browser_never_starts_without_a_usable_proxy(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram, url: str
) -> None:
    error = await _refusal(web, request(ProxyAssignment(assignment_id=1, url=SecretStr(url))))

    assert error.failure is Failure.NETWORK
    assert instagram.seen == []
    assert proxy.unauthorised == 0


async def test_every_request_goes_through_the_proxy_with_its_credentials(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram
) -> None:
    await web.login(request(proxy.assignment))

    # the pretend host does not resolve: whatever loaded came through the proxy
    assert {s.path for s in instagram.seen} >= {"/accounts/login/", "/accounts/login/two_step_verification/"}
    assert proxy.foreign == []


async def test_the_device_is_reused_on_the_next_login(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram, sessions: FakeSessions
) -> None:
    first = await web.login(request(proxy.assignment))
    instagram.seen.clear()

    second = await web.login(request(proxy.assignment, device=first.logged_in.device))

    first_visit = instagram.seen[0]
    assert first_visit.cookies.get("ig_did") == "device-1"
    # the old session is not carried into a new login
    assert "sessionid" not in first_visit.cookies
    assert "HeadlessChrome" not in first_visit.user_agent
    assert {s.user_agent for s in instagram.seen} == {first_visit.user_agent}
    assert sessions.opened[1].app_device == {"uuids": {"phone_id": "p-1"}, "device_settings": {"model": "x"}}
    assert second.logged_in.device.data != b""


async def test_the_device_keeps_no_session(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram
) -> None:
    result = await web.login(request(proxy.assignment))

    assert instagram.sessionid.encode() not in result.logged_in.device.data


async def test_password_code_and_session_never_reach_the_log(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    await web.login(request(proxy.assignment))
    instagram.after_password = "/brand/new/screen/"
    short = WebLogin(
        clock=FakeClock(AT_59_SECONDS),
        sessions=FakeSessions(),
        base_url=BASE_URL,
        poll_interval=0.1,
        deadline=2,
    )
    await _refusal(short, request(proxy.assignment))

    logged = "\n".join(f"{r.getMessage()} {r.__dict__}" for r in caplog.records)
    for secret in ("correct horse", TOTP_CODE, TOTP_SECRET, "Qm9ndXNTZXNzaW9u", "px-pa"):
        assert secret not in logged


async def test_a_device_it_cannot_read_is_replaced_by_a_new_one(
    web: WebLogin, proxy: ProxyServer, sessions: FakeSessions
) -> None:
    result = await web.login(request(proxy.assignment, device=Device(b"device-1")))

    assert result.logged_in.account.external_id == "1789"
    assert sessions.opened[0].app_device is None


async def test_a_network_failure_after_the_browser_login_is_not_retry_safe(
    web: WebLogin, proxy: ProxyServer, sessions: FakeSessions
) -> None:
    sessions.failure = Failure.NETWORK

    error = await _refusal(web, request(proxy.assignment))

    # the password already reached Instagram: CRM must not retry it as an offline Source
    assert error.failure is Failure.REJECTED


async def test_a_login_form_without_a_password_field_ends_in_time(
    sessions: FakeSessions, proxy: ProxyServer, instagram: WebInstagram
) -> None:
    web = WebLogin(
        clock=FakeClock(AT_59_SECONDS), sessions=sessions, base_url=BASE_URL, poll_interval=0.1, deadline=2
    )
    instagram.password_field = False

    async with asyncio.timeout(10):
        error = await _refusal(web, request(proxy.assignment))

    assert error.failure is Failure.REJECTED
    assert not any(s.method == "POST" for s in instagram.seen)


async def test_a_cancelled_login_leaves_no_browser_running(
    web: WebLogin, proxy: ProxyServer, instagram: WebInstagram
) -> None:
    instagram.after_password = "/brand/new/screen/"
    before = _browsers()
    task = asyncio.create_task(web.login(request(proxy.assignment)))
    async with asyncio.timeout(10):
        while not any(s.path == "/brand/new/screen/" for s in instagram.seen):  # noqa: ASYNC110
            await asyncio.sleep(0.05)
    assert _browsers() - before

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not _browsers() - before


def _browsers() -> set[int]:
    """Our Chromium processes: descendants of this process whose command is a browser."""
    procs: dict[int, tuple[int, str]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
            cmd = (entry / "cmdline").read_bytes().decode(errors="replace")
        except OSError:
            continue
        procs[int(entry.name)] = (int(stat.rsplit(")", 1)[1].split()[1]), cmd)
    me = os.getpid()

    def mine(pid: int) -> bool:
        while pid > 1:
            pid = procs.get(pid, (0, ""))[0]
            if pid == me:
                return True
        return False

    return {pid for pid, (_, cmd) in procs.items() if "chrom" in cmd.lower() and mine(pid)}
