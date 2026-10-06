"""A pretend Instagram web login, reachable only as an HTTP proxy.

The browser asks the proxy for `http://www.instagram.test/...`: that host does not resolve,
so a page loads only if the browser went through the proxy. The proxy answers itself with
small pages that carry Instagram's texts and paths (seen live 06.10).
"""

import asyncio
import base64
import html
import itertools
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from http.cookies import SimpleCookie
from urllib.parse import parse_qs, urlsplit

from pydantic import SecretStr

from ig_connector.proxy import ProxyAssignment

BASE_URL = "http://www.instagram.test"
PROXY_USER = "px-user"
# ":" must survive URL encoding
PROXY_PASSWORD = "px-pa:ss"


@dataclass(frozen=True, slots=True)
class Seen:
    method: str
    path: str
    cookies: dict[str, str]
    user_agent: str


@dataclass
class WebInstagram:
    """What the pretend Instagram does; tests change the knobs before logging in."""

    login: str = "anna.shop"
    password: str = "correct horse"
    # the code the 2FA page accepts; None: the account has no 2FA
    totp_code: str | None = "287082"
    user_id: str = "1789"
    # where the password leads instead of 2FA / home (a challenge, an SMS code page, ...)
    after_password: str | None = None
    # where the code leads instead of home
    after_code: str | None = None
    cookie_banner: bool = False
    # False: the form asks for the username first, like Instagram's two-step form
    password_field: bool = True
    seen: list[Seen] = field(default_factory=list)
    codes: list[str] = field(default_factory=list)
    _devices: "itertools.count[int]" = field(default_factory=lambda: itertools.count(1))

    @property
    def sessionid(self) -> str:
        return f"{self.user_id}%3AQm9ndXNTZXNzaW9uSWRGb3JUZXN0cw%3A12%3AAYc"

    def handle(self, method: str, path: str, cookies: dict[str, str], form: dict[str, str]) -> "Reply":
        reply = self._route(method, path, cookies, form)
        if "ig_did" not in cookies:
            # the device cookie, set on the first visit like Instagram's own
            reply.cookies.append(f"ig_did=device-{next(self._devices)}; Path=/; Max-Age=31536000")
        return reply

    def _route(self, method: str, path: str, cookies: dict[str, str], form: dict[str, str]) -> "Reply":
        route = urlsplit(path).path
        if route == "/accounts/login/" and method == "GET":
            banner = self.cookie_banner and "consent" not in cookies
            return Reply(page(_login_form(banner=banner, password=self.password_field)))
        if route == "/accounts/login/ajax/" and method == "POST":
            if form.get("username") != self.login or form.get("password") != self.password:
                return Reply(
                    page(
                        "<p>Sorry, your password was incorrect. Please double-check your password.</p>"
                        + _login_form(banner=False)
                    )
                )
            if self.after_password is not None:
                return redirect(self.after_password)
            if self.totp_code is not None:
                return redirect("/accounts/login/two_step_verification/?next=%2F")
            return self._logged_in("/")
        if route == "/accounts/login/two_step_verification/" and method == "GET":
            return Reply(page(_code_form(urlsplit(path).query)))
        if route == "/accounts/login/two_step_verification/" and method == "POST":
            self.codes.append(form.get("c", ""))
            if form.get("c") != self.totp_code:
                return Reply(page("<p>This code isn't valid. Check the code and try again.</p>"))
            return self._logged_in(self.after_code or "/accounts/onetap/?next=%2F")
        if route.startswith("/challenge/"):
            return Reply(page("<h2>Help us confirm it's you</h2><button>Send security code</button>"))
        if route == "/accounts/onetap/":
            return Reply(page("<h2>Save your login info?</h2><button>Save info</button>"))
        if route == "/":
            return Reply(page("<h2>Home</h2>"))
        if route == "/favicon.ico":
            return Reply("", status=404)
        return Reply(page("<h2>Something entirely new</h2>"))

    def _logged_in(self, to: str) -> "Reply":
        reply = redirect(to)
        reply.cookies.append(f"sessionid={self.sessionid}; Path=/; HttpOnly")
        reply.cookies.append(f"ds_user_id={self.user_id}; Path=/")
        return reply


@dataclass
class Reply:
    body: str
    status: int = 200
    location: str | None = None
    cookies: list[str] = field(default_factory=list)


def redirect(to: str) -> Reply:
    return Reply("", status=302, location=to)


def page(body: str) -> str:
    return f"<!doctype html><html><head><title>Instagram</title></head><body>{body}</body></html>"


def _login_form(*, banner: bool, password: bool = True) -> str:
    consent = (
        '<div id="banner" style="position:fixed;inset:0;background:#fff">'
        "<p>Allow the use of cookies by Instagram?</p>"
        "<button onclick=\"document.cookie='consent=1; path=/';"
        "document.getElementById('banner').remove()\">Allow all cookies</button></div>"
        if banner
        else ""
    )
    password_input = '<input name="password" type="password" aria-label="Password">' if password else ""
    return (
        consent + '<form method="post" action="/accounts/login/ajax/">'
        '<input name="username" aria-label="Phone number, username, or email">'
        + password_input
        + '<button type="submit">Log in</button></form><a href="#">Forgot password?</a>'
    )


def _code_form(query: str) -> str:
    if "sms" in query:
        text = "Check your text messages. Enter the 6-digit code we sent to your number ending in 42."
    else:
        text = (
            "Go to your authentication app. Enter the 6-digit code for this account from the "
            "two-factor authentication app that you set up."
        )
    # the field has none of the names the old selectors look for, as seen live 06.10
    return (
        f"<p>{html.escape(text)}</p>"
        f'<form method="post" action="/accounts/login/two_step_verification/?{html.escape(query)}">'
        '<input type="hidden" name="t" value="x"><input name="c" type="text" aria-label="Code">'
        "</form><button>Try another way</button>"
    )


class ProxyServer:
    """An HTTP proxy that requires Basic credentials and serves WebInstagram for its host."""

    def __init__(self, instagram: WebInstagram) -> None:
        self.instagram = instagram
        self.port = 0
        # requests that came without proxy credentials
        self.unauthorised = 0
        # requests for a host other than the pretend Instagram
        self.foreign: list[str] = []

    @property
    def assignment(self) -> ProxyAssignment:
        return proxy_assignment(self.port)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            request_line, *header_lines = head.split("\r\n")
            method, target, _ = request_line.split(" ", 2)
            headers = {
                k.strip().lower(): v.strip() for k, _, v in (h.partition(":") for h in header_lines if h)
            }
            body = await reader.readexactly(int(headers.get("content-length", "0")))
            writer.write(self._answer(method, target, headers, body))
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, ValueError):
            pass
        finally:
            writer.close()

    def _answer(self, method: str, target: str, headers: dict[str, str], body: bytes) -> bytes:
        expected = base64.b64encode(f"{PROXY_USER}:{PROXY_PASSWORD}".encode()).decode()
        if headers.get("proxy-authorization") != f"Basic {expected}":
            self.unauthorised += 1
            return _http(407, "", extra=['Proxy-Authenticate: Basic realm="proxy"'])
        url = urlsplit(target)
        if method == "CONNECT" or f"{url.scheme}://{url.netloc}" != BASE_URL:
            self.foreign.append(target)
            return _http(502, "")
        jar = SimpleCookie(headers.get("cookie", ""))
        cookies = {k: m.value for k, m in jar.items()}
        path = url.path + (f"?{url.query}" if url.query else "")
        self.instagram.seen.append(Seen(method, url.path, cookies, headers.get("user-agent", "")))
        form = {k: v[0] for k, v in parse_qs(body.decode()).items()}
        reply = self.instagram.handle(method, path, cookies, form)
        extra = [f"Set-Cookie: {c}" for c in reply.cookies]
        if reply.location is not None:
            extra.append(f"Location: {BASE_URL}{reply.location}")
        return _http(reply.status, reply.body, extra=extra)


def proxy_assignment(port: int) -> ProxyAssignment:
    user = PROXY_USER
    password = PROXY_PASSWORD.replace(":", "%3A")
    return ProxyAssignment(assignment_id=7, url=SecretStr(f"http://{user}:{password}@127.0.0.1:{port}"))


def _http(status: int, body: str, *, extra: list[str] | None = None) -> bytes:
    data = body.encode()
    lines = [
        f"HTTP/1.1 {status} X",
        "Content-Type: text/html; charset=utf-8",
        f"Content-Length: {len(data)}",
        "Connection: close",
        *(extra or []),
    ]
    return ("\r\n".join(lines) + "\r\n\r\n").encode() + data


@asynccontextmanager
async def running_proxy(instagram: WebInstagram) -> AsyncIterator[ProxyServer]:
    proxy = ProxyServer(instagram)
    server = await asyncio.start_server(proxy._serve, "127.0.0.1", 0)
    proxy.port = server.sockets[0].getsockname()[1]
    async with server:
        yield proxy
