"""A pretend Instagram private API under a real aiograpi Client, at the HTTP transport.

Everything above the transport is the real thing: aiograpi builds the requests, reads
the status codes and parses the answers; only the network is replaced. Answers are the
shapes seen live on 06.10 (send, history) and aiograpi's own extractors' expectations.
"""

import asyncio
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl

import httpx
from curl_cffi.const import CurlECode
from curl_cffi.requests.exceptions import RequestException
from pydantic import SecretStr

from ig_connector.instagram import SessionData
from ig_connector.instagram.adapter.client import new_client
from ig_connector.proxy import ProxyAssignment

VIEWER = "1789"
SESSIONID = f"{VIEWER}%3A" + "s" * 40
PROXY = ProxyAssignment(assignment_id=7, url=SecretStr("http://u:p@127.0.0.1:1"))


def session_data(viewer: str = VIEWER) -> SessionData:
    """A saved Session as the web login stores it (aiograpi get_settings), cut to the login."""
    settings = {
        "authorization_data": {
            "ds_user_id": viewer,
            "sessionid": f"{viewer}%3A" + "s" * 40,
            "should_use_header_over_cookies": True,
        },
        "cookies": {},
    }
    return SessionData(json.dumps(settings).encode())


@dataclass(frozen=True)
class Call:
    method: str
    # without /api/v1/
    path: str
    params: Mapping[str, str]
    form: Mapping[str, str]


Reply = Mapping[str, Any] | httpx.Response | Exception


def status(code: int, body: Mapping[str, Any] | None = None) -> httpx.Response:
    return httpx.Response(code, json=body or {"status": "fail"})


def curl_failure(code: CurlECode) -> httpx.ConnectError:
    """What aiograpi's curl transport raises: every curl failure as an httpx ConnectError."""
    try:
        raise RequestException(f"curl: ({int(code)})", code)
    except RequestException as curl:
        error = httpx.ConnectError("curl private transport failed (RequestException)")
        error.__cause__ = curl
        return error


@dataclass
class FakeApi:
    # by path (no /api/v1/): one reply per call, the last one repeats
    routes: dict[str, list[Reply]] = field(default_factory=dict)
    calls: list[Call] = field(default_factory=list)
    # by path: the answer waits until the event is set (a request in flight)
    hold: dict[str, asyncio.Event] = field(default_factory=dict)

    def on(self, path: str, *replies: Reply) -> None:
        self.routes[path] = list(replies)

    def paths(self) -> list[str]:
        return [c.path for c in self.calls]

    async def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api/v1/")
        form = dict(parse_qsl(request.content.decode())) if request.content else {}
        self.calls.append(Call(request.method, path, dict(request.url.params), form))
        held = self.hold.get(path)
        if held is not None:
            await held.wait()
        replies = self.routes.get(path)
        if not replies:
            return httpx.Response(404, json={"status": "fail", "message": f"no route {path}"})
        reply = replies.pop(0) if len(replies) > 1 else replies[0]
        if isinstance(reply, Exception):
            raise reply
        if isinstance(reply, httpx.Response):
            return reply
        return httpx.Response(200, json=reply)

    def clients(self) -> Callable[[Mapping[str, Any] | None, str], Any]:
        """The adapter's Client factory, with this pretend Instagram as the network."""

        def make(settings: Mapping[str, Any] | None, proxy_url: str) -> Any:
            client = new_client(settings, proxy_url)
            # public and graphql too: aiograpi falls back to them, and they must not reach out
            # and every time aiograpi rebuilds its HTTP clients (login_by_sessionid does)
            for session in (client.private, client.public, client.graphql):

                def pretend(session: Any = session) -> None:
                    session._client = httpx.AsyncClient(transport=httpx.MockTransport(self.handle))

                session._set_client = pretend
                pretend()
            # a broadcast's own connection (redirect handling stays the Client's)
            client.broadcast_transport = lambda: httpx.MockTransport(self.handle)
            # no pause between requests, also after aiograpi re-reads its settings (init)
            client.request_timeout = 0
            client.settings["request_timeout"] = 0

            async def no_delay() -> None:
                return None

            client.small_delay = no_delay
            return client

        return make


def micros(at: datetime) -> int:
    return int(at.timestamp()) * 1_000_000 + at.microsecond


def item(
    item_id: str,
    at: datetime,
    *,
    user_id: str,
    text: str | None = "hello",
    item_type: str = "text",
    client_context: str | None = None,
    by_viewer: bool | None = None,
    reply_to: str | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        "item_id": item_id,
        "user_id": int(user_id),
        "timestamp": micros(at),
        "item_type": item_type,
    }
    if text is not None:
        data["text"] = text
    if client_context is not None:
        data["client_context"] = client_context
    if by_viewer is not None:
        data["is_sent_by_viewer"] = by_viewer
    if reply_to is not None:
        data["replied_to_message"] = {"item_id": reply_to, "user_id": int(VIEWER), "timestamp": 1}
    return data


def user(pk: str, username: str | None = None, full_name: str = "") -> dict[str, Any]:
    return {"pk": int(pk), "pk_id": pk, "username": username or f"user{pk}", "full_name": full_name}


def thread(
    thread_id: str,
    users: list[dict[str, Any]],
    items: list[dict[str, Any]],
    *,
    is_group: bool = False,
    pending: bool = False,
    has_older: bool = False,
    oldest_cursor: str | None = None,
) -> dict[str, Any]:
    """Items newest first, as Instagram sends them."""
    last = max((i.get("timestamp", 0) for i in items), default=0)
    return {
        "thread_id": thread_id,
        "thread_v2_id": f"v2-{thread_id}",
        "users": users,
        "items": items,
        "is_group": is_group,
        "pending": pending,
        "last_activity_at": last,
        "has_older": has_older,
        "oldest_cursor": oldest_cursor,
        "thread_type": "group" if is_group else "private",
        "viewer_id": int(VIEWER),
    }


def inbox(
    threads: list[dict[str, Any]], *, has_older: bool = False, cursor: str | None = None
) -> dict[str, Any]:
    return {
        "status": "ok",
        "inbox": {"threads": threads, "has_older": has_older, "oldest_cursor": cursor},
    }


def user_info(pk: str, username: str, full_name: str = "") -> dict[str, Any]:
    """users/<id>/info/ and users/<name>/usernameinfo/: the fields aiograpi's User requires."""
    return {
        "status": "ok",
        "user": {
            "pk": pk,
            "username": username,
            "full_name": full_name,
            "is_private": False,
            "profile_pic_url": "https://example.com/p.jpg",
            "is_verified": False,
            "media_count": 0,
            "follower_count": 0,
            "following_count": 0,
            "is_business": False,
        },
    }
