"""A fake proxy service over real HTTP, after its openapi 1.0.0 (client endpoints only).

Validates bodies like the service does (422 with FastAPI's `detail` list), answers 409
for an empty pool or a stale expected_assignment_id, 401 for a wrong token. Records every
request. Shapes of free-form answers can be overridden to test defensive parsing.
"""

import json
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

from aiohttp import web
from aiohttp.test_utils import TestServer

TOKEN = "client-token-1"
_TRANSPORT = {"unknown", "connected", "draining", "closed"}
_NETWORK = {"datacenter", "residential", "mobile"}


@dataclass
class Request:
    method: str
    path: str
    token: str | None
    query: dict[str, str]
    body: Any


@dataclass
class _Assignment:
    id: int
    proxy_id: UUID
    channel_type: str
    transport: str = "unknown"


@dataclass
class FakeProxyServer:
    requests: list[Request] = field(default_factory=list)
    pool_empty: bool = False
    # account_id -> pending job, shown in GET /v1/assignments/{account_id}
    pending: dict[str, dict[str, str]] = field(default_factory=dict)
    # replaces the reserve answer when set
    reserve_answer: Mapping[str, Any] | None = None
    # forced status for the next requests (e.g. 500)
    fail_with: int | None = None
    assignments: dict[str, _Assignment] = field(default_factory=dict)
    _next: int = 1

    def app(self) -> web.Application:
        app = web.Application(middlewares=[self._record])
        app.router.add_post("/v1/assignments/reserve", self._reserve)
        app.router.add_get("/v1/assignments/{account_id}", self._get)
        app.router.add_delete("/v1/assignments/{account_id}", self._release)
        app.router.add_post("/v1/assignments/{account_id}/heartbeat", self._heartbeat)
        app.router.add_post("/v1/assignments/{account_id}/connection-failed", self._failed)
        app.router.add_post("/v1/assignments/{account_id}/rebalance", self._rebalance)
        return app

    @web.middleware
    async def _record(self, request: web.Request, handler: Any) -> web.StreamResponse:
        raw = await request.read()
        body = json.loads(raw) if raw else None
        self.requests.append(
            Request(
                request.method, request.path, request.headers.get("x-proxy-token"), dict(request.query), body
            )
        )
        if request.headers.get("x-proxy-token") != TOKEN:
            return web.json_response({"detail": "invalid token"}, status=401)
        if self.fail_with is not None:
            return web.json_response({"detail": "boom"}, status=self.fail_with)
        response: web.StreamResponse = await handler(request)
        return response

    async def _reserve(self, request: web.Request) -> web.Response:
        body = await request.json()
        problems = _missing(body, "account_id", "country_code")
        if "account_id" in body and _uuid(body["account_id"]) is None:
            problems.append(_problem("account_id", "uuid_parsing"))
        channel = body.get("channel_type", "telegram")
        if not isinstance(channel, str) or not 1 <= len(channel) <= 32:
            problems.append(_problem("channel_type", "string_too_long"))
        if body.get("network_type", "datacenter") not in _NETWORK:
            problems.append(_problem("network_type", "enum"))
        if problems:
            return _invalid(problems)
        account = body["account_id"]
        held = self.assignments.get(account)
        if held is None:
            if self.pool_empty:
                return web.json_response({"detail": "no working proxy available"}, status=409)
            held = self.assignments[account] = _Assignment(self._next, uuid4(), channel)
            self._next += 1
        if self.reserve_answer is not None:
            return web.json_response(dict(self.reserve_answer))
        return web.json_response(
            {
                "assignment_id": held.id,
                "account_id": account,
                "proxy": {
                    "id": str(held.proxy_id),
                    "protocol": "socks5",
                    "host": f"10.0.0.{held.id}",
                    "port": 1080,
                    "username": f"u{held.id}",
                    "password": f"p@ss/{held.id}",
                },
            }
        )

    def _held(self, request: web.Request, expected: Any) -> _Assignment | web.Response:
        account = request.match_info["account_id"]
        if _uuid(account) is None:
            return _invalid([{"loc": ["path", "account_id"], "msg": "bad", "type": "uuid_parsing"}])
        held = self.assignments.get(account)
        if held is None or (expected is not None and held.id != expected):
            return web.json_response({"detail": "assignment changed"}, status=409)
        return held

    async def _get(self, request: web.Request) -> web.Response:
        held = self._held(request, None)
        if isinstance(held, web.Response):
            return held
        account = request.match_info["account_id"]
        return web.json_response({"assignment_id": held.id, "pending_rebalance": self.pending.get(account)})

    async def _heartbeat(self, request: web.Request) -> web.Response:
        body = await request.json()
        problems = _missing(body, "expected_assignment_id") + _positive(body, "expected_assignment_id")
        if body.get("transport_state", "unknown") not in _TRANSPORT:
            problems.append(_problem("transport_state", "enum"))
        if problems:
            return _invalid(problems)
        held = self._held(request, body["expected_assignment_id"])
        if isinstance(held, web.Response):
            return held
        held.transport = body.get("transport_state", "unknown")
        return web.json_response({"status": "ok"})

    async def _failed(self, request: web.Request) -> web.Response:
        body = await request.json()
        problems = _missing(body, "reason", "expected_assignment_id") + _positive(
            body, "expected_assignment_id"
        )
        if isinstance(body.get("reason"), str) and not 1 <= len(body["reason"]) <= 500:
            problems.append(_problem("reason", "string_too_long"))
        if problems:
            return _invalid(problems)
        held = self._held(request, body["expected_assignment_id"])
        if isinstance(held, web.Response):
            return held
        del self.assignments[request.match_info["account_id"]]
        return web.json_response({"status": "closed"})

    async def _rebalance(self, request: web.Request) -> web.Response:
        body = await request.json()
        problems = _missing(body, "target_proxy_id", "expected_assignment_id", "job_id")
        problems += _positive(body, "expected_assignment_id")
        for key in ("target_proxy_id", "job_id"):
            if key in body and _uuid(body[key]) is None:
                problems.append(_problem(key, "uuid_parsing"))
        if problems:
            return _invalid(problems)
        held = self._held(request, body["expected_assignment_id"])
        if isinstance(held, web.Response):
            return held
        account = request.match_info["account_id"]
        job = self.pending.get(account)
        if job is None or job["job_id"] != body["job_id"] or held.transport != "closed":
            return web.json_response({"detail": "no such rebalance job or transport not closed"}, status=409)
        del self.pending[account]
        self.assignments[account] = _Assignment(self._next, UUID(body["target_proxy_id"]), held.channel_type)
        self._next += 1
        return web.json_response({"status": "moved"})

    async def _release(self, request: web.Request) -> web.Response:
        raw = request.query.get("expected_assignment_id")
        if raw is None or not raw.isdigit():
            return _invalid(
                [{"loc": ["query", "expected_assignment_id"], "msg": "bad", "type": "int_parsing"}]
            )
        held = self._held(request, int(raw))
        if isinstance(held, web.Response):
            return held
        del self.assignments[request.match_info["account_id"]]
        return web.json_response({"status": "released"})


@asynccontextmanager
async def serving(server: FakeProxyServer) -> AsyncIterator[str]:
    """The base URL of the running fake."""
    test_server = TestServer(server.app())
    await test_server.start_server()
    try:
        yield str(test_server.make_url("")).rstrip("/")
    finally:
        await test_server.close()


def _uuid(value: Any) -> UUID | None:
    try:
        return UUID(value) if isinstance(value, str) else None
    except ValueError:
        return None


def _problem(name: str, kind: str) -> dict[str, Any]:
    return {"loc": ["body", name], "msg": "invalid", "type": kind}


def _missing(body: Any, *names: str) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return [_problem("", "model_attributes_type")]
    return [_problem(name, "missing") for name in names if name not in body]


def _positive(body: Any, name: str) -> list[dict[str, Any]]:
    value = body.get(name) if isinstance(body, dict) else None
    if value is None:
        return []
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        return [_problem(name, "greater_than")]
    return []


def _invalid(problems: list[dict[str, Any]]) -> web.Response:
    return web.json_response({"detail": problems}, status=422)
