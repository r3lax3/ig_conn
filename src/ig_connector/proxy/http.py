"""HTTP client of the proxy service (contract 8, the service's openapi 1.0.0).

Only the client endpoints under /v1/assignments are called: /v1/proxies* and /v1/clients
are the service admin's. account_id = source_id. channel_type is always sent: the
service defaults to "telegram". The reserve answer has no schema (a free-form object),
so it is read defensively, and neither it nor any proxy credential is ever logged.
"""

import logging
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx
from pydantic import SecretStr

from ig_connector.masking import mask_text
from ig_connector.proxy import (
    AssignmentLost,
    Heartbeat,
    ProxyAssignment,
    ProxyRequirement,
    ProxyUnavailable,
    Rebalance,
    TransportState,
)

__all__ = ["ProxyService"]

log = logging.getLogger(__name__)

# a call that hangs must not hold its Source: heartbeats are every 60 s
DEFAULT_TIMEOUT = 10.0
# ConnectionFailure.reason
_REASON_LIMIT = 500


class ProxyService:
    """ProxyProvider over the service's HTTP API, with this client's x-proxy-token."""

    def __init__(
        self,
        *,
        base_url: str,
        token: SecretStr,
        channel_type: str,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._channel_type = channel_type
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"x-proxy-token": token.get_secret_value()},
            timeout=timeout,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def reserve(self, source_id: UUID, requirement: ProxyRequirement) -> ProxyAssignment:
        body: dict[str, Any] = {
            "account_id": str(source_id),
            "channel_type": self._channel_type,
            "country_code": requirement.country_code,
        }
        if requirement.network_type is not None:
            body["network_type"] = requirement.network_type
        data = await self._call("POST", "/v1/assignments/reserve", source_id, json=body, reserving=True)
        return _assignment(data, source_id)

    async def heartbeat(self, source_id: UUID, assignment_id: int, transport: TransportState) -> Heartbeat:
        body = {"expected_assignment_id": assignment_id, "transport_state": transport.value}
        data = await self._call("POST", f"/v1/assignments/{source_id}/heartbeat", source_id, json=body)
        if "pending_rebalance" not in data:
            # the GET is what is documented to carry pending_rebalance (and counts as a heartbeat too)
            data = await self._call("GET", f"/v1/assignments/{source_id}", source_id)
            current = _assignment_id(data)
            if current is not None and current != assignment_id:
                raise AssignmentLost("the Source has another assignment now")
        return Heartbeat(pending_rebalance=_rebalance(data, source_id))

    async def connection_failed(self, source_id: UUID, assignment_id: int, reason: str) -> None:
        reason = reason[:_REASON_LIMIT] or "network failure"
        body = {"reason": reason, "expected_assignment_id": assignment_id}
        await self._call("POST", f"/v1/assignments/{source_id}/connection-failed", source_id, json=body)

    async def rebalance(self, source_id: UUID, assignment_id: int, move: Rebalance) -> None:
        body = {
            "target_proxy_id": str(move.target_proxy_id),
            "expected_assignment_id": assignment_id,
            "job_id": str(move.job_id),
        }
        await self._call("POST", f"/v1/assignments/{source_id}/rebalance", source_id, json=body)

    async def release(self, source_id: UUID, assignment_id: int) -> None:
        params = {"expected_assignment_id": assignment_id}
        await self._call("DELETE", f"/v1/assignments/{source_id}", source_id, params=params)

    async def _call(
        self,
        method: str,
        path: str,
        source_id: UUID,
        *,
        json: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        reserving: bool = False,
    ) -> dict[str, Any]:
        ids = {"source_id": str(source_id), "method": method, "path": path}
        try:
            response = await self._http.request(method, path, json=json, params=params)
        except httpx.HTTPError as exc:
            log.warning("proxy service unreachable (%s)", type(exc).__name__, extra=ids)
            raise ProxyUnavailable("proxy service unreachable") from None
        status = response.status_code
        if status == 409:
            detail = mask_text(_detail(response))[:200] or "conflict"
            log.warning("proxy service refused: %s", detail, extra={**ids, "http_status": status})
            if reserving:
                raise ProxyUnavailable(detail)
            # the expected assignment is not the Source's any more (closed or replaced)
            raise AssignmentLost(detail)
        if status == 404 and not reserving:
            log.warning("proxy service has no such assignment", extra={**ids, "http_status": status})
            raise AssignmentLost("no assignment for the Source")
        if status == 422:
            # our request is wrong: field locations and error types only, never the input
            log.error("proxy service rejected the request", extra={**ids, "problems": _problems(response)})
            raise ProxyUnavailable("proxy service rejected the request")
        if status in (401, 403):
            log.error("proxy service refused the token", extra={**ids, "http_status": status})
            raise ProxyUnavailable("proxy service refused the token")
        if not response.is_success:
            log.warning("proxy service failed", extra={**ids, "http_status": status})
            raise ProxyUnavailable("proxy service failed")
        try:
            data = response.json()
        except ValueError:
            data = None
        return data if isinstance(data, dict) else {}


def _detail(response: httpx.Response) -> str:
    try:
        data = response.json()
    except ValueError:
        return ""
    detail = data.get("detail") if isinstance(data, dict) else None
    return detail if isinstance(detail, str) else ""


def _problems(response: httpx.Response) -> list[str]:
    try:
        data = response.json()
    except ValueError:
        return []
    detail = data.get("detail") if isinstance(data, dict) else None
    if not isinstance(detail, list):
        return []
    items = [item for item in detail if isinstance(item, dict)]
    return [f"{'.'.join(map(str, item.get('loc', ())))}: {item.get('type')}" for item in items]


def _assignment(data: Mapping[str, Any], source_id: UUID) -> ProxyAssignment:
    assignment_id = _assignment_id(data)
    url = _proxy_url(data)
    if assignment_id is None or url is None:
        # the keys only: the values hold the proxy's credentials
        log.error(
            "proxy service answered without a usable assignment",
            extra={"source_id": str(source_id), "keys": sorted(_keys(data))},
        )
        raise ProxyUnavailable("proxy service answered without a usable proxy")
    return ProxyAssignment(assignment_id=assignment_id, url=SecretStr(url))


def _keys(data: Mapping[str, Any]) -> set[str]:
    keys = set(data)
    for key, value in data.items():
        if isinstance(value, Mapping):
            keys |= {f"{key}.{inner}" for inner in value}
    return keys


def _int(value: Any) -> int | None:
    # bool is an int to Python, never an assignment id
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    if isinstance(value, str) and value.isdigit() and int(value) > 0:
        return int(value)
    return None


def _assignment_id(data: Mapping[str, Any]) -> int | None:
    for key in ("assignment_id", "id"):
        if (found := _int(data.get(key))) is not None:
            return found
    nested = data.get("assignment")
    if isinstance(nested, Mapping):
        return _assignment_id(nested)
    return None


def _str(data: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
    return None


def _proxy_url(data: Mapping[str, Any]) -> str | None:
    """scheme://user:password@host:port from whatever shape the answer has."""
    for scope in (data, data.get("proxy"), data.get("assignment")):
        if not isinstance(scope, Mapping):
            continue
        url = _str(scope, "proxy_url", "url", "uri")
        if url is not None and "://" in url:
            return url
        host = _str(scope, "host", "ip", "address")
        port = _str(scope, "port")
        if host is None or port is None:
            if scope is not data and (inner := _proxy_url(scope)) is not None:
                return inner
            continue
        scheme = (_str(scope, "scheme", "protocol") or "http").lower()
        user = _str(scope, "username", "login", "user")
        password = _str(scope, "password", "pass")
        credentials = ""
        if user is not None:
            credentials = quote(user, safe="")
            if password is not None:
                credentials += ":" + quote(password, safe="")
            credentials += "@"
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"{scheme}://{credentials}{host}:{port}"
    return None


def _uuid(value: Any) -> UUID | None:
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError:
            return None
    return None


def _rebalance(data: Mapping[str, Any], source_id: UUID) -> Rebalance | None:
    pending = data.get("pending_rebalance")
    if not pending:
        return None
    # a nested job, or a flag with the job next to it
    scope = pending if isinstance(pending, Mapping) else data
    job_id = _uuid(scope.get("job_id")) or _uuid(scope.get("rebalance_job_id"))
    if job_id is None and isinstance(pending, Mapping):
        job_id = _uuid(pending.get("id"))
    target = _uuid(scope.get("target_proxy_id")) or _uuid(scope.get("to_proxy_id"))
    if job_id is None or target is None:
        # cannot be confirmed without them; asked again on the next heartbeat
        log.warning(
            "pending_rebalance in an unknown shape",
            extra={"source_id": str(source_id), "keys": sorted(_keys(data))},
        )
        return None
    return Rebalance(job_id=job_id, target_proxy_id=target)
