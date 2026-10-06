"""aiograpi errors as the port's refusals (Failure), for reads and for sends.

NETWORK moves the Source to another proxy, so it is said only for failures of the
connection itself: name lookup, TCP, the proxy handshake, TLS. A read that was asked and
got no answer is NO_ANSWER: it can be repeated, the proxy is not proven at fault. A send
cannot be repeated: NETWORK, RATE_LIMITED and FLOOD make CRM retry, so they are said only
when the message provably did not leave (Instagram answered with a refusal, or the
connection never got as far as writing the request). Anything else after the call
started is UNKNOWN_AFTER_SEND, and the send handler looks for the message by its label.
"""

from typing import Any

import httpx
from aiograpi.exceptions import (
    ClientError,
    ClientNotFoundError,
    NotFoundError,
    PreLoginRequired,
    ProxyAddressIsBlocked,
)
from curl_cffi.const import CurlECode
from curl_cffi.curl import CurlError

from ig_connector.instagram import Failure, PlatformError
from ig_connector.instagram.weblogin.session import aiograpi_failure

__all__ = ["read_refusal", "send_refusal"]

# curl failures before the request is written: name lookup, TCP, proxy handshake, TLS
_NOT_WRITTEN = frozenset(
    {
        CurlECode.COULDNT_RESOLVE_PROXY,
        CurlECode.COULDNT_RESOLVE_HOST,
        CurlECode.COULDNT_CONNECT,
        CurlECode.PROXY,
        CurlECode.SSL_CONNECT_ERROR,
    }
)


def read_refusal(exc: Exception, *, curl: bool = True) -> PlatformError:
    """The refusal for a failed read (lookups, history, inbox, liveness check).

    `curl`: as for send_refusal, the Client's private transport is aiograpi's curl one.
    """
    if isinstance(exc, PlatformError):
        return exc
    if isinstance(exc, PreLoginRequired):
        return PlatformError(Failure.SESSION_REVOKED, "aiograpi PreLoginRequired")
    if isinstance(exc, NotFoundError | ClientNotFoundError):
        return PlatformError(Failure.NOT_FOUND, f"aiograpi {type(exc).__name__}")
    if isinstance(exc, ClientError | OSError | TimeoutError | httpx.TransportError):
        detail = f"aiograpi {type(exc).__name__}"
        if _not_written(exc, curl=curl):
            return PlatformError(Failure.NETWORK, detail)
        refusal = aiograpi_failure(exc)
        # Instagram's own word that the proxy's address is blocked does blame the proxy
        if isinstance(exc, ProxyAddressIsBlocked):
            return refusal
        if refusal.failure is Failure.NETWORK or _transport_failed(exc):
            return PlatformError(Failure.NO_ANSWER, detail)
        return refusal
    # aiograpi lets parsing errors of Instagram's answers through (pydantic, KeyError, assert)
    return PlatformError(Failure.REJECTED, f"aiograpi {type(exc).__name__}")


def send_refusal(exc: Exception, answer: Any, *, curl: bool = True) -> PlatformError:
    """The refusal for a failed send; `answer` is the HTTP response aiograpi got, if any.

    `curl`: the Client's private transport is aiograpi's curl one, which reports every
    failure, even one after the answer arrived, as an httpx ConnectError.
    """
    if isinstance(exc, PlatformError):
        return exc
    if isinstance(exc, PreLoginRequired):
        # refused before any request
        return PlatformError(Failure.SESSION_REVOKED, "aiograpi PreLoginRequired")
    detail = f"aiograpi {type(exc).__name__}"
    if answer is not None:
        code = int(getattr(answer, "status_code", 0))
        if 400 <= code < 500:
            # Instagram read the request and refused it: nothing was posted
            return aiograpi_failure(exc)
        if code < 300 and _says_fail(answer):
            return PlatformError(Failure.REJECTED, detail)
        # a server error or an answer we cannot read: it may be stored
        return PlatformError(Failure.UNKNOWN_AFTER_SEND, f"{detail} after HTTP {code}")
    if _not_written(exc, curl=curl):
        return PlatformError(Failure.NETWORK, detail)
    return PlatformError(Failure.UNKNOWN_AFTER_SEND, detail)


def _says_fail(answer: Any) -> bool:
    try:
        body = answer.json()
    except Exception:
        return False
    return isinstance(body, dict) and body.get("status") == "fail"


def _transport_failed(exc: BaseException) -> bool:
    """Whether the cause chain holds a transport failure (aiograpi wraps read timeouts and
    broken answers into its own errors)."""
    return any(isinstance(e, httpx.TransportError | CurlError | OSError | TimeoutError) for e in _chain(exc))


def _chain(exc: BaseException) -> list[BaseException]:
    seen: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and all(current is not e for e in seen):
        seen.append(current)
        current = current.__cause__ or current.__context__
    return seen


def _not_written(exc: BaseException, *, curl: bool) -> bool:
    """Whether the cause chain shows the connection failed before the request was written."""
    for current in _chain(exc):
        if isinstance(current, CurlError):
            return current.code in _NOT_WRITTEN
        if not curl and isinstance(current, httpx.ConnectError | httpx.ConnectTimeout | httpx.ProxyError):
            # httpx's own transport raises these only while connecting
            return True
    return False
