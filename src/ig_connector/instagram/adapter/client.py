"""The aiograpi Client as the connector uses it: our message label, one attempt per send.

aiograpi draws the `client_context` of a message from `generate_mutation_token`; the
connector saves its own label before sending, so the token is taken from the
calling task (`labelled`). aiograpi also sends a request again after an HTTP 408 or a
broken read, and its HTTP client follows a 307 with the same POST; curl resends a request
whose pooled connection died. For a message each of those is a second send, so a
broadcast goes out once, on a connection of its own, without following redirects: a
failure to connect then proves that nothing was written.
"""

import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, cast

import httpx
from aiograpi import Client
from aiograpi.exceptions import PreLoginRequired

__all__ = ["ConnectorClient", "labelled", "new_client"]

_LABEL: ContextVar[str | None] = ContextVar("ig_connector_client_context", default=None)
# endpoints that put something on Instagram: never sent twice by aiograpi itself
_ONCE = ("direct_v2/threads/broadcast/",)

# aiograpi logs request URLs, Instagram's answers and message texts: kept out of the
# service log; the adapter logs what it needs itself
for _name in ("aiograpi", "private_request", "public_request", "graphql_request"):
    logging.getLogger(_name).setLevel(logging.CRITICAL + 1)
# httpx logs every request URL at INFO: usernames looked up, thread ids
logging.getLogger("httpx").setLevel(logging.WARNING)


@contextmanager
def labelled(client_context: str) -> Iterator[None]:
    """Messages sent by this task inside the block carry `client_context` as their label."""
    token = _LABEL.set(client_context)
    try:
        yield
    finally:
        _LABEL.reset(token)


class ConnectorClient(Client):  # type: ignore[misc]
    def generate_mutation_token(self) -> str:
        label = _LABEL.get()
        return label if label is not None else str(super().generate_mutation_token())

    async def private_request(
        self,
        endpoint: str,
        data: Any = None,
        params: Any = None,
        login: bool = False,
        with_signature: bool = True,
        headers: dict[str, str] | None = None,
        extra_sig: Any = None,
        domain: str | None = None,
    ) -> Any:
        if not endpoint.startswith(_ONCE):
            return await super().private_request(
                endpoint,
                data=data,
                params=params,
                login=login,
                with_signature=with_signature,
                headers=headers,
                extra_sig=extra_sig,
                domain=domain,
            )
        # aiograpi's private_request without its repeats and its challenge handling
        if not self.user_id:
            raise PreLoginRequired("no Session")
        if self.authorization:
            headers = {"Authorization": self.authorization, **(headers or {})}
        self.private_requests_count += 1
        pooled = self.private._client
        own = self.broadcast_http_client(pooled.cookies)
        self.private._client = own
        try:
            return await self._send_private_request(
                endpoint,
                data=data,
                params=params,
                login=login,
                with_signature=with_signature,
                headers=headers,
                extra_sig=extra_sig,
                domain=domain,
            )
        finally:
            self.private._client = pooled
            pooled.cookies.update(own.cookies)
            await own.aclose()

    def broadcast_http_client(self, cookies: httpx.Cookies) -> httpx.AsyncClient:
        """The HTTP client of one broadcast: a fresh connection, no redirects, no retries."""
        return httpx.AsyncClient(
            transport=self.broadcast_transport(),
            follow_redirects=False,
            trust_env=False,
            cookies=cookies,
        )

    def broadcast_transport(self) -> httpx.AsyncBaseTransport:
        if self.private_transport != "curl":
            # httpx's own: a new client is a new connection, and it never resends
            return httpx.AsyncHTTPTransport(proxy=self.private.proxy, verify=self.private.verify)
        from aiograpi.transports import CurlH2Transport
        from curl_cffi import CurlOpt

        transport = CurlH2Transport(proxy=self.private.proxy, verify=self.private.verify)
        # curl resends only on a reused connection: with none, a connect error means "not written"
        transport._curl_options.update({CurlOpt.FRESH_CONNECT: 1, CurlOpt.FORBID_REUSE: 1})
        return cast(httpx.AsyncBaseTransport, transport)


def new_client(settings: Mapping[str, Any] | None, proxy_url: str) -> Any:
    """A Client with these settings behind the proxy. An empty proxy is refused: aiograpi
    takes it as "go direct"."""
    if not proxy_url:
        raise ValueError("an aiograpi Client always goes through a proxy")
    return ConnectorClient(settings=dict(settings) if settings else None, proxy=proxy_url)
