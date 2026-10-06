"""The proxy service's HTTP client against a fake HTTP server built from the service's openapi."""

import logging
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr

from ig_connector.proxy import (
    AssignmentLost,
    ProxyRequirement,
    ProxyUnavailable,
    Rebalance,
    TransportState,
)
from ig_connector.proxy.http import ProxyService
from tests.support.proxy_server import TOKEN, FakeProxyServer, serving

CHANNEL = "individual_instagram_account"
NEED = ProxyRequirement("DE", "residential")


@pytest.fixture
def server() -> FakeProxyServer:
    return FakeProxyServer()


@pytest.fixture
async def client(server: FakeProxyServer) -> AsyncIterator[ProxyService]:
    async with serving(server) as url:
        service = ProxyService(base_url=url, token=SecretStr(TOKEN), channel_type=CHANNEL)
        try:
            yield service
        finally:
            await service.aclose()


async def test_reserve_sends_our_channel_type_and_the_requirement(
    client: ProxyService, server: FakeProxyServer
) -> None:
    source = uuid4()

    assignment = await client.reserve(source, NEED)

    request = server.requests[-1]
    assert (request.method, request.path, request.token) == ("POST", "/v1/assignments/reserve", TOKEN)
    assert request.body == {
        "account_id": str(source),
        "channel_type": CHANNEL,
        "country_code": "DE",
        "network_type": "residential",
    }
    assert assignment.assignment_id == 1
    assert assignment.url.get_secret_value() == "socks5://u1:p%40ss%2F1@10.0.0.1:1080"
    assert assignment.scheme == "socks5"


async def test_reserve_leaves_network_type_to_the_service_when_not_asked(
    client: ProxyService, server: FakeProxyServer
) -> None:
    await client.reserve(uuid4(), ProxyRequirement("ZZ"))

    assert "network_type" not in server.requests[-1].body


async def test_reserve_is_the_same_assignment_again(client: ProxyService) -> None:
    source = uuid4()

    first = await client.reserve(source, NEED)
    again = await client.reserve(source, NEED)

    assert again.assignment_id == first.assignment_id


async def test_empty_pool_is_proxy_unavailable_with_the_services_reason(
    client: ProxyService, server: FakeProxyServer
) -> None:
    server.pool_empty = True

    with pytest.raises(ProxyUnavailable, match="no working proxy available") as raised:
        await client.reserve(uuid4(), NEED)
    assert not isinstance(raised.value, AssignmentLost)


async def test_validation_error_is_unavailable_and_logs_no_input(
    client: ProxyService, caplog: pytest.LogCaptureFixture
) -> None:
    with pytest.raises(ProxyUnavailable, match="rejected"):
        await client.reserve(uuid4(), ProxyRequirement("DE", "satellite"))

    record = next(r for r in caplog.records if r.message == "proxy service rejected the request")
    assert record.problems == ["body.network_type: enum"]  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("answer", "url"),
    [
        ({"id": 7, "proxy_url": "http://a:b@h:1"}, "http://a:b@h:1"),
        ({"assignment": {"id": 7, "proxy": {"host": "h", "port": 3128}}}, "http://h:3128"),
        (
            {"assignment_id": "7", "scheme": "HTTP", "host": "h", "port": "8080", "login": "a"},
            "http://a@h:8080",
        ),
    ],
)
async def test_reserve_answer_is_read_in_any_reasonable_shape(
    client: ProxyService, server: FakeProxyServer, answer: dict[str, object], url: str
) -> None:
    server.reserve_answer = answer

    assignment = await client.reserve(uuid4(), NEED)

    assert (assignment.assignment_id, assignment.url.get_secret_value()) == (7, url)


async def test_unusable_reserve_answer_is_unavailable_and_logs_keys_not_values(
    client: ProxyService, server: FakeProxyServer, caplog: pytest.LogCaptureFixture
) -> None:
    server.reserve_answer = {"id": 3, "proxy": {"password": "hunter2"}}

    with pytest.raises(ProxyUnavailable):
        await client.reserve(uuid4(), NEED)

    assert "hunter2" not in caplog.text
    record = next(r for r in caplog.records if "without a usable" in r.message)
    assert "proxy.password" in record.keys  # type: ignore[attr-defined]


async def test_credentials_never_reach_the_logs(
    client: ProxyService, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    assignment = await client.reserve(uuid4(), NEED)

    assert "p@ss" not in caplog.text and "p%40ss" not in caplog.text
    assert "p%40ss" not in repr(assignment)


async def test_heartbeat_reports_transport_and_reads_pending_rebalance(
    client: ProxyService, server: FakeProxyServer
) -> None:
    source = uuid4()
    assignment = await client.reserve(source, NEED)
    job, target = uuid4(), uuid4()

    quiet = await client.heartbeat(source, assignment.assignment_id, TransportState.CONNECTED)
    server.pending[str(source)] = {"job_id": str(job), "target_proxy_id": str(target)}
    asked = await client.heartbeat(source, assignment.assignment_id, TransportState.CONNECTED)

    assert quiet.pending_rebalance is None
    assert asked.pending_rebalance == Rebalance(job_id=job, target_proxy_id=target)
    posts = [r for r in server.requests if r.path.endswith("/heartbeat")]
    assert posts[0].body == {
        "expected_assignment_id": assignment.assignment_id,
        "transport_state": "connected",
    }


async def test_heartbeat_for_a_replaced_assignment_is_lost(client: ProxyService) -> None:
    source = uuid4()
    assignment = await client.reserve(source, NEED)

    with pytest.raises(AssignmentLost):
        await client.heartbeat(source, assignment.assignment_id + 1, TransportState.UNKNOWN)


async def test_connection_failed_closes_the_assignment_and_reserve_gives_another(
    client: ProxyService, server: FakeProxyServer
) -> None:
    source = uuid4()
    first = await client.reserve(source, NEED)

    await client.connection_failed(source, first.assignment_id, "connect timeout")
    second = await client.reserve(source, NEED)

    assert server.requests[-2].body == {
        "reason": "connect timeout",
        "expected_assignment_id": first.assignment_id,
    }
    assert second.assignment_id != first.assignment_id


async def test_connection_failed_on_a_stale_assignment_is_lost(client: ProxyService) -> None:
    source = uuid4()
    first = await client.reserve(source, NEED)
    await client.connection_failed(source, first.assignment_id, "refused")

    with pytest.raises(AssignmentLost):
        await client.connection_failed(source, first.assignment_id, "refused")


async def test_rebalance_confirms_the_job_after_closing_transport(
    client: ProxyService, server: FakeProxyServer
) -> None:
    source = uuid4()
    first = await client.reserve(source, NEED)
    move = Rebalance(job_id=uuid4(), target_proxy_id=uuid4())
    server.pending[str(source)] = {"job_id": str(move.job_id), "target_proxy_id": str(move.target_proxy_id)}

    await client.heartbeat(source, first.assignment_id, TransportState.CLOSED)
    await client.rebalance(source, first.assignment_id, move)
    moved = await client.reserve(source, NEED)

    assert server.requests[-2].body == {
        "target_proxy_id": str(move.target_proxy_id),
        "expected_assignment_id": first.assignment_id,
        "job_id": str(move.job_id),
    }
    assert moved.assignment_id != first.assignment_id


async def test_rebalance_refused_by_the_service_is_lost(client: ProxyService) -> None:
    source = uuid4()
    first = await client.reserve(source, NEED)

    with pytest.raises(AssignmentLost):
        await client.rebalance(source, first.assignment_id, Rebalance(uuid4(), uuid4()))


async def test_release_names_the_expected_assignment(client: ProxyService, server: FakeProxyServer) -> None:
    source = uuid4()
    first = await client.reserve(source, NEED)

    await client.release(source, first.assignment_id)

    request = server.requests[-1]
    assert (request.method, request.path) == ("DELETE", f"/v1/assignments/{source}")
    assert request.query == {"expected_assignment_id": str(first.assignment_id)}
    assert str(source) not in server.assignments


async def test_wrong_token_and_server_errors_are_unavailable(server: FakeProxyServer) -> None:
    async with serving(server) as url:
        stranger = ProxyService(base_url=url, token=SecretStr("nope"), channel_type=CHANNEL)
        with pytest.raises(ProxyUnavailable, match="token"):
            await stranger.reserve(uuid4(), NEED)
        await stranger.aclose()

        server.fail_with = 500
        service = ProxyService(base_url=url, token=SecretStr(TOKEN), channel_type=CHANNEL)
        with pytest.raises(ProxyUnavailable, match="failed"):
            await service.reserve(uuid4(), NEED)
        await service.aclose()


async def test_unreachable_service_is_unavailable() -> None:
    service = ProxyService(
        base_url="http://127.0.0.1:9", token=SecretStr(TOKEN), channel_type=CHANNEL, timeout=2
    )
    try:
        with pytest.raises(ProxyUnavailable, match="unreachable"):
            await service.reserve(UUID(int=1), NEED)
    finally:
        await service.aclose()
