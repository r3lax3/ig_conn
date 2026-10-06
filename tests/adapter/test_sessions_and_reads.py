"""Sessions (login, logout, restore, check), user lookups and dialog history in the adapter."""

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
import pytest
from curl_cffi.const import CurlECode
from pydantic import SecretStr

from ig_connector.instagram import (
    Account,
    DialogMessage,
    Failure,
    LoggedIn,
    LoginRequest,
    PlatformError,
    SessionData,
    User,
)
from ig_connector.instagram.adapter.platform import AiograpiPlatform
from ig_connector.instagram.weblogin import WebSession
from ig_connector.instagram.weblogin.device import decode_device
from ig_connector.proxy import ProxyAssignment
from tests.adapter.conftest import DEVICE, PEER, THREAD, NoWebLogin, restored
from tests.support.instagram_api import (
    PROXY,
    VIEWER,
    FakeApi,
    curl_failure,
    inbox,
    item,
    session_data,
    status,
    thread,
    user,
    user_info,
)

AT = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


class ScriptedWebLogin:
    """WebLogin with the browser part done: hands out Clients on the pretend API."""

    def __init__(self, api: FakeApi) -> None:
        self._api = api
        self.logins = 0

    async def login(self, request: LoginRequest) -> WebSession:
        self.logins += 1
        session = session_data()
        client = self._api.clients()(json.loads(session.data), request.proxy.url.get_secret_value())
        tagged = SessionData(session.data + f" {self.logins}".encode())
        logged_in = LoggedIn(Account(VIEWER, "anna.shop"), tagged, DEVICE)
        return WebSession(logged_in=logged_in, client=client)


def request(source_id: Any) -> LoginRequest:
    return LoginRequest(source_id, "anna.shop", SecretStr("pw"), None, PROXY, DEVICE)


# lookups


async def test_users_are_found_by_username_and_by_id(api: FakeApi) -> None:
    api.on("users/bob/usernameinfo/", user_info(PEER, "bob", "Bob B"))
    api.on(f"users/{PEER}/info/", user_info(PEER, "bob", ""))
    platform, source_id = await restored(api)

    assert await platform.user_by_username(source_id, "bob") == User(PEER, "bob", "Bob B")
    assert await platform.user_by_id(source_id, PEER) == User(PEER, "bob", None)


@pytest.mark.parametrize(
    "answer",
    [
        status(404, {"status": "fail", "message": "User not found"}),
        status(400, {"status": "fail", "message": "User not found"}),
        {"status": "ok"},
    ],
)
async def test_no_such_user(api: FakeApi, answer: Any) -> None:
    api.on("users/nobody/usernameinfo/", answer)
    platform, source_id = await restored(api)

    with pytest.raises(PlatformError) as refused:
        await platform.user_by_username(source_id, "nobody")

    assert refused.value.failure is Failure.NOT_FOUND


async def test_a_lookup_is_one_request_through_the_session(api: FakeApi) -> None:
    # aiograpi's user_info_by_username would fall back to the public web and repeat
    api.on("users/bob/usernameinfo/", status(429, {"status": "fail"}))
    platform, source_id = await restored(api)

    with pytest.raises(PlatformError) as refused:
        await platform.user_by_username(source_id, "bob")

    assert refused.value.failure is Failure.RATE_LIMITED
    assert api.paths() == ["users/bob/usernameinfo/"]


# history


async def test_recent_messages_carry_their_labels(api: FakeApi) -> None:
    api.on(
        "direct_v2/inbox/",
        inbox([thread(THREAD, [user(PEER)], [item("1", AT, user_id=PEER)])]),
    )
    api.on(
        f"direct_v2/threads/{THREAD}/",
        {
            "status": "ok",
            "thread": thread(
                THREAD,
                [user(PEER)],
                [
                    item("3", AT, user_id=VIEWER, client_context="6800012345678901234"),
                    item("2", AT, user_id=PEER),
                ],
            ),
        },
    )
    platform, source_id = await restored(api)

    history = await platform.recent_messages(source_id, PEER)

    assert set(history) == {DialogMessage("3", "6800012345678901234"), DialogMessage("2", None)}
    [read] = [c for c in api.calls if c.path == f"direct_v2/threads/{THREAD}/"]
    assert read.params["limit"] == "20"
    assert all(c.method == "GET" for c in api.calls)


# restore and check


async def test_a_restored_session_is_checked_with_one_read(api: FakeApi) -> None:
    api.on("accounts/current_user/", {"status": "ok", "user": {"pk": VIEWER, "username": "anna.shop"}})
    platform, source_id = await restored(api)

    await platform.check(source_id)

    assert api.paths() == ["accounts/current_user/"]
    assert api.calls[0].params["edit"] == "true"


async def test_a_revoked_session_fails_the_check(api: FakeApi) -> None:
    api.on("accounts/current_user/", status(403, {"status": "fail", "message": "login_required"}))
    platform, source_id = await restored(api)

    with pytest.raises(PlatformError) as refused:
        await platform.check(source_id)

    assert refused.value.failure is Failure.SESSION_REVOKED


async def test_never_restored_without_a_proxy(api: FakeApi) -> None:
    platform = AiograpiPlatform(web=NoWebLogin(), clients=api.clients())

    with pytest.raises(PlatformError) as refused:
        await platform.restore(
            uuid4(), session_data(), DEVICE, ProxyAssignment(assignment_id=1, url=SecretStr(""))
        )

    assert refused.value.failure is Failure.NETWORK


async def test_an_unreadable_saved_session_is_revoked(api: FakeApi) -> None:
    platform = AiograpiPlatform(web=NoWebLogin(), clients=api.clients())

    with pytest.raises(PlatformError) as refused:
        await platform.restore(uuid4(), SessionData(b"session-1789-1"), DEVICE, PROXY)

    assert refused.value.failure is Failure.SESSION_REVOKED


async def test_the_session_goes_through_the_sources_proxy(api: FakeApi) -> None:
    platform, source_id = await restored(api)

    client = platform._client(source_id)

    assert client.private.proxy == PROXY.url.get_secret_value()
    assert client.public.proxy == PROXY.url.get_secret_value()


# login and logout


async def test_a_login_makes_its_session_the_live_one(api: FakeApi) -> None:
    api.on("accounts/current_user/", {"status": "ok", "user": {"pk": VIEWER, "username": "anna.shop"}})
    platform = AiograpiPlatform(web=ScriptedWebLogin(api), clients=api.clients())
    source_id = uuid4()

    logged_in = await platform.login(request(source_id))
    await platform.check(source_id)

    assert logged_in.account.external_id == VIEWER


async def test_logging_out_a_refused_login_brings_the_previous_session_back(api: FakeApi) -> None:
    api.on("accounts/logout/", {"status": "ok"})
    api.on("accounts/current_user/", {"status": "ok", "user": {"pk": VIEWER, "username": "anna.shop"}})
    platform = AiograpiPlatform(web=ScriptedWebLogin(api), clients=api.clients())
    source_id = uuid4()
    await platform.login(request(source_id))
    previous = platform._client(source_id)
    second = await platform.login(request(source_id))

    await platform.logout(source_id, second.session, PROXY)

    assert platform._client(source_id) is previous
    assert api.paths() == ["accounts/logout/"]


def closed(client: Any) -> bool:
    return bool(client.private._client.is_closed)


async def test_the_replaced_session_is_closed_once_the_source_works_on(api: FakeApi) -> None:
    # the Source's next call comes after its login flow: the flow kept the new Session
    api.on("accounts/current_user/", {"status": "ok", "user": {"pk": VIEWER, "username": "anna.shop"}})
    platform = AiograpiPlatform(web=ScriptedWebLogin(api), clients=api.clients())
    source_id = uuid4()
    await platform.login(request(source_id))
    previous = platform._client(source_id)
    await platform.login(request(source_id))

    await platform.check(source_id)

    assert closed(previous)
    assert not closed(platform._client(source_id))


async def test_a_session_instagram_ended_is_closed(api: FakeApi) -> None:
    api.on("accounts/current_user/", status(403, {"status": "fail", "message": "login_required"}))
    platform, source_id = await restored(api)
    client = platform._client(source_id)

    with pytest.raises(PlatformError):
        await platform.check(source_id)

    assert closed(client)
    with pytest.raises(PlatformError) as refused:
        await platform.check(source_id)
    assert refused.value.failure is Failure.SESSION_REVOKED
    assert api.paths() == ["accounts/current_user/"]


async def test_a_disconnected_source_has_no_session_and_no_connections(api: FakeApi) -> None:
    # a proxy change: nothing goes out for the Source until restore brings it up again
    platform, source_id = await restored(api)
    client = platform._client(source_id)

    await platform.disconnect(source_id)
    await platform.disconnect(source_id)

    assert closed(client)
    with pytest.raises(PlatformError) as refused:
        await platform.check(source_id)
    assert refused.value.failure is Failure.SESSION_REVOKED
    assert api.calls == []


async def test_logging_out_the_only_session_leaves_none(api: FakeApi) -> None:
    api.on("accounts/logout/", {"status": "ok"})
    platform = AiograpiPlatform(web=ScriptedWebLogin(api), clients=api.clients())
    source_id = uuid4()
    first = await platform.login(request(source_id))

    await platform.logout(source_id, first.session, PROXY)

    with pytest.raises(PlatformError) as refused:
        await platform.check(source_id)
    assert refused.value.failure is Failure.SESSION_REVOKED


def test_a_new_device_is_a_browser_profile_the_web_login_reads() -> None:
    device = AiograpiPlatform(web=NoWebLogin()).new_device()

    assert decode_device(device).app is None
    assert decode_device(device).browser.viewport_width > 0


async def test_a_link_we_sent_carries_its_label_inside_the_link(api: FakeApi) -> None:
    sent_link = item("3", AT, user_id=VIEWER, text=None, item_type="link")
    sent_link["link"] = {
        "text": "https://example.com",
        "link_context": {"link_url": "https://example.com"},
        "client_context": "6800012345678901234",
    }
    api.on("direct_v2/inbox/", inbox([thread(THREAD, [user(PEER)], [item("1", AT, user_id=PEER)])]))
    api.on(
        f"direct_v2/threads/{THREAD}/",
        {"status": "ok", "thread": thread(THREAD, [user(PEER)], [sent_link])},
    )
    platform, source_id = await restored(api)

    history = await platform.recent_messages(source_id, PEER)

    assert set(history) == {DialogMessage("3", "6800012345678901234")}


@pytest.mark.parametrize(
    ("answer", "failure"),
    [
        # the proxy or the connection to Instagram failed: the proxy is to blame
        (curl_failure(CurlECode.COULDNT_CONNECT), Failure.NETWORK),
        (curl_failure(CurlECode.COULDNT_RESOLVE_PROXY), Failure.NETWORK),
        (curl_failure(CurlECode.PROXY), Failure.NETWORK),
        (curl_failure(CurlECode.SSL_CONNECT_ERROR), Failure.NETWORK),
        # asked, and no answer came: not proven to be the proxy
        (curl_failure(CurlECode.OPERATION_TIMEDOUT), Failure.NO_ANSWER),
        (curl_failure(CurlECode.RECV_ERROR), Failure.NO_ANSWER),
        (httpx.ReadTimeout("read timed out"), Failure.NO_ANSWER),
        (httpx.RemoteProtocolError("server disconnected"), Failure.NO_ANSWER),
    ],
)
async def test_only_connection_failures_of_a_read_blame_the_proxy(
    api: FakeApi, answer: Exception, failure: Failure
) -> None:
    api.on("users/bob/usernameinfo/", answer)
    platform, source_id = await restored(api)

    with pytest.raises(PlatformError) as refused:
        await platform.user_by_username(source_id, "bob")

    assert refused.value.failure is failure
