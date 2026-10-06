"""Sending through the aiograpi adapter: our label, the existing dialog, honest refusals.

A refusal other than UNKNOWN_AFTER_SEND tells CRM it may retry (rate_limited, peer_flood,
network_error): the adapter says so only when the message provably did not go out.
"""

from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from curl_cffi import CurlOpt
from curl_cffi.const import CurlECode

from ig_connector.instagram import Failure, OutgoingText, PlatformError
from ig_connector.instagram.adapter.client import new_client
from tests.adapter.conftest import PEER, THREAD, restored
from tests.support.instagram_api import PROXY, FakeApi, curl_failure, inbox, item, status, thread, user

LABEL = "6800012345678901234"
AT = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
SENT = {
    "status": "ok",
    "payload": {
        "item_id": "31000000000000000000000000000000001",
        "client_context": LABEL,
        "timestamp": "1791288000000000",
        "thread_id": THREAD,
    },
}


def dialog_with_peer(api: FakeApi) -> None:
    api.on(
        "direct_v2/inbox/",
        inbox(
            [thread(THREAD, [user(PEER)], [item("30000000000000000000000000000000001", AT, user_id=PEER)])]
        ),
    )


async def test_the_message_carries_our_label_and_goes_into_the_existing_dialog(api: FakeApi) -> None:
    dialog_with_peer(api)
    api.on("direct_v2/threads/broadcast/text/", SENT)
    platform, source_id = await restored(api)

    assert await platform.has_dialog(source_id, PEER)
    sent = await platform.send_text(OutgoingText(source_id, PEER, "hi there", LABEL))

    assert (sent.message_id, sent.thread_id) == ("31000000000000000000000000000000001", THREAD)
    [send] = [c for c in api.calls if c.path.startswith("direct_v2/threads/broadcast/")]
    assert send.form["client_context"] == LABEL
    assert send.form["mutation_token"] == LABEL
    assert send.form["offline_threading_id"] == LABEL
    # by thread, never by user ids: that would open a new dialog
    assert send.form["thread_ids"] == f"[{THREAD}]"
    assert "recipient_users" not in send.form


async def test_no_dialog_with_the_peer_is_said_without_sending(api: FakeApi) -> None:
    api.on("direct_v2/inbox/", inbox([]))
    api.on("direct_v2/threads/get_by_participants/", {"status": "ok", "users": []})
    platform, source_id = await restored(api)

    assert not await platform.has_dialog(source_id, PEER)
    with pytest.raises(PlatformError) as refused:
        await platform.send_text(OutgoingText(source_id, PEER, "hi", LABEL))

    assert refused.value.failure is Failure.REJECTED
    assert not any(p.startswith("direct_v2/threads/broadcast/") for p in api.paths())


async def test_a_dialog_found_by_participants_counts_too(api: FakeApi) -> None:
    api.on("direct_v2/inbox/", inbox([]))
    api.on(
        "direct_v2/threads/get_by_participants/",
        {
            "status": "ok",
            "users": [user(PEER)],
            "thread": thread(
                THREAD, [user(PEER)], [item("30000000000000000000000000000000001", AT, user_id=PEER)]
            ),
        },
    )
    platform, source_id = await restored(api)

    assert await platform.has_dialog(source_id, PEER)


async def test_a_pending_request_is_not_a_dialog(api: FakeApi) -> None:
    api.on("direct_v2/inbox/", inbox([]))
    pending = thread(THREAD, [user(PEER)], [item("1", AT, user_id=PEER)], pending=True)
    api.on("direct_v2/threads/get_by_participants/", {"status": "ok", "users": [], "thread": pending})
    platform, source_id = await restored(api)

    assert not await platform.has_dialog(source_id, PEER)


@pytest.mark.parametrize(
    ("answer", "failure"),
    [
        (status(400, {"status": "fail", "message": "feedback_required"}), Failure.FLOOD),
        (status(429, {"status": "fail", "message": "rate limited"}), Failure.RATE_LIMITED),
        (
            status(400, {"status": "fail", "message": "Please wait a few minutes before you try again."}),
            Failure.RATE_LIMITED,
        ),
        (status(403, {"status": "fail", "message": "login_required"}), Failure.SESSION_REVOKED),
        (status(400, {"status": "fail", "message": "challenge_required"}), Failure.CHALLENGE),
        # an answer with status fail: Instagram did not take it
        (httpx.Response(200, json={"status": "fail", "message": "nope"}), Failure.REJECTED),
        # the proxy never connected: nothing left
        (curl_failure(CurlECode.COULDNT_CONNECT), Failure.NETWORK),
        (curl_failure(CurlECode.COULDNT_RESOLVE_PROXY), Failure.NETWORK),
        (curl_failure(CurlECode.PROXY), Failure.NETWORK),
        (curl_failure(CurlECode.SSL_CONNECT_ERROR), Failure.NETWORK),
    ],
)
async def test_refusals_that_prove_nothing_went_out(
    api: FakeApi, answer: httpx.Response | Exception, failure: Failure
) -> None:
    dialog_with_peer(api)
    api.on("direct_v2/threads/broadcast/text/", answer)
    platform, source_id = await restored(api)
    await platform.has_dialog(source_id, PEER)

    with pytest.raises(PlatformError) as refused:
        await platform.send_text(OutgoingText(source_id, PEER, "hi", LABEL))

    assert refused.value.failure is failure


@pytest.mark.parametrize(
    "answer",
    [
        # written, then the answer never came or broke
        curl_failure(CurlECode.OPERATION_TIMEDOUT),
        curl_failure(CurlECode.RECV_ERROR),
        curl_failure(CurlECode.SEND_ERROR),
        httpx.ReadTimeout("read timed out"),
        httpx.RemoteProtocolError("server disconnected"),
        # Instagram itself failed: it may have stored the message
        status(500, {"status": "fail"}),
        status(502, {"status": "fail"}),
        # aiograpi's curl transport, after Instagram's whole answer arrived over HTTP/1.1
        httpx.ConnectError("curl private transport did not negotiate HTTP/2"),
        # a redirect would POST the message again: not followed
        httpx.Response(307, headers={"location": "https://i.instagram.com/api/v1/again/"}),
        # it answered 200, but not in a form we read
        httpx.Response(200, content=b"<html>oops</html>"),
        httpx.Response(200, json={"status": "ok"}),
    ],
)
async def test_anything_after_the_request_was_written_is_unknown(
    api: FakeApi, answer: httpx.Response | Exception
) -> None:
    dialog_with_peer(api)
    api.on("direct_v2/threads/broadcast/text/", answer)
    platform, source_id = await restored(api)
    await platform.has_dialog(source_id, PEER)

    with pytest.raises(PlatformError) as refused:
        await platform.send_text(OutgoingText(source_id, PEER, "hi", LABEL))

    assert refused.value.failure is Failure.UNKNOWN_AFTER_SEND


async def test_a_message_goes_out_once_even_when_aiograpi_would_repeat_it(api: FakeApi) -> None:
    # aiograpi sends again after HTTP 408 (60 s later) and after a broken read
    dialog_with_peer(api)
    api.on("direct_v2/threads/broadcast/text/", status(408, {"status": "fail"}), SENT)
    platform, source_id = await restored(api)
    await platform.has_dialog(source_id, PEER)

    with pytest.raises(PlatformError):
        await platform.send_text(OutgoingText(source_id, PEER, "hi", LABEL))

    assert sum(p.startswith("direct_v2/threads/broadcast/") for p in api.paths()) == 1


async def test_sending_without_a_live_session_is_refused_before_anything_goes_out(api: FakeApi) -> None:
    platform, _ = await restored(api)

    with pytest.raises(PlatformError) as refused:
        await platform.send_text(OutgoingText(uuid4(), PEER, "hi", LABEL))

    assert refused.value.failure is Failure.SESSION_REVOKED
    assert api.calls == []


async def test_the_label_is_ours_only_for_that_send(api: FakeApi) -> None:
    dialog_with_peer(api)
    api.on("direct_v2/threads/broadcast/text/", SENT)
    platform, source_id = await restored(api)
    await platform.has_dialog(source_id, PEER)
    await platform.send_text(OutgoingText(source_id, PEER, "one", LABEL))
    await platform.send_text(OutgoingText(source_id, PEER, "two", "6800099999999999999"))

    labels = [
        c.form["client_context"] for c in api.calls if c.path.startswith("direct_v2/threads/broadcast/")
    ]
    assert labels == [LABEL, "6800099999999999999"]


async def test_a_message_never_follows_a_redirect(api: FakeApi) -> None:
    dialog_with_peer(api)
    moved = httpx.Response(308, headers={"location": "https://i.instagram.com/api/v1/again/"})
    api.on("direct_v2/threads/broadcast/text/", moved)
    api.on("again/", SENT)
    platform, source_id = await restored(api)
    await platform.has_dialog(source_id, PEER)

    with pytest.raises(PlatformError):
        await platform.send_text(OutgoingText(source_id, PEER, "hi", LABEL))

    assert "again/" not in api.paths()


def test_a_message_goes_on_a_connection_of_its_own_that_curl_never_resends_on() -> None:
    # curl resends a request whose pooled connection died: then a connect error would lie
    client = new_client(None, PROXY.url.get_secret_value())

    transport = client.broadcast_transport()

    assert transport._curl_options[CurlOpt.FRESH_CONNECT] == 1
    assert transport._curl_options[CurlOpt.FORBID_REUSE] == 1
    assert transport._proxy == PROXY.url.get_secret_value()
