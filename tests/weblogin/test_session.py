"""aiograpi's side of the web login, as far as it goes without Instagram."""

import asyncio
from uuid import uuid4

import pytest
from aiograpi.exceptions import (
    AuthRequiredProxyError,
    ChallengeRequired,
    ClientConnectionError,
    ClientError,
    ClientRequestTimeout,
    ConnectProxyError,
    FeedbackRequired,
    LoginRequired,
    PleaseWaitFewMinutes,
    ProxyAddressIsBlocked,
    RecaptchaChallengeForm,
)
from pydantic import SecretStr

from ig_connector.instagram import Failure, PlatformError
from ig_connector.instagram.weblogin import AiograpiSessions, aiograpi_failure
from ig_connector.proxy import ProxyAssignment
from tests.support.instagram_api import SESSIONID, VIEWER, FakeApi, status, user_info

PROXY = ProxyAssignment(assignment_id=1, url=SecretStr("http://u:p@127.0.0.1:1"))


@pytest.mark.parametrize(
    ("error", "failure"),
    [
        (ChallengeRequired(), Failure.CHALLENGE),
        (RecaptchaChallengeForm(), Failure.CHALLENGE),
        (FeedbackRequired(), Failure.FLOOD),
        (LoginRequired(), Failure.SESSION_REVOKED),
        (PleaseWaitFewMinutes(), Failure.RATE_LIMITED),
        (ProxyAddressIsBlocked(), Failure.NETWORK),
        (ConnectProxyError(), Failure.NETWORK),
        (AuthRequiredProxyError(), Failure.NETWORK),
        # HTTP 408: Instagram answered
        (ClientRequestTimeout(), Failure.REJECTED),
        (ClientConnectionError(), Failure.NETWORK),
        (ConnectionRefusedError(), Failure.NETWORK),
        (ClientError(), Failure.REJECTED),
    ],
)
def test_aiograpi_errors_become_typed_refusals(error: Exception, failure: Failure) -> None:
    assert aiograpi_failure(error).failure is failure


def test_the_refusal_names_the_error_type_not_its_message() -> None:
    refusal = aiograpi_failure(LoginRequired("sessionid=1789%3Asecret rejected"))

    assert "secret" not in refusal.detail
    assert "LoginRequired" in refusal.detail


async def test_no_proxy_no_session() -> None:
    with pytest.raises(PlatformError) as caught:
        await AiograpiSessions().open(
            uuid4(),
            SecretStr("1789%3A" + "x" * 40),
            ProxyAssignment(assignment_id=1, url=SecretStr("")),
            None,
        )

    assert caught.value.failure is Failure.NETWORK


async def test_a_session_id_aiograpi_cannot_read_is_refused_before_any_request() -> None:
    with pytest.raises(PlatformError) as caught:
        await AiograpiSessions().open(uuid4(), SecretStr("short"), PROXY, None)

    assert caught.value.failure is Failure.REJECTED


async def test_a_web_session_aiograpi_cannot_take_over_is_logged_out() -> None:
    api = FakeApi()
    api.on(f"users/{VIEWER}/info/", status(429, {"status": "fail"}))
    api.on("accounts/logout/", {"status": "ok"})

    with pytest.raises(PlatformError):
        await AiograpiSessions(clients=api.clients()).open(uuid4(), SecretStr(SESSIONID), PROXY, None)

    [logout] = [c for c in api.calls if c.path == "accounts/logout/"]
    assert logout.method == "POST"


async def test_a_web_session_is_logged_out_when_the_login_times_out_while_taking_it_over() -> None:
    # the login flow's deadline cancels login_by_sessionid: the commonest failure
    api = FakeApi()
    api.hold[f"users/{VIEWER}/info/"] = asyncio.Event()
    api.on("accounts/logout/", {"status": "ok"})
    opening = asyncio.create_task(
        AiograpiSessions(clients=api.clients()).open(uuid4(), SecretStr(SESSIONID), PROXY, None)
    )
    while not api.calls:  # noqa: ASYNC110  the fake has no event for "request arrived"
        await asyncio.sleep(0.01)

    opening.cancel()
    with pytest.raises(asyncio.CancelledError):
        await opening

    assert "accounts/logout/" in api.paths()


async def test_a_web_session_aiograpi_took_over_stays_logged_in() -> None:
    api = FakeApi()
    api.on(f"users/{VIEWER}/info/", user_info(VIEWER, "anna.shop"))

    opened = await AiograpiSessions(clients=api.clients()).open(uuid4(), SecretStr(SESSIONID), PROXY, None)

    assert opened.account.external_id == VIEWER
    assert "accounts/logout/" not in api.paths()
