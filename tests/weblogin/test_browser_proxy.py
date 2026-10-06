"""The browser's proxy: what Chromium cannot use is refused before it starts."""

import pytest
from pydantic import SecretStr

from ig_connector.instagram import Failure, UnsupportedProxy
from ig_connector.instagram.weblogin.browser import browser_proxy
from ig_connector.proxy import ProxyAssignment


def test_socks5_with_credentials_is_not_for_the_login() -> None:
    proxy = ProxyAssignment(assignment_id=1, url=SecretStr("socks5://u:p@10.0.0.1:1080"))

    with pytest.raises(UnsupportedProxy) as refused:
        browser_proxy(proxy)

    # not NETWORK: another proxy of the pool would fail the same, no fail-over
    assert refused.value.failure is Failure.REJECTED


def test_socks5_without_credentials_is_fine() -> None:
    proxy = ProxyAssignment(assignment_id=1, url=SecretStr("socks5://10.0.0.1:1080"))

    assert browser_proxy(proxy)["server"] == "socks5://10.0.0.1:1080"
