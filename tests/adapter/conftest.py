"""The aiograpi adapter over a pretend Instagram private API (tests/support/instagram_api.py)."""

from uuid import UUID, uuid4

import pytest

from ig_connector.instagram import Device, LoginRequest
from ig_connector.instagram.adapter.platform import AiograpiPlatform
from ig_connector.instagram.weblogin import WebSession
from tests.support.instagram_api import PROXY, FakeApi, session_data

PEER = "4242"
THREAD = "340282366841710300949128000000000001"
DEVICE = Device(b"{}")


@pytest.fixture
def api() -> FakeApi:
    return FakeApi()


class NoWebLogin:
    async def login(self, request: LoginRequest) -> WebSession:
        raise AssertionError("no login in this test")


async def restored(api: FakeApi, *, source_id: UUID | None = None) -> tuple[AiograpiPlatform, UUID]:
    """An adapter with one Source's saved Session live again (as after a restart)."""
    platform = AiograpiPlatform(web=NoWebLogin(), clients=api.clients())
    source_id = source_id or uuid4()
    await platform.restore(source_id, session_data(), DEVICE, PROXY)
    return platform, source_id
