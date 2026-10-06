"""The service's wiring of the real Instagram adapter (Kafka-backed runs: tests/integration)."""

from contextlib import AsyncExitStack
from typing import Any, cast

from ig_connector.app import Infrastructure, platform_wiring
from ig_connector.contract.envelope import CommandType
from ig_connector.runtime.send import SendText
from ig_connector.runtime.watched import WatchedPlatform
from tests.behaviour.conftest import START
from tests.support.crm import CHANNEL_TYPE
from tests.support.fake_clock import FakeClock
from tests.support.fake_proxy import FakeProxyService
from tests.support.memory_bus import MemoryBus


def _infra(resources: AsyncExitStack) -> Infrastructure:
    # the wiring only keeps references to bus and store
    bus, store = cast(Any, MemoryBus(CHANNEL_TYPE)), cast(Any, object())
    return Infrastructure(cast(Any, None), store, bus, FakeClock(START), resources)


async def test_with_a_proxy_provider_sources_log_in_restore_and_are_polled() -> None:
    async with AsyncExitStack() as resources:
        wiring = await platform_wiring(_infra(resources), provider=FakeProxyService())

    assert wiring.login is not None
    assert wiring.restore is not None
    assert wiring.proxies is not None
    assert isinstance(wiring.inbound, WatchedPlatform)
    assert isinstance(wiring.handlers[CommandType.SEND].run, SendText)


async def test_without_a_proxy_provider_logins_and_restores_run_but_nothing_is_polled() -> None:
    async with AsyncExitStack() as resources:
        wiring = await platform_wiring(_infra(resources), provider=None)

    # they answer source_offline (no proxy, never direct): tests/behaviour/test_proxy_lifecycle.py
    assert wiring.login is not None and wiring.restore is not None
    assert (wiring.inbound, wiring.proxies) == (None, None)
