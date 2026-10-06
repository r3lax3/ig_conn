"""Behaviour tests: the connector as a whole over an in-memory bus, Postgres and a fake clock.

Every test here is marked `behaviour` and writes a run trace (tests/support/tracing.py).
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.support.crm import CHANNEL_TYPE
from tests.support.fake_clock import FakeClock
from tests.support.fake_instagram import FakeInstagram
from tests.support.fake_proxy import FakeProxyService
from tests.support.memory_bus import MemoryBus
from tests.support.tracing import Trace

START = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
_HERE = Path(__file__).parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if item.path.is_relative_to(_HERE):
            item.add_marker(pytest.mark.behaviour)


@pytest.fixture
def clock(trace: Trace) -> FakeClock:
    clock = FakeClock(START)
    trace.clock = clock
    return clock


@pytest.fixture
def bus(trace: Trace) -> MemoryBus:
    return MemoryBus(CHANNEL_TYPE, listener=trace.record)


@pytest.fixture(autouse=True)
def _traced(trace: Trace) -> Trace:
    return trace


@pytest.fixture
def instagram(trace: Trace) -> FakeInstagram:
    return FakeInstagram(listener=trace.record)


@pytest.fixture
def proxies(trace: Trace) -> FakeProxyService:
    return FakeProxyService(listener=trace.record)
