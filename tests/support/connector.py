"""Builds and runs the real connector for behaviour tests.

Rebuilding it over the same DSN and the same MemoryBus is a process restart.
"""

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager

from ig_connector.bus import Bus
from ig_connector.clock import Clock
from ig_connector.contract.envelope import CommandType
from ig_connector.instagram import Platform
from ig_connector.proxy import ProxyProvider
from ig_connector.runtime import (
    Connector,
    Handler,
    InboundPolling,
    LoginFlows,
    SourceStates,
    Statuses,
    default_handlers,
)
from ig_connector.runtime.proxying import ProxyLifecycle
from ig_connector.runtime.restore import SessionRestore
from ig_connector.runtime.watched import WatchedPlatform
from ig_connector.store.postgres import PostgresStore
from tests.support.fake_instagram import FakeInstagram
from tests.support.fake_proxy import FakeProxyService
from tests.support.postgres import SESSION_KEY


@asynccontextmanager
async def running_connector(
    bus: Bus,
    clock: Clock,
    dsn: str,
    *,
    sources: SourceStates | None = None,
    handlers: Mapping[CommandType, Handler] | None = None,
    max_parallel_sources: int = 20,
    store_type: type[PostgresStore] = PostgresStore,
    instagram: Platform | None = None,
    proxies: ProxyProvider | None = None,
    login_timeout: float | None = None,
    login_enabled: bool = True,
    inbound_interval: float | None = None,
) -> AsyncIterator[Connector]:
    """Run the connector until the block ends; a crash inside it fails the test on exit.

    Defaults: Source states as this process confirmed them (restored Sessions, logins,
    platform answers), the service's handlers, and a FakeInstagram and FakeProxyService
    nobody looks at (pass your own to script and observe them). Connected Sources are
    restored at start, as after a restart. Inboxes are polled only with
    `inbound_interval` (seconds), from the same instagram.
    """
    store = await store_type.open(dsn, session_key=SESSION_KEY)
    raw = instagram or FakeInstagram()
    proxies = proxies or FakeProxyService()
    statuses = Statuses(bus=bus, store=store, clock=clock)
    # the proxy lifecycle as in production: heartbeats every 60 s on the clock, moves, fail-over
    lifecycle = ProxyLifecycle(provider=proxies, platform=raw, statuses=statuses, store=store, clock=clock)
    failed_over = lifecycle.network_failed
    platform: Platform = (
        WatchedPlatform(raw, statuses, on_network_failure=failed_over)
        if isinstance(raw, FakeInstagram)
        else raw
    )
    restore = SessionRestore(
        store=store,
        clock=clock,
        platform=raw,
        proxies=lifecycle,
        statuses=statuses,
        on_network_failure=failed_over,
    )
    inbound = None
    if inbound_interval is not None:
        assert isinstance(platform, WatchedPlatform), "inbound polling reads the fake's inbox"
        # no random first delay: tests drive every poll with the clock
        inbound = InboundPolling(
            bus=bus, store=store, clock=clock, source=platform, interval=inbound_interval, stagger=False
        )
    login = LoginFlows(
        bus=bus,
        store=store,
        clock=clock,
        platform=platform,
        proxies=lifecycle,
        statuses=statuses,
        **({} if login_timeout is None else {"login_timeout": login_timeout}),
    )
    connector = Connector(
        bus=bus,
        store=store,
        clock=clock,
        sources=sources or statuses,
        handlers=default_handlers(platform, store) if handlers is None else handlers,
        max_parallel_sources=max_parallel_sources,
        login=login if login_enabled else None,
        inbound=inbound,
        restore=restore,
        proxies=lifecycle,
    )
    task = asyncio.create_task(connector.run())
    try:
        yield connector
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            await store.close()


async def eventually(condition: Callable[[], bool], within: float = 2.0) -> None:
    """Wait in real time for something the bus does not announce (a fake's call list)."""
    async with asyncio.timeout(within):
        while not condition():  # noqa: ASYNC110  the fakes have no event to wait on
            await asyncio.sleep(0.01)
