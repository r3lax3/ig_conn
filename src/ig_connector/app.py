"""The service: built from env, runs the connector over Kafka and Postgres until SIGTERM.

ig-connector            run the service (config from env, migrations on start)
ig-connector health     exit 0 if the running service is healthy, 1 otherwise
"""

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from ig_connector import health
from ig_connector.bus.kafka import KafkaBus
from ig_connector.clock import Clock, SystemClock
from ig_connector.contract.envelope import CommandType
from ig_connector.instagram import InboundSource
from ig_connector.instagram.adapter.platform import AiograpiPlatform
from ig_connector.instagram.weblogin import WebLogin
from ig_connector.logs import configure_logging
from ig_connector.proxy import ProxyProvider
from ig_connector.proxy.http import ProxyService
from ig_connector.proxy.none import NoProxies
from ig_connector.proxy.static import StaticProxy
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
from ig_connector.settings import Settings
from ig_connector.store.postgres import PostgresStore

log = logging.getLogger(__name__)

# on SIGTERM, running commands get this long to finish: below docker stop's default 10 s
DRAIN_GRACE = 8.0


@dataclass(frozen=True, slots=True)
class Infrastructure:
    """What the domain wiring gets to build its ports from."""

    settings: Settings
    store: PostgresStore
    bus: KafkaBus
    clock: Clock
    # register closers of clients opened while wiring (HTTP sessions, browsers)
    resources: AsyncExitStack


@dataclass(frozen=True, slots=True)
class Wiring:
    sources: SourceStates
    handlers: Mapping[CommandType, Handler]
    # without it connect.* are answered failed at once
    login: LoginFlows | None = None
    # inboxes are not polled without it; polled every settings.inbound_poll_interval
    inbound: InboundSource | None = None
    # brings connected Sources up at start; without it none is active after a restart.
    # Its Statuses must be the `sources` above and the login flow's; give it the bare adapter
    # (it reports its own outcomes), the WatchedPlatform to handlers and inbound. Sources in
    # error are rechecked only on the inbound poll cadence: wire `inbound` with it
    restore: SessionRestore | None = None
    # heartbeats, planned moves and fail-over of the Sources' proxies (contract 8). Build it
    # from proxy_provider(...) and hand THE SAME instance to the login flow and the restore as
    # their `proxies`, its `network_failed` to WatchedPlatform and SessionRestore
    proxies: ProxyLifecycle | None = None
    # long-running loops next to the connector (proxy heartbeats); one failing stops the service
    background: Sequence[Callable[[], Coroutine[Any, Any, None]]] = field(default=())


Wire = Callable[[Infrastructure], Awaitable[Wiring]]


def proxy_provider(settings: Settings, resources: AsyncExitStack) -> ProxyProvider | None:
    """Where Sources get proxies from; None: no Source may go to the network at all."""
    if settings.proxy_service_url is not None and settings.proxy_service_token is not None:
        service = ProxyService(
            base_url=settings.proxy_service_url,
            token=settings.proxy_service_token,
            channel_type=settings.channel_type,
        )
        resources.push_async_callback(service.aclose)
        return service
    if settings.static_proxy_url is not None:
        return StaticProxy(settings.static_proxy_url)
    return None


async def default_wiring(infra: Infrastructure) -> Wiring:
    """The domain ports the service runs with; new adapters are built from infra.settings here."""
    return await platform_wiring(infra, provider=proxy_provider(infra.settings, infra.resources))


async def platform_wiring(infra: Infrastructure, *, provider: ProxyProvider | None) -> Wiring:
    """Instagram through aiograpi and the web login, every Source behind its proxy.

    Without a proxy provider nothing goes to Instagram (never direct): logins fail and
    connected Sources report `error` + `source_offline`, their commands are answered
    source_offline, and nothing is polled.
    """
    statuses = Statuses(bus=infra.bus, store=infra.store, clock=infra.clock)
    adapter = AiograpiPlatform(web=WebLogin(clock=infra.clock))
    infra.resources.push_async_callback(adapter.close)
    if provider is None:
        platform = WatchedPlatform(adapter, statuses)
        return Wiring(
            sources=statuses,
            handlers=default_handlers(platform, infra.store),
            login=LoginFlows(
                bus=infra.bus,
                store=infra.store,
                clock=infra.clock,
                platform=platform,
                proxies=NoProxies(),
                statuses=statuses,
            ),
            restore=SessionRestore(
                store=infra.store, clock=infra.clock, platform=adapter, proxies=NoProxies(), statuses=statuses
            ),
        )
    # the one provider of login and restore: it remembers, heartbeats and moves assignments
    lifecycle = ProxyLifecycle(
        provider=provider, platform=adapter, statuses=statuses, store=infra.store, clock=infra.clock
    )
    # handlers, polls and the login flow: every answer moves the Source's status, a confirmed
    # network failure moves it to another proxy; restore reports its own outcomes
    platform = WatchedPlatform(adapter, statuses, on_network_failure=lifecycle.network_failed)
    return Wiring(
        sources=statuses,
        handlers=default_handlers(platform, infra.store),
        login=LoginFlows(
            bus=infra.bus,
            store=infra.store,
            clock=infra.clock,
            platform=platform,
            proxies=lifecycle,
            statuses=statuses,
        ),
        inbound=platform,
        restore=SessionRestore(
            store=infra.store,
            clock=infra.clock,
            platform=adapter,
            proxies=lifecycle,
            statuses=statuses,
            on_network_failure=lifecycle.network_failed,
        ),
        proxies=lifecycle,
    )


async def serve(
    settings: Settings,
    *,
    bus: KafkaBus | None = None,
    wire: Wire = default_wiring,
    clock: Clock | None = None,
    stop: asyncio.Event | None = None,
) -> None:
    """Run until cancelled or until a part fails (raises).

    `stop` set: a clean stop. Running commands get DRAIN_GRACE seconds to finish, then
    it ends like a cancel (raises CancelledError).
    """
    clock = clock or SystemClock()
    bus = bus or KafkaBus.from_settings(settings)
    async with AsyncExitStack() as resources:
        store = await PostgresStore.open(
            settings.database_url.get_secret_value(), session_key=settings.session_encryption_key
        )
        resources.push_async_callback(store.close)
        resources.push_async_callback(bus.stop)  # stop() is safe on a bus that never started
        await bus.start()
        wiring = await wire(Infrastructure(settings, store, bus, clock, resources))
        connector = Connector(
            bus=bus,
            store=store,
            clock=clock,
            sources=wiring.sources,
            handlers=wiring.handlers,
            max_parallel_sources=settings.max_parallel_sources,
            login=wiring.login,
            inbound=None
            if wiring.inbound is None
            else InboundPolling(
                bus=bus,
                store=store,
                clock=clock,
                source=wiring.inbound,
                interval=settings.inbound_poll_interval,
            ),
            restore=wiring.restore,
            proxies=wiring.proxies,
        )
        serving = asyncio.current_task()

        async def stopping(stop: asyncio.Event) -> None:
            await stop.wait()
            log.info("stopping: letting running commands finish")
            await connector.drain(DRAIN_GRACE)
            if serving is not None:
                serving.cancel()

        async def consuming() -> bool:
            return bus.is_consuming()

        log.info("service started", extra={"channel_type": settings.channel_type})
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(connector.run(), name="connector")
            tasks.create_task(
                health.beat(settings.health_file, [store.ping, consuming], clock), name="health"
            )
            for loop in wiring.background:
                tasks.create_task(loop())
            if stop is not None:
                tasks.create_task(stopping(stop), name="stopping")


class _HealthSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    health_file: Path = health.DEFAULT_FILE


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ig-connector", description=__doc__.split("\n\n")[0])
    parser.add_argument("action", nargs="?", choices=["run", "health"], default="run")
    args = parser.parse_args(argv)
    if args.action == "health":
        sys.exit(0 if health.is_healthy(_HealthSettings().health_file) else 1)

    try:
        settings = Settings()
    except ValidationError as exc:
        configure_logging()
        # field names only: the error's own text repeats the input, secrets included
        problems = [f"{'.'.join(map(str, e['loc']))}: {e['type']}" for e in exc.errors()]
        log.error("invalid configuration", extra={"problems": problems})
        sys.exit(2)
    configure_logging(settings.log_level)
    sys.exit(asyncio.run(_run(settings)))


async def _run(settings: Settings) -> int:
    stopping = asyncio.Event()
    service = asyncio.create_task(serve(settings, stop=stopping))
    loop = asyncio.get_running_loop()

    def stop() -> None:
        # once: a second signal must not cancel the drain or the cleanup (leaving the group,
        # closing the pool)
        if stopping.is_set():
            return
        stopping.set()
        # the drain only runs once the connector does: a start hanging on Kafka or Postgres,
        # or a drain overrunning, is cancelled the old way
        loop.call_later(DRAIN_GRACE + 1.0, service.cancel)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop)
    try:
        await service
    except asyncio.CancelledError:
        # commands still in flight after the drain are not committed and come again
        log.info("service stopped")
        return 0
    except Exception:
        log.exception("service failed")
        return 1
    return 0


if __name__ == "__main__":
    main()
