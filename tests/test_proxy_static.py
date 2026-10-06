from uuid import uuid4

from pydantic import SecretStr

from ig_connector.proxy import ProxyRequirement, TransportState
from ig_connector.proxy.static import StaticProxy


async def test_every_source_gets_the_configured_proxy_and_is_never_moved() -> None:
    static = StaticProxy(SecretStr("http://u:p@proxy.test:3128"))

    first = await static.reserve(uuid4(), ProxyRequirement("DE"))
    second = await static.reserve(uuid4(), ProxyRequirement("US", "mobile"))
    beat = await static.heartbeat(uuid4(), first.assignment_id, TransportState.CONNECTED)
    await static.connection_failed(uuid4(), first.assignment_id, "refused")
    after_failure = await static.reserve(uuid4(), ProxyRequirement("DE"))

    assert first == second == after_failure
    assert first.url.get_secret_value() == "http://u:p@proxy.test:3128"
    assert beat.pending_rebalance is None
