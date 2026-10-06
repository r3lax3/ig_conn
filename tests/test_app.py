import json
import logging
import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from ig_connector.app import main
from tests.test_settings import ENV


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    monkeypatch.chdir(tmp_path)  # no project .env
    for name in [*ENV, "HEALTH_FILE"]:
        monkeypatch.delenv(name, raising=False)
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def _health(monkeypatch: pytest.MonkeyPatch, path: Path) -> int:
    monkeypatch.setenv("HEALTH_FILE", str(path))
    with pytest.raises(SystemExit) as exit_:
        main(["health"])
    return int(exit_.value.code or 0)


def test_health_is_ok_only_while_the_file_is_fresh(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "health"
    assert _health(monkeypatch, path) == 1  # service never started

    path.touch()  # works without any service config in env
    assert _health(monkeypatch, path) == 0

    old = time.time() - 31
    os.utime(path, (old, old))
    assert _health(monkeypatch, path) == 1  # loop hung or a probe failing for 30 s


def test_bad_config_stops_with_field_names_and_no_secret_values(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("CHANNEL_TYPE")
    monkeypatch.setenv("MAX_PARALLEL_SOURCES", "0")

    with pytest.raises(SystemExit) as exit_:
        main([])

    assert exit_.value.code == 2
    out = capsys.readouterr().out
    [line] = [json.loads(text) for text in out.splitlines()]
    assert line["message"] == "invalid configuration"
    assert {problem.split(":")[0] for problem in line["problems"]} == {"channel_type", "max_parallel_sources"}
    for secret in ("kafka-secret", "s3-secret", "db-secret"):
        assert secret not in out


async def test_proxy_provider_follows_the_config(monkeypatch: pytest.MonkeyPatch) -> None:
    from contextlib import AsyncExitStack

    from ig_connector.app import proxy_provider
    from ig_connector.proxy.http import ProxyService
    from ig_connector.proxy.static import StaticProxy
    from ig_connector.settings import Settings

    for name, value in ENV.items():
        monkeypatch.setenv(name, value)
    async with AsyncExitStack() as resources:
        assert proxy_provider(Settings(), resources) is None
        monkeypatch.setenv("STATIC_PROXY_URL", "http://u:p@h:1")
        assert isinstance(proxy_provider(Settings(), resources), StaticProxy)
        monkeypatch.delenv("STATIC_PROXY_URL")
        monkeypatch.setenv("PROXY_SERVICE_URL", "http://proxy.test")
        monkeypatch.setenv("PROXY_SERVICE_TOKEN", "tok")
        assert isinstance(proxy_provider(Settings(), resources), ProxyService)
