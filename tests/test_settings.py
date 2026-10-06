from pathlib import Path

import pytest
from pydantic import ValidationError

from ig_connector.settings import Settings

ENV = {
    "CHANNEL_TYPE": "individual_instagram_account",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "KAFKA_USERNAME": "individual-instagram-account-connector",
    "KAFKA_PASSWORD": "kafka-secret",
    "S3_ENDPOINT": "http://localhost:3900",
    "S3_BUCKET": "individual-instagram-account",
    "S3_ACCESS_KEY_ID": "GK1",
    "S3_SECRET_ACCESS_KEY": "s3-secret",
    "DATABASE_URL": "postgresql://u:db-secret@localhost/db",
    "SESSION_ENCRYPTION_KEY": "sGsqVXbPXcJ4ePv4wZ0VZm0Yw3hb2u2x8cXyY4j1Zyc=",
}


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)  # no project .env
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)


def test_bus_names_follow_channel_type() -> None:
    settings = Settings()
    assert settings.commands_topic == "crm.connector.commands.individual_instagram_account"
    assert settings.events_topic == "crm.connector.events.individual_instagram_account"
    assert settings.consumer_group == "connector.individual_instagram_account"


def test_region_defaults_to_crm_bus() -> None:
    assert Settings().s3_region == "crm-bus"


def test_secrets_do_not_leak_in_repr() -> None:
    text = repr(Settings())
    for secret in ("kafka-secret", "s3-secret", "db-secret", "sGsqVXbPXcJ4"):
        assert secret not in text


def test_missing_required_setting_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KAFKA_PASSWORD")
    with pytest.raises(ValidationError, match="kafka_password"):
        Settings()


def test_parallel_sources_default_to_twenty_and_must_be_positive(monkeypatch: pytest.MonkeyPatch) -> None:
    assert Settings().max_parallel_sources == 20
    monkeypatch.setenv("MAX_PARALLEL_SOURCES", "0")
    with pytest.raises(ValidationError, match="max_parallel_sources"):
        Settings()


def test_session_encryption_key_must_be_a_fernet_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SESSION_ENCRYPTION_KEY", "not-a-key")
    with pytest.raises(ValidationError, match="session_encryption_key"):
        Settings()


def test_inbound_poll_interval_defaults_to_thirty_seconds_and_has_a_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert Settings().inbound_poll_interval == 30
    monkeypatch.setenv("INBOUND_POLL_INTERVAL", "1")
    with pytest.raises(ValidationError, match="inbound_poll_interval"):
        Settings()


def test_no_proxy_configured_by_default() -> None:
    settings = Settings()
    assert (settings.proxy_service_url, settings.proxy_service_token, settings.static_proxy_url) == (
        None,
        None,
        None,
    )


def test_proxy_service_needs_its_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROXY_SERVICE_URL", "http://proxy.test")
    with pytest.raises(ValidationError, match="PROXY_SERVICE_TOKEN"):
        Settings()


def test_proxy_service_and_static_proxy_exclude_each_other(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROXY_SERVICE_URL", "http://proxy.test")
    monkeypatch.setenv("PROXY_SERVICE_TOKEN", "tok")
    monkeypatch.setenv("STATIC_PROXY_URL", "http://u:p@h:1")
    with pytest.raises(ValidationError, match="one of"):
        Settings()


def test_proxy_secrets_do_not_leak_in_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STATIC_PROXY_URL", "http://u:static-secret@h:1")
    assert "static-secret" not in repr(Settings())


def test_empty_proxy_settings_mean_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROXY_SERVICE_URL", "")
    monkeypatch.setenv("PROXY_SERVICE_TOKEN", "")
    assert Settings().proxy_service_url is None
