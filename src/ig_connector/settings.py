from pathlib import Path
from typing import Literal

from cryptography.fernet import Fernet
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from ig_connector.bus import commands_topic, events_topic
from ig_connector.health import DEFAULT_FILE as DEFAULT_HEALTH_FILE


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    channel_type: str = Field(min_length=1)

    kafka_bootstrap_servers: str
    kafka_username: str
    kafka_password: SecretStr

    s3_endpoint: str
    s3_region: str = "crm-bus"
    s3_bucket: str
    s3_access_key_id: str
    s3_secret_access_key: SecretStr

    database_url: SecretStr
    # Fernet key for Sessions and devices at rest: `Fernet.generate_key()`
    session_encryption_key: SecretStr

    # Sources talking to Instagram at the same time (commands, inbox polls, restores)
    max_parallel_sources: int = Field(default=20, ge=1)
    # seconds between inbox polls of a Source: the inbound delay CRM sees, at most
    inbound_poll_interval: float = Field(default=30, ge=5)

    # where Sources get their proxies: the proxy service (contract 8), or one static
    # proxy for all. Neither: no Source goes to the network (never directly)
    proxy_service_url: str | None = None
    proxy_service_token: SecretStr | None = None
    static_proxy_url: SecretStr | None = None

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    # touched while the service is healthy; `ig-connector health` checks how fresh it is
    health_file: Path = DEFAULT_HEALTH_FILE

    @field_validator("session_encryption_key")
    @classmethod
    def _fernet_key(cls, value: SecretStr) -> SecretStr:
        try:
            Fernet(value.get_secret_value())
        except ValueError:
            # the message only: never the key
            raise ValueError("must be a Fernet key (32 url-safe base64 bytes)") from None
        return value

    @field_validator("proxy_service_url", "proxy_service_token", "static_proxy_url", mode="before")
    @classmethod
    def _empty_is_unset(cls, value: object) -> object:
        # `NAME=` as left in a copied .env.example
        return None if value == "" else value

    @model_validator(mode="after")
    def _one_proxy_source(self) -> "Settings":
        if self.proxy_service_url is not None and self.proxy_service_token is None:
            raise ValueError("PROXY_SERVICE_URL needs PROXY_SERVICE_TOKEN")
        if self.proxy_service_url is not None and self.static_proxy_url is not None:
            raise ValueError("set one of PROXY_SERVICE_URL and STATIC_PROXY_URL")
        return self

    @property
    def commands_topic(self) -> str:
        return commands_topic(self.channel_type)

    @property
    def events_topic(self) -> str:
        return events_topic(self.channel_type)

    @property
    def consumer_group(self) -> str:
        return f"connector.{self.channel_type}"
