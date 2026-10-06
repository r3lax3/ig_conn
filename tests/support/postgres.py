"""A clean Postgres schema per test on the local stand (DATABASE_URL from env or .env).

Tests share one database with other runs, so each test gets its own schema, set as
search_path in the DSN, and the schema is dropped afterwards. Without a reachable
Postgres the test is skipped with the reason.
"""

import asyncio
from collections.abc import AsyncIterator
from uuid import uuid4

import asyncpg
import pytest
from cryptography.fernet import Fernet
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# one per test session: a connector rebuilt over the same schema (a restart) reads its Sessions
SESSION_KEY = SecretStr(Fernet.generate_key().decode())

SKIP_HINT = "behaviour tests need the local Postgres: set DATABASE_URL in .env (see .env.example)"


class _DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: SecretStr | None = None


def with_search_path(dsn: str, schema: str) -> str:
    # asyncpg passes unknown DSN query parameters to the server as settings
    return f"{dsn}{'&' if '?' in dsn else '?'}search_path={schema}"


@pytest.fixture
async def postgres_dsn() -> AsyncIterator[str]:
    """DSN of an empty schema for this test only; reuse it to rebuild the connector."""
    url = _DatabaseSettings().database_url
    if url is None:
        pytest.skip(f"no DATABASE_URL; {SKIP_HINT}")
    dsn = url.get_secret_value()
    try:
        admin = await asyncio.wait_for(asyncpg.connect(dsn), timeout=5)
    except (OSError, TimeoutError, asyncpg.PostgresError) as exc:
        pytest.skip(f"Postgres not reachable ({type(exc).__name__}); {SKIP_HINT}")
    schema = f"test_{uuid4().hex}"
    try:
        await admin.execute(f'create schema "{schema}"')
        try:
            yield with_search_path(dsn, schema)
        finally:
            # a connection left open by a broken test may hold locks: fail loudly, do not hang
            await admin.execute(f'drop schema "{schema}" cascade', timeout=10)
    finally:
        await admin.close()
