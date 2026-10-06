# Needs Kafka, S3 and Postgres from .env: uv run pytest -m integration

from uuid import uuid4

import asyncpg
import pytest
from aiobotocore.session import get_session
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import SecretStr

from ig_connector.bus import kafka_auth
from ig_connector.settings import Settings

pytestmark = pytest.mark.integration


@pytest.fixture
def settings() -> Settings:
    return Settings()  # filled from .env


async def test_connector_sees_exactly_its_two_topics(settings: Settings) -> None:
    consumer = AIOKafkaConsumer(
        **kafka_auth(settings.kafka_bootstrap_servers, settings.kafka_username, settings.kafka_password)
    )
    await consumer.start()
    try:
        topics = {topic for topic in await consumer.topics() if not topic.startswith("__")}
    finally:
        await consumer.stop()
    assert topics == {settings.commands_topic, settings.events_topic}


async def test_wrong_kafka_password_is_rejected(settings: Settings) -> None:
    producer = AIOKafkaProducer(
        **kafka_auth(settings.kafka_bootstrap_servers, settings.kafka_username, SecretStr("wrong")),
        request_timeout_ms=5_000,
    )
    with pytest.raises(KafkaError):
        await producer.start()
    await producer.stop()


def _s3_config() -> Config:
    return Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1})


async def test_s3_put_get_in_channel_bucket(settings: Settings) -> None:
    key = f"inbound/test/{uuid4().hex}.txt"
    async with get_session().create_client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key.get_secret_value(),
        config=_s3_config(),
    ) as s3:
        await s3.put_object(Bucket=settings.s3_bucket, Key=key, Body="привет".encode())
        response = await s3.get_object(Bucket=settings.s3_bucket, Key=key)
        async with response["Body"] as stream:
            assert (await stream.read()).decode() == "привет"
        await s3.delete_object(Bucket=settings.s3_bucket, Key=key)


async def test_s3_wrong_secret_is_rejected(settings: Settings) -> None:
    async with get_session().create_client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key="0" * 64,
        config=_s3_config(),
    ) as s3:
        with pytest.raises(ClientError):
            await s3.head_bucket(Bucket=settings.s3_bucket)


async def test_postgres_is_reachable(settings: Settings) -> None:
    connection = await asyncpg.connect(settings.database_url.get_secret_value())
    try:
        assert await connection.fetchval("select 1") == 1
    finally:
        await connection.close()
