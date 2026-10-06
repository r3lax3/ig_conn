import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import SecretStr

from ig_connector.bus import Delivery, kafka_auth
from ig_connector.bus.kafka import KafkaBus
from ig_connector.contract.codec import ContractViolationError, parse_command, reply_to
from ig_connector.contract.envelope import CommandType
from ig_connector.contract.events import AckPayload
from tests.support.crm import CHANNEL_TYPE, command


def test_kafka_auth_matches_contract_client_settings() -> None:
    config = kafka_auth("broker:9092", "individual-instagram-account-connector", SecretStr("pw"))
    assert config == {
        "bootstrap_servers": "broker:9092",
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_plain_username": "individual-instagram-account-connector",
        "sasl_plain_password": "pw",
    }


def _kafka_bus() -> KafkaBus:
    # never started: these refusals must come before any network
    return KafkaBus(
        channel_type=CHANNEL_TYPE,
        bootstrap_servers="localhost:1",
        username="u",
        password=SecretStr("pw"),
        group_id=f"connector.{CHANNEL_TYPE}",
    )


async def test_kafka_bus_refuses_an_event_of_a_foreign_channel() -> None:
    resolve = {"recipient_kind": "username", "value": "anna"}
    command_ = parse_command(json.dumps(command(CommandType.RESOLVE_RECIPIENT, resolve, source_id=uuid4())))
    event = reply_to(command_, AckPayload(), occurred_at=datetime.now(UTC))
    with pytest.raises(ContractViolationError, match="channel_type"):
        await _kafka_bus().publish(event.model_copy(update={"channel_type": "max_bot"}))


async def test_kafka_bus_refuses_a_commit_without_a_consumer_session() -> None:
    delivery = Delivery(f"crm.connector.commands.{CHANNEL_TYPE}", 0, 5, None, b"{}")
    with pytest.raises(RuntimeError, match="no consumer session"):
        await _kafka_bus().commit(delivery)
