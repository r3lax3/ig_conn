import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from ig_connector.contract.codec import (
    ContractViolationError,
    build_event,
    command_from,
    parse_command,
    parse_envelope,
    parse_event,
    reply_to,
    serialize,
)
from ig_connector.contract.commands import SendPayload
from ig_connector.contract.envelope import CONTRACT_VERSION, CommandType, EventEnvelope, EventType
from ig_connector.contract.events import AckPayload, EventPayload, InboundMessagePayload

SOURCE_ID = UUID("3f9a2b7e-9c41-4a7e-8f2d-0b1c2d3e4f50")
CHANNEL = "individual_instagram_account"
NOW = datetime(2026, 10, 3, 10, 0, 1, tzinfo=UTC)
SEND = {"message_id": "m1", "external_chat_id": "c1", "text": "hi", "format": [], "attachments": []}


def _command(type_: str, payload: dict[str, Any], **envelope: Any) -> bytes:
    message = {
        "contract_version": "2.1",
        "operation_id": str(uuid4()),
        "source_id": str(SOURCE_ID),
        "channel_type": CHANNEL,
        "type": type_,
        "payload": payload,
        "occurred_at": "2026-10-03T10:00:00+00:00",
        **envelope,
    }
    return json.dumps(message).encode()


def _event(payload: EventPayload) -> EventEnvelope:
    return build_event(
        payload=payload,
        operation_id=uuid4(),
        source_id=SOURCE_ID,
        channel_type=CHANNEL,
        contract_version=CONTRACT_VERSION,
        occurred_at=NOW,
    )


def test_command_is_parsed_into_typed_payload() -> None:
    command = parse_command(_command("command.send", SEND, retry_count=1))
    assert command.type is CommandType.SEND
    assert isinstance(command.payload, SendPayload)


def test_unknown_command_type_is_rejected() -> None:
    with pytest.raises(ContractViolationError, match="unknown command type"):
        parse_command(_command("command.typing", {}))


def test_bad_payload_is_a_contract_violation() -> None:
    with pytest.raises(ContractViolationError, match=r"command\.send"):
        parse_command(_command("command.send", {"text": "no chat"}))


def test_bad_payload_still_leaves_an_envelope_to_answer() -> None:
    envelope = parse_envelope(_command("command.send", {"text": "no chat"}))
    assert envelope.source_id == SOURCE_ID
    with pytest.raises(ContractViolationError, match=r"command\.send"):
        command_from(envelope)


@pytest.mark.parametrize("raw", [b"", b"not json", b"[]", b'{"type": "command.send"}'])
def test_garbage_is_a_contract_violation(raw: bytes) -> None:
    with pytest.raises(ContractViolationError):
        parse_command(raw)


def test_reply_echoes_operation_source_channel_and_version() -> None:
    command = parse_command(_command("command.send", SEND, contract_version="2.3"))
    event = reply_to(command, AckPayload(), occurred_at=NOW)
    assert event.type is EventType.ACK
    assert event.operation_id == command.envelope.operation_id
    assert event.source_id == command.envelope.source_id
    assert event.channel_type == command.envelope.channel_type
    assert event.contract_version == "2.3"
    assert event.occurred_at == NOW


def test_serialized_event_round_trips_and_drops_nulls() -> None:
    payload = InboundMessagePayload(
        external_chat_id="c", external_message_id="m", external_user_id="u", text="привет 👋", kind="text"
    )
    event = _event(payload)
    raw = serialize(event)
    assert b"reply_to_external_id" not in raw
    envelope, parsed = parse_event(raw)
    assert envelope.type is EventType.INBOUND_MESSAGE
    assert parsed == payload


def test_invalid_event_from_the_wire_is_a_contract_violation() -> None:
    event = _event(AckPayload())
    broken = json.loads(serialize(event))
    broken["type"] = "result"
    with pytest.raises(ContractViolationError):
        parse_event(json.dumps(broken))
