from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ig_connector.contract.envelope import CommandEnvelope, EventEnvelope


def _envelope(**overrides: Any) -> dict[str, Any]:
    return {
        "contract_version": "2.1",
        "operation_id": str(uuid4()),
        "source_id": str(uuid4()),
        "channel_type": "individual_instagram_account",
        "type": "command.send",
        "payload": {},
        "occurred_at": "2026-10-03T10:00:00+00:00",
        **overrides,
    }


def test_unknown_command_fields_are_ignored() -> None:
    envelope = CommandEnvelope.model_validate(_envelope(retry_count=2, trace_id="x"))
    assert "retry_count" not in envelope.model_dump()


def test_future_contract_version_is_accepted() -> None:
    assert CommandEnvelope.model_validate(_envelope(contract_version="2.7")).contract_version == "2.7"


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValidationError):
        CommandEnvelope.model_validate(_envelope(occurred_at="2026-10-03T10:00:00"))


def test_connect_start_and_confirm_with_one_operation_id_are_not_duplicates() -> None:
    operation_id = str(uuid4())
    start = CommandEnvelope.model_validate(_envelope(type="command.connect.start", operation_id=operation_id))
    confirm = CommandEnvelope.model_validate(
        _envelope(type="command.connect.confirm", operation_id=operation_id)
    )
    assert start.dedup_key != confirm.dedup_key


def test_redelivery_is_a_duplicate_but_another_operation_is_not() -> None:
    raw = _envelope()
    first = CommandEnvelope.model_validate(raw)
    assert CommandEnvelope.model_validate(raw).dedup_key == first.dedup_key
    other = CommandEnvelope.model_validate({**raw, "operation_id": str(uuid4())})
    assert other.dedup_key != first.dedup_key


def test_unknown_command_type_passes_the_envelope() -> None:
    # rejected later by the codec with a clear message
    assert CommandEnvelope.model_validate(_envelope(type="command.typing")).type == "command.typing"


def test_event_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate({**_envelope(type="ack"), "retry_count": 1})


def test_event_rejects_unknown_type() -> None:
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(_envelope(type="event.typing"))


@pytest.mark.parametrize("field", ["contract_version", "occurred_at"])
def test_event_has_no_silent_defaults(field: str) -> None:
    raw = _envelope(type="ack")
    del raw[field]
    with pytest.raises(ValidationError, match=field):
        EventEnvelope.model_validate(raw)


def test_broken_envelope_error_does_not_quote_the_payload() -> None:
    # validation errors go to logs; the payload may carry a 2FA code or a message text
    broken = _envelope(type="command.connect.confirm", payload={"flow_id": "f", "code": "987654"})
    del broken["occurred_at"]
    with pytest.raises(ValidationError) as error:
        CommandEnvelope.model_validate(broken)
    assert "987654" not in str(error.value)
