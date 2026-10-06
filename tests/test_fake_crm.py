import json
from pathlib import Path
from uuid import uuid4

import pytest

from ig_connector.contract.codec import parse_command
from ig_connector.contract.commands import ConnectConfirmPayload, ConnectStartPayload, SendPayload
from ig_connector.contract.envelope import CommandType
from ig_connector.devtools.fake_crm import FakeCrmSettings, _command_from_args, _parser
from tests.test_settings import ENV


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeCrmSettings:
    monkeypatch.chdir(tmp_path)
    for name, value in {**ENV, "FAKE_CRM_KAFKA_USERNAME": "crm-dev", "FAKE_CRM_KAFKA_PASSWORD": "x"}.items():
        monkeypatch.setenv(name, value)
    return FakeCrmSettings()


def _command(settings: FakeCrmSettings, *argv: str) -> bytes:
    return json.dumps(_command_from_args(settings, _parser().parse_args(argv))).encode()


def test_send_builds_a_valid_command(settings: FakeCrmSettings) -> None:
    source_id = str(uuid4())
    command = parse_command(
        _command(settings, "send", "--source-id", source_id, "--chat", "c1", "--text", "hi")
    )
    assert command.type is CommandType.SEND
    assert isinstance(command.payload, SendPayload)
    assert str(command.envelope.source_id) == source_id
    assert command.envelope.channel_type == settings.channel_type


def test_first_connect_uses_flow_id_as_source_and_operation(settings: FakeCrmSettings) -> None:
    command = parse_command(_command(settings, "connect", "--phone", "+491701234567"))
    assert isinstance(command.payload, ConnectStartPayload)
    assert command.payload.method == "pairing_code"
    assert command.payload.flow_id == command.envelope.source_id == command.envelope.operation_id


def test_confirm_reuses_connect_operation_id(settings: FakeCrmSettings) -> None:
    operation_id = str(uuid4())
    command = parse_command(_command(settings, "confirm", "--operation-id", operation_id, "--code", "123456"))
    assert isinstance(command.payload, ConnectConfirmPayload)
    assert str(command.envelope.operation_id) == operation_id


def test_resolve_rejects_unknown_kind(settings: FakeCrmSettings) -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(["resolve", "--source-id", str(uuid4()), "--kind", "email", "--value", "x"])
