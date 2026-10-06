"""The operation log as the service writes it: one JSON line per event, no message texts."""

import json
import logging
from uuid import uuid4

import pytest

from ig_connector.contract.envelope import CommandType, EventType
from ig_connector.contract.events import ResultPayload
from ig_connector.logs import JsonFormatter
from ig_connector.runtime import CommandContext, Handler, default_handlers
from tests.support.connector import running_connector
from tests.support.crm import command
from tests.support.fake_clock import FakeClock
from tests.support.memory_bus import MemoryBus

PRIVATE = "встречаемся у Анны в 19:00"


def _rendered(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    formatter = JsonFormatter()
    return [json.loads(formatter.format(record)) for record in caplog.records]


async def test_every_answered_command_is_logged_with_its_ids_code_and_duration(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    source_id = uuid4()
    edit = command(
        CommandType.EDIT,
        {"message_id": "m1", "external_chat_id": "1789", "external_message_id": "x1", "text": PRIVATE},
        source_id=source_id,
    )
    delivery = bus.submit(edit)

    with caplog.at_level(logging.INFO, logger="ig_connector"):
        async with running_connector(bus, clock, postgres_dsn):
            await bus.wait_until(lambda: bus.is_committed(delivery))

    [answered] = [line for line in _rendered(caplog) if line["message"] == "command answered"]
    assert answered["source_id"] == str(source_id)
    assert answered["operation_id"] == edit["operation_id"]
    assert answered["command"] == "command.edit"
    assert answered["error_code"] == "edit_unsupported"
    assert isinstance(answered["duration_ms"], int)
    assert PRIVATE not in json.dumps(_rendered(caplog), ensure_ascii=False)


async def test_a_failing_handler_does_not_put_the_message_text_into_the_log(
    bus: MemoryBus, clock: FakeClock, postgres_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    async def send(ctx: CommandContext) -> ResultPayload:
        # libraries quote the request they failed on
        raise RuntimeError(f'send failed: {{"text": "{PRIVATE}", "password": "pw-1"}}')

    handlers = {**default_handlers(), CommandType.SEND: Handler(send, needs_active_source=False)}
    delivery = bus.submit(
        command(
            CommandType.SEND,
            {"message_id": "m1", "external_chat_id": "1789", "text": PRIVATE},
            source_id=uuid4(),
        )
    )

    with caplog.at_level(logging.INFO, logger="ig_connector"):
        async with running_connector(bus, clock, postgres_dsn, handlers=handlers):
            await bus.wait_until(lambda: bus.is_committed(delivery))

    assert [event.envelope.type for event in bus.events()] == [EventType.ACK, EventType.RESULT]
    lines = _rendered(caplog)
    failed = next(line for line in lines if line["message"] == "handler failed")
    assert "RuntimeError" in str(failed["exception"])
    text = json.dumps(lines, ensure_ascii=False)
    assert PRIVATE not in text
    assert "pw-1" not in text
