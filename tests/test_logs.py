import io
import json
import logging
from collections.abc import Iterator

import pytest
from pydantic import SecretStr

from ig_connector.logs import configure_logging


@pytest.fixture
def output() -> Iterator[io.StringIO]:
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    stream = io.StringIO()
    configure_logging("INFO", stream=stream)
    yield stream
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)


def _lines(stream: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_one_json_line_per_event_with_the_operation_fields(output: io.StringIO) -> None:
    log = logging.getLogger("ig_connector.runtime.connector")
    log.info(
        "command answered",
        extra={"source_id": "s-1", "operation_id": "o-1", "command": "command.send", "duration_ms": 12},
    )
    log.debug("not shown at INFO")

    [line] = _lines(output)
    assert line["level"] == "INFO"
    assert line["logger"] == "ig_connector.runtime.connector"
    assert line["message"] == "command answered"
    assert line["source_id"] == "s-1"
    assert line["operation_id"] == "o-1"
    assert line["command"] == "command.send"
    assert line["duration_ms"] == 12
    assert str(line["time"]).endswith("Z")


def test_secrets_do_not_reach_the_output(output: io.StringIO) -> None:
    log = logging.getLogger("aiokafka.conn")
    log.warning("connect to %s failed", "postgresql://user:db-pass@db/x")
    log.info("login", extra={"password": "hunter2", "session": {"sessionid": "abc123"}})
    log.info("proxy %s", SecretStr("proxy-pass"))
    try:
        raise RuntimeError("access_token=tok-secret")
    except RuntimeError:
        log.exception("failed with password: pw-in-text")

    text = output.getvalue()
    for secret in ("db-pass", "hunter2", "abc123", "proxy-pass", "tok-secret", "pw-in-text"):
        assert secret not in text
    lines = _lines(output)
    assert len(lines) == 4
    assert "RuntimeError" in str(lines[-1]["exception"])


def test_message_texts_are_logged_as_their_length_only(output: io.StringIO) -> None:
    logging.getLogger("ig_connector.handlers").info(
        "sent", extra={"text": "привет, это личное", "caption": "фото"}
    )

    [line] = _lines(output)
    assert "text" not in line
    assert "caption" not in line
    assert line["text_length"] == 18
    assert line["caption_length"] == 4
    assert "личное" not in output.getvalue()


def test_level_comes_from_the_setting(output: io.StringIO) -> None:
    configure_logging("WARNING", stream=output)
    logging.getLogger("ig_connector").info("quiet")
    logging.getLogger("ig_connector").warning("loud")

    assert [line["message"] for line in _lines(output)] == ["loud"]
