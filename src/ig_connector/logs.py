"""Process logging: one JSON line per event on stdout, masked.

Every record that reaches the handler, ours or a library's, goes through mask() first:
it is the second line of defence after SecretStr. Message texts never appear: known text
fields in `extra` are replaced by their length, and exceptions are logged as their types
and frames only, because library errors quote the request they failed on.
"""

import json
import logging
import sys
import traceback
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, TextIO

from ig_connector.contract.envelope import CommandEnvelope
from ig_connector.masking import mask, mask_text

# extra fields that carry correspondence: logged as <name>_length only
_TEXT_FIELDS = frozenset({"text", "caption", "body", "message_text"})
_STANDARD = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime", "taskName"}


def command_ids(envelope: CommandEnvelope) -> dict[str, str]:
    """The `extra` that ties a log line to its command."""
    return {
        "source_id": str(envelope.source_id),
        "operation_id": str(envelope.operation_id),
        "command": envelope.type,
    }


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        line: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds")[:-6] + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": mask_text(record.getMessage()),
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD or key.startswith("_"):
                continue
            if key in _TEXT_FIELDS:
                line[f"{key}_length"] = len(value) if isinstance(value, str) else None
            else:
                line[key] = mask({key: value})[key]
        if record.exc_info:
            line["exception"] = mask_text(_exception(record.exc_info))
        if record.stack_info:
            line["stack"] = mask_text(self.formatStack(record.stack_info))
        return json.dumps(line, ensure_ascii=False, default=str)


def _exception(
    exc_info: tuple[type[BaseException], BaseException, TracebackType | None] | tuple[None, None, None],
) -> str:
    if exc_info[1] is None:
        return ""
    return "".join(_render(traceback.TracebackException.from_exception(exc_info[1]), ""))


def _render(exc: traceback.TracebackException, indent: str) -> list[str]:
    lines: list[str] = []
    if exc.__cause__ is not None:
        lines += [*_render(exc.__cause__, indent), f"{indent}The above caused:\n"]
    elif exc.__context__ is not None and not exc.__suppress_context__:
        lines += [*_render(exc.__context__, indent), f"{indent}During handling of the above:\n"]
    lines += [indent + frame for frame in exc.stack.format()]
    lines.append(f"{indent}{exc.exc_type.__module__}.{exc.exc_type.__qualname__}\n" if exc.exc_type else "")
    for inner in exc.exceptions or ():
        lines += _render(inner, indent + "  ")
    return lines


def configure_logging(level: str = "INFO", *, stream: TextIO | None = None) -> None:
    """Route all logging to one JSON handler; calling it again replaces our handler."""
    root = logging.getLogger()
    for handler in [h for h in root.handlers if getattr(h, "_ig_connector", False)]:
        root.removeHandler(handler)
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler._ig_connector = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(level.upper())
