"""Run trace for behaviour tests: trace.jsonl + summary.md per test.

Layout: <runs>/<run started at>/<test>/; <runs> is .test-runs/ in the project root or
$IG_TEST_RUNS_DIR. Only the last RUNS_KEPT runs are kept. Everything recorded goes
through the production masker first.
"""

import json
import logging
import os
import re
import shutil
import time
from collections.abc import Generator, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from ig_connector.clock import Clock
from ig_connector.masking import mask, mask_text

RUNS_KEPT = 10
_RUN_NAME = re.compile(r"\d{8}-\d{6}-\d{6}")

_RUN_DIR = pytest.StashKey[Path]()
_TRACE_DIR = pytest.StashKey[Path]()
_REPORTS = pytest.StashKey[dict[str, pytest.TestReport]]()
_FAILED_TRACES = pytest.StashKey[list[Path]]()


@dataclass(frozen=True, slots=True)
class Entry:
    seq: int
    elapsed_ms: float
    clock: str | None
    kind: str
    data: Any


class Trace:
    """What happened during one test, in order. Fakes and the bus feed it via record()."""

    def __init__(self) -> None:
        self.entries: list[Entry] = []
        # set by the test (or a fixture) to stamp entries with the connector's time
        self.clock: Clock | None = None
        self._started = time.monotonic()

    def record(self, kind: str, data: Mapping[str, Any] | None = None) -> None:
        self.entries.append(
            Entry(
                seq=len(self.entries) + 1,
                elapsed_ms=round((time.monotonic() - self._started) * 1000, 1),
                clock=self.clock.now().isoformat() if self.clock else None,
                kind=kind,
                data=mask(dict(data or {})),
            )
        )

    def write(self, directory: Path, *, test: str, outcome: str, failure: str | None) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps(_as_json(entry), ensure_ascii=False, default=str) for entry in self.entries]
        (directory / "trace.jsonl").write_text("".join(f"{line}\n" for line in lines))
        (directory / "summary.md").write_text(self._summary(test, outcome, failure))

    def _summary(self, test: str, outcome: str, failure: str | None) -> str:
        out = [
            f"# {test}",
            "",
            f"- outcome: **{outcome}**",
            f"- steps: {len(self.entries)}",
            "",
        ]
        if failure:
            out += ["## Failure", "", "```text", mask_text(failure), "```", ""]
        out += ["## Timeline", "", "| # | +ms | clock | step |", "|---|---|---|---|"]
        for entry in self.entries:
            clock = entry.clock[11:19] if entry.clock else ""
            step = describe(entry.kind, entry.data).replace("|", "\\|")
            out.append(f"| {entry.seq} | {entry.elapsed_ms:g} | {clock} | {step} |")
        return "\n".join(out) + "\n"


def describe(kind: str, data: Any) -> str:
    """One readable line per entry; unknown kinds fall back to compact JSON."""
    if kind == "bus.command" and isinstance(data.get("command"), dict):
        command = data["command"]
        return (
            f"CRM → {command.get('type')} op={_short(command.get('operation_id'))} "
            f"src={_short(command.get('source_id'))} (p{data['partition']}@{data['offset']})"
        )
    if kind == "bus.deliver":
        return f"connector ← p{data['partition']}@{data['offset']}"
    if kind == "bus.publish":
        event = data["event"]
        payload = json.dumps(event.get("payload"), ensure_ascii=False, default=str)
        return f"connector → {event.get('type')} op={_short(event.get('operation_id'))} {payload}"
    if kind == "bus.commit":
        return f"commit p{data['partition']}@{data['offset']}"
    if kind == "log":
        return f"log {data.get('level')} {data.get('logger')}: {data.get('message')}"
    text = json.dumps(data, ensure_ascii=False, default=str)
    return f"{kind} {text[:300]}"


def _short(value: Any) -> str:
    return str(value)[:8]


def _as_json(entry: Entry) -> dict[str, Any]:
    return {
        "seq": entry.seq,
        "elapsed_ms": entry.elapsed_ms,
        "clock": entry.clock,
        "kind": entry.kind,
        "data": entry.data,
    }


class _LogToTrace(logging.Handler):
    def __init__(self, trace: Trace) -> None:
        super().__init__(logging.DEBUG)
        self._trace = trace

    def emit(self, record: logging.LogRecord) -> None:
        # our own debug lines are useful, the libraries' are noise
        if record.levelno < logging.INFO and not record.name.startswith("ig_connector"):
            return
        self._trace.record(
            "log", {"level": record.levelname, "logger": record.name, "message": record.getMessage()}
        )


def _run_dir(config: pytest.Config) -> Path:
    if _RUN_DIR not in config.stash:
        root = Path(os.environ.get("IG_TEST_RUNS_DIR") or config.rootpath / ".test-runs")
        run = root / datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        run.mkdir(parents=True)
        # only our own run directories: the root may be shared via IG_TEST_RUNS_DIR
        ours = (path for path in root.iterdir() if path.is_dir() and _RUN_NAME.fullmatch(path.name))
        for stale in sorted(ours)[:-RUNS_KEPT]:
            shutil.rmtree(stale, ignore_errors=True)
        config.stash[_RUN_DIR] = run
    return config.stash[_RUN_DIR]


def _test_dir_name(item: pytest.Item) -> str:
    name = item.nodeid.rsplit("/", 1)[-1].replace("::", "__")
    return re.sub(r"[^\w.\[\]-]", "_", name)[:150]


@pytest.fixture
def trace(request: pytest.FixtureRequest) -> Iterator[Trace]:
    item = request.node
    directory = _run_dir(request.config) / _test_dir_name(item)
    item.stash[_TRACE_DIR] = directory
    recorder = Trace()
    handler = _LogToTrace(recorder)
    root = logging.getLogger()
    level = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        yield recorder
    finally:
        root.removeHandler(handler)
        root.setLevel(level)
        reports = item.stash.get(_REPORTS, {})
        # no call report when setup failed or skipped (e.g. no Postgres)
        report = reports.get("call") or reports["setup"]
        failure = report.longreprtext if report.failed else None
        recorder.write(directory, test=item.nodeid, outcome=report.outcome, failure=failure)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    item.stash.setdefault(_REPORTS, {})[report.when] = report
    directory = item.stash.get(_TRACE_DIR, None)
    if directory is not None and report.failed:
        report.sections.append(("trace", f"trace: {directory}"))
        failed = item.config.stash.setdefault(_FAILED_TRACES, [])
        if directory not in failed:
            failed.append(directory)
    return report


def pytest_terminal_summary(terminalreporter: Any, config: pytest.Config) -> None:
    failed = config.stash.get(_FAILED_TRACES, [])
    if failed:
        terminalreporter.section("traces of failed tests")
        for directory in failed:
            terminalreporter.write_line(f"trace: {directory}")
