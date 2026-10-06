import json
from pathlib import Path

import pytest

TRACED_TESTS = """
import logging

from pydantic import SecretStr

# built at runtime so the traceback's source lines do not carry the literal values
PASSWORD, TOKEN = "hun" + "ter2", "t0" + "k3n"


def test_ok(trace):
    trace.record("bus.commit", {"partition": 0, "offset": 3})


def test_broken(trace):
    trace.record("note", {"password": PASSWORD, "token": SecretStr(TOKEN)})
    logging.getLogger("ig_connector.demo").warning("login via http://user:pa55@proxy:1 failed")
    assert 1 == 2, "sessionid=1%3Aleak"
"""


@pytest.fixture
def runs(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> Path:
    runs = pytester.path / "runs"
    monkeypatch.setenv("IG_TEST_RUNS_DIR", str(runs))
    pytester.makeini("[pytest]\nasyncio_default_fixture_loop_scope = function\n")
    pytester.makeconftest("pytest_plugins = ['tests.support.tracing']")
    pytester.makepyfile(test_traced=TRACED_TESTS)
    return runs


def _only_run(runs: Path) -> Path:
    [run] = runs.iterdir()
    return run


def test_every_traced_test_leaves_jsonl_and_summary(pytester: pytest.Pytester, runs: Path) -> None:
    pytester.runpytest("-p", "no:cacheprovider").assert_outcomes(passed=1, failed=1)

    run = _only_run(runs)
    ok, broken = run / "test_traced.py__test_ok", run / "test_traced.py__test_broken"
    [entry] = [json.loads(line) for line in (ok / "trace.jsonl").read_text().splitlines()]
    assert entry["kind"] == "bus.commit"
    assert entry["data"] == {"partition": 0, "offset": 3}
    assert "passed" in (ok / "summary.md").read_text()

    summary = (broken / "summary.md").read_text()
    assert "failed" in summary
    assert "assert 1 == 2" in summary
    kinds = [json.loads(line)["kind"] for line in (broken / "trace.jsonl").read_text().splitlines()]
    assert kinds == ["note", "log"]


def test_secrets_never_reach_the_trace(pytester: pytest.Pytester, runs: Path) -> None:
    pytester.runpytest("-p", "no:cacheprovider")

    broken = _only_run(runs) / "test_traced.py__test_broken"
    written = (broken / "trace.jsonl").read_text() + (broken / "summary.md").read_text()
    for secret in ("hunter2", "t0k3n", "pa55", "1%3Aleak"):
        assert secret not in written


def test_failure_output_points_at_the_trace(pytester: pytest.Pytester, runs: Path) -> None:
    result = pytester.runpytest("-p", "no:cacheprovider")

    broken = _only_run(runs) / "test_traced.py__test_broken"
    result.stdout.fnmatch_lines([f"*trace: {broken}*"])
    assert "test_traced.py__test_ok" not in result.stdout.str()


def test_only_the_last_ten_runs_are_kept(pytester: pytest.Pytester, runs: Path) -> None:
    old = [runs / f"20000101-0000{n:02d}-000000" for n in range(12)]
    for run in old:
        run.mkdir(parents=True)
    unrelated = runs / "aaa-not-a-run"
    unrelated.mkdir()

    pytester.runpytest("-p", "no:cacheprovider")

    kept = sorted(path.name for path in runs.iterdir() if path != unrelated)
    assert len(kept) == 10
    assert kept[:9] == [run.name for run in old[-9:]]
    assert unrelated.is_dir()
