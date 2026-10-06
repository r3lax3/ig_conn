"""The deployment kit stays complete: the customer's team fills .env from .env.example only."""

import re
from pathlib import Path

from ig_connector.devtools.fake_crm import FakeCrmSettings

ROOT = Path(__file__).resolve().parent.parent


def _example_names() -> set[str]:
    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    # set or commented out with a value: `NAME=` / `# NAME=...`
    return set(re.findall(r"^#? ?([A-Z][A-Z0-9_]*)=", text, flags=re.MULTILINE))


def test_env_example_names_every_setting() -> None:
    settings = {name.upper() for name in FakeCrmSettings.model_fields}
    assert settings - _example_names() == set()


def test_env_example_names_every_variable_compose_reads() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    used = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)", compose))
    assert used, "docker-compose.yml reads nothing from .env"
    assert used - _example_names() == set()


def test_image_gets_no_local_files_but_the_build_inputs() -> None:
    # an allow list: .env, sessions and anything else lying around never reach the build context
    lines = [
        line.strip()
        for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert lines[0] == "*"
    assert {line.removeprefix("!") for line in lines[1:]} == {"src", "pyproject.toml", "uv.lock", "README.md"}
