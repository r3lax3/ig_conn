# Every JSON example from the contract doc must pass our models.
# The doc shortens UUIDs with an ellipsis, those get replaced with a real one.

import json
import re
from pathlib import Path
from typing import Any

import pytest

from ig_connector.contract.codec import COMMAND_PAYLOADS, EVENT_PAYLOADS
from ig_connector.contract.envelope import CommandEnvelope, CommandType, EventEnvelope, EventType

CONTRACT_DOC = Path(__file__).parents[2] / "docs" / "kafka-contract-2.1.md"

_CODE_BLOCK = re.compile(r"```json\n(.*?)```", re.DOTALL)
_SHORT_UUID = re.compile(r'"[0-9a-f]{4,8}-…"')
_PLACEHOLDER_UUID = '"00000000-0000-4000-8000-000000000000"'


def _expand_uuids(text: str) -> str:
    return _SHORT_UUID.sub(_PLACEHOLDER_UUID, text)


def _examples() -> list[tuple[str, dict[str, Any]]]:
    examples: list[tuple[str, dict[str, Any]]] = []
    decoder = json.JSONDecoder()
    for block_no, block in enumerate(_CODE_BLOCK.findall(CONTRACT_DOC.read_text(encoding="utf-8"))):
        text = "\n".join(line for line in block.splitlines() if not line.lstrip().startswith("//"))
        text = _expand_uuids(text)
        pos = 0
        while (start := text.find("{", pos)) != -1:
            obj, pos = decoder.raw_decode(text, start)
            if "type" in obj:
                examples.append((f"block{block_no}:{obj['type']}", obj))
    return examples


EXAMPLES = _examples()


def _is_illustrative(example: dict[str, Any]) -> bool:
    return example.get("payload") == {"...": "..."}


def test_doc_has_examples() -> None:
    assert len(EXAMPLES) > 30


@pytest.mark.parametrize(("name", "example"), EXAMPLES, ids=[name for name, _ in EXAMPLES])
def test_example_matches_models(name: str, example: dict[str, Any]) -> None:
    if _is_illustrative(example):
        pytest.skip("generic envelope sample, ids are placeholders")
    message_type = example["type"]
    payload = example.get("payload", {})
    is_full_message = "contract_version" in example
    if message_type in CommandType:
        if is_full_message:
            CommandEnvelope.model_validate(example)
        COMMAND_PAYLOADS[CommandType(message_type)].model_validate(payload)
    else:
        if is_full_message:
            EventEnvelope.model_validate(example)
        EVENT_PAYLOADS[EventType(message_type)].model_validate(payload)


def test_every_message_type_has_an_example() -> None:
    seen = {example["type"] for _, example in EXAMPLES}
    assert set(CommandType) | set(EventType) <= seen
