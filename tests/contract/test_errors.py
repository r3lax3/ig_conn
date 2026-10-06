import re

from ig_connector.contract.errors import RETRIED_BY_CRM, ResultErrorCode
from tests.contract.test_doc_examples import CONTRACT_DOC

# a row of the result codes table in contract 9: | `code` | when | will CRM retry |
_RESULT_ROW = re.compile(r"^\| `(\w+)` \| .* \| (да|нет) \|$", re.MULTILINE)


def test_retried_codes_match_the_contract_table() -> None:
    rows = dict(_RESULT_ROW.findall(CONTRACT_DOC.read_text(encoding="utf-8")))
    assert set(rows) == set(ResultErrorCode)
    assert {ResultErrorCode(code) for code, retried in rows.items() if retried == "да"} == RETRIED_BY_CRM
