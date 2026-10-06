import pytest
from pydantic import ValidationError

from ig_connector.contract.errors import ResultErrorCode
from ig_connector.contract.events import (
    ConnectQrReadyPayload,
    ConnectStatusPayload,
    MessagePinnedPayload,
    ReadReceiptPayload,
    ResultPayload,
    StatusPayload,
)


def test_done_with_phone_instead_of_account_phone_is_rejected() -> None:
    with pytest.raises(ValidationError, match="phone"):
        ConnectStatusPayload.model_validate({"state": "done", "external_account_id": "1", "phone": "+4917"})


def test_done_without_external_account_id_is_rejected() -> None:
    with pytest.raises(ValidationError, match="external_account_id"):
        ConnectStatusPayload(state="done", account_phone="+491701234567")


def test_failed_connect_needs_error_code() -> None:
    with pytest.raises(ValidationError):
        ConnectStatusPayload(state="failed")


def test_status_error_needs_code() -> None:
    with pytest.raises(ValidationError):
        StatusPayload(status="error")


def test_status_vocabulary_is_closed() -> None:
    with pytest.raises(ValidationError):
        StatusPayload.model_validate({"status": "online"})


def test_result_failure_needs_code_from_closed_list() -> None:
    with pytest.raises(ValidationError):
        ResultPayload(ok=False)
    with pytest.raises(ValidationError):
        ResultPayload.model_validate({"ok": False, "error_code": "flood_wait"})


def test_result_failure_cannot_claim_a_sent_message() -> None:
    with pytest.raises(ValidationError):
        ResultPayload(ok=False, error_code=ResultErrorCode.NETWORK_ERROR, external_message_id="1")


def test_result_success_cannot_carry_error_code() -> None:
    with pytest.raises(ValidationError):
        ResultPayload(ok=True, error_code=ResultErrorCode.NETWORK_ERROR)


def test_result_ok_must_be_a_real_bool() -> None:
    with pytest.raises(ValidationError):
        ResultPayload.model_validate({"ok": "true"})


def test_pinned_must_be_a_real_bool() -> None:
    with pytest.raises(ValidationError):
        MessagePinnedPayload.model_validate(
            {"external_chat_id": "c", "external_message_id": "m", "pinned": None}
        )


def test_read_receipt_watermark_is_numeric() -> None:
    with pytest.raises(ValidationError):
        ReadReceiptPayload(external_chat_id="c", max_external_message_id="abc")


def test_qr_must_be_a_data_url() -> None:
    with pytest.raises(ValidationError):
        ConnectQrReadyPayload(qr_data_url="https://example.com/qr.png")
