from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ig_connector.contract.commands import (
    ConnectConfirmPayload,
    ConnectStartPayload,
    ResolveRecipientPayload,
    SendPayload,
)
from ig_connector.contract.media import Attachment

SEND = {"message_id": "m1", "external_chat_id": "c1", "text": "hi", "format": [], "attachments": []}
PHONE = "+491701234567"
TOTP_SECRET = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP"
ATTACHMENT = {
    "kind": "image",
    "bucket": "b",
    "key": "crm/1/outbound/2/0-photo.jpg",
    "mime": "image/jpeg",
    "size_bytes": 10,
}


def _connect(**fields: Any) -> dict[str, Any]:
    return {"flow_id": str(uuid4()), "role": "operator_channel", "method": "pairing_code", **fields}


def test_unknown_payload_fields_are_ignored() -> None:
    assert SendPayload.model_validate({**SEND, "priority": "high"}).text == "hi"


def test_send_without_chat_is_rejected() -> None:
    with pytest.raises(ValidationError):
        SendPayload.model_validate({k: v for k, v in SEND.items() if k != "external_chat_id"})


def test_send_attachments_are_parsed() -> None:
    payload = SendPayload.model_validate({**SEND, "attachments": [ATTACHMENT]})
    assert payload.attachments == [Attachment.model_validate(ATTACHMENT)]


def test_send_with_platform_attachment_kind_is_rejected() -> None:
    with pytest.raises(ValidationError):
        SendPayload.model_validate({**SEND, "attachments": [{**ATTACHMENT, "kind": "photo"}]})


def test_recipient_kind_is_closed() -> None:
    with pytest.raises(ValidationError):
        ResolveRecipientPayload.model_validate({"recipient_kind": "email", "value": "a@b.c"})


def test_connect_method_is_closed() -> None:
    with pytest.raises(ValidationError):
        ConnectStartPayload.model_validate(_connect(method="sms", phone=PHONE))


def test_unknown_role_and_network_type_do_not_break_login() -> None:
    payload = ConnectStartPayload.model_validate(
        _connect(phone=PHONE, role="new_role", proxy_network_type="satellite")
    )
    assert payload.role == "new_role"


def test_first_pairing_without_phone_is_rejected() -> None:
    with pytest.raises(ValidationError, match="requires phone or login"):
        ConnectStartPayload.model_validate(_connect(phone=None))


def test_first_pairing_by_login_needs_no_phone() -> None:
    payload = ConnectStartPayload.model_validate(_connect(phone=None, login="some_user"))
    assert payload.login == "some_user"


def test_reconnect_by_code_may_come_without_phone() -> None:
    payload = ConnectStartPayload.model_validate(_connect(phone=None, reconnect_source_id=str(uuid4())))
    assert payload.phone is None


@pytest.mark.parametrize(
    ("field", "value", "extra"),
    [
        ("code", "HUNTER2SECRET", {}),
        ("password", "HUNTER2SECRET", {}),
        ("totp_secret", TOTP_SECRET, {"password": "pw"}),
    ],
)
def test_confirm_secrets_never_show_in_repr_or_dump(field: str, value: str, extra: dict[str, str]) -> None:
    payload = ConnectConfirmPayload.model_validate({"flow_id": str(uuid4()), field: value, **extra})
    assert value not in repr(payload)
    assert value not in payload.model_dump_json()
    secret = getattr(payload, field)
    assert secret is not None
    assert secret.get_secret_value() == value


@pytest.mark.parametrize(
    "payload",
    [
        {"code": None, "password": None},
        {"code": "1", "password": "2"},
        {"code": "", "password": None},
        {"totp_secret": TOTP_SECRET},
        {"code": "123456", "totp_secret": TOTP_SECRET},
        {"password": "pw", "totp_secret": ""},
        {"password": "pw", "totp_secret": "not base32 at all 189"},
    ],
)
def test_confirm_rejects_bad_secret_combinations(payload: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        ConnectConfirmPayload.model_validate({"flow_id": str(uuid4()), **payload})


def test_totp_secret_is_normalized_as_pasted() -> None:
    payload = ConnectConfirmPayload.model_validate(
        {"flow_id": str(uuid4()), "password": "pw", "totp_secret": "jbsw y3dp ehpk 3pxp jbsw y3dp ehpk 3pxp"}
    )
    assert payload.totp_secret is not None
    assert payload.totp_secret.get_secret_value() == "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP"


def test_validation_errors_do_not_echo_payload() -> None:
    with pytest.raises(ValidationError) as error:
        SendPayload.model_validate({"message_id": "m1", "text": "passport 4510 123456"})
    assert "4510 123456" not in str(error.value)
    with pytest.raises(ValidationError) as error:
        ConnectConfirmPayload.model_validate(
            {"flow_id": str(uuid4()), "password": "pw", "totp_secret": "leaked secret 189"}
        )
    assert "leaked secret" not in str(error.value)
