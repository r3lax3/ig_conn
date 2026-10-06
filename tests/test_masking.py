import json
from dataclasses import dataclass

from pydantic import BaseModel, SecretStr

from ig_connector.masking import MASK, mask, mask_text


def test_secret_str_anywhere_in_a_structure_is_masked() -> None:
    masked = mask({"outer": [{"inner": SecretStr("hunter2")}], "n": 1})
    assert masked == {"outer": [{"inner": MASK}], "n": 1}


def test_values_under_secret_keys_are_masked_at_any_depth() -> None:
    data = {
        "payload": {
            "flow_id": "f-1",
            "password": "hunter2",
            "totp_secret": "JBSWY3DPEHPK3PXP",
            "code": "123456",
            "proxy": {"host": "10.0.0.1", "port": 8080, "username": "u", "password": "p"},
        },
        "settings": {"authorization_data": {"ds_user_id": "1", "sessionid": "1%3Aabc"}},
        "cookies": {"sessionid": "1%3Aabc", "csrftoken": "x"},
        "Kafka_Password": "k",
        "s3_secret_access_key": "s",
    }
    assert mask(data) == {
        "payload": {
            "flow_id": "f-1",
            "password": MASK,
            "totp_secret": MASK,
            "code": MASK,
            "proxy": {"host": "10.0.0.1", "port": 8080, "username": "u", "password": MASK},
        },
        "settings": {"authorization_data": MASK},
        "cookies": MASK,
        "Kafka_Password": MASK,
        "s3_secret_access_key": MASK,
    }


def test_harmless_fields_survive() -> None:
    data = {
        "login": "alice",
        "text_length": 12,
        "status": "active",
        "error_code": "deauthorized",
        "session_restored": True,
    }
    assert mask(data) == data


def test_secrets_inside_free_text_are_masked() -> None:
    text = (
        "login failed: Cookie: sessionid=1%3AAbC; csrftoken=zzz "
        "proxy http://user:pa55@proxy.example:8080 "
        'body {"password": "hunter 2", "totp_secret":"JBSWY3DP"} '
        "query ?password=hunter2&next=/ Authorization: Bearer eyJhbGciOi.x.y "
        "BadPassword: password='s3cr3t'"
    )
    masked = mask_text(text)
    for secret in ("1%3AAbC", "pa55", "hunter 2", "hunter2", "JBSWY3DP", "eyJhbGciOi", "s3cr3t"):
        assert secret not in masked
    assert "csrftoken=zzz" in masked
    assert "proxy.example:8080" in masked
    assert "next=/" in masked


def test_secrets_inside_a_serialized_command_are_masked() -> None:
    raw = json.dumps({"type": "command.connect.confirm", "payload": {"password": "hunter2"}})
    assert "hunter2" not in mask(raw)
    assert "hunter2" not in mask(raw.encode())


def test_models_and_dataclasses_are_walked() -> None:
    class Confirm(BaseModel):
        flow_id: str
        password: SecretStr

    @dataclass
    class Assignment:
        host: str
        proxy_url: str

    masked = mask([Confirm(flow_id="f", password=SecretStr("x")), Assignment("h", "http://a:b@h:1")])
    assert masked == [{"flow_id": "f", "password": MASK}, {"host": "h", "proxy_url": MASK}]


def test_masking_does_not_touch_the_original() -> None:
    data = {"password": "hunter2"}
    mask(data)
    assert data == {"password": "hunter2"}


def test_unknown_objects_are_masked_through_their_text() -> None:
    error = RuntimeError("login failed for http://u:pw@h:1 with password=hunter2")
    masked = mask({"error": error})
    assert "hunter2" not in masked["error"]
    assert "pw@" not in masked["error"]
    plain = {"n": 1, "f": 1.5, "b": True, "none": None}
    assert mask(plain) == plain


def test_one_time_codes_in_free_text_are_masked() -> None:
    masked = mask_text('confirm {"code": "123456"} retry code=654321 error_code=deauthorized')
    assert "123456" not in masked
    assert "654321" not in masked
    assert "error_code=deauthorized" in masked


def test_url_passwords_with_slash_or_at_are_masked() -> None:
    masked = mask_text("via http://user:a/b@c@proxy.example:8080/path")
    assert masked == f"via http://{MASK}@proxy.example:8080/path"
