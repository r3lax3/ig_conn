import pytest
from pydantic import ValidationError

from ig_connector.contract.media import Attachment, InboundMedia

MEDIA = {"kind": "image", "bucket": "b", "key": "inbound/1/a.jpg", "mime": "image/jpeg", "size_bytes": 10}


def test_inbound_media_must_not_use_crm_prefix() -> None:
    with pytest.raises(ValidationError, match="crm/"):
        InboundMedia.model_validate({**MEDIA, "key": "crm/x/a.jpg"})


def test_outbound_attachment_lives_under_crm_prefix() -> None:
    assert Attachment.model_validate({**MEDIA, "key": "crm/x/a.jpg"}).key == "crm/x/a.jpg"


def test_platform_kinds_are_not_contract_kinds() -> None:
    with pytest.raises(ValidationError):
        InboundMedia.model_validate({**MEDIA, "kind": "photo"})


def test_kind_must_match_mime_for_image_and_video() -> None:
    with pytest.raises(ValidationError):
        InboundMedia.model_validate({**MEDIA, "kind": "video"})


def test_image_may_be_sent_as_file() -> None:
    assert InboundMedia.model_validate({**MEDIA, "kind": "file"}).kind == "file"


def test_size_cannot_be_negative() -> None:
    with pytest.raises(ValidationError):
        InboundMedia.model_validate({**MEDIA, "size_bytes": -1})


def test_typo_in_inbound_field_name_is_rejected() -> None:
    with pytest.raises(ValidationError, match="filename"):
        InboundMedia.model_validate({**MEDIA, "filename": "a.jpg"})
