from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Objects under this prefix belong to CRM: read only, never write or delete.
CRM_KEY_PREFIX = "crm/"

AttachmentKind = Literal["image", "video", "file"]


class Attachment(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    kind: AttachmentKind
    bucket: str = Field(min_length=1)
    key: str = Field(min_length=1)
    mime: str = Field(min_length=1)
    file_name: str | None = None
    size_bytes: int = Field(ge=0)


class InboundMedia(Attachment):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="after")
    def _check_key_and_kind(self) -> Self:
        if self.key.startswith(CRM_KEY_PREFIX):
            raise ValueError(f"key must not use the CRM-owned prefix {CRM_KEY_PREFIX!r}")
        # `file` may legitimately carry an image sent as a document; image/video must match mime
        if self.kind == "image" and not self.mime.startswith("image/"):
            raise ValueError("kind=image requires an image/* mime")
        if self.kind == "video" and not self.mime.startswith("video/"):
            raise ValueError("kind=video requires a video/* mime")
        return self
