# Events are strict: CRM silently drops unknown fields, so a typo like `phone`
# instead of `account_phone` has to fail on our side.

from typing import Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StrictBool, model_validator

from ig_connector.contract.errors import ResultErrorCode, StatusErrorCode
from ig_connector.contract.media import InboundMedia


class _Event(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class InboundMessagePayload(_Event):
    external_chat_id: str
    external_message_id: str
    # The person, not the chat; CRM builds the client card from it.
    external_user_id: str
    user_display_name: str | None = None
    text: str = ""
    kind: Literal["text", "image", "video", "file", "unsupported"]
    is_edit: bool = False
    reply_to_external_id: str | None = None
    media: list[InboundMedia] = []


class StatusPayload(_Event):
    # TODO(contract 2.2): reason, resume_at, action_required next to the status
    status: Literal["active", "error", "needs_reconnect", "disabled"]
    error_code: StatusErrorCode | None = None
    error_text: str | None = None

    @model_validator(mode="after")
    def _error_needs_code(self) -> Self:
        if self.status == "error" and self.error_code is None:
            raise ValueError("status=error requires error_code")
        return self


class ReadReceiptPayload(_Event):
    external_chat_id: str
    # A numeric watermark: everything with id <= this is read.
    max_external_message_id: str = Field(pattern=r"^\d+$")


class MessageDeletedPayload(_Event):
    external_chat_id: str
    external_message_id: str
    external_user_id: str | None = None


class MessagePinnedPayload(_Event):
    external_chat_id: str
    external_message_id: str
    pinned: StrictBool


class ConnectQrReadyPayload(_Event):
    qr_data_url: str = Field(pattern=r"^data:image/")
    expires_at: AwareDatetime | None = None


class ConnectCodeReadyPayload(_Event):
    user_code: str = Field(min_length=1)
    expires_at: AwareDatetime | None = None


class ConnectStatusPayload(_Event):
    # `expired` is set by CRM; qr_ready/code_sent are implied by their own event types.
    state: Literal["pending", "confirming", "done", "failed"]
    external_account_id: str | None = None
    external_account_name: str | None = None
    external_account_url: str | None = None
    account_username: str | None = None
    account_phone: str | None = None
    two_factor_enabled: bool | None = None
    session_label: str | None = None
    error_code: StatusErrorCode | None = None
    error_text: str | None = None

    @model_validator(mode="after")
    def _terminal_fields(self) -> Self:
        if self.state == "done" and not self.external_account_id:
            raise ValueError(
                "state=done requires external_account_id, otherwise CRM never creates the source"
            )
        if self.state == "failed" and self.error_code is None:
            raise ValueError("state=failed requires error_code")
        return self


class AckPayload(_Event):
    pass


class ResultPayload(_Event):
    ok: StrictBool
    # Success fields, depending on the command.
    external_message_id: str | None = None
    delivered: bool | None = None
    external_chat_id: str | None = None
    display_name: str | None = None
    # Failure fields.
    error_code: ResultErrorCode | None = None
    error_text: str | None = None

    @model_validator(mode="after")
    def _ok_consistency(self) -> Self:
        success_fields = (self.external_message_id, self.delivered, self.external_chat_id, self.display_name)
        if self.ok and self.error_code is not None:
            raise ValueError("ok=true must not carry error_code")
        if not self.ok and self.error_code is None:
            raise ValueError("ok=false requires error_code")
        if not self.ok and any(field is not None for field in success_fields):
            raise ValueError("ok=false must not carry success fields")
        return self


EventPayload = (
    InboundMessagePayload
    | StatusPayload
    | ReadReceiptPayload
    | MessageDeletedPayload
    | MessagePinnedPayload
    | ConnectQrReadyPayload
    | ConnectCodeReadyPayload
    | ConnectStatusPayload
    | AckPayload
    | ResultPayload
)
