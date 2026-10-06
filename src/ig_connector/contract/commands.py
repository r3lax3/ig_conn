# CRM may add fields at any time, so commands ignore unknown ones.

import re
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from ig_connector.contract.media import Attachment


class _Command(BaseModel):
    # hide_input_in_errors: validation errors end up in logs, and payloads carry message
    # texts, phones and logins
    model_config = ConfigDict(extra="ignore", frozen=True, hide_input_in_errors=True)


class FormatSpan(_Command):
    type: str
    # both in UTF-16 code units, not Python str indices
    offset: int = Field(ge=0)
    length: int = Field(ge=0)


class SendPayload(_Command):
    message_id: str
    external_chat_id: str
    text: str
    format: list[FormatSpan] = []
    attachments: list[Attachment] = []
    reply_to_external_id: str | None = None


class EditPayload(_Command):
    message_id: str
    external_chat_id: str
    external_message_id: str
    text: str
    format: list[FormatSpan] = []


class DeletePayload(_Command):
    message_id: str
    external_chat_id: str
    external_message_id: str


class ResolveRecipientPayload(_Command):
    recipient_kind: Literal["phone", "username", "external_id"]
    value: str = Field(min_length=1)


class ConnectStartPayload(_Command):
    flow_id: UUID
    # role and proxy_network_type are not closed lists in the contract, so plain str:
    # a new value from CRM must not break the login
    role: str
    # "session" is reserved by the contract, CRM does not send it yet
    method: Literal["qr", "pairing_code", "session"]
    phone: str | None = None
    # not in contract 2.1, proposed to CRM: username, phone or email used as Instagram login
    login: str | None = Field(default=None, min_length=1)
    reconnect_source_id: UUID | None = None
    # null means "go direct" per contract; the connector never goes direct and refuses it
    proxy_country_code: str | None = None
    proxy_network_type: str | None = None

    @model_validator(mode="after")
    def _identity_for_first_pairing(self) -> Self:
        # contract 5.5: pairing_code needs an identity, except on reconnect where CRM may not know it
        if (
            self.method == "pairing_code"
            and self.reconnect_source_id is None
            and not (self.phone or self.login)
        ):
            raise ValueError("pairing_code without reconnect_source_id requires phone or login")
        return self


class ConnectConfirmPayload(_Command):
    flow_id: UUID
    code: SecretStr | None = Field(default=None, min_length=1)
    password: SecretStr | None = Field(default=None, min_length=1)
    # not in contract 2.1, proposed to CRM: lets the connector generate 2FA codes itself
    totp_secret: SecretStr | None = Field(default=None, min_length=16)

    @field_validator("totp_secret", mode="before")
    @classmethod
    def _normalize_totp_secret(cls, value: object) -> object:
        # operators paste it as shown by Instagram: "D5J5 MYZN ...", sometimes lowercase
        if not isinstance(value, str):
            return value
        secret = value.replace(" ", "").upper()
        if not re.fullmatch(r"[A-Z2-7]+=*", secret):
            raise ValueError("totp_secret must be base32")
        return secret

    @model_validator(mode="after")
    def _code_or_password(self) -> Self:
        if (self.code is None) == (self.password is None):
            raise ValueError("exactly one of code/password must be set")
        if self.totp_secret is not None and self.password is None:
            raise ValueError("totp_secret comes only together with password")
        return self


CommandPayload = (
    SendPayload
    | EditPayload
    | DeletePayload
    | ResolveRecipientPayload
    | ConnectStartPayload
    | ConnectConfirmPayload
)
