from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict

# TODO(contract 2.2): our own events (status, inbound) still say 2.1; replies echo the command
CONTRACT_VERSION = "2.1"


class CommandType(StrEnum):
    SEND = "command.send"
    EDIT = "command.edit"
    DELETE = "command.delete"
    RESOLVE_RECIPIENT = "command.resolve_recipient"
    CONNECT_START = "command.connect.start"
    CONNECT_CONFIRM = "command.connect.confirm"


class EventType(StrEnum):
    INBOUND_MESSAGE = "event.inbound_message"
    STATUS = "event.status"
    READ_RECEIPT = "event.read_receipt"
    MESSAGE_DELETED = "event.message_deleted"
    MESSAGE_PINNED = "event.message_pinned"
    CONNECT_QR_READY = "event.connect.qr_ready"
    CONNECT_CODE_READY = "event.connect.code_ready"
    CONNECT_STATUS = "event.connect.status"
    ACK = "ack"
    RESULT = "result"


class CommandEnvelope(BaseModel):
    # hide_input_in_errors: a broken envelope's error is logged, and its payload may hold
    # a 2FA code or a message text
    model_config = ConfigDict(extra="ignore", frozen=True, hide_input_in_errors=True)

    # not a Literal: the version grows with every contract edit
    contract_version: str
    operation_id: UUID
    source_id: UUID
    # contract 4: must match the channel_type in the topic name
    channel_type: str
    # plain str so an unknown type gets a clear error in the codec, not a generic one here
    type: str
    payload: dict[str, Any]
    occurred_at: AwareDatetime

    @property
    def dedup_key(self) -> tuple[UUID, str]:
        # connect.start and connect.confirm share operation_id, so type is part of the key
        return (self.operation_id, self.type)


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    # no defaults on purpose: a reply echoes the command's version, and occurred_at is
    # when the thing happened (e.g. message time on the platform), not publish time
    contract_version: str
    operation_id: UUID
    source_id: UUID
    channel_type: str
    type: EventType
    payload: dict[str, Any]
    occurred_at: AwareDatetime
