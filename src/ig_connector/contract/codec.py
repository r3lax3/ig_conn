from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ValidationError

from ig_connector.contract.commands import (
    CommandPayload,
    ConnectConfirmPayload,
    ConnectStartPayload,
    DeletePayload,
    EditPayload,
    ResolveRecipientPayload,
    SendPayload,
)
from ig_connector.contract.envelope import CommandEnvelope, CommandType, EventEnvelope, EventType
from ig_connector.contract.events import (
    AckPayload,
    ConnectCodeReadyPayload,
    ConnectQrReadyPayload,
    ConnectStatusPayload,
    EventPayload,
    InboundMessagePayload,
    MessageDeletedPayload,
    MessagePinnedPayload,
    ReadReceiptPayload,
    ResultPayload,
    StatusPayload,
)

COMMAND_PAYLOADS: dict[CommandType, type[CommandPayload]] = {
    CommandType.SEND: SendPayload,
    CommandType.EDIT: EditPayload,
    CommandType.DELETE: DeletePayload,
    CommandType.RESOLVE_RECIPIENT: ResolveRecipientPayload,
    CommandType.CONNECT_START: ConnectStartPayload,
    CommandType.CONNECT_CONFIRM: ConnectConfirmPayload,
}

EVENT_PAYLOADS: dict[EventType, type[EventPayload]] = {
    EventType.INBOUND_MESSAGE: InboundMessagePayload,
    EventType.STATUS: StatusPayload,
    EventType.READ_RECEIPT: ReadReceiptPayload,
    EventType.MESSAGE_DELETED: MessageDeletedPayload,
    EventType.MESSAGE_PINNED: MessagePinnedPayload,
    EventType.CONNECT_QR_READY: ConnectQrReadyPayload,
    EventType.CONNECT_CODE_READY: ConnectCodeReadyPayload,
    EventType.CONNECT_STATUS: ConnectStatusPayload,
    EventType.ACK: AckPayload,
    EventType.RESULT: ResultPayload,
}

_PAYLOAD_EVENT_TYPES: dict[type[BaseModel], EventType] = {
    model: kind for kind, model in EVENT_PAYLOADS.items()
}


class ContractViolationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class Command:
    envelope: CommandEnvelope
    type: CommandType
    payload: CommandPayload


def parse_command(raw: bytes | str) -> Command:
    return command_from(parse_envelope(raw))


def parse_envelope(raw: bytes | str) -> CommandEnvelope:
    try:
        return CommandEnvelope.model_validate_json(raw)
    except ValidationError as exc:
        raise ContractViolationError(f"bad envelope: {exc}") from exc


def command_from(envelope: CommandEnvelope) -> Command:
    try:
        command_type = CommandType(envelope.type)
    except ValueError as exc:
        raise ContractViolationError(f"unknown command type {envelope.type!r}") from exc
    try:
        payload = COMMAND_PAYLOADS[command_type].model_validate(envelope.payload)
    except ValidationError as exc:
        raise ContractViolationError(f"bad {command_type} payload: {exc}") from exc
    return Command(envelope=envelope, type=command_type, payload=payload)


def parse_event(raw: bytes | str) -> tuple[EventEnvelope, EventPayload]:
    try:
        envelope = EventEnvelope.model_validate_json(raw)
        payload = EVENT_PAYLOADS[envelope.type].model_validate(envelope.payload)
    except ValidationError as exc:
        raise ContractViolationError(str(exc)) from exc
    return envelope, payload


def build_event(
    *,
    payload: EventPayload,
    operation_id: UUID,
    source_id: UUID,
    channel_type: str,
    contract_version: str,
    occurred_at: datetime,
) -> EventEnvelope:
    return EventEnvelope(
        contract_version=contract_version,
        operation_id=operation_id,
        source_id=source_id,
        channel_type=channel_type,
        type=_PAYLOAD_EVENT_TYPES[type(payload)],
        payload=payload.model_dump(mode="json", exclude_none=True),
        occurred_at=occurred_at,
    )


def reply_to(
    command: Command | CommandEnvelope, payload: EventPayload, *, occurred_at: datetime
) -> EventEnvelope:
    # an envelope alone is enough: a command with an invalid payload still gets its result
    envelope = command.envelope if isinstance(command, Command) else command
    return build_event(
        payload=payload,
        operation_id=envelope.operation_id,
        source_id=envelope.source_id,
        channel_type=envelope.channel_type,
        contract_version=envelope.contract_version,
        occurred_at=occurred_at,
    )


def serialize(envelope: EventEnvelope) -> bytes:
    return envelope.model_dump_json().encode()
