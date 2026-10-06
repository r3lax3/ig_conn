"""Commands as the CRM would put them on the bus, for tests."""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from ig_connector.contract.envelope import CONTRACT_VERSION, CommandType

CHANNEL_TYPE = "individual_instagram_account"


def command(
    command_type: CommandType,
    payload: dict[str, Any],
    *,
    source_id: UUID,
    operation_id: UUID | None = None,
    channel_type: str = CHANNEL_TYPE,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "operation_id": str(operation_id or uuid4()),
        "source_id": str(source_id),
        "channel_type": channel_type,
        "type": command_type.value,
        "payload": payload,
        "occurred_at": (occurred_at or datetime.now(UTC)).isoformat(),
    }
