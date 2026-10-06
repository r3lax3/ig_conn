# Closed lists, contract section 9.
from enum import StrEnum


class ResultErrorCode(StrEnum):
    NETWORK_ERROR = "network_error"
    CHANNEL_REJECTED = "channel_rejected"
    BLOCKED_BY_USER = "blocked_by_user"
    PEER_FLOOD = "peer_flood"
    RECIPIENT_NOT_FOUND = "recipient_not_found"
    SOURCE_OFFLINE = "source_offline"
    RATE_LIMITED = "rate_limited"
    EDIT_UNSUPPORTED = "edit_unsupported"
    MESSAGE_DELETED = "message_deleted"
    SEND_UNCONFIRMED = "send_unconfirmed"
    INTERNAL_ERROR = "internal_error"


class StatusErrorCode(StrEnum):
    DEAUTHORIZED = "deauthorized"
    NOT_AUTHORIZED = "not_authorized"
    CONNECT_FAILED = "connect_failed"
    VERIFY_FAILED = "verify_failed"
    PEER_FLOOD = "peer_flood"
    SOURCE_OFFLINE = "source_offline"


# Contract 9, "will CRM retry": on these CRM sends the same operation_id again as a new
# delivery, and that delivery needs a fresh attempt, not the saved answer.
RETRIED_BY_CRM = frozenset(
    {
        ResultErrorCode.NETWORK_ERROR,
        ResultErrorCode.PEER_FLOOD,
        ResultErrorCode.SOURCE_OFFLINE,
        ResultErrorCode.RATE_LIMITED,
    }
)
