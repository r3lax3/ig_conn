"""What a refusal of the platform means: for the command that met it, and for its Source.

Texts are fixed: error_text goes to CRM, the adapter's detail only to our logs.
"""

from ig_connector.contract.errors import ResultErrorCode, StatusErrorCode
from ig_connector.contract.events import ResultPayload, StatusPayload
from ig_connector.instagram import Failure

NO_ANSWER = "Instagram did not answer in time"


def failure(code: ResultErrorCode, text: str) -> ResultPayload:
    return ResultPayload(ok=False, error_code=code, error_text=text)


# a refusal before anything reached a recipient: CRM retries those it retries
_RESULTS: dict[Failure, tuple[ResultErrorCode, str]] = {
    Failure.SESSION_REVOKED: (ResultErrorCode.SOURCE_OFFLINE, "Instagram no longer accepts the session"),
    Failure.BAD_CREDENTIALS: (ResultErrorCode.SOURCE_OFFLINE, "Instagram no longer accepts the session"),
    Failure.CHALLENGE: (ResultErrorCode.SOURCE_OFFLINE, "Instagram asks the owner to pass a check"),
    # TODO(contract 2.2): peer_flood becomes rate_limited with reason=antispam
    Failure.FLOOD: (ResultErrorCode.PEER_FLOOD, "Instagram restricted the account"),
    Failure.RATE_LIMITED: (ResultErrorCode.RATE_LIMITED, "Instagram asks to slow down"),
    Failure.REJECTED: (ResultErrorCode.CHANNEL_REJECTED, "Instagram refused the request"),
    Failure.NOT_FOUND: (ResultErrorCode.RECIPIENT_NOT_FOUND, "Instagram does not know the recipient"),
    Failure.NETWORK: (ResultErrorCode.NETWORK_ERROR, "Instagram could not be reached"),
    Failure.NO_ANSWER: (ResultErrorCode.NETWORK_ERROR, NO_ANSWER),
}

# what a refusal says about the Source; the rest only fail their own call
_STATUSES: dict[Failure, StatusPayload] = {
    Failure.SESSION_REVOKED: StatusPayload(
        status="needs_reconnect",
        error_code=StatusErrorCode.DEAUTHORIZED,
        error_text="Instagram no longer accepts the session",
    ),
    Failure.BAD_CREDENTIALS: StatusPayload(
        status="needs_reconnect",
        error_code=StatusErrorCode.DEAUTHORIZED,
        error_text="Instagram no longer accepts the session",
    ),
    Failure.CHALLENGE: StatusPayload(
        status="needs_reconnect",
        error_code=StatusErrorCode.DEAUTHORIZED,
        error_text="Instagram asks the owner to pass a check",
    ),
    Failure.FLOOD: StatusPayload(
        status="error", error_code=StatusErrorCode.PEER_FLOOD, error_text="Instagram restricted the account"
    ),
    Failure.NETWORK: StatusPayload(
        status="error", error_code=StatusErrorCode.SOURCE_OFFLINE, error_text="no proxy or network failure"
    ),
}


def refused(refusal: Failure) -> ResultPayload:
    """The result of a command the platform refused before anything reached the recipient."""
    answer = _RESULTS.get(refusal)
    if answer is None:
        # UNKNOWN_AFTER_SEND from a read: an adapter bug, never a reason to retry
        return failure(ResultErrorCode.INTERNAL_ERROR, f"unexpected platform answer {refusal}")
    return failure(*answer)


def status_for(failure: Failure) -> StatusPayload | None:
    """The Source status a refusal means; None when it says nothing about the Source."""
    return _STATUSES.get(failure)
