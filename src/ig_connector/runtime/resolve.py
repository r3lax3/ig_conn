"""command.resolve_recipient: is there such a recipient on Instagram, and how is it addressed.

A private dialog is addressed by the user's Instagram ID, so `external_chat_id` is the pk.
Only a lookup is made: nothing reaches the recipient.
"""

import logging
from typing import TYPE_CHECKING

from ig_connector.clock import within
from ig_connector.contract.commands import ResolveRecipientPayload
from ig_connector.contract.errors import ResultErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Platform, PlatformError, User
from ig_connector.logs import command_ids
from ig_connector.masking import mask_text
from ig_connector.runtime.deadline import DEADLINE_PASSED
from ig_connector.runtime.refusals import NO_ANSWER, failure, refused

if TYPE_CHECKING:
    from ig_connector.runtime.handlers import CommandContext

_log = logging.getLogger(__name__)

# CRM waits 10 s for the result from the ack (contract 9): the lookup ends before that,
# counted from receipt, so time spent in the Source queue is included
LOOKUP_DEADLINE = 8.0


class ResolveRecipient:
    def __init__(self, platform: Platform, *, deadline: float = LOOKUP_DEADLINE) -> None:
        self._platform = platform
        self._deadline = deadline

    async def __call__(self, ctx: "CommandContext") -> ResultPayload:
        payload = ctx.command.payload
        if not isinstance(payload, ResolveRecipientPayload):
            raise TypeError(f"resolve handler got {type(payload).__name__}")
        source_id = ctx.command.envelope.source_id
        if payload.recipient_kind == "phone":
            # a personal account cannot look users up by phone
            return failure(ResultErrorCode.RECIPIENT_NOT_FOUND, "Instagram users cannot be found by phone")
        left = ctx.time_left(self._deadline)
        if left <= 0:
            # CRM's time ran out in the queue: nothing asked, a retry is safe
            return failure(ResultErrorCode.NETWORK_ERROR, DEADLINE_PASSED)
        if payload.recipient_kind == "username":
            lookup = self._platform.user_by_username(source_id, payload.value)
        elif payload.value.isascii() and payload.value.isdigit():
            lookup = self._platform.user_by_id(source_id, payload.value)
        else:
            return failure(ResultErrorCode.RECIPIENT_NOT_FOUND, "an Instagram ID is a number")
        try:
            user: User = await within(ctx.clock, left, lookup)
        except TimeoutError:
            return failure(ResultErrorCode.NETWORK_ERROR, NO_ANSWER)
        except PlatformError as refusal:
            _log.info(
                "user lookup refused",
                extra={
                    **command_ids(ctx.command.envelope),
                    "failure": str(refusal.failure),
                    "detail": mask_text(refusal.detail),
                },
            )
            return refused(refusal.failure)
        return ResultPayload(
            ok=True, external_chat_id=user.external_id, display_name=user.full_name or user.username
        )
