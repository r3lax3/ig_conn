"""command.send: text only, into an existing personal dialog only.

The message gets our own label (client_context) and the Operation moves to sending with
it before the platform is called: from then on the message may be out, so nothing
after that point answers with a code CRM would retry by sending again. An unclear outcome,
right away or found on a redelivery after a crash, is reconciled: the dialog's latest
messages are read and the label looked up. Found = sent; not found or no history =
send_unconfirmed, never a blind second send.
"""

import logging
from dataclasses import dataclass

from ig_connector.clock import within
from ig_connector.contract.commands import SendPayload
from ig_connector.contract.errors import ResultErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Failure, OutgoingText, Platform, PlatformError, new_client_context
from ig_connector.logs import command_ids
from ig_connector.masking import mask_text
from ig_connector.runtime.deadline import DEADLINE_PASSED
from ig_connector.runtime.handlers import CommandContext, Handler
from ig_connector.runtime.refusals import NO_ANSWER, failure, refused
from ig_connector.runtime.sources import SourceCondition
from ig_connector.store import OperationState, Store

log = logging.getLogger(__name__)

# CRM gives up on a send 300 s after the ack (contract 9): the answer comes within
# SEND_BUDGET of receipt. Each step has its own cap; on a send that waited long in the
# queue they shrink to what is left, and the send is not started without SEND_MIN_WINDOW
# for itself plus the time to reconcile it (a send cut short is unconfirmed)
SEND_BUDGET = 290.0
DIALOG_CHECK_TIMEOUT = 30.0
SEND_TIMEOUT = 210.0
SEND_MIN_WINDOW = 10.0
RECONCILE_TIMEOUT = 20.0

# TODO(contract 2.2): not_supported instead of channel_rejected for both
NO_ATTACHMENTS = "attachments are not supported yet"
NO_NEW_DIALOGS = "starting new dialogs is stage 3"
UNCONFIRMED = "the message may have been sent, it could not be confirmed"


def send_handler(platform: Platform, store: Store) -> Handler:
    return Handler(SendText(platform, store))


@dataclass(frozen=True, slots=True)
class SendText:
    platform: Platform
    store: Store

    async def __call__(self, ctx: CommandContext) -> ResultPayload:
        payload = ctx.command.payload
        if not isinstance(payload, SendPayload):
            raise TypeError(f"send handler got {type(payload).__name__}")
        if payload.attachments:
            return failure(ResultErrorCode.CHANNEL_REJECTED, NO_ATTACHMENTS)
        if ctx.operation.state is OperationState.SENDING:
            # a delivery that found the previous attempt mid-send (crash, restart): the
            # message may be out, never send it twice
            log.warning("send found mid-send, reconciling", extra=command_ids(ctx.command.envelope))
            # only a read, whatever CRM's timer says: the truth is worth more than a late answer
            return await self._reconcile(
                ctx, payload.external_chat_id, ctx.operation.client_context, RECONCILE_TIMEOUT
            )
        left = ctx.time_left(SEND_BUDGET)
        if left <= 0:
            # CRM's time ran out in the queue: nothing was sent, a retry is safe
            return failure(ResultErrorCode.NETWORK_ERROR, DEADLINE_PASSED)
        source_id = ctx.command.envelope.source_id
        try:
            exists = await within(
                ctx.clock,
                min(DIALOG_CHECK_TIMEOUT, left),
                self.platform.has_dialog(source_id, payload.external_chat_id),
            )
        except TimeoutError:
            return failure(ResultErrorCode.NETWORK_ERROR, NO_ANSWER)
        except PlatformError as exc:
            return _refused(ctx, exc)
        if not exists:
            return failure(ResultErrorCode.CHANNEL_REJECTED, NO_NEW_DIALOGS)

        window = min(SEND_TIMEOUT, ctx.time_left(SEND_BUDGET) - RECONCILE_TIMEOUT)
        if window < SEND_MIN_WINDOW:
            return failure(ResultErrorCode.NETWORK_ERROR, DEADLINE_PASSED)
        message = OutgoingText(
            source_id=source_id,
            peer_id=payload.external_chat_id,
            text=payload.text,
            client_context=new_client_context(),
            reply_to=payload.reply_to_external_id,
        )
        await self.store.start_sending(ctx.operation, message.client_context, now=ctx.now())
        try:
            sent = await within(ctx.clock, window, self.platform.send_text(message))
        except TimeoutError:
            log.warning("send did not answer in time", extra=command_ids(ctx.command.envelope))
        except PlatformError as exc:
            if exc.failure is not Failure.UNKNOWN_AFTER_SEND:
                return _refused(ctx, exc)
            log.warning(
                "send outcome unknown: %s", mask_text(exc.detail), extra=command_ids(ctx.command.envelope)
            )
        except Exception:
            # an adapter bug once the request may be out is still "sent, not sure", not a retry
            log.exception("send failed unexpectedly", extra=command_ids(ctx.command.envelope))
        else:
            return _sent(sent.message_id)
        window = min(RECONCILE_TIMEOUT, ctx.time_left(SEND_BUDGET))
        return await self._reconcile(ctx, message.peer_id, message.client_context, window)

    async def _reconcile(
        self, ctx: CommandContext, peer_id: str, label: str | None, window: float
    ) -> ResultPayload:
        """ok with the message found by our label in the dialog, else send_unconfirmed."""
        if label is None:
            log.error("send mid-send without a label", extra=command_ids(ctx.command.envelope))
            return failure(ResultErrorCode.SEND_UNCONFIRMED, UNCONFIRMED)
        if ctx.source.condition is not SourceCondition.ACTIVE:
            # no working Session (e.g. right after a restart): the history cannot be read
            log.warning(
                "send not reconciled: the source is not active", extra=command_ids(ctx.command.envelope)
            )
            return failure(ResultErrorCode.SEND_UNCONFIRMED, UNCONFIRMED)
        if window <= 0:
            log.warning("send not reconciled: no time left", extra=command_ids(ctx.command.envelope))
            return failure(ResultErrorCode.SEND_UNCONFIRMED, UNCONFIRMED)
        source_id = ctx.command.envelope.source_id
        try:
            history = await within(ctx.clock, window, self.platform.recent_messages(source_id, peer_id))
        except TimeoutError:
            log.warning(
                "send not reconciled: history did not answer in time", extra=command_ids(ctx.command.envelope)
            )
            return failure(ResultErrorCode.SEND_UNCONFIRMED, UNCONFIRMED)
        except Exception as exc:
            detail = (
                f"{exc.failure}: {mask_text(exc.detail)}"
                if isinstance(exc, PlatformError)
                else type(exc).__name__
            )
            log.warning(
                "send not reconciled: history unavailable (%s)",
                detail,
                extra=command_ids(ctx.command.envelope),
            )
            return failure(ResultErrorCode.SEND_UNCONFIRMED, UNCONFIRMED)
        for found in history:
            if found.client_context == label:
                log.info("send confirmed by its label in the dialog", extra=command_ids(ctx.command.envelope))
                return _sent(found.message_id)
        log.warning("send not reconciled: label not in the dialog", extra=command_ids(ctx.command.envelope))
        return failure(ResultErrorCode.SEND_UNCONFIRMED, UNCONFIRMED)


def _sent(message_id: str) -> ResultPayload:
    # Instagram gives no delivery receipt for Direct: `delivered` stays unset
    return ResultPayload(ok=True, external_message_id=message_id)


def _refused(ctx: CommandContext, exc: PlatformError) -> ResultPayload:
    log.info(
        "send refused: %s (%s)", exc.failure, mask_text(exc.detail), extra=command_ids(ctx.command.envelope)
    )
    return refused(exc.failure)
