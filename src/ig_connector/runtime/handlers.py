"""Command handlers: one per command type, looked up by the runtime.

A handler gets the parsed command, its saved Operation and the Source state, and returns
the result for this delivery. Everything else (ack, dedup, saving the result, publishing
it, committing the offset, internal_error on an exception) is the runtime's job.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from ig_connector.clock import Clock
from ig_connector.contract.codec import Command
from ig_connector.contract.envelope import CommandType
from ig_connector.contract.errors import ResultErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Platform
from ig_connector.runtime.deadline import time_left
from ig_connector.runtime.refusals import failure
from ig_connector.runtime.resolve import ResolveRecipient
from ig_connector.runtime.sources import SourceState
from ig_connector.store import Operation, Store


@dataclass(frozen=True, slots=True)
class CommandContext:
    command: Command
    # as saved before this delivery: a redelivery after a crash sees the old state here
    operation: Operation
    source: SourceState
    clock: Clock
    # where CRM's timeout for this attempt started: the first ack of the command, or of
    # CRM's retry after a retried code; never the handler's start
    received_at: datetime

    def now(self) -> datetime:
        return self.clock.now()

    def time_left(self, budget: float) -> float:
        """Seconds left of `budget` counted from receipt; zero or less once spent."""
        return time_left(budget, received_at=self.received_at, now=self.clock.now())


@dataclass(frozen=True, slots=True)
class Handler:
    run: Callable[[CommandContext], Awaitable[ResultPayload]]
    # the runtime answers source_offline / peer_flood itself when the Source is not active
    needs_active_source: bool = True


async def _edit_unsupported(ctx: CommandContext) -> ResultPayload:
    return failure(ResultErrorCode.EDIT_UNSUPPORTED, "editing sent messages is not supported")


async def _delete_rejected(ctx: CommandContext) -> ResultPayload:
    # TODO(contract 2.2): not_supported instead of channel_rejected
    return failure(ResultErrorCode.CHANNEL_REJECTED, "deleting sent messages is not supported")


async def _not_built_yet(ctx: CommandContext) -> ResultPayload:
    return failure(ResultErrorCode.INTERNAL_ERROR, f"{ctx.command.type} is not implemented yet")


def default_handlers(
    platform: Platform | None = None, store: Store | None = None
) -> dict[CommandType, Handler]:
    """The registry the service runs with. Login flow commands (connect.*) are not here.

    resolve_recipient needs the platform, send the platform and the store.
    """
    # TODO(contract 2.2): command.disconnect (close the Session, then status disabled)
    handlers = {
        CommandType.EDIT: Handler(_edit_unsupported, needs_active_source=False),
        CommandType.DELETE: Handler(_delete_rejected, needs_active_source=False),
        CommandType.SEND: Handler(_not_built_yet),
        CommandType.RESOLVE_RECIPIENT: Handler(_not_built_yet),
    }
    if platform is not None:
        handlers[CommandType.RESOLVE_RECIPIENT] = Handler(ResolveRecipient(platform))
    if platform is not None and store is not None:
        from ig_connector.runtime.send import send_handler  # it builds on this module

        handlers[CommandType.SEND] = send_handler(platform, store)
    return handlers
