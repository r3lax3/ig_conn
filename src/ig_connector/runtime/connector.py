"""The core: reads command deliveries, runs them per Source, answers and commits.

The consumer only routes deliveries by source_id. Each Source has its own executor task
that takes its commands strictly one at a time; Sources run in parallel, at most
max_parallel_sources talking to Instagram at once (commands, polls, restores). The
in-memory queue is only the order of deliveries already read, never a retry queue (CRM
retries, we do not): an offset is committed once its command is answered, along the
contiguous tail of its partition.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from ig_connector.bus import Bus, Delivery, commands_topic
from ig_connector.clock import Clock, within
from ig_connector.contract.codec import ContractViolationError, command_from, parse_envelope, reply_to
from ig_connector.contract.envelope import CommandEnvelope, CommandType
from ig_connector.contract.errors import RETRIED_BY_CRM, ResultErrorCode, StatusErrorCode
from ig_connector.contract.events import AckPayload, ConnectStatusPayload, EventPayload, ResultPayload
from ig_connector.logs import command_ids
from ig_connector.runtime.handlers import CommandContext, Handler
from ig_connector.runtime.inbound import InboundPolling
from ig_connector.runtime.login import LoginFlows
from ig_connector.runtime.offsets import PartitionOffsets
from ig_connector.runtime.proxying import ProxyLifecycle
from ig_connector.runtime.refusals import failure
from ig_connector.runtime.restore import SessionRestore
from ig_connector.runtime.slots import Slots
from ig_connector.runtime.sources import SourceCondition, SourceStates
from ig_connector.store import Operation, OperationState, Store

log = logging.getLogger(__name__)

# answered with event.connect.* only, never ack/result (contract 6.7)
_LOGIN_FLOW = frozenset({CommandType.CONNECT_START, CommandType.CONNECT_CONFIRM})


@dataclass(frozen=True, slots=True)
class _Queued:
    delivery: Delivery
    envelope: CommandEnvelope
    # saved and acked on receipt
    acked: bool
    # handler deadlines count from here, like CRM's timeout from the ack
    received_at: datetime


@dataclass(frozen=True, slots=True)
class _Job:
    """Work on the Source itself (a proxy change), run between its commands."""

    run: Callable[[], Awaitable[None]]


_Item = _Queued | _Job


class Connector:
    def __init__(
        self,
        *,
        bus: Bus,
        store: Store,
        clock: Clock,
        sources: SourceStates,
        handlers: Mapping[CommandType, Handler],
        max_parallel_sources: int = 20,
        login: LoginFlows | None = None,
        inbound: InboundPolling | None = None,
        restore: SessionRestore | None = None,
        proxies: ProxyLifecycle | None = None,
    ) -> None:
        self._bus = bus
        self._store = store
        self._clock = clock
        self._sources = sources
        self._handlers = dict(handlers)
        self._login = login
        self._inbound = inbound
        # commands, polls, restores and proxy changes alike: no more Sources talk to
        # Instagram at once; a waiting command goes first
        self._slots = Slots(max_parallel_sources)
        self._offsets = PartitionOffsets(bus)
        self._queues: dict[UUID, asyncio.Queue[_Item]] = {}
        self._restore = restore
        self._proxies = proxies
        self._tasks: asyncio.TaskGroup | None = None
        self._stopping = False
        # commands being executed right now; a drain waits for them
        self._running = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def run(self) -> None:
        """Run until cancelled. Never returns: a bus failure or the end of the stream raises."""
        async with asyncio.TaskGroup() as tasks:
            self._tasks = tasks
            if self._proxies is not None:
                tasks.create_task(self._proxies.run(self.between_commands, self._restore), name="proxies")
            if self._inbound is not None or self._restore is not None:
                # restored and polled even before (or without) any command for them
                for source_id in await self._store.connected_sources():
                    self._queue(source_id, tasks, restore=True)
            async for delivery in self._bus.deliveries():
                if self._stopping:
                    # not taken: uncommitted, it comes again after the restart
                    continue
                self._offsets.track(delivery)
                envelope = self._admit(delivery)
                if envelope is None:
                    await self._offsets.finish(delivery)
                    continue
                queue = self._queue(envelope.source_id, tasks)
                received_at = self._clock.now()
                acked = envelope.type not in _LOGIN_FLOW and await self._acknowledge(envelope)
                queue.put_nowait(_Queued(delivery, envelope, acked, received_at))
            # a stream that ends (lost partitions, consumer stopped) must not leave a silent
            # process behind: fail, and let the supervisor restart us
            raise RuntimeError("delivery stream ended")

    def _admit(self, delivery: Delivery) -> CommandEnvelope | None:
        """The envelope if this record is a command for us to answer, else None (logged)."""
        where = {"partition": delivery.partition, "offset": delivery.offset}
        try:
            envelope = parse_envelope(delivery.value)
        except ContractViolationError as exc:
            log.error("unreadable command skipped: %s", exc, extra=where)
            return None
        if delivery.topic != commands_topic(envelope.channel_type):
            log.error(
                "command skipped: channel_type %r does not match topic %s",
                envelope.channel_type,
                delivery.topic,
                extra={**where, **command_ids(envelope)},
            )
            return None
        return envelope

    async def _acknowledge(self, envelope: CommandEnvelope) -> bool:
        """Save the command and ack it on receipt, before its turn in the Source queue."""
        try:
            await self._store.record_operation(envelope, now=self._clock.now())
        except Exception:
            # the executor tries again at the command's turn, and answers source_offline
            log.exception("command not saved on receipt", extra=command_ids(envelope))
            return False
        await self._publish(envelope, AckPayload())
        return True

    def between_commands(self, source_id: UUID, job: Callable[[], Awaitable[None]]) -> bool:
        """Run the job in the Source's executor once its running command is answered.

        False if the connector is not running (or stopping): the job is not taken.
        """
        if self._tasks is None or self._stopping:
            return False
        self._queue(source_id, self._tasks).put_nowait(_Job(job))
        return True

    async def drain(self, grace: float) -> None:
        """Stop taking deliveries and commands; wait up to `grace` s for those running to end.

        For a clean stop: cancel run() afterwards. Commands taken but not started, and any
        still running after the grace period, are not committed and come again after the restart.
        """
        self._stopping = True
        try:
            await within(self._clock, grace, self._idle.wait())
        except TimeoutError:
            log.warning("stopping with commands still running", extra={"running": self._running})

    def _queue(
        self, source_id: UUID, tasks: asyncio.TaskGroup, *, restore: bool = False
    ) -> asyncio.Queue[_Item]:
        """The Source's queue; its executor starts with it (restoring its Session first if asked)."""
        queue = self._queues.get(source_id)
        if queue is None:
            queue = self._queues[source_id] = asyncio.Queue()
            executing = self._execute_source(source_id, queue, restore=restore)
            tasks.create_task(executing, name=f"source-{source_id}")
        return queue

    async def _restore_source(self, source_id: UUID) -> None:
        if self._restore is None:
            return
        try:
            async with self._slots.hold(command=False):
                await self._restore.restore(source_id)
        except Exception:
            # the Source stays offline; its commands are answered source_offline
            log.exception("source restore failed", extra={"source_id": str(source_id)})

    async def _execute_source(self, source_id: UUID, queue: asyncio.Queue[_Item], *, restore: bool) -> None:
        if restore:
            # its commands wait in the queue meanwhile: the Session first
            await self._restore_source(source_id)
        inbound = self._inbound
        if inbound is not None:
            next_poll = self._clock.now() + timedelta(seconds=inbound.first_delay())
        while True:
            if inbound is None:
                item = await queue.get()
            else:
                wait = (next_poll - self._clock.now()).total_seconds()
                if wait <= 0:
                    started = self._clock.now()
                    arrived, polled = await self._poll_unless_command(inbound, source_id, queue)
                    if polled:
                        # a steady cadence: the interval counts from the start of the poll
                        next_poll = started + timedelta(seconds=inbound.interval)
                    if arrived is None:
                        continue
                    # interrupted: still due, the poll runs again right after this command
                    item = arrived
                else:
                    try:
                        item = await within(self._clock, wait, queue.get())
                    except TimeoutError:
                        continue
            if self._stopping:
                return
            if isinstance(item, _Job):
                await self._run_job(source_id, item)
                continue
            self._running += 1
            self._idle.clear()
            try:
                async with self._slots.hold(command=True):
                    if self._stopping:
                        # it waited for a slot: not started, it comes again after the restart
                        return
                    await self._process(item.envelope, acked=item.acked, received_at=item.received_at)
                await self._offsets.finish(item.delivery)
            finally:
                self._running -= 1
                if self._running == 0:
                    self._idle.set()

    async def _run_job(self, source_id: UUID, job: _Job) -> None:
        # a drain lets it finish like a command: a Session half moved is worse than a late stop
        self._running += 1
        self._idle.clear()
        try:
            async with self._slots.hold(command=False):
                await job.run()
        except Exception:
            log.exception("source job failed", extra={"source_id": str(source_id)})
        finally:
            self._running -= 1
            if self._running == 0:
                self._idle.set()

    async def _poll_unless_command(
        self, inbound: InboundPolling, source_id: UUID, queue: asyncio.Queue[_Item]
    ) -> tuple[_Item | None, bool]:
        """Poll the inbox; a command of the Source arriving meanwhile cancels the poll.

        Never next to a command of this Source, and never in its way: CRM waits only
        10 s for resolve_recipient. A cancelled poll loses nothing (positions move only
        after publishing). Returns what arrived meanwhile (a command, or a job the poll
        itself caused), if anything, and whether the poll ran to its end.
        """
        polling = asyncio.ensure_future(self._poll(inbound, source_id))
        arriving = asyncio.ensure_future(queue.get())
        try:
            await asyncio.wait((polling, arriving), return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (polling, arriving):
                task.cancel()
            await asyncio.wait((polling, arriving))
        if arriving.cancelled():
            return None, True
        if not polling.cancelled():
            # it finished in the same turn: nothing was interrupted
            return arriving.result(), True
        log.info("inbox poll interrupted by a command", extra={"source_id": str(source_id)})
        return arriving.result(), False

    async def _poll(self, inbound: InboundPolling, source_id: UUID) -> None:
        try:
            async with self._slots.hold(command=False):
                if self._stopping:
                    return
                condition = (await self._sources.state(source_id)).condition
                if condition is SourceCondition.ACTIVE:
                    await inbound.poll(source_id)
                elif condition is not SourceCondition.DISABLED and self._restore is not None:
                    # on the poll cadence: a Source in error comes back once the platform answers
                    await self._restore.recheck(source_id)
        except Exception:
            # positions did not move past anything unpublished: the next poll takes it again
            log.exception("inbox poll failed", extra={"source_id": str(source_id)})

    async def _process(self, envelope: CommandEnvelope, *, acked: bool, received_at: datetime) -> None:
        if envelope.type in _LOGIN_FLOW:
            await self._run_login_flow(envelope)
            return
        started = self._clock.now()
        try:
            # again at its turn: the receipt may have failed, and a duplicate record read
            # meanwhile must see the outcome of the one before it
            operation = await self._store.record_operation(envelope, now=self._clock.now())
        except Exception:
            log.exception("command not saved", extra=command_ids(envelope))
            # infrastructure, CRM retries; no ack: ack means "saved"
            unsaved = failure(ResultErrorCode.SOURCE_OFFLINE, "storage unavailable")
            await self._answer(envelope, unsaved, started)
            return
        if not acked:
            await self._publish(envelope, AckPayload())
        # TODO(contract 2.2): idempotency_conflict when a known key comes with another payload
        saved = _final_result(operation)
        if saved is not None:
            log.info("repeated command answered from the saved result", extra=command_ids(envelope))
            await self._answer(envelope, saved, started)
            return
        result = await self._run_handler(envelope, operation, received_at)
        try:
            await self._store.finish_operation(operation, result, now=self._clock.now())
        except Exception:
            # the outcome is real (a message may be out): report it as it is, never as a failure
            log.exception("result not saved, a repeated delivery will run again", extra=command_ids(envelope))
        await self._answer(envelope, result, started)

    async def _run_login_flow(self, envelope: CommandEnvelope) -> None:
        if self._login is None:
            log.error(
                "login flow commands are not handled: no login flow configured", extra=command_ids(envelope)
            )
            # an honest refusal now, not CRM's `expired` in five minutes
            await self._publish(envelope, _login_failed("logging in is not available"))
            return
        try:
            await self._login.handle(envelope)
        except Exception as exc:
            log.exception("login flow step failed", extra=command_ids(envelope))
            # a redelivery after this finds the flow mid-login and fails it: no second login
            await self._publish(envelope, _login_failed(f"unexpected {type(exc).__name__}"))

    async def _run_handler(
        self, envelope: CommandEnvelope, operation: Operation, received_at: datetime
    ) -> ResultPayload:
        try:
            command = command_from(envelope)
        except ContractViolationError as exc:
            # validation errors hide the input, so the text carries no message or secret
            log.error("invalid command", extra=command_ids(envelope))
            return failure(ResultErrorCode.INTERNAL_ERROR, str(exc))
        handler = self._handlers.get(command.type)
        if handler is None:
            return failure(ResultErrorCode.INTERNAL_ERROR, f"no handler for {command.type}")
        try:
            source = await self._sources.state(envelope.source_id)
            # an Operation found mid-send may have its message out: a retried code here would
            # let CRM's retry send it again, so its handler decides even for an inactive Source
            mid_send = operation.state is OperationState.SENDING
            if (
                handler.needs_active_source
                and not mid_send
                and source.condition is not SourceCondition.ACTIVE
            ):
                if source.condition is SourceCondition.DISABLED:
                    # switched off on purpose: a retry would never help
                    return failure(
                        ResultErrorCode.CHANNEL_REJECTED, "the account is connected as another source now"
                    )
                if source.condition is SourceCondition.LIMITED:
                    return failure(ResultErrorCode.PEER_FLOOD, "the account is restricted by Instagram")
                return failure(ResultErrorCode.SOURCE_OFFLINE, "the account has no working session")
            # CRM's timer runs from the first ack; only its own retry (after a retried code
            # was answered) starts a new one. A redelivery of an unfinished command does not
            timer_from = received_at if operation.state is OperationState.DONE else operation.received_at
            context = CommandContext(command, operation, source, self._clock, min(timer_from, received_at))
            return await handler.run(context)
        except Exception as exc:
            log.exception("handler failed", extra=command_ids(envelope))
            return _internal_error(exc)

    async def _publish(self, envelope: CommandEnvelope, payload: EventPayload) -> None:
        await self._bus.publish(reply_to(envelope, payload, occurred_at=self._clock.now()))

    async def _answer(self, envelope: CommandEnvelope, result: ResultPayload, started: datetime) -> None:
        await self._publish(envelope, result)
        elapsed = self._clock.now() - started
        log.info(
            "command answered",
            extra={
                **command_ids(envelope),
                "error_code": result.error_code if result.error_code is not None else "ok",
                "duration_ms": int(elapsed.total_seconds() * 1000),
            },
        )


def _final_result(operation: Operation) -> ResultPayload | None:
    """The saved result a new delivery must get as is, without a new attempt."""
    result = operation.result
    if operation.state is not OperationState.DONE or result is None:
        return None
    # a retryable failure comes back as the CRM's retry: it deserves a fresh attempt
    if not result.ok and result.error_code in RETRIED_BY_CRM:
        return None
    return result


def _internal_error(exc: Exception) -> ResultPayload:
    # the type only: exception messages may carry texts or secrets
    return failure(ResultErrorCode.INTERNAL_ERROR, f"unexpected {type(exc).__name__}")


def _login_failed(text: str) -> ConnectStatusPayload:
    return ConnectStatusPayload(state="failed", error_code=StatusErrorCode.CONNECT_FAILED, error_text=text)
