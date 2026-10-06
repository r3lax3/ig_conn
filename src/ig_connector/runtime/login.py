"""The Flow of login: connect.start reserves a proxy, connect.confirm logs in.

connect.* get no ack and no result: every step is answered with event.connect.status
on the flow's operation_id (contract 5.5). The flow is saved before every step that
matters, so a redelivered command (dedup by the flow and its state) never starts a
second login: a confirm finding its flow mid-login after a restart fails it instead.

A step is finished, and its offset may be committed, once its state is saved and its
event published: connect.start does not wait for connect.confirm (that arrives later in
the same Source queue).
"""

import logging
import re
from dataclasses import replace as _with
from datetime import timedelta
from uuid import UUID

from ig_connector.bus import Bus
from ig_connector.clock import Clock, within
from ig_connector.contract.codec import ContractViolationError, command_from, reply_to
from ig_connector.contract.commands import ConnectConfirmPayload, ConnectStartPayload
from ig_connector.contract.envelope import CommandEnvelope, CommandType
from ig_connector.contract.errors import StatusErrorCode
from ig_connector.contract.events import ConnectStatusPayload, StatusPayload
from ig_connector.instagram import (
    Account,
    Device,
    Failure,
    LoggedIn,
    LoginRequest,
    Platform,
    PlatformError,
    UnsupportedProxy,
)
from ig_connector.logs import command_ids
from ig_connector.masking import mask_text
from ig_connector.proxy import ProxyAssignment, ProxyProvider, ProxyRequirement, ProxyUnavailable
from ig_connector.runtime.refusals import NO_ANSWER, status_for
from ig_connector.runtime.statuses import Statuses
from ig_connector.store import (
    AccountMismatch,
    AccountTaken,
    FlowState,
    LoginFlow,
    SavedSession,
    Source,
    Store,
)

log = logging.getLogger(__name__)

# contract 5.5: CRM shows `expired` when a flow is not done or failed within 5 minutes
FLOW_LIFETIME = timedelta(minutes=5)
# a login must end, with done or failed, before CRM gives up on the flow
LOGIN_TIMEOUT = 240.0
LOGIN_MARGIN = 15.0
# a rejected Session is dropped quickly: the failed event waits for it
LOGOUT_TIMEOUT = 10.0

# stage 1 has no local forwarder for SOCKS5 proxies with credentials: http proxies wanted
PROXY_NOT_FOR_LOGIN = "proxy scheme not supported for login"

CHALLENGE_HINT = (
    "Instagram asks for a check the connector cannot pass (SMS, email, checkpoint or 2FA "
    "without a secret): pass it in the Instagram app, then connect again"
)


class _Failed(Exception):
    def __init__(self, code: StatusErrorCode, text: str) -> None:
        super().__init__(text)
        self.code = code
        self.text = text


class LoginFlows:
    def __init__(
        self,
        *,
        bus: Bus,
        store: Store,
        clock: Clock,
        platform: Platform,
        proxies: ProxyProvider,
        login_timeout: float = LOGIN_TIMEOUT,
        statuses: Statuses | None = None,
    ) -> None:
        self._bus = bus
        self._store = store
        self._clock = clock
        self._platform = platform
        self._proxies = proxies
        self._login_timeout = login_timeout
        # the runtime's own, so that a login makes the Source active for its commands too
        self._statuses = statuses or Statuses(bus=bus, store=store, clock=clock)

    async def handle(self, envelope: CommandEnvelope) -> None:
        if envelope.type == CommandType.CONNECT_START:
            await self._start(envelope)
        elif envelope.type == CommandType.CONNECT_CONFIRM:
            await self._confirm(envelope)
        else:
            raise ValueError(f"not a login flow command: {envelope.type}")

    async def _start(self, envelope: CommandEnvelope) -> None:
        saved = await self._store.flow(envelope.operation_id)
        if saved is not None:
            await self._repeated(envelope, saved)
            return
        # our receipt, not CRM's occurred_at (another clock); CRM's 5 minutes started a
        # little earlier, LOGIN_MARGIN covers that
        started_at = self._clock.now()
        flow = LoginFlow(
            operation_id=envelope.operation_id,
            source_id=envelope.source_id,
            channel_type=envelope.channel_type,
            state=FlowState.CONFIRMING,
            login=None,
            proxy=None,
            started_at=started_at,
            expires_at=started_at + FLOW_LIFETIME,
        )
        try:
            # TODO(contract 2.2): method=session, importing a ready Session, is not implemented
            payload = _payload(envelope, ConnectStartPayload)
            reconnect = payload.reconnect_source_id
            if reconnect is not None and reconnect != envelope.source_id:
                # contract 5.5: a reconnect comes under the Source's own source_id
                raise _Failed(StatusErrorCode.CONNECT_FAILED, "reconnect_source_id differs from source_id")
            login = payload.login or payload.phone or await self._known_login(envelope.source_id)
            if login is None:
                raise _Failed(StatusErrorCode.CONNECT_FAILED, "connect.start needs a login")
            flow = _with(flow, login=login)
            if payload.proxy_country_code is None:
                # the contract's "go direct": the connector never goes to Instagram without a proxy
                raise _Failed(StatusErrorCode.SOURCE_OFFLINE, "no proxy requirement, direct is not allowed")
            requirement = ProxyRequirement(payload.proxy_country_code, payload.proxy_network_type)
            flow = _with(flow, proxy=requirement)
            await self._reserve(flow)
        except _Failed as failed:
            await self._store.start_flow(_with(flow, state=FlowState.FAILED))
            await self._fail(envelope, failed)
            return
        saved = await self._store.start_flow(flow)
        if saved.state is FlowState.CONFIRMING:
            await self._publish(envelope, ConnectStatusPayload(state="confirming"))

    async def _known_login(self, source_id: UUID) -> str | None:
        # a reconnect may come without a login (contract 5.5): the one that worked last time
        source = await self._store.source(source_id)
        return None if source is None else source.login

    async def _repeated(self, envelope: CommandEnvelope, flow: LoginFlow) -> None:
        if flow.state is FlowState.CONFIRMING:
            # it may have been saved and not published before a crash; saying it twice is harmless
            await self._publish(envelope, ConnectStatusPayload(state="confirming"))
        else:
            log.info("repeated connect.start ignored", extra=command_ids(envelope))

    async def _confirm(self, envelope: CommandEnvelope) -> None:
        flow = await self._store.flow(envelope.operation_id)
        if flow is None or flow.source_id != envelope.source_id:
            log.warning("connect.confirm for an unknown flow ignored", extra=command_ids(envelope))
            return
        if flow.state is FlowState.LOGGING_IN:
            # we crashed mid-login: one attempt per confirm, CRM decides on another one
            await self._finish_failed(
                envelope,
                flow,
                FlowState.LOGGING_IN,
                _Failed(StatusErrorCode.CONNECT_FAILED, "login interrupted"),
            )
            return
        if flow.state is FlowState.DONE:
            await self._repeat_done(envelope, flow)
            return
        if flow.state is not FlowState.CONFIRMING:
            log.info("connect.confirm for a finished flow ignored", extra=command_ids(envelope))
            return
        if not await self._store.move_flow(
            flow.operation_id, expected=FlowState.CONFIRMING, to=FlowState.LOGGING_IN, now=self._clock.now()
        ):
            log.info("connect.confirm lost the race for its flow", extra=command_ids(envelope))
            return
        try:
            logged_in, proxy = await self._login(envelope, flow)
            await self._keep(envelope, flow, logged_in, proxy)
        except _Failed as failed:
            await self._finish_failed(envelope, flow, FlowState.LOGGING_IN, failed)
            return
        except Exception as exc:
            log.exception("login flow failed unexpectedly", extra=command_ids(envelope))
            unexpected = _Failed(StatusErrorCode.CONNECT_FAILED, f"unexpected {type(exc).__name__}")
            await self._finish_failed(envelope, flow, FlowState.LOGGING_IN, unexpected)
            return
        await self._announce(envelope, flow, logged_in.account)

    async def _keep(
        self, envelope: CommandEnvelope, flow: LoginFlow, logged_in: LoggedIn, proxy: ProxyAssignment
    ) -> None:
        """Save the new Session to its Source; a Session refused for its Account is logged out.

        Only a refusal is sure to have saved nothing: after any other error the Session
        may be the Source's live one already, so it is left alone.
        """
        session = SavedSession(logged_in.session, logged_in.device)
        try:
            try:
                await self._store.connect_source(flow, logged_in.account, session, now=self._clock.now())
            except AccountTaken as taken:
                holder = await self._dead_holder(taken)
                if holder is None:
                    raise
                await self._store.connect_source(
                    flow, logged_in.account, session, now=self._clock.now(), take_from=holder
                )
                await self._switched_off(envelope, holder)
        except AccountTaken:
            # one Account, one Source: we refuse ourselves, done would carry an
            # external_account_id CRM refuses too
            # TODO(contract 2.2): already_connected instead of connect_failed
            await self._logout(envelope, flow, logged_in, proxy)
            raise _Failed(
                StatusErrorCode.CONNECT_FAILED,
                "this Instagram account is already connected as another source",
            ) from None
        except AccountMismatch:
            await self._logout(envelope, flow, logged_in, proxy)
            raise _Failed(
                StatusErrorCode.CONNECT_FAILED,
                "logged into another Instagram account than the one this source belongs to",
            ) from None

    async def _dead_holder(self, taken: AccountTaken) -> Source | None:
        """The Source holding the Account if it cannot use it any more, else None.

        Dead: it needs a reconnect, or it is in error and this process has no live Session
        for it. A live holder keeps its Account.
        """
        if taken.holder is None:
            return None
        holder = await self._store.source(taken.holder)
        if holder is None or holder.status is None:
            return None
        if holder.status.status == "needs_reconnect":
            return holder
        if holder.status.status == "error" and not self._statuses.has_session(holder.source_id):
            return holder
        return None

    async def _switched_off(self, envelope: CommandEnvelope, holder: Source) -> None:
        ids = {**command_ids(envelope), "previous_source_id": str(holder.source_id)}
        log.info("account moved from a dead source", extra=ids)
        await self._platform.disconnect(holder.source_id)
        self._statuses.session_closed(holder.source_id)
        try:
            # contract 6.2: switched off deliberately
            await self._statuses.report(holder.source_id, StatusPayload(status="disabled"))
        except Exception:
            log.exception("disabled status not reported", extra=ids)

    async def _logout(
        self, envelope: CommandEnvelope, flow: LoginFlow, logged_in: LoggedIn, proxy: ProxyAssignment
    ) -> None:
        try:
            await within(
                self._clock,
                LOGOUT_TIMEOUT,
                self._platform.logout(flow.source_id, logged_in.session, proxy),
            )
        except PlatformError as exc:
            log.warning(
                "rejected session not logged out: %s (%s)",
                exc.failure,
                mask_text(exc.detail),
                extra=command_ids(envelope),
            )
        except TimeoutError:
            log.warning("rejected session not logged out: timed out", extra=command_ids(envelope))
        except Exception:
            log.exception("rejected session not logged out", extra=command_ids(envelope))
        else:
            log.info("rejected session logged out", extra=command_ids(envelope))

    async def _repeat_done(self, envelope: CommandEnvelope, flow: LoginFlow) -> None:
        # the first done may have been lost with the process before the offset was committed;
        # a repeated done for the same flow is harmless, a lost one costs a new login
        source = await self._store.source(flow.source_id)
        if source is None:
            log.error("done flow without its Source", extra=command_ids(envelope))
            return
        log.info("repeated connect.confirm answered with the saved done", extra=command_ids(envelope))
        # after a restart the Session is the restore's to confirm: `active` only if it did
        await self._announce(
            envelope, flow, source.account, active=self._statuses.has_session(flow.source_id)
        )

    async def _announce(
        self, envelope: CommandEnvelope, flow: LoginFlow, account: Account, *, active: bool = True
    ) -> None:
        await self._publish(
            envelope,
            ConnectStatusPayload(
                state="done",
                external_account_id=account.external_id,
                external_account_name=account.username,
                account_username=account.username,
                account_phone=flow.login if _is_phone(flow.login) else None,
            ),
        )
        if not active:
            return
        try:
            await self._statuses.report(flow.source_id, StatusPayload(status="active"))
        except Exception:
            # done is out and stands: never follow it with failed. The restore after the
            # next restart confirms the Session and reports the status
            log.exception("active status not reported after done", extra=command_ids(envelope))

    async def _login(self, envelope: CommandEnvelope, flow: LoginFlow) -> tuple[LoggedIn, ProxyAssignment]:
        # TODO(contract 2.2): challenges via state=challenge (password, then totp/code/manual)
        payload = _payload(envelope, ConnectConfirmPayload)
        if payload.password is None:
            raise _Failed(StatusErrorCode.CONNECT_FAILED, "Instagram login needs password and totp_secret")
        if flow.login is None:
            raise _Failed(StatusErrorCode.CONNECT_FAILED, "the flow has no login")
        left = (flow.expires_at - self._clock.now()).total_seconds() - LOGIN_MARGIN
        timeout = min(self._login_timeout, left)
        if timeout <= 0:
            raise _Failed(StatusErrorCode.CONNECT_FAILED, "login flow expired")
        proxy = await self._reserve(flow)
        request = LoginRequest(
            source_id=flow.source_id,
            login=flow.login,
            password=payload.password,
            totp_secret=payload.totp_secret,
            proxy=proxy,
            device=await self._device(flow.source_id),
        )
        try:
            return await within(self._clock, timeout, self._platform.login(request)), proxy
        except TimeoutError:
            raise _Failed(StatusErrorCode.CONNECT_FAILED, "login timed out") from None
        except UnsupportedProxy as exc:
            log.info("login refused: %s", mask_text(exc.detail), extra=command_ids(envelope))
            raise _Failed(StatusErrorCode.CONNECT_FAILED, PROXY_NOT_FOR_LOGIN) from None
        except PlatformError as exc:
            # the adapter's detail may quote Instagram's answer: logs only, masked
            log.info(
                "login refused: %s (%s)", exc.failure, mask_text(exc.detail), extra=command_ids(envelope)
            )
            raise _refusal(exc) from None

    async def _device(self, source_id: UUID) -> Device:
        """The Source's "phone": a new one on every attempt provokes checks.

        Made and saved before the first attempt, so a failed attempt does not lose it.
        """
        device = await self._store.device(source_id)
        if device is None:
            device = self._platform.new_device()
            await self._store.save_device(source_id, device, now=self._clock.now())
        return device

    async def _reserve(self, flow: LoginFlow) -> ProxyAssignment:
        if flow.proxy is None:
            raise _Failed(StatusErrorCode.SOURCE_OFFLINE, "no proxy requirement")
        try:
            return await self._proxies.reserve(flow.source_id, flow.proxy)
        except ProxyUnavailable as exc:
            log.info(
                "no proxy for the flow: %s", mask_text(str(exc)), extra={"source_id": str(flow.source_id)}
            )
            raise _Failed(StatusErrorCode.SOURCE_OFFLINE, "no proxy available for the account") from None

    async def _finish_failed(
        self, envelope: CommandEnvelope, flow: LoginFlow, expected: FlowState, failed: _Failed
    ) -> None:
        await self._store.move_flow(
            flow.operation_id, expected=expected, to=FlowState.FAILED, now=self._clock.now()
        )
        await self._fail(envelope, failed)

    async def _fail(self, envelope: CommandEnvelope, failed: _Failed) -> None:
        log.info("login flow failed: %s", failed.code, extra=command_ids(envelope))
        await self._publish(
            envelope, ConnectStatusPayload(state="failed", error_code=failed.code, error_text=failed.text)
        )

    async def _publish(self, envelope: CommandEnvelope, payload: ConnectStatusPayload) -> None:
        await self._bus.publish(reply_to(envelope, payload, occurred_at=self._clock.now()))


def _is_phone(login: str | None) -> bool:
    # E.164, as CRM sends `phone`; an Instagram username or email never looks like it
    return login is not None and _E164.fullmatch(login) is not None


_E164 = re.compile(r"\+[1-9]\d{6,14}")


def _refusal(exc: PlatformError) -> _Failed:
    # fixed texts: error_text goes to CRM, the adapter's detail only to our logs
    match exc.failure:
        case Failure.BAD_CREDENTIALS:
            return _Failed(StatusErrorCode.VERIFY_FAILED, "wrong login, password or 2FA code")
        case Failure.CHALLENGE:
            return _Failed(StatusErrorCode.CONNECT_FAILED, CHALLENGE_HINT)
        case Failure.NO_ANSWER:
            return _Failed(StatusErrorCode.SOURCE_OFFLINE, NO_ANSWER)
    # a failure that puts a Source in `error` (flood, network) fails its login the same way
    status = status_for(exc.failure)
    if status is not None and status.status == "error" and status.error_code is not None:
        return _Failed(status.error_code, status.error_text or str(status.error_code))
    return _Failed(StatusErrorCode.CONNECT_FAILED, f"Instagram refused the login ({exc.failure})")


def _payload[P: (ConnectStartPayload, ConnectConfirmPayload)](envelope: CommandEnvelope, kind: type[P]) -> P:
    try:
        payload = command_from(envelope).payload
    except ContractViolationError:
        # validation errors hide the input, but the password is not worth the risk: type only
        raise _Failed(StatusErrorCode.CONNECT_FAILED, f"invalid {envelope.type}") from None
    if not isinstance(payload, kind):
        raise _Failed(StatusErrorCode.CONNECT_FAILED, f"invalid {envelope.type}")
    return payload
