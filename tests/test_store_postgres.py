"""Postgres store adapter on the local stand (skipped without Postgres)."""

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from pydantic import SecretStr

from ig_connector.contract.envelope import CONTRACT_VERSION, CommandEnvelope, CommandType
from ig_connector.contract.errors import ResultErrorCode, StatusErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Account, Device, InboxPositions, SessionData, ThreadPosition
from ig_connector.proxy import ProxyRequirement
from ig_connector.store import FlowState, LoginFlow, OperationState, SavedSession, SourceStatus
from ig_connector.store.postgres import PostgresStore, apply_migrations
from tests.support.postgres import SESSION_KEY

NOW = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


def _envelope(operation_id: Any = None, command_type: CommandType = CommandType.SEND) -> CommandEnvelope:
    return CommandEnvelope(
        contract_version=CONTRACT_VERSION,
        operation_id=operation_id or uuid4(),
        source_id=uuid4(),
        channel_type="individual_instagram_account",
        type=command_type.value,
        payload={},
        occurred_at=NOW,
    )


@pytest.fixture
async def store(postgres_dsn: str) -> AsyncIterator[PostgresStore]:
    store = await PostgresStore.open(postgres_dsn, session_key=SESSION_KEY)
    try:
        yield store
    finally:
        await store.close()


async def test_operation_is_saved_once_and_returned_on_repeat(store: PostgresStore) -> None:
    envelope = _envelope()
    first = await store.record_operation(envelope, now=NOW)
    result = ResultPayload(ok=False, error_code=ResultErrorCode.SOURCE_OFFLINE, error_text="offline")
    await store.finish_operation(first, result, now=NOW)

    again = await store.record_operation(envelope, now=datetime(2026, 10, 7, tzinfo=UTC))

    assert first.state is OperationState.RECEIVED
    assert (again.state, again.result, again.received_at) == (OperationState.DONE, result, NOW)


async def test_same_operation_id_with_another_type_is_another_operation(store: PostgresStore) -> None:
    operation_id = uuid4()
    start = await store.record_operation(_envelope(operation_id, CommandType.CONNECT_START), now=NOW)
    await store.finish_operation(start, ResultPayload(ok=True), now=NOW)

    confirm = await store.record_operation(_envelope(operation_id, CommandType.CONNECT_CONFIRM), now=NOW)

    assert confirm.state is OperationState.RECEIVED


async def test_migrations_run_once_and_new_files_apply_later(postgres_dsn: str, tmp_path: Path) -> None:
    (tmp_path / "0001_a.sql").write_text("create table a (id int);")
    db = await asyncpg.connect(postgres_dsn)
    try:
        assert await apply_migrations(db, tmp_path) == ["0001_a.sql"]
        assert await apply_migrations(db, tmp_path) == []

        (tmp_path / "0002_b.sql").write_text("alter table a add column b text;")
        assert await apply_migrations(db, tmp_path) == ["0002_b.sql"]
        await db.execute("insert into a (id, b) values (1, 'x')")
    finally:
        await db.close()


def _flow(**changes: Any) -> LoginFlow:
    flow = LoginFlow(
        operation_id=uuid4(),
        source_id=uuid4(),
        channel_type="individual_instagram_account",
        state=FlowState.CONFIRMING,
        login="anna.shop",
        proxy=ProxyRequirement("DE", "residential"),
        started_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )
    return replace(flow, **changes)


async def test_flow_is_saved_once_and_moves_only_from_the_expected_state(store: PostgresStore) -> None:
    flow = _flow()
    assert await store.start_flow(flow) == flow
    assert await store.start_flow(replace(flow, login="other")) == flow

    assert await store.move_flow(
        flow.operation_id, expected=FlowState.CONFIRMING, to=FlowState.LOGGING_IN, now=NOW
    )
    assert not await store.move_flow(
        flow.operation_id, expected=FlowState.CONFIRMING, to=FlowState.LOGGING_IN, now=NOW
    )
    saved = await store.flow(flow.operation_id)
    assert saved is not None
    assert saved.state is FlowState.LOGGING_IN
    assert await store.flow(uuid4()) is None


async def test_connected_source_keeps_its_session_encrypted(store: PostgresStore, postgres_dsn: str) -> None:
    flow = await store.start_flow(_flow(state=FlowState.LOGGING_IN))
    account = Account("1789", "anna.shop", "Anna")
    saved = SavedSession(SessionData(b"sessionid-plain-123"), Device(b"device-plain-456"))

    source = await store.connect_source(flow, account, saved, now=NOW)

    assert (source.account, source.proxy, source.status) == (account, flow.proxy, None)
    assert await store.session(flow.source_id) == saved
    done = await store.flow(flow.operation_id)
    assert done is not None
    assert done.state is FlowState.DONE
    db = await asyncpg.connect(postgres_dsn)
    try:
        row = await db.fetchrow("select session, device from sessions")
    finally:
        await db.close()
    assert b"sessionid-plain" not in row["session"]
    assert b"device-plain" not in row["device"]


async def test_reconnect_replaces_the_session_and_keeps_the_status(store: PostgresStore) -> None:
    first = await store.start_flow(_flow(state=FlowState.LOGGING_IN))
    account = Account("1789", "anna.shop")
    await store.connect_source(first, account, SavedSession(SessionData(b"s1"), Device(b"d1")), now=NOW)
    await store.save_status(first.source_id, SourceStatus("active"), now=NOW)

    again = await store.start_flow(_flow(source_id=first.source_id, state=FlowState.LOGGING_IN))
    renamed = Account("1789", "anna.new")
    source = await store.connect_source(
        again, renamed, SavedSession(SessionData(b"s2"), Device(b"d1")), now=NOW
    )

    assert source.account == renamed
    assert source.status == SourceStatus("active")
    assert await store.session(first.source_id) == SavedSession(SessionData(b"s2"), Device(b"d1"))


async def test_status_is_kept_with_its_code(store: PostgresStore) -> None:
    flow = await store.start_flow(_flow(state=FlowState.LOGGING_IN))
    await store.connect_source(
        flow, Account("1", "a"), SavedSession(SessionData(b"s"), Device(b"d")), now=NOW
    )

    await store.save_status(flow.source_id, SourceStatus("error", StatusErrorCode.PEER_FLOOD), now=NOW)

    source = await store.source(flow.source_id)
    assert source is not None
    assert source.status == SourceStatus("error", StatusErrorCode.PEER_FLOOD)
    assert await store.source(uuid4()) is None
    assert await store.session(uuid4()) is None


async def test_store_refuses_a_bad_session_key(postgres_dsn: str) -> None:
    with pytest.raises(ValueError):
        await PostgresStore.open(postgres_dsn, session_key=SecretStr("not a fernet key"))


async def test_inbox_starts_afresh_at_each_connect(store: PostgresStore) -> None:
    first = await store.start_flow(_flow(state=FlowState.LOGGING_IN))
    source_id = first.source_id
    session = SavedSession(SessionData(b"s1"), Device(b"d1"))
    assert await store.inbox_positions(source_id) is None

    await store.connect_source(first, Account("1789", "anna.shop"), session, now=NOW)
    sent_at = NOW + timedelta(seconds=5)
    await store.move_inbox_position(source_id, "t1", ThreadPosition("101", sent_at), now=NOW)
    await store.move_inbox_position(source_id, "t1", ThreadPosition("102", sent_at), now=NOW)
    assert await store.inbox_positions(source_id) == InboxPositions(
        since=NOW, threads={"t1": ThreadPosition("102", sent_at)}
    )
    later = NOW + timedelta(days=1)
    again = await store.start_flow(_flow(source_id=source_id, state=FlowState.LOGGING_IN))
    await store.connect_source(again, Account("1789", "anna.shop"), session, now=later)

    assert await store.inbox_positions(source_id) == InboxPositions(since=later, threads={})
    assert await store.connected_sources() == [source_id]
