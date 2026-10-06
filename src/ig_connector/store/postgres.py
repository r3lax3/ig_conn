"""Postgres store (asyncpg, no ORM). SQL migrations in migrations/ are applied on open."""

import logging
from datetime import datetime
from importlib.resources import files
from importlib.resources.abc import Traversable
from typing import Any, Self
from uuid import UUID

import asyncpg
from cryptography.fernet import Fernet
from pydantic import SecretStr

from ig_connector.contract.envelope import CommandEnvelope
from ig_connector.contract.errors import StatusErrorCode
from ig_connector.contract.events import ResultPayload
from ig_connector.instagram import Account, Device, InboxPositions, SessionData, ThreadPosition
from ig_connector.proxy import ProxyRequirement
from ig_connector.store import (
    AccountMismatch,
    AccountTaken,
    FlowState,
    LoginFlow,
    Operation,
    OperationState,
    SavedSession,
    Source,
    SourceStatus,
)

log = logging.getLogger(__name__)

_MIGRATIONS = files("ig_connector.store").joinpath("migrations")


async def apply_migrations(db: Any, directory: Traversable = _MIGRATIONS) -> list[str]:
    """Apply every NNNN_*.sql not applied yet, in name order, each in its own transaction.

    Returns the names applied now. An applied file is never run again, so a schema change
    is always a new file.
    """
    await db.execute(
        "create table if not exists schema_migrations"
        " (name text primary key, applied_at timestamptz not null)"
    )
    applied = {row["name"] for row in await db.fetch("select name from schema_migrations")}
    new = []
    for name in sorted(item.name for item in directory.iterdir() if item.name.endswith(".sql")):
        if name in applied:
            continue
        async with db.transaction():
            await db.execute(directory.joinpath(name).read_text(encoding="utf-8"))
            await db.execute("insert into schema_migrations values ($1, now())", name)
        log.info("migration applied", extra={"migration": name})
        new.append(name)
    return new


class PostgresStore:
    def __init__(self, pool: Any, session_key: SecretStr) -> None:
        self._pool = pool
        # Sessions and devices are encrypted with it
        self._fernet = Fernet(session_key.get_secret_value())

    @classmethod
    async def open(cls, dsn: str, *, session_key: SecretStr) -> Self:
        """Connect and migrate. session_key is a Fernet key (Fernet.generate_key())."""
        Fernet(session_key.get_secret_value())  # a bad key fails before anything else
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        try:
            async with pool.acquire() as db:
                await apply_migrations(db)
        except BaseException:
            await pool.close()
            raise
        return cls(pool, session_key)

    async def close(self) -> None:
        await self._pool.close()

    async def ping(self) -> bool:
        return bool(await self._pool.fetchval("select true"))

    async def record_operation(self, envelope: CommandEnvelope, *, now: datetime) -> Operation:
        row = await self._pool.fetchrow(
            """
            with inserted as (
                insert into operations (operation_id, type, source_id, state, received_at, updated_at)
                values ($1, $2, $3, $4, $5, $5)
                on conflict (operation_id, type) do nothing
                returning *
            )
            select * from inserted
            union all
            select * from operations where operation_id = $1 and type = $2
            limit 1
            """,
            envelope.operation_id,
            envelope.type,
            envelope.source_id,
            OperationState.RECEIVED.value,
            now,
        )
        return _operation(row)

    async def finish_operation(
        self, operation: Operation, result: ResultPayload, *, now: datetime
    ) -> Operation:
        row = await self._pool.fetchrow(
            """
            update operations set state = $3, result = $4::jsonb, updated_at = $5
            where operation_id = $1 and type = $2
            returning *
            """,
            operation.operation_id,
            operation.type,
            OperationState.DONE.value,
            result.model_dump_json(exclude_none=True),
            now,
        )
        return _operation(row)

    async def start_sending(self, operation: Operation, client_context: str, *, now: datetime) -> Operation:
        row = await self._pool.fetchrow(
            """
            update operations set state = $3, client_context = $4, updated_at = $5
            where operation_id = $1 and type = $2
            returning *
            """,
            operation.operation_id,
            operation.type,
            OperationState.SENDING.value,
            client_context,
            now,
        )
        return _operation(row)

    async def start_flow(self, flow: LoginFlow) -> LoginFlow:
        row = await self._pool.fetchrow(
            """
            with inserted as (
                insert into login_flows (operation_id, source_id, channel_type, state, login,
                    proxy_country_code, proxy_network_type, started_at, expires_at, updated_at)
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $8)
                on conflict (operation_id) do nothing
                returning *
            )
            select * from inserted
            union all
            select * from login_flows where operation_id = $1
            limit 1
            """,
            flow.operation_id,
            flow.source_id,
            flow.channel_type,
            flow.state.value,
            flow.login,
            flow.proxy.country_code if flow.proxy else None,
            flow.proxy.network_type if flow.proxy else None,
            flow.started_at,
            flow.expires_at,
        )
        return _flow(row)

    async def flow(self, operation_id: UUID) -> LoginFlow | None:
        row = await self._pool.fetchrow("select * from login_flows where operation_id = $1", operation_id)
        return None if row is None else _flow(row)

    async def move_flow(
        self, operation_id: UUID, *, expected: FlowState, to: FlowState, now: datetime
    ) -> bool:
        moved = await self._pool.fetchval(
            """
            update login_flows set state = $3, updated_at = $4
            where operation_id = $1 and state = $2
            returning true
            """,
            operation_id,
            expected.value,
            to.value,
            now,
        )
        return bool(moved)

    async def connect_source(
        self,
        flow: LoginFlow,
        account: Account,
        session: SavedSession,
        *,
        now: datetime,
        take_from: Source | None = None,
    ) -> Source:
        if flow.proxy is None:
            raise ValueError("a Source is connected only through a proxy")
        try:
            return await self._connect_source(flow, flow.proxy, account, session, now, take_from)
        except asyncpg.UniqueViolationError as exc:
            if exc.constraint_name != "sources_account":
                raise
        holder = await self._pool.fetchval(
            """
            select source_id from sources
            where channel_type = $1 and external_account_id = $2 and disabled_at is null
            """,
            flow.channel_type,
            account.external_id,
        )
        raise AccountTaken(account.external_id, holder)

    async def _connect_source(
        self,
        flow: LoginFlow,
        proxy: ProxyRequirement,
        account: Account,
        session: SavedSession,
        now: datetime,
        take_from: Source | None,
    ) -> Source:
        async with self._pool.acquire() as db, db.transaction():
            if take_from is not None:
                await self._switch_off(db, take_from, now)
            row = await self._upsert_source(db, flow, proxy, account, now)
            if row is None:
                raise AccountMismatch(flow.source_id)
            await db.execute(
                """
                insert into sessions (source_id, session, device, updated_at) values ($1, $2, $3, $4)
                on conflict (source_id) do update set
                    session = excluded.session, device = excluded.device, updated_at = excluded.updated_at
                """,
                flow.source_id,
                self._fernet.encrypt(session.session.data),
                self._fernet.encrypt(session.device.data),
                now,
            )
            await db.execute(
                """
                insert into inbox_starts (source_id, since) values ($1, $2)
                on conflict (source_id) do update set since = excluded.since
                """,
                flow.source_id,
                now,
            )
            # catching up on what came while the Source was offline is stage 2
            await db.execute("delete from inbox_positions where source_id = $1", flow.source_id)
            await db.execute(
                "update login_flows set state = $2, updated_at = $3 where operation_id = $1",
                flow.operation_id,
                FlowState.DONE.value,
                now,
            )
        return _source(row)

    @staticmethod
    async def _switch_off(db: Any, holder: Source, now: datetime) -> None:
        """The Account's holder lets it go; AccountTaken if it is not as read any more."""
        status = holder.status
        moved = await db.fetchval(
            """
            update sources set disabled_at = $2, updated_at = $2
            where source_id = $1 and disabled_at is null
                and status is not distinct from $3 and status_code is not distinct from $4
            returning source_id
            """,
            holder.source_id,
            now,
            None if status is None else status.status,
            None if status is None or status.error_code is None else status.error_code.value,
        )
        if moved is None:
            raise AccountTaken(holder.account.external_id, holder.source_id)
        for table in ("sessions", "devices", "inbox_positions"):
            await db.execute(f"delete from {table} where source_id = $1", holder.source_id)  # noqa: S608

    @staticmethod
    async def _upsert_source(
        db: Any, flow: LoginFlow, proxy: ProxyRequirement, account: Account, now: datetime
    ) -> Any:
        """The Source's row, or None when the Source holds another Account (left as it is)."""
        return await db.fetchrow(
            """
            insert into sources (source_id, channel_type, external_account_id, username,
                full_name, proxy_country_code, proxy_network_type, login, created_at, updated_at)
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
            on conflict (source_id) do update set
                channel_type = excluded.channel_type,
                username = excluded.username,
                full_name = excluded.full_name,
                proxy_country_code = excluded.proxy_country_code,
                proxy_network_type = excluded.proxy_network_type,
                login = excluded.login,
                disabled_at = null,
                updated_at = excluded.updated_at
            where sources.external_account_id = excluded.external_account_id
            returning *
            """,
            flow.source_id,
            flow.channel_type,
            account.external_id,
            account.username,
            account.full_name,
            proxy.country_code,
            proxy.network_type,
            flow.login,
            now,
        )

    async def source(self, source_id: UUID) -> Source | None:
        row = await self._pool.fetchrow("select * from sources where source_id = $1", source_id)
        return None if row is None else _source(row)

    async def session(self, source_id: UUID) -> SavedSession | None:
        row = await self._pool.fetchrow(
            "select session, device from sessions where source_id = $1", source_id
        )
        if row is None:
            return None
        return SavedSession(
            session=SessionData(self._fernet.decrypt(row["session"])),
            device=Device(self._fernet.decrypt(row["device"])),
        )

    async def device(self, source_id: UUID) -> Device | None:
        row = await self._pool.fetchrow(
            """
            select coalesce(
                (select device from sessions where source_id = $1),
                (select device from devices where source_id = $1)
            ) as device
            """,
            source_id,
        )
        data = None if row is None else row["device"]
        return None if data is None else Device(self._fernet.decrypt(data))

    async def save_device(self, source_id: UUID, device: Device, *, now: datetime) -> None:
        await self._pool.execute(
            """
            insert into devices (source_id, device, updated_at) values ($1, $2, $3)
            on conflict (source_id) do update set device = excluded.device, updated_at = excluded.updated_at
            """,
            source_id,
            self._fernet.encrypt(device.data),
            now,
        )

    async def save_status(self, source_id: UUID, status: SourceStatus, *, now: datetime) -> None:
        await self._pool.execute(
            "update sources set status = $2, status_code = $3, updated_at = $4 where source_id = $1",
            source_id,
            status.status,
            None if status.error_code is None else status.error_code.value,
            now,
        )

    async def connected_sources(self) -> list[UUID]:
        rows = await self._pool.fetch(
            "select source_id from sources where disabled_at is null order by created_at"
        )
        return [row["source_id"] for row in rows]

    async def inbox_positions(self, source_id: UUID) -> InboxPositions | None:
        since = await self._pool.fetchval("select since from inbox_starts where source_id = $1", source_id)
        if since is None:
            return None
        rows = await self._pool.fetch(
            "select thread_id, message_id, sent_at from inbox_positions where source_id = $1", source_id
        )
        threads = {row["thread_id"]: ThreadPosition(row["message_id"], row["sent_at"]) for row in rows}
        return InboxPositions(since=since, threads=threads)

    async def move_inbox_position(
        self, source_id: UUID, thread_id: str, position: ThreadPosition, *, now: datetime
    ) -> None:
        await self._pool.execute(
            """
            insert into inbox_positions (source_id, thread_id, message_id, sent_at, updated_at)
            values ($1, $2, $3, $4, $5)
            on conflict (source_id, thread_id) do update set
                message_id = excluded.message_id, sent_at = excluded.sent_at,
                updated_at = excluded.updated_at
            """,
            source_id,
            thread_id,
            position.message_id,
            position.sent_at,
            now,
        )


def _flow(row: Any) -> LoginFlow:
    country = row["proxy_country_code"]
    return LoginFlow(
        operation_id=row["operation_id"],
        source_id=row["source_id"],
        channel_type=row["channel_type"],
        state=FlowState(row["state"]),
        login=row["login"],
        proxy=None if country is None else ProxyRequirement(country, row["proxy_network_type"]),
        started_at=row["started_at"],
        expires_at=row["expires_at"],
    )


def _source(row: Any) -> Source:
    status = row["status"]
    code = row["status_code"]
    return Source(
        source_id=row["source_id"],
        channel_type=row["channel_type"],
        account=Account(row["external_account_id"], row["username"], row["full_name"]),
        proxy=ProxyRequirement(row["proxy_country_code"], row["proxy_network_type"]),
        status=None
        if status is None
        else SourceStatus(status, None if code is None else StatusErrorCode(code)),
        login=row["login"],
        disabled=row["disabled_at"] is not None,
    )


def _operation(row: Any) -> Operation:
    result = row["result"]
    return Operation(
        operation_id=row["operation_id"],
        type=row["type"],
        source_id=row["source_id"],
        state=OperationState(row["state"]),
        result=None if result is None else ResultPayload.model_validate_json(result),
        received_at=row["received_at"],
        updated_at=row["updated_at"],
        client_context=row["client_context"],
    )
