"""Fake CRM for a test bus: publishes commands and prints events validated against the contract.

    uv run fake-crm listen
    uv run fake-crm send --source-id <uuid> --chat <external_chat_id> --text "hi"
    uv run fake-crm resolve --source-id <uuid> --kind username --value some_user
    uv run fake-crm connect --phone +491701234567
    uv run fake-crm confirm --operation-id <uuid> --code 123456

Needs a principal allowed to write commands (FAKE_CRM_KAFKA_*), which exists only on test stands.
"""

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import SecretStr
from pydantic_settings import SettingsConfigDict

from ig_connector.bus import kafka_auth
from ig_connector.contract.codec import ContractViolationError, parse_event
from ig_connector.contract.envelope import CONTRACT_VERSION, CommandType
from ig_connector.settings import Settings


class FakeCrmSettings(Settings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    fake_crm_kafka_username: str
    fake_crm_kafka_password: SecretStr

    def kafka(self) -> dict[str, Any]:
        return kafka_auth(
            self.kafka_bootstrap_servers, self.fake_crm_kafka_username, self.fake_crm_kafka_password
        )


def build_command(
    settings: Settings,
    command_type: CommandType,
    payload: dict[str, Any],
    *,
    source_id: UUID,
    operation_id: UUID | None = None,
) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "operation_id": str(operation_id or uuid4()),
        "source_id": str(source_id),
        "channel_type": settings.channel_type,
        "type": command_type.value,
        "payload": payload,
        "occurred_at": datetime.now(UTC).isoformat(),
    }


async def publish(settings: FakeCrmSettings, command: dict[str, Any]) -> None:
    producer = AIOKafkaProducer(**settings.kafka(), enable_idempotence=True)
    await producer.start()
    try:
        await producer.send_and_wait(
            settings.commands_topic,
            json.dumps(command, ensure_ascii=False).encode(),
            key=command["source_id"].encode(),
        )
    finally:
        await producer.stop()
    print(json.dumps(command, ensure_ascii=False, indent=2))


async def listen(settings: FakeCrmSettings) -> None:
    consumer = AIOKafkaConsumer(
        settings.events_topic, **settings.kafka(), group_id="fake-crm", auto_offset_reset="latest"
    )
    await consumer.start()
    try:
        async for record in consumer:
            try:
                envelope, payload = parse_event(record.value)
            except ContractViolationError as exc:
                print(
                    f"CONTRACT VIOLATION at offset {record.offset}: {exc}\n{record.value!r}", file=sys.stderr
                )
                continue
            print(f"{envelope.occurred_at:%H:%M:%S} {envelope.type} op={envelope.operation_id}", flush=True)
            print(f"  {payload.model_dump_json(exclude_none=True)}", flush=True)
    finally:
        await consumer.stop()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fake-crm", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="action", required=True)

    sub.add_parser("listen", help="print events from the connector")

    send = sub.add_parser("send", help="command.send")
    send.add_argument("--source-id", type=UUID, required=True)
    send.add_argument("--chat", required=True, help="external_chat_id")
    send.add_argument("--text", required=True)
    send.add_argument("--reply-to", default=None, help="reply_to_external_id")

    resolve = sub.add_parser("resolve", help="command.resolve_recipient")
    resolve.add_argument("--source-id", type=UUID, required=True)
    resolve.add_argument("--kind", choices=["phone", "username", "external_id"], required=True)
    resolve.add_argument("--value", required=True)

    connect = sub.add_parser("connect", help="command.connect.start (first connection)")
    connect.add_argument("--phone", required=True, help="E.164")
    connect.add_argument("--reconnect-source-id", type=UUID, default=None)

    confirm = sub.add_parser("confirm", help="command.connect.confirm")
    confirm.add_argument("--operation-id", type=UUID, required=True, help="operation_id of connect.start")
    secret = confirm.add_mutually_exclusive_group(required=True)
    secret.add_argument("--code")
    secret.add_argument("--password")
    return parser


def _command_from_args(settings: FakeCrmSettings, args: argparse.Namespace) -> dict[str, Any]:
    if args.action == "send":
        payload = {
            "message_id": str(uuid4()),
            "external_chat_id": args.chat,
            "text": args.text,
            "format": [],
            "attachments": [],
            "reply_to_external_id": args.reply_to,
        }
        return build_command(settings, CommandType.SEND, payload, source_id=args.source_id)
    if args.action == "resolve":
        payload = {"recipient_kind": args.kind, "value": args.value}
        return build_command(settings, CommandType.RESOLVE_RECIPIENT, payload, source_id=args.source_id)
    if args.action == "connect":
        # First connection: flow_id == source_id == operation_id (contract 5.5).
        flow_id = uuid4()
        payload = {
            "flow_id": str(flow_id),
            "role": "operator_channel",
            "method": "pairing_code",
            "phone": args.phone,
            "reconnect_source_id": str(args.reconnect_source_id) if args.reconnect_source_id else None,
            "proxy_country_code": None,
            "proxy_network_type": None,
        }
        source_id = args.reconnect_source_id or flow_id
        return build_command(
            settings, CommandType.CONNECT_START, payload, source_id=source_id, operation_id=flow_id
        )
    payload = {"flow_id": str(args.operation_id), "code": args.code, "password": args.password}
    return build_command(
        settings,
        CommandType.CONNECT_CONFIRM,
        payload,
        source_id=args.operation_id,
        operation_id=args.operation_id,
    )


def main() -> None:
    args = _parser().parse_args()
    settings = FakeCrmSettings()
    try:
        if args.action == "listen":
            asyncio.run(listen(settings))
        else:
            asyncio.run(publish(settings, _command_from_args(settings, args)))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
