"""Kafka contract 2.1 (docs/kafka-contract-2.1.md) as pydantic models, and their codec.

The only place the wire format lives: past the codec everything works with these
models, never with raw JSON. A new message or field starts here, with its test.
"""
