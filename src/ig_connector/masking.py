"""Secret masking for anything that leaves the process as text: logs and test traces.

Second line of defence after SecretStr: it catches secrets that slipped into plain
strings, dicts from aiograpi, exception messages and raw command JSON.
"""

import dataclasses
import re
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, SecretBytes, SecretStr

MASK = "***"

# Matched against the key lowercased with everything but letters and digits removed, so
# "Kafka_Password", "proxy-password" and "proxyPassword" all hit "password".
_SECRET_KEY_PARTS = (
    "password",
    "passwd",
    "secret",
    "totp",
    "sessionid",
    "cookie",
    "authorization",
    "token",
    "proxyurl",
    "databaseurl",
    "dsn",
    "fernet",
)
# One-time 2FA code in connect.confirm; must not catch error_code or user_code.
_SECRET_KEYS_EXACT = frozenset({"code", "verificationcode", "otp"})

_TEXT_KEY_PARTS = (
    "password",
    "passwd",
    "secret",
    "totp",
    "sessionid",
    "cookie",
    "authorization",
    "access_token",
    "refresh_token",
    "auth_token",
)
_TEXT_KEYS_EXACT = ("code", "otp", "verification_code")
# greedy up to the last "@": proxy passwords may contain "/" and "@"; over-masking is fine
_URL_CREDENTIALS = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s:@\"']+:[^\s\"']*@")
_AUTH_SCHEME = re.compile(r"(?i)\b(?P<scheme>bearer|basic)\s+[A-Za-z0-9._~+/=-]+")
_KEY_VALUE = re.compile(
    r"(?i)(?P<key>\\?[\"']?(?<![\w-])(?:[\w-]*?(?:"
    + "|".join(_TEXT_KEY_PARTS)
    + r")[\w-]*|"
    + "|".join(_TEXT_KEYS_EXACT)
    + r")\\?[\"']?\s*[:=]\s*)"
    + r"(?:\\\"(?:[^\\]|\\[^\"])*?\\\"|\"[^\"]*\"|'[^']*'|[^\s,;&}]+)"
)


def _is_secret_key(key: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    return normalized in _SECRET_KEYS_EXACT or any(part in normalized for part in _SECRET_KEY_PARTS)


def mask_text(text: str) -> str:
    text = _URL_CREDENTIALS.sub(rf"\g<scheme>{MASK}@", text)
    text = _AUTH_SCHEME.sub(rf"\g<scheme> {MASK}", text)
    return _KEY_VALUE.sub(rf"\g<key>{MASK}", text)


def mask(value: Any) -> Any:
    """Return a masked copy of value: strings, bytes, mappings, sequences, models, dataclasses."""
    if isinstance(value, SecretStr | SecretBytes):
        return MASK
    if isinstance(value, str):
        return mask_text(value)
    if isinstance(value, bytes | bytearray):
        return mask_text(bytes(value).decode("utf-8", errors="replace"))
    if isinstance(value, BaseModel):
        return mask(value.model_dump(mode="python"))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return mask({field.name: getattr(value, field.name) for field in dataclasses.fields(value)})
    if isinstance(value, Mapping):
        return {
            key: MASK if isinstance(key, str) and _is_secret_key(key) else mask(item)
            for key, item in value.items()
        }
    if isinstance(value, list | tuple | set | frozenset):
        return [mask(item) for item in value]
    if value is None or isinstance(value, bool | int | float):
        return value
    # exceptions, UUIDs, aiograpi objects: whatever they print may carry a secret
    return mask_text(str(value))
