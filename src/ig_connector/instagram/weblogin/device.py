"""The Source's device for the web login: browser profile plus aiograpi device settings.

Created once, on the first login, and reused on every later one. The browser part is
Playwright's storage state (Instagram's device cookies such as ig_did, mid, datr) with a
fixed user agent, viewport and locale; the app part is what aiograpi calls a device
(uuids, device_settings, user agent). The session cookie is never kept here: it belongs
to the Session, and an old one in the browser would skip the login it is meant to redo.
"""

import json
import logging
import secrets
from dataclasses import dataclass, field, replace
from typing import Any

from ig_connector.instagram import Device

__all__ = ["SESSION_COOKIES", "BrowserProfile", "WebDevice", "decode_device", "encode_device"]

log = logging.getLogger(__name__)

_VERSION = 1
# the browser's language: page recognition reads English texts
LOCALE = "en-US"
# common desktop sizes; one is picked per device and kept
_VIEWPORTS = ((1280, 800), (1366, 768), (1440, 900), (1536, 864), (1920, 1080))
SESSION_COOKIES = frozenset({"sessionid"})


@dataclass(frozen=True, slots=True)
class BrowserProfile:
    viewport_width: int
    viewport_height: int
    locale: str = LOCALE
    # None until the first launch fills it from the browser's version
    user_agent: str | None = None
    timezone_id: str | None = None
    # Playwright storage state: cookies and local storage per origin
    storage_state: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def new(cls) -> "BrowserProfile":
        width, height = secrets.choice(_VIEWPORTS)
        return cls(viewport_width=width, viewport_height=height)

    def with_state(self, storage_state: dict[str, Any]) -> "BrowserProfile":
        cookies = [c for c in storage_state.get("cookies", []) if c.get("name") not in SESSION_COOKIES]
        return replace(self, storage_state={**storage_state, "cookies": cookies})


@dataclass(frozen=True, slots=True)
class WebDevice:
    browser: BrowserProfile
    # aiograpi device settings; None until the first login_by_sessionid
    app: dict[str, Any] | None = field(default=None, repr=False)


def encode_device(device: WebDevice) -> Device:
    b = device.browser
    data = {
        "version": _VERSION,
        "browser": {
            "viewport": {"width": b.viewport_width, "height": b.viewport_height},
            "locale": b.locale,
            "user_agent": b.user_agent,
            "timezone_id": b.timezone_id,
            "storage_state": b.storage_state,
        },
        "app": device.app,
    }
    return Device(json.dumps(data, separators=(",", ":")).encode())


def decode_device(device: Device | None) -> WebDevice:
    """The stored device, or a new one for the first login (or one we cannot read)."""
    if device is None:
        return WebDevice(BrowserProfile.new())
    try:
        data = json.loads(device.data)
        if data.get("version") != _VERSION:
            raise ValueError("unknown device version")
        b = data["browser"]
        profile = BrowserProfile(
            viewport_width=int(b["viewport"]["width"]),
            viewport_height=int(b["viewport"]["height"]),
            locale=str(b["locale"]),
            user_agent=b["user_agent"],
            timezone_id=b["timezone_id"],
            storage_state=dict(b["storage_state"]),
        )
        app = data["app"]
        return WebDevice(profile, None if app is None else dict(app))
    except (ValueError, KeyError, TypeError, AttributeError):
        # a device from elsewhere (an older format, a fake): a new one beats no login at all
        log.warning("stored device unreadable, a new one is used")
        return WebDevice(BrowserProfile.new())
