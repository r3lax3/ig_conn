"""Which Instagram web page the browser is on, from its URL and visible text.

Instagram's markup changes often, its wording and paths less so. The browser runs with
locale en-US, so the texts are English. Order matters: a wrong-code message shows on the
code page, a captcha on the login page.
"""

import re
from enum import StrEnum
from urllib.parse import urlsplit

__all__ = ["Page", "recognise"]


class Page(StrEnum):
    LOGIN_FORM = "login_form"
    # the 2FA code page for a code from an authentication app: the only one we can pass
    TWO_FACTOR_APP = "two_factor_app"
    # the 2FA code went by SMS, WhatsApp or email
    TWO_FACTOR_OTHER = "two_factor_other"
    WRONG_CREDENTIALS = "wrong_credentials"
    # checkpoint, captcha, suspension, email or SMS confirmation
    CHALLENGE = "challenge"
    RATE_LIMITED = "rate_limited"
    UNKNOWN = "unknown"


_CHALLENGE_PATH = re.compile(r"^/(challenge|checkpoint|auth_platform)\b|^/accounts/(suspended|disabled)\b")
_TWO_FACTOR_PATH = re.compile(r"^/(accounts/login/)?(two_step_verification|two_factor)\b")
_LOGIN_PATH = re.compile(r"^/(accounts/login/?)?$")

_CHALLENGE_TEXT = re.compile(
    r"confirm (that )?you.re (a )?human|i.m not a robot|recaptcha|unusual login attempt"
    r"|suspicious login attempt|help us confirm it.s you",
    re.I,
)
_RATE_LIMITED_TEXT = re.compile(r"wait a few minutes before you try again|too many (login )?attempts", re.I)
_WRONG_TEXT = re.compile(
    r"password was incorrect|incorrect password|code isn.t valid|check the (security )?code and try again"
    r"|doesn.t belong to an account|isn.t connected to an account|couldn.t find your account"
    r"|no account found|login information you entered is incorrect",
    re.I,
)
_APP_TEXT = re.compile(r"authentication app|code generator|authenticator", re.I)
# the code went elsewhere; wins over a mention of the app ("try another way: authentication app")
_OTHER_TEXT = re.compile(r"we sent|sent to your|text message|\bsms\b|whatsapp|check your email", re.I)
_CODE_TEXT = re.compile(r"6-digit|security code|login code|two-factor|enter (the )?code", re.I)
_LOGIN_TEXT = re.compile(r"forgot password", re.I)


def recognise(url: str, text: str) -> Page:
    path = urlsplit(url).path or "/"
    if _CHALLENGE_PATH.search(path) or _CHALLENGE_TEXT.search(text):
        return Page.CHALLENGE
    if _RATE_LIMITED_TEXT.search(text):
        return Page.RATE_LIMITED
    if _WRONG_TEXT.search(text):
        return Page.WRONG_CREDENTIALS
    if _TWO_FACTOR_PATH.search(path) or _CODE_TEXT.search(text):
        if _OTHER_TEXT.search(text):
            return Page.TWO_FACTOR_OTHER
        if _APP_TEXT.search(text):
            return Page.TWO_FACTOR_APP
        # a code page whose text has not rendered yet: look again
        return Page.UNKNOWN
    if _LOGIN_PATH.search(path) and _LOGIN_TEXT.search(text):
        return Page.LOGIN_FORM
    return Page.UNKNOWN
