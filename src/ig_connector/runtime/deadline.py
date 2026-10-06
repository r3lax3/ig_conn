"""CRM's timeouts (contract 9) run from the ack, sent on receipt: a handler's budget too.

Time a command spent queued behind others of its Source, or waiting for a free slot, is
already gone when its handler starts.
"""

from datetime import datetime

# the error_text when nothing was attempted: nothing went out, a retry is safe
DEADLINE_PASSED = "deadline passed before execution"


def time_left(budget: float, *, received_at: datetime, now: datetime) -> float:
    return budget - (now - received_at).total_seconds()
