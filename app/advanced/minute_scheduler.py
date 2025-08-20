#!/usr/bin/env python3
from __future__ import annotations
import datetime as dt
import time
from typing import Callable, Optional

def wait_until_next_minute_boundary(tz_name: str) -> dt.datetime:
    """
    Sleeps until the next minute boundary in the given timezone, returns that boundary as tz-aware datetime.
    """
    import pytz
    tz = pytz.timezone(tz_name)
    now = dt.datetime.now(tz)
    boundary = now.replace(second=0, microsecond=0) + dt.timedelta(minutes=1)
    sleep_s = max(0.0, (boundary - now).total_seconds())
    time.sleep(sleep_s)
    return boundary

def run_minutely(
    tz_name: str,
    should_continue: Callable[[], bool],
    on_tick: Callable[[dt.datetime], None],
    align_immediately: bool = True,
) -> None:
    """
    Calls on_tick at each minute boundary in tz_name until should_continue() is False.
    on_tick receives the boundary datetime (tz-aware) for that minute.
    """
    if align_immediately:
        boundary = wait_until_next_minute_boundary(tz_name)
    else:
        boundary = dt.datetime.now().astimezone()
    while should_continue():
        try:
            on_tick(boundary)
        except Exception as e:
            print(f"[MINUTE-SCHED] on_tick error: {e}")
        boundary = wait_until_next_minute_boundary(tz_name)
