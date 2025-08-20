#!/usr/bin/env python3
from __future__ import annotations
import datetime as dt
from typing import Tuple, Optional

def parse_hhmm(s: str) -> Tuple[int, int]:
    hh, mm = s.split(":")
    return int(hh), int(mm)

def now_in_tz(tz_name: str) -> dt.datetime:
    import pytz
    tz = pytz.timezone(tz_name)
    return dt.datetime.now(tz)

def is_in_session(now_ist: dt.datetime, start_hhmm: str, end_hhmm: str) -> bool:
    sh, sm = parse_hhmm(start_hhmm)
    eh, em = parse_hhmm(end_hhmm)
    t = now_ist.time()
    return (t >= dt.time(sh, sm)) and (t <= dt.time(eh, em))

def seconds_until_hhmm(now_ist: dt.datetime, target_hhmm: str) -> int:
    h, m = parse_hhmm(target_hhmm)
    target = now_ist.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= now_ist:
        # if already past, return 0
        return 0
    return int((target - now_ist).total_seconds())

def today_date_str(now_ist: dt.datetime, fmt: str) -> str:
    return now_ist.date().strftime(fmt)
