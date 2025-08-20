#!/usr/bin/env python3
from __future__ import annotations
import csv
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional

# Expected columns (adjust constants if your dump uses different names)
COL_TOKEN = "instrument_token"
COL_SYMBOL = "tradingsymbol"
COL_EXPIRY = "expiry"           # "YYYY-MM-DD"
COL_STRIKE = "strike"
COL_TYPE = "instrument_type"    # "CE" or "PE"
COL_NAME = "name"               # index name, e.g., "NIFTY", "BANKNIFTY", "SENSEX"

@dataclass(frozen=True)
class LegInfo:
    ce_token: Optional[int]
    pe_token: Optional[int]
    strike: int

@dataclass
class InstrumentCache:
    # index -> expiry_date -> strike -> { "CE": token, "PE": token }
    index_expiry_strike: Dict[str, Dict[str, Dict[int, Dict[str, int]]]]

def parse_instruments_dump(csv_path: str) -> InstrumentCache:
    out: Dict[str, Dict[str, Dict[int, Dict[str, int]]]] = {}
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                name = row[COL_NAME].strip().upper()
                itype = row[COL_TYPE].strip().upper()
                if itype not in {"CE", "PE"}:
                    continue
                expiry = (row.get(COL_EXPIRY) or "").strip()
                strike = int(float(row[COL_STRIKE]))
                token = int(row[COL_TOKEN])
            except Exception:
                continue
            d1 = out.setdefault(name, {})
            d2 = d1.setdefault(expiry, {})
            d3 = d2.setdefault(strike, {})
            d3[itype] = token
    return InstrumentCache(index_expiry_strike=out)

def list_available_expiries(cache: InstrumentCache, index: str) -> List[dt.date]:
    dates: List[dt.date] = []
    m = cache.index_expiry_strike.get(index.upper(), {})
    for exp_str in m.keys():
        try:
            y, mm, dd = exp_str.split("-")
            dates.append(dt.date(int(y), int(mm), int(dd)))
        except Exception:
            continue
    dates.sort()
    return dates

def compute_expiry_buckets(today: dt.date, available_expiries: List[dt.date]) -> Dict[str, Optional[dt.date]]:
    # Buckets resolve based on available expiries (no Flux)
    fut = [e for e in available_expiries if e >= today]
    this_week = fut[0] if len(fut) >= 1 else None
    next_week = fut[1] if len(fut) >= 2 else None

    this_month = None
    next_month = None
    for e in fut:
        if this_month is None and e.month == today.month and e.year == today.year:
            this_month = e
        if (e.month > today.month and e.year == today.year) or (e.year > today.year):
            next_month = e
            break

    return {
        "this_week": this_week,
        "next_week": next_week,
        "this_month": this_month,
        "next_month": next_month,
    }

def get_leg_info_for_offset(cache: InstrumentCache, index: str, expiry: dt.date, atm_strike: int, offset: int, step: int) -> LegInfo:
    strike = atm_strike + offset * step
    ekey = expiry.isoformat()
    entry = cache.index_expiry_strike.get(index.upper(), {}).get(ekey, {}).get(strike, {})
    ce = entry.get("CE")
    pe = entry.get("PE")
    return LegInfo(ce_token=ce, pe_token=pe, strike=strike)

def validate_bucket_legs(cache: InstrumentCache, index: str, expiry: dt.date, atm_strike: int, step: int) -> Dict[int, bool]:
    # Offsets: -2,-1,0,+1,+2
    res: Dict[int, bool] = {}
    for off in (-2, -1, 0, 1, 2):
        li = get_leg_info_for_offset(cache, index, expiry, atm_strike, off, step)
        res[off] = (li.ce_token is not None and li.pe_token is not None)
    return res
