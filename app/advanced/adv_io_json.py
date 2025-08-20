#!/usr/bin/env python3
from __future__ import annotations
import json
import datetime as dt
from typing import Iterable, Dict, Tuple

# Target row format expected by the aggregator:
# {
#   "ts": ISO8601 string,
#   "index": "SENSEX",
#   "expiry_code": "this_week",
#   "strike_offset": "atm" | "atm_p1" | "atm_p2" | "atm_m1" | "atm_m2",
#   "ce_ltp": "289.3" or "",
#   "pe_ltp": "375.2" or ""
# }

def _minute_bucket(ts_iso: str) -> str:
    # Normalize to minute by trimming seconds and below
    try:
        from dateutil import parser
        t = parser.isoparse(ts_iso)
    except Exception:
        t = dt.datetime.fromisoformat(ts_iso)  # may raise
    # Return a normalized ISO with seconds/micros stripped, preserve original tz
    t2 = t.replace(second=0, microsecond=0)
    s = t2.isoformat()
    return s

def _derive_offset(atm_strike: int, strike: int, step: int = 100) -> str:
    # Derive atm buckets relative to atm_strike
    # For indices with 50 steps, set step=50 via environment if needed and pass down.
    # Offset count limited to +-2 → beyond maps to nearest p2/m2 bucket.
    diff = strike - atm_strike
    if step <= 0:
        step = 100
    k = round(diff / step)
    if k <= -2:
        return "atm_m2"
    if k == -1:
        return "atm_m1"
    if k == 0:
        return "atm"
    if k == 1:
        return "atm_p1"
    return "atm_p2"

def read_core_raw_json(path: str, default_index: str, default_expiry_code: str, step_hint: int = 100) -> Iterable[Dict[str, str]]:
    """
    Reads your JSON schema and yields aggregator-compatible rows.
    Example input record:
      {
        "timestamp": "2025-08-18T12:52:30+05:30",
        "index": "SENSEX",
        "bucket": "this_week",
        "side": "CALL",
        "expiry": "2025-08-19",
        "atm_strike": 81400,
        "last_price": 289.3,
        ...
      }
    Notes:
    - strike is not explicitly present in your example. If individual leg strike is missing,
      we fall back to "atm" offset. If later you include "strike" per record, we will derive offsets.
    - We merge CALL/PUT per (minute key, index, expiry_code, strike_offset).
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data if isinstance(data, list) else data.get("records") or data.get("data") or []
    # Key: (ts_minute, index, expiry_code, strike_offset)
    ce: Dict[Tuple[str, str, str, str], float] = {}
    pe: Dict[Tuple[str, str, str, str], float] = {}

    for rec in records:
        try:
            ts = rec.get("timestamp") or rec.get("ts")  # prefer "timestamp"
            if not ts:
                continue
            ts_min = _minute_bucket(ts)

            index = (rec.get("index") or default_index).strip()
            expiry_code = (rec.get("bucket") or rec.get("expiry_code") or default_expiry_code).strip()

            side = (rec.get("side") or "").upper()
            ltp = rec.get("last_price")
            if ltp is None:
                ltp = rec.get("ltp")
            if ltp is None:
                continue
            ltp = float(ltp)

            atm_strike = rec.get("atm_strike")
            strike = rec.get("strike")

            # Derive strike_offset if possible; else assume "atm"
            if strike is not None and atm_strike is not None:
                try:
                    so = _derive_offset(int(atm_strike), int(strike), int(step_hint))
                except Exception:
                    so = "atm"
            else:
                so = "atm"

            key = (ts_min, index, expiry_code, so)

            if side in {"CALL", "CE"}:
                ce[key] = ltp
            elif side in {"PUT", "PE"}:
                pe[key] = ltp
            else:
                # Unknown side; skip record
                continue
        except Exception:
            # Skip malformed
            continue

    # Merge and yield
    keys = sorted(set(ce.keys()) | set(pe.keys()))
    for k in keys:
        ts_min, idx, exp, so = k
        cval = ce.get(k)
        pval = pe.get(k)
        yield {
            "ts": ts_min,
            "index": idx,
            "expiry_code": exp,
            "strike_offset": so,
            "ce_ltp": "" if cval is None else f"{cval}",
            "pe_ltp": "" if pval is None else f"{pval}",
        }
