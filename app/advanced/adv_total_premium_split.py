#!/usr/bin/env python3
from __future__ import annotations
import csv
import glob
import os
import datetime as dt
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

from .adv_config import AdvConfig
from .adv_io_json import read_core_raw_json

# Output CSV schema (per strike file)
# Columns are intentionally compact and stable for downstream use.
CSV_HEADER = [
    "ts_ist",          # ISO8601 IST at minute precision
    "index",
    "expiry_code",
    "strike_offset",   # atm, atm_p1, atm_m1, atm_p2, atm_m2
    "ce_ltp",          # blank if missing
    "pe_ltp",          # blank if missing
    "total_premium",   # ce_ltp + pe_ltp if both present, else blank
]

@dataclass(frozen=True)
class MinuteKey:
    ts_ist: str
    index: str
    expiry_code: str
    strike_offset: str

def _minute_iso_to_ist_minute(iso_ts: str, tz_name: str) -> str:
    """
    Given any ISO8601 timestamp string (tz-aware or naive UTC), return
    the equivalent IST minute boundary timestamp (seconds and micros zeroed),
    as ISO8601 with timezone.
    """
    try:
        from dateutil import parser
        t = parser.isoparse(iso_ts)
    except Exception:
        t = dt.datetime.fromisoformat(iso_ts)
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    try:
        import pytz
        ist = pytz.timezone(tz_name)
        t_ist = t.astimezone(ist)
    except Exception:
        # Fallback to fixed offset if pytz not available (less precise around DST in other zones)
        offset = dt.timedelta(hours=5, minutes=30)
        t_ist = (t.astimezone(dt.timezone.utc) + offset).replace(tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30)))
    t_ist = t_ist.replace(second=0, microsecond=0)
    return t_ist.isoformat()

def accumulate_per_minute(
    config: AdvConfig,
    index: str,
    expiry_code: str,
    json_files: List[str],
    step_hint: int = 100,
) -> Dict[MinuteKey, Tuple[float | None, float | None]]:
    """
    Merge CALL and PUT per minute per (index, expiry_code, strike_offset).
    Returns a dict keyed by MinuteKey with values (ce_ltp, pe_ltp).
    """
    merged: Dict[MinuteKey, Tuple[float | None, float | None]] = {}

    for path in json_files:
        rows = read_core_raw_json(
            path,
            default_index=index,
            default_expiry_code=expiry_code,
            step_hint=step_hint,
        )
        for r in rows:
            ts_ist = _minute_iso_to_ist_minute(r["ts"], config.MARKET_TIMEZONE)
            key = MinuteKey(
                ts_ist=ts_ist,
                index=r["index"],
                expiry_code=r["expiry_code"],
                strike_offset=r["strike_offset"],
            )
            ce = None if r["ce_ltp"] == "" else float(r["ce_ltp"])
            pe = None if r["pe_ltp"] == "" else float(r["pe_ltp"])

            prev = merged.get(key, (None, None))
            ce_out = ce if ce is not None else prev[0]
            pe_out = pe if pe is not None else prev[1]
            merged[key] = (ce_out, pe_out)

    return merged

def split_by_strike_offset(
    merged: Dict[MinuteKey, Tuple[float | None, float | None]]
) -> Dict[Tuple[str, str, str], List[Tuple[str, float | None, float | None]]]:
    """
    Group merged entries into buckets by (index, expiry_code, strike_offset).
    Returns:
      { (index, expiry_code, strike_offset): [ (ts_ist, ce, pe), ... ] }
    """
    out: Dict[Tuple[str, str, str], List[Tuple[str, float | None, float | None]]] = {}
    for key, (ce, pe) in merged.items():
        gk = (key.index, key.expiry_code, key.strike_offset)
        out.setdefault(gk, []).append((key.ts_ist, ce, pe))
    # Sort each group by ts_ist
    for gk in out:
        out[gk].sort(key=lambda x: x[0])
    return out

def timestamped_filename(now_utc: dt.datetime | None = None) -> str:
    """
    Build a timestamped filename component: YYYYMMDD_HHMMSS.csv
    Uses UTC now by default (stable across zones).
    """
    if now_utc is None:
        now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    return now_utc.strftime("%Y%m%d_%H%M%S") + ".csv"

def write_split_csvs(
    base_dir: str,
    groups: Dict[Tuple[str, str, str], List[Tuple[str, float | None, float | None]]],
    atomic: bool = True,
    filename_ts: str | None = None,
) -> List[str]:
    """
    Write one CSV per (index, expiry_code, strike_offset) under:
      base_dir/INDEX/EXPIRY/STRIKE/TIMESTAMP.csv

    Returns the list of written file paths.
    """
    written: List[str] = []
    if filename_ts is None:
        filename_ts = timestamped_filename()

    for (index, expiry_code, strike_offset), rows in groups.items():
        out_dir = os.path.join(base_dir, index, expiry_code, strike_offset)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, filename_ts)

        # Prepare rows for CSV
        csv_rows = []
        for ts_ist, ce, pe in rows:
            tot = None
            if ce is not None and pe is not None:
                tot = ce + pe
            csv_rows.append({
                "ts_ist": ts_ist,
                "index": index,
                "expiry_code": expiry_code,
                "strike_offset": strike_offset,
                "ce_ltp": "" if ce is None else f"{ce:.6f}",
                "pe_ltp": "" if pe is None else f"{pe:.6f}",
                "total_premium": "" if tot is None else f"{tot:.6f}",
            })

        if atomic:
            import tempfile
            fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=out_dir, suffix=".csv")
            os.close(fd)
            try:
                with open(tmp, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=CSV_HEADER)
                    w.writeheader()
                    for row in csv_rows:
                        w.writerow(row)
                os.replace(tmp, out_path)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
        else:
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=CSV_HEADER)
                w.writeheader()
                for row in csv_rows:
                    w.writerow(row)

        written.append(out_path)

    return written
