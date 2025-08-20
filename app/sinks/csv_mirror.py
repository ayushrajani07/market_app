from __future__ import annotations
import csv
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Iterable

def _flatten(d: Dict[str, Any], parent: str = "", sep: str = ".") -> Dict[str, Any]:
    out = {}
    for k, v in d.items():
        key = f"{parent}{sep}{k}" if parent else str(k)
        if isinstance(v, dict):
            out.update(_flatten(v, key, sep=sep))
        else:
            out[key] = v
    return out

def _as_utc_iso(ts) -> str:
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    try:
        return ts.astimezone(timezone.utc).isoformat()
    except Exception:
        return str(ts)

def append_influx_mirror_row(
    base_dir: str,
    measurement: str,
    record: Dict[str, Any],
    extra_tags: Dict[str, Any] | None = None,
    ts_field: str = "timestamp"
):
    """
    Append a CSV row mirroring what we send to Influx for this measurement.
    - base_dir: root dir for CSV (e.g., data/influx_mirror)
    - measurement: e.g., "atm_option_quote" or "index_overview"
    - record: the dict used to write Influx (pre-mapped), may contain 'timestamp'
    - extra_tags: host/app/env/etc
    - ts_field: name of the datetime field in record; if absent, we derive written_at_utc
    """
    flat = _flatten(record)
    # Normalize timestamp
    written_at_utc = None
    ts_val = record.get(ts_field)
    written_at_utc = _as_utc_iso(ts_val)
    flat["written_at_utc"] = written_at_utc

    # Attach metadata
    if extra_tags:
        for k, v in extra_tags.items():
            flat[str(k)] = v
    flat["measurement"] = measurement

    # Stable column order (optional preface)
    preferred_order = [
        "written_at_utc", "measurement", "env", "app", "host",
        "timestamp", "_time", "index", "symbol", "side", "bucket",
        "expiry", "atm_strike", "strike", "strike_offset"
    ]
    # Build final header
    keys = list(flat.keys())
    # Move preferred to the front if present, keep the rest sorted
    front = [k for k in preferred_order if k in flat]
    rest = sorted([k for k in keys if k not in front])
    header = front + rest

    # Path by day
    day = written_at_utc[:10]  # YYYY-MM-DD
    out_dir = Path(base_dir) / measurement
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{day}.csv"

    # Append row
    file_exists = out_path.exists()
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        w.writerow({k: flat.get(k) for k in header})
