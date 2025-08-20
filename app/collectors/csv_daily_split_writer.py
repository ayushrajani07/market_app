#!/usr/bin/env python3
# app/collectors/csv_daily_split_writer.py
#
# Writes per-minute, per-index, per-expiry, per-offset CSV “daily split” files.
# - Path: {base_dir}/{index}/{expiry_code}/{strike_offset}/{YYYY-MM-DD}.csv
# - Header: preferred, readable columns first; all other fields appended alphabetically.
from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Dict

def ensure_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def daily_csv_path(base_dir: Path, index: str, expiry_code: str, strike_offset: str, date_str: str) -> Path:
    return base_dir / index / expiry_code / strike_offset / f"{date_str}.csv"

def append_rows(base_dir: str, index: str, expiry_code: str, strike_offset: str, date_str: str, rows: List[Dict]) -> None:
    """
    Append rows to the daily CSV at:
      {base_dir}/{index}/{expiry_code}/{strike_offset}/{date_str}.csv
    - Writes header if missing.
    - Stable column order: preferred first, rest alphabetical.
    """
    if not rows:
        return

    out_path = daily_csv_path(Path(base_dir), index, expiry_code, strike_offset, date_str)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    preferred_order = [
        "timestamp", "ts_ist",
        "index", "expiry_code", "strike_offset",
        "call_strike", "put_strike",
        "call_last_price", "put_last_price",
        "call_iv", "put_iv",
        "call_volume", "put_volume",
        "call_oi", "put_oi",
        "tp_sum", "total_premium",
        "group", "days_to_expiry", "atm_strike", "symbol",
    ]

    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())

    front = [k for k in preferred_order if k in all_keys]
    rest = sorted(k for k in all_keys if k not in front)
    fieldnames = front + rest

    file_exists = out_path.exists()
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
