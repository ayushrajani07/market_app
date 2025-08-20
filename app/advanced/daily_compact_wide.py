#!/usr/bin/env python3
from __future__ import annotations
import csv
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple

def read_daily_split(csv_path: Path) -> Dict[str, float]:
    """
    Returns {HH:MM: total_premium_float} for rows that have total_premium.
    """
    out: Dict[str, float] = {}
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            tot_raw = row.get("total_premium", "")
            if not tot_raw:
                continue
            try:
                tot = float(tot_raw)
            except Exception:
                continue
            ts = row.get("ts_ist")
            if not ts:
                continue
            hhmm = ts.split("T", 1)[1][:5]
            out[hhmm] = tot
    return out

def compact_wide_for_index_expiry(
    csv_root: Path, index: str, expiry: str, date_str: str, out_root: Path
) -> Path | None:
    idx_dir = csv_root / index / expiry
    if not idx_dir.exists():
        return None

    # Collect all strike dirs that have date_str.csv
    strikes = []
    for strike_dir in idx_dir.iterdir():
        if not strike_dir.is_dir():
            continue
        daily = strike_dir / f"{date_str}.csv"
        if daily.exists():
            strikes.append((strike_dir.name, daily))

    if not strikes:
        return None

    # Read all into wide rows keyed by HH:MM
    hhmm_map: Dict[str, Dict[str, float]] = {}
    all_cols: List[str] = []
    for strike, path in sorted(strikes):
        data = read_daily_split(path)
        all_cols.append(strike)
        for hhmm, val in data.items():
            hhmm_map.setdefault(hhmm, {})[strike] = val

    # Prepare output
    out_dir = out_root / index / expiry
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{date_str}_wide.csv"
    cols = ["time"] + all_cols

    # Write
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for hhmm in sorted(hhmm_map.keys()):
            row = [hhmm]
            for strike in all_cols:
                v = hhmm_map[hhmm].get(strike)
                row.append("" if v is None else f"{v:.6f}")
            w.writerow(row)

    return out_path
