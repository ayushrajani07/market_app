#!/usr/bin/env python3
import csv
import os
from typing import Dict

CSV_FIELDS = [
    "timestamp",           # ISO8601, timezone-aware
    "index",               # NIFTY/SENSEX/BANKNIFTY
    "bucket",              # this_week/next_week/this_month/next_month
    "expiry",              # YYYY-MM-DD
    "side",                # CALL/PUT
    "atm_strike",          # int
    "strike",              # optional if available later; blank for now
    "strike_offset",       # optional (atm, atm_p1, ...); blank for now
    "last_price",
    "average_price",
    "volume",
    "oi",
    "oi_open",
    "oi_change",
    "iv",
    "ohlc_open",
    "ohlc_high",
    "ohlc_low",
    "ohlc_close",
    "net_change",
    "net_change_percent",
    "day_change",
    "day_change_percent",
    "days_to_expiry",
]

def _ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def csv_path_for_json(json_path: str) -> str:
    base, _ = os.path.splitext(json_path)
    return base + ".csv"

def append_leg_csv(json_path: str, rec: Dict) -> None:
    """
    Append one option leg row to a sibling CSV next to its JSON.
    Safe to call once per JSON write; creates header if file is new.
    """
    csv_path = csv_path_for_json(json_path)
    _ensure_parent(csv_path)
    write_header = not os.path.exists(csv_path)

    row = {
        "timestamp": rec.get("timestamp", ""),
        "index": rec.get("index", ""),
        "bucket": rec.get("bucket", ""),
        "expiry": rec.get("expiry", ""),
        "side": rec.get("side", ""),
        "atm_strike": rec.get("atm_strike", ""),
        "strike": rec.get("strike", ""),                  # may be absent; blank ok
        "strike_offset": rec.get("strike_offset", ""),    # may be absent; blank ok
        "last_price": rec.get("last_price", ""),
        "average_price": rec.get("average_price", ""),
        "volume": rec.get("volume", ""),
        "oi": rec.get("oi", ""),
        "oi_open": rec.get("oi_open", ""),
        "oi_change": rec.get("oi_change", ""),
        "iv": rec.get("iv", ""),
        "ohlc_open": (rec.get("ohlc.open") or rec.get("ohlc_open") or ""),
        "ohlc_high": (rec.get("ohlc.high") or rec.get("ohlc_high") or ""),
        "ohlc_low": (rec.get("ohlc.low") or rec.get("ohlc_low") or ""),
        "ohlc_close": (rec.get("ohlc.close") or rec.get("ohlc_close") or ""),
        "net_change": rec.get("net_change", ""),
        "net_change_percent": rec.get("net_change_percent", ""),
        "day_change": rec.get("day_change", ""),
        "day_change_percent": rec.get("day_change_percent", ""),
        "days_to_expiry": rec.get("days_to_expiry", ""),
    }

    # Append
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            w.writeheader()
        w.writerow(row)
