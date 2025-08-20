# app/sinks/influx_sink.py
# Updated to:
# - Keep strict typing for Influx writes.
# - Mirror every successfully written record to CSV so CSV always matches what went to Influx.
# - Organize atm_option_quote CSV mirror under: {INDEX}/{EXPIRY_BUCKET}/{STRIKE_OFFSET}/{SIDE}/{YYYY-MM-DD}.csv
# - Organize index_overview CSV mirror under: {INDEX}/{YYYY-MM-DD}.csv
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import csv
import os
import socket
from typing import Dict, Any, Callable

from influxdb_client import Point, WritePrecision

# ---------- helpers ----------

def _ffloat(val):
    return float(val) if isinstance(val, (int, float)) else None

def _fint(val):
    return int(val) if isinstance(val, (int, float)) else None

def _as_utc_iso(ts) -> str:
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    try:
        return ts.astimezone(timezone.utc).isoformat()
    except Exception:
        return str(ts)

def _flatten(d: Dict[str, Any], parent: str = "", sep: str = ".") -> Dict[str, Any]:
    out = {}
    for k, v in d.items():
        key = f"{parent}{sep}{k}" if parent else str(k)
        if isinstance(v, dict):
            out.update(_flatten(v, key, sep=sep))
        else:
            out[key] = v
    return out

def _append_influx_mirror_row(
    base_dir: str,
    measurement: str,
    record: Dict[str, Any],
    extra_tags: Dict[str, Any] | None = None,
    ts_field: str = "timestamp",
    path_builder: Callable[[Dict[str, Any]], list] | None = None,
):
    """
    Append a CSV row mirroring what we sent to Influx for this measurement.
    - base_dir: root dir for CSV mirror (e.g., data/influx_mirror)
    - measurement: e.g., "atm_option_quote" or "index_overview"
    - record: the original dict used to map/write to Influx (may include 'timestamp')
    - extra_tags: env/app/host or any other metadata to append as columns
    - path_builder: returns a list of subfolders to nest under {base}/{measurement}/...
    """
    flat = _flatten(record)

    # Normalize write timestamp column for the CSV row
    ts_val = record.get(ts_field)
    written_at_utc = _as_utc_iso(ts_val)
    flat["written_at_utc"] = written_at_utc
    flat["measurement"] = measurement

    if extra_tags:
        for k, v in extra_tags.items():
            flat[str(k)] = v

    # Preferred header order first, then the rest sorted alphabetically
    preferred_order = [
        "written_at_utc", "measurement", "env", "app", "host",
        "timestamp", "_time",
        "index", "symbol",
        "bucket", "expiry", "expiry_code",
        "strike_offset", "atm_strike", "strike", "option_type", "side",
        "days_to_expiry",
        "last_price", "average_price",
        "ohlc.open", "ohlc.high", "ohlc.low", "ohlc.close",
        "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close",
        "net_change", "net_change_percent", "day_change", "day_change_percent",
        "iv", "volume", "oi", "oi_open", "oi_change",
        "atm_strike_val",
        # overview aggregates
        "THIS_WEEK_TP", "NEXT_WEEK_TP", "THIS_MONTH_TP", "NEXT_MONTH_TP",
        "THIS_WEEK_OI_CALL", "THIS_WEEK_OI_PUT",
        "NEXT_WEEK_OI_CALL", "NEXT_WEEK_OI_PUT",
        "THIS_MONTH_OI_CALL", "THIS_MONTH_OI_PUT",
        "NEXT_MONTH_OI_CALL", "NEXT_MONTH_OI_PUT",
        "pcr_this_week", "pcr_next_week", "pcr_this_month", "pcr_next_month",
        "this_week_iv_open", "this_week_iv_day_change", "this_week_atm_iv", "this_week_days_to_expiry",
        "next_week_iv_open", "next_week_iv_day_change", "next_week_atm_iv", "next_week_days_to_expiry",
        "this_month_iv_open", "this_month_iv_day_change", "this_month_atm_iv", "this_month_days_to_expiry",
        "next_month_iv_open", "next_month_iv_day_change", "next_month_atm_iv", "next_month_days_to_expiry",
    ]
    keys = set(flat.keys())
    front = [k for k in preferred_order if k in keys]
    rest = sorted([k for k in keys if k not in front])
    header = front + rest

    # Build nested output path parts
    day = written_at_utc[:10]  # YYYY-MM-DD
    parts = [measurement]
    if path_builder is not None:
        try:
            subparts = path_builder(record) or []
            parts.extend([str(p).replace("/", "_") for p in subparts])
        except Exception:
            pass

    out_dir = Path(base_dir)
    for p in parts:
        out_dir = out_dir / p
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{day}.csv"

    file_exists = out_path.exists()
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        w.writerow({k: flat.get(k) for k in header})

# ---------- Influx writers with CSV mirroring ----------

def write_atm_leg(rec: dict, writer):
    """
    Write ATM option leg to InfluxDB with strict types.
    Also mirrors the successfully written record to CSV, into:
      data/influx_mirror/atm_option_quote/{INDEX}/{EXPIRY_BUCKET}/{STRIKE_OFFSET}/{SIDE}/{YYYY-MM-DD}.csv
    """
    try:
        ts = datetime.fromisoformat(rec["timestamp"])

        tags = {
            "index": rec.get("index"),
            "option_type": "CE" if rec.get("side") == "CALL" else "PE",
            "expiration": rec.get("expiry"),
            "strike": str(rec.get("atm_strike")),
            "bucket": rec.get("bucket"),
        }

        fields = {
            "last_price": _ffloat(rec.get("last_price")),
            "average_price": _ffloat(rec.get("average_price")),
            "ohlc_open": _ffloat(rec.get("ohlc.open")),
            "ohlc_high": _ffloat(rec.get("ohlc.high")),
            "ohlc_low": _ffloat(rec.get("ohlc.low")),
            "ohlc_close": _ffloat(rec.get("ohlc.close")),
            "net_change": _ffloat(rec.get("net_change")),
            "net_change_percent": _ffloat(rec.get("net_change_percent")),
            "day_change": _ffloat(rec.get("day_change")),
            "day_change_percent": _ffloat(rec.get("day_change_percent")),
            "iv": _ffloat(rec.get("iv")),
            "volume": _fint(rec.get("volume")),
            "oi": _fint(rec.get("oi")),
            "oi_open": _fint(rec.get("oi_open")),
            "oi_change": _fint(rec.get("oi_change")),
            "days_to_expiry": _fint(rec.get("days_to_expiry")),
            "atm_strike_val": _fint(rec.get("atm_strike")),
        }

        fields = {k: v for k, v in fields.items() if v is not None}
        if not fields:
            return

        p = Point("atm_option_quote")
        for k, v in tags.items():
            if v is not None:
                p = p.tag(k, str(v))
        for k, v in fields.items():
            p = p.field(k, v)
        p = p.time(ts, WritePrecision.NS)

        writer.write_api.write(bucket=writer.bucket, record=p)

        # Mirror to CSV after a successful write
        def _atm_path(r: dict) -> list:
            idx = r.get("index") or "UNKNOWN_IDX"
            exp_bucket = r.get("bucket") or "UNKNOWN_BUCKET"  # this_week / next_week / this_month / next_month
            strike_off = r.get("strike_offset") or "UNKNOWN_STRIKE"  # atm / atm_p1 / atm_m1 / ...
            side_dir = "CE" if r.get("side") == "CALL" else "PE" if r.get("side") == "PUT" else "UNK"
            return [idx, exp_bucket, strike_off, side_dir]

        _append_influx_mirror_row(
            base_dir=os.getenv("INFLUX_MIRROR_DIR", os.path.join("data", "influx_mirror")),
            measurement="atm_option_quote",
            record=rec,
            extra_tags={
                "host": socket.gethostname(),
                "app": "logger",
                "env": os.getenv("ENV", "local"),
            },
            ts_field="timestamp",
            path_builder=_atm_path,
        )

    except Exception as e:
        print(f"[WARN] Influx write atm_option_quote failed: {e}")

def write_index_overview(rec: dict, writer):
    """
    Write index overview to InfluxDB with strict types.
    Also mirrors the successfully written record to CSV, into:
      data/influx_mirror/index_overview/{INDEX}/{YYYY-MM-DD}.csv
    """
    try:
        ts = datetime.fromisoformat(rec["timestamp"])

        tags = {
            "index": rec.get("symbol"),
        }

        fields = {
            "atm_strike": _fint(rec.get("atm_strike")),
            "last_price": _ffloat(rec.get("last_price")),
            "open": _ffloat(rec.get("open")),
            "high": _ffloat(rec.get("high")),
            "low": _ffloat(rec.get("low")),
            "close": _ffloat(rec.get("close")),
            "net_change": _ffloat(rec.get("net_change")),
            "net_change_percent": _ffloat(rec.get("net_change_percent")),
            "day_change": _ffloat(rec.get("day_change")),
            "day_change_percent": _ffloat(rec.get("day_change_percent")),
            "day_width": _ffloat(rec.get("day_width")),
            "day_width_percent": _ffloat(rec.get("day_width_percent")),
        }

        # Include ATM aggregates copied from option collector
        for k, v in rec.items():
            if k.endswith("_TP") or k.endswith("_OI_CALL") or k.endswith("_OI_PUT"):
                fv = _ffloat(v)
                if fv is not None:
                    fields[k] = fv
            elif k.startswith("pcr_") or k.endswith("_iv_open") or k.endswith("_iv_day_change") or k.endswith("_atm_iv"):
                fv = _ffloat(v)
                if fv is not None:
                    fields[k] = fv
            elif k.endswith("_days_to_expiry"):
                iv = _fint(v)
                if iv is not None:
                    fields[k] = iv

        fields = {k: v for k, v in fields.items() if v is not None}
        if not fields:
            return

        p = Point("index_overview")
        for k, v in tags.items():
            if v is not None:
                p = p.tag(k, str(v))
        for k, v in fields.items():
            p = p.field(k, v)
        p = p.time(ts, WritePrecision.NS)

        writer.write_api.write(bucket=writer.bucket, record=p)

        # Mirror to CSV after a successful write
        def _ov_path(r: dict) -> list:
            idx = r.get("symbol") or "UNKNOWN_IDX"
            return [idx]

        _append_influx_mirror_row(
            base_dir=os.getenv("INFLUX_MIRROR_DIR", os.path.join("data", "influx_mirror")),
            measurement="index_overview",
            record=rec,
            extra_tags={
                "host": socket.gethostname(),
                "app": "logger",
                "env": os.getenv("ENV", "local"),
            },
            ts_field="timestamp",
            path_builder=_ov_path,
        )

    except Exception as e:
        print(f"[WARN] Influx write index_overview failed: {e}")
