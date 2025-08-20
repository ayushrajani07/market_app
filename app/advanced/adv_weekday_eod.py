#!/usr/bin/env python3
from __future__ import annotations
import argparse
import os
import sys
import datetime as dt
from typing import List

from .adv_config import AdvConfig
from .adv_io import read_core_raw_csv
from .adv_io_json import read_core_raw_json
from .adv_aggregator import AggregationKey, aggregate_day_for_key, weekday_code_from_date
from .adv_influx_writer import InfluxWeekdayUpdater
from .adv_paths import raw_path_for

def _read_rows_auto(path: str, default_index: str, default_expiry_code: str) -> list[dict]:
    if path.lower().endswith(".json"):
        # Step hint selection can be improved per index; 100 is typical for SENSEX/BANKNIFTY
        return list(read_core_raw_json(path, default_index=default_index, default_expiry_code=default_expiry_code, step_hint=100))
    return list(read_core_raw_csv(path))

def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Advanced EOD Weekday Master Aggregation")
    parser.add_argument("--date", required=True, help="Expiry date (YYYY-MM-DD) used in filename placeholder {date}")
    parser.add_argument("--index", required=True, help='e.g., "SENSEX", "NIFTY 50", "NIFTY BANK"')
    parser.add_argument(
        "--sources",
        nargs="+",
        default=["this_week", "this_month"],
        help="Sources to read: overview,this_week,next_week,this_month,next_month",
    )
    parser.add_argument(
        "--offsets",
        nargs="+",
        default=["atm_m2","atm_m1","atm","atm_p1","atm_p2","atm_p1m1","atm_p2m2"],
        help="Strike offsets to aggregate for options sources.",
    )
    parser.add_argument("--option", default="PUT", help="Filename placeholder {option}, e.g., PUT or CALL")
    parser.add_argument("--stamp", default="", help="Filename placeholder {stamp}, e.g., 20250818_125430")
    parser.add_argument("--dry-run", action="store_true", help="No writes (CSV/Influx)")
    args = parser.parse_args(argv)

    cfg = AdvConfig.from_env()
    try:
        eod_date = dt.date.fromisoformat(args.date)  # expiry date per your pattern
    except Exception:
        print("Invalid --date (YYYY-MM-DD expected)", file=sys.stderr)
        return 2

    weekday = weekday_code_from_date(eod_date)
    influx = InfluxWeekdayUpdater() if cfg.ENABLE_INFLUX_WRITES else None

    any_processed = False
    for source in args.sources:
        # Resolve raw file path using placeholders
        try:
            raw_path = raw_path_for(cfg, source_code=source, index=args.index, expiry_date=args.date, option=args.option, stamp=args.stamp)
        except ValueError as e:
            print(f"[WARN] {e}", file=sys.stderr)
            continue

        if not raw_path or not os.path.exists(raw_path):
            print(f"[WARN] Raw file not found for {source}: {raw_path}", file=sys.stderr)
            continue

        rows = _read_rows_auto(raw_path, default_index=args.index, default_expiry_code=source)

        if source == "overview":
            print(f"[INFO] Skipping overview for offsets; no ATM total premium expected.")
            continue

        for off in args.offsets:
            agg_key = AggregationKey(
                index=args.index,
                expiry_code=source,
                strike_offset=off,
                weekday=weekday,
            )
            aggregate_day_for_key(
                config=cfg,
                agg_key=agg_key,
                rows_iter=rows,
                influx=influx,
                dry_run=args.dry_run,
                date_str=args.date,  # strict per-day ledger idempotency
            )
            print(f"[EOD] {args.index} {source} {off} {args.date} weekday={weekday}")
            any_processed = True

    if not any_processed:
        print("[INFO] Nothing processed (missing inputs or only overview).")
    else:
        print("[OK] EOD aggregation complete.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
