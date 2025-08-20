#!/usr/bin/env python3
from __future__ import annotations
import argparse
from app.advanced.weekday_master_bulk import run_bulk_eod

def main():
    ap = argparse.ArgumentParser(description="Bulk EOD: update all weekday masters from daily CSVs")
    ap.add_argument("--csv-root", default="data/csv_data", help="Root of daily split CSVs")
    ap.add_argument("--weekday-root", default="WEEKDAY", help="Root for weekday master files")
    ap.add_argument("--date", default="", help="YYYY-MM-DD (defaults to today)")
    ap.add_argument("--no-ledger", action="store_true", help="Disable idempotency ledger")
    args = ap.parse_args()

    paths = run_bulk_eod(
        csv_root=args.csv_root,
        weekday_root=args.weekday_root,
        date_iso=args.date or None,
        use_ledger=(not args.no_ledger),
    )
    if not paths:
        print("[INFO] No masters updated (no daily CSVs found for date).")
    else:
        print("[OK] Updated masters:")
        for p in sorted(set(paths)):
            print(" -", p)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
