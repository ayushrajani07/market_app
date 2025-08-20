#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path
from app.advanced.daily_compact_wide import compact_wide_for_index_expiry

def main():
    ap = argparse.ArgumentParser(description="Create wide daily compact CSV per index/expiry")
    ap.add_argument("--csv-root", default="data/csv_data")
    ap.add_argument("--out-root", default="data_compact")
    ap.add_argument("--index", required=True)
    ap.add_argument("--expiry", required=True)
    ap.add_argument("--date", required=True)
    args = ap.parse_args()

    p = compact_wide_for_index_expiry(
        csv_root=Path(args.csv_root),
        index=args.index,
        expiry=args.expiry,
        date_str=args.date,
        out_root=Path(args.out_root),
    )
    if p:
        print(f"[OK] Wrote {p}")
    else:
        print("[INFO] Nothing to compact (no daily split files found).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
