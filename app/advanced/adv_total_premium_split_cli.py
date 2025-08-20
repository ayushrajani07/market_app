#!/usr/bin/env python3
from __future__ import annotations
import argparse
import glob
import os
import sys
from typing import List

from .adv_config import AdvConfig
from .adv_total_premium_split import accumulate_per_minute, split_by_strike_offset, write_split_csvs

def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Split per-minute total premium into separate CSVs per strike/expiry/index")
    parser.add_argument("--index", required=True, help='e.g., "SENSEX"')
    parser.add_argument("--source", required=True, help='Expiry code, e.g., this_week,this_month')
    parser.add_argument("--glob", required=True, help='Glob for input JSONs (e.g., CALL and PUT): "C:/path/SENSEX_*_this_week_2025-08-19_*.json"')
    parser.add_argument("--out-root", default="", help='Base output dir (default: csv_data under ADV_ROOT)')
    parser.add_argument("--step-hint", type=int, default=100, help="Strike step (50/100) to derive offsets when strike available")
    parser.add_argument("--no-atomic", action="store_true", help="Disable atomic writes")
    parser.add_argument("--filename-ts", default="", help='Override TIMESTAMP filename (e.g., "20250818_130000.csv")')
    args = parser.parse_args(argv)

    cfg = AdvConfig.from_env()

    file_list = sorted(glob.glob(args.glob))
    if not file_list:
        print(f"[ERROR] No files matched glob: {args.glob}", file=sys.stderr)
        return 2

    merged = accumulate_per_minute(
        config=cfg,
        index=args.index,
        expiry_code=args.source,
        json_files=file_list,
        step_hint=args.step_hint,
    )
    groups = split_by_strike_offset(merged)

    out_root = args.out_root or os.path.join(cfg.ADV_ROOT, "csv_data")
    filename_ts = args.filename_ts if args.filename_ts else None

    written = write_split_csvs(
        base_dir=out_root,
        groups=groups,
        atomic=not args.no_atomic,
        filename_ts=filename_ts,
    )
    print("[OK] Wrote files:")
    for p in written:
        print("  -", p)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
