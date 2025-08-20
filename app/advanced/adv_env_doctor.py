#!/usr/bin/env python3
from __future__ import annotations
import argparse
import os
from typing import List

from .adv_config import AdvConfig
from .adv_paths import raw_csv_path_for

def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Env Doctor: resolve raw snapshot paths")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--indexes", required=True, help="Comma-separated indexes (e.g., 'NIFTY 50,NIFTY BANK')")
    parser.add_argument("--sources", required=True, help="Comma-separated sources (overview,this_week,next_week,this_month,next_month)")
    args = parser.parse_args(argv)

    cfg = AdvConfig.from_env()
    indexes = [x.strip() for x in args.indexes.split(",") if x.strip()]
    sources = [x.strip() for x in args.sources.split(",") if x.strip()]

    print("[ENV-DOCTOR] Configuration summary:")
    print(f"  MARKET_TIMEZONE={cfg.MARKET_TIMEZONE}")
    print(f"  WEEKDAY_MASTER_PATH={cfg.WEEKDAY_MASTER_PATH}")
    print(f"  RAW_FILE_NAME_PATTERN={cfg.RAW_FILE_NAME_PATTERN}")
    print("  RAW roots:")
    print(f"    overview={cfg.RAW_OVERVIEW_ROOT or '(unset)'}")
    print(f"    this_week={cfg.RAW_OPTIONS_THIS_WEEK_ROOT or '(unset)'}")
    print(f"    next_week={cfg.RAW_OPTIONS_NEXT_WEEK_ROOT or '(unset)'}")
    print(f"    this_month={cfg.RAW_OPTIONS_THIS_MONTH_ROOT or '(unset)'}")
    print(f"    next_month={cfg.RAW_OPTIONS_NEXT_MONTH_ROOT or '(unset)'}")

    print("\n[ENV-DOCTOR] Resolved raw CSV paths:")
    missing = 0
    for idx in indexes:
        for src in sources:
            try:
                path = raw_csv_path_for(cfg, src, idx, args.date)
            except ValueError as e:
                print(f"  [WARN] {idx} {src}: {e}")
                continue
            exists = os.path.exists(path)
            mark = "OK " if exists else "MISS"
            print(f"  [{mark}] {src:10s} | {idx:12s} | {path}")
            if not exists:
                missing += 1

    if missing:
        print(f"\n[ENV-DOCTOR] Missing {missing} file(s). Check roots and filename pattern.")
        return 1
    print("\n[ENV-DOCTOR] All paths resolved and present.")
    return 0

if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))
