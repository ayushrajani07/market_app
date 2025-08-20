#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import tempfile
from dataclasses import dataclass
from typing import Dict, Iterable

WEEKDAY_MASTER_HEADER = [
    "time_bucket",
    "n_tot",
    "sum_tot",
    "avg_tot",
    "min_tot",
    "max_tot",
    "last_updated",
]

@dataclass
class WeekdayRow:
    time_bucket: str
    n_tot: int
    sum_tot: float
    avg_tot: float
    min_tot: float
    max_tot: float
    last_updated: str

def ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def read_weekday_master(path: str) -> Dict[str, WeekdayRow]:
    out: Dict[str, WeekdayRow] = {}
    if not os.path.exists(path):
        return out
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                tb = row["time_bucket"].strip()
                out[tb] = WeekdayRow(
                    time_bucket=tb,
                    n_tot=int(row["n_tot"]),
                    sum_tot=float(row["sum_tot"]),
                    avg_tot=float(row["avg_tot"]),
                    min_tot=float(row["min_tot"]),
                    max_tot=float(row["max_tot"]),
                    last_updated=row.get("last_updated", "").strip(),
                )
            except Exception:
                continue
    return out

def write_weekday_master(path: str, rows: Dict[str, WeekdayRow], atomic: bool = True) -> None:
    ensure_dir(path)

    def write_to_file(fp: str):
        with open(fp, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=WEEKDAY_MASTER_HEADER)
            writer.writeheader()
            for tb in sorted(rows.keys()):
                r = rows[tb]
                writer.writerow({
                    "time_bucket": r.time_bucket,
                    "n_tot": r.n_tot,
                    "sum_tot": f"{r.sum_tot:.6f}",
                    "avg_tot": f"{r.avg_tot:.6f}",
                    "min_tot": f"{r.min_tot:.6f}",
                    "max_tot": f"{r.max_tot:.6f}",
                    "last_updated": r.last_updated,
                })

    if not atomic:
        write_to_file(path)
        return

    import time
    d = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=d, suffix=".csv")
    os.close(fd)
    try:
        write_to_file(tmp)
        for attempt in range(5):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                time.sleep(0.2 + 0.2 * attempt)
        # Last resort: non-atomic
        write_to_file(path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass

def debug(msg: str) -> None:
    print(msg, flush=True)
