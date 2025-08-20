#!/usr/bin/env python3
from __future__ import annotations
import csv
import os
from pathlib import Path
from typing import Dict, Tuple, List, Optional
from datetime import datetime, date

MASTER_COLUMNS = ["HH:MM", "HIST_AVG", "COUNTER", "LAST_UPDATED"]

SESSION_START_HHMM = os.getenv("SESSION_START_HHMM", "09:15")
SESSION_END_HHMM = os.getenv("SESSION_END_HHMM", "15:30")

def _in_session(hhmm: str, start_hhmm: str, end_hhmm: str) -> bool:
    try:
        sh, sm = map(int, start_hhmm.split(":"))
        eh, em = map(int, end_hhmm.split(":"))
        h, m = map(int, hhmm.split(":"))
        t = (h, m)
        return (t >= (sh, sm)) and (t <= (eh, em))
    except Exception:
        return True  # be permissive if parsing fails

def hhmm_from_ts_ist(ts_iso: str) -> str:
    try:
        tpart = ts_iso.split("T", 1)[1]
        return tpart[0:5]
    except Exception:
        from dateutil import parser
        dt = parser.isoparse(ts_iso)
        return f"{dt.hour:02d}:{dt.minute:02d}"

def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def master_path(weekday_root: Path, index: str, expiry: str, strike: str) -> Path:
    fname = f"WEEKDAY_{index}_{expiry}_{strike}_MASTER.csv"
    return weekday_root / index / expiry / strike / fname

def ledger_path_for_master(master_csv_path: Path) -> Path:
    return master_csv_path.with_suffix(master_csv_path.suffix + ".ledger")

def read_ledger(ledger_path: Path) -> set[str]:
    if not ledger_path.exists():
        return set()
    out = set()
    with open(ledger_path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                out.add(s)
    return out

def append_ledger(ledger_path: Path, day_str: str) -> None:
    ensure_parent(ledger_path)
    with open(ledger_path, "a", encoding="utf-8") as f:
        f.write(day_str + "\n")

def load_master(path: Path) -> Dict[str, Tuple[float, int, str]]:
    out: Dict[str, Tuple[float, int, str]] = {}
    if not path.exists():
        return out
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                hhmm = row.get("HH:MM")
                if not hhmm:
                    continue
                avg = float(row.get("HIST_AVG") or 0.0)
                cnt = int(float(row.get("COUNTER") or 0))
                lu = row.get("LAST_UPDATED") or ""
                out[hhmm] = (avg, cnt, lu)
            except Exception:
                continue
    return out

def write_master_atomic(path: Path, rows: Dict[str, Tuple[float, int, str]]) -> None:
    ensure_parent(path)
    items = sorted(rows.items(), key=lambda kv: kv[0])  # sort by HH:MM
    tmp = path.with_name("." + path.name + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(MASTER_COLUMNS)
        for hhmm, (avg, cnt, lu) in items:
            w.writerow([hhmm, f"{avg:.6f}", cnt, lu])
    os.replace(tmp, path)

def update_master_from_daily(
    weekday_root: Path,
    daily_csv: Path,
    idx: str,
    exp: str,
    strike: str,
    day_str: str,
    use_ledger: bool = True,
    session_start: str = SESSION_START_HHMM,
    session_end: str = SESSION_END_HHMM,
) -> Optional[Path]:
    master = master_path(weekday_root, idx, exp, strike)
    ledger = ledger_path_for_master(master)

    if use_ledger:
        done = read_ledger(ledger)
        if day_str in done:
            # Already processed this day for this master
            return master

    rows = load_master(master)
    updated = False

    with open(daily_csv, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            tot_raw = row.get("total_premium", "")
            if not tot_raw:
                continue
            try:
                tot = float(tot_raw)
            except Exception:
                continue

            ts_ist = row.get("ts_ist")
            if not ts_ist:
                continue
            hhmm = hhmm_from_ts_ist(ts_ist)
            if not _in_session(hhmm, session_start, session_end):
                continue

            prev = rows.get(hhmm)
            if prev:
                avg, cnt, _lu = prev
                new_cnt = cnt + 1
                new_avg = (avg * cnt + tot) / new_cnt
                rows[hhmm] = (new_avg, new_cnt, day_str)
            else:
                rows[hhmm] = (tot, 1, day_str)
            updated = True

    if updated:
        write_master_atomic(master, rows)
        if use_ledger:
            append_ledger(ledger, day_str)

    return master

def find_daily_csvs(csv_root: Path, day_str: str) -> List[Tuple[Path, str, str, str]]:
    """
    Returns tuples of (daily_csv_path, index, expiry, strike) for the given date.
    Scans: csv_root/INDEX/EXPIRY/STRIKE/DATE.csv
    """
    results: List[Tuple[Path, str, str, str]] = []
    if not csv_root.exists():
        return results
    for index_dir in csv_root.iterdir():
        if not index_dir.is_dir():
            continue
        idx = index_dir.name
        for exp_dir in index_dir.iterdir():
            if not exp_dir.is_dir():
                continue
            exp = exp_dir.name
            for strike_dir in exp_dir.iterdir():
                if not strike_dir.is_dir():
                    continue
                strike = strike_dir.name
                daily = strike_dir / f"{day_str}.csv"
                if daily.exists():
                    results.append((daily, idx, exp, strike))
    return results

def run_bulk_eod(
    csv_root: str = "data/csv_data",
    weekday_root: str = "WEEKDAY",
    date_iso: Optional[str] = None,
    use_ledger: bool = True,
) -> List[Path]:
    """
    Bulk process all available daily CSVs for the given date and update weekday masters.
    Returns list of updated master file paths.
    """
    d = date.fromisoformat(date_iso) if date_iso else datetime.now().astimezone().date()
    day_str = d.isoformat()
    csv_root_p = Path(csv_root)
    weekday_root_p = Path(weekday_root)

    found = find_daily_csvs(csv_root_p, day_str)
    updated_paths: List[Path] = []
    for daily_csv, idx, exp, strike in found:
        mp = update_master_from_daily(
            weekday_root=weekday_root_p,
            daily_csv=daily_csv,
            idx=idx,
            exp=exp,
            strike=strike,
            day_str=day_str,
            use_ledger=use_ledger,
        )
        if mp:
            updated_paths.append(mp)
    return updated_paths
