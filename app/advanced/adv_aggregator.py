#!/usr/bin/env python3
from __future__ import annotations

import os
import csv
import datetime as dt
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple, List, DefaultDict
from collections import defaultdict

from .adv_config import AdvConfig, WEEKDAY_CODE, BASE_OFFSETS, PAIR_OFFSETS
from .adv_io import (
    WeekdayRow,
    read_weekday_master,
    write_weekday_master,
    ensure_dir,
    debug,
)
from .adv_influx_writer import InfluxWeekdayUpdater
from .adv_ledger import load_ledger, append_ledger

COL_TS_PRIMARY = ("ts", "ts_ist", "timestamp")
COL_PREMIUM_PRIMARY = ("total_premium", "tp_sum")
COL_CALL = "call_last_price"
COL_PUT = "put_last_price"

SPLIT_INDEX_DIR = {
    "NIFTY 50": "NIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "SENSEX": "SENSEX",
}

@dataclass
class AggregationKey:
    index: str
    expiry_code: str
    strike_offset: str
    weekday: str

def weekday_code_from_date(d: dt.date) -> str:
    return WEEKDAY_CODE[d.weekday()]

def normalize_offset_name(off: str) -> str:
    s = (off or "").strip().lower()
    mapping = {"m2": "atm_m2", "m1": "atm_m1", "p1": "atm_p1", "p2": "atm_p2"}
    if s in {"atm_m2", "atm_m1", "atm", "atm_p1", "atm_p2", "atm_p1m1", "atm_p2m2"}:
        return s
    return mapping.get(s, s)

def _parse_hhmm(hhmm: str) -> Tuple[int, int]:
    hh, mm = hhmm.split(":")
    return int(hh), int(mm)

def in_session_ist(dt_obj: dt.datetime, start_hhmm: str, end_hhmm: str) -> bool:
    sh, sm = _parse_hhmm(start_hhmm)
    eh, em = _parse_hhmm(end_hhmm)
    t = dt_obj.time()
    return (t >= dt.time(sh, sm)) and (t <= dt.time(eh, em))

def to_utc_iso(dt_obj: dt.datetime) -> str:
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    else:
        dt_obj = dt_obj.astimezone(dt.timezone.utc)
    return dt_obj.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def to_ist(ts_val: str, tz_name: str) -> Optional[dt.datetime]:
    src: Optional[dt.datetime] = None
    try:
        import dateutil.parser
        src = dateutil.parser.isoparse(ts_val)
    except Exception:
        try:
            if ts_val.endswith("Z"):
                ts_val = ts_val.replace("Z", "+00:00")
            src = dt.datetime.fromisoformat(ts_val)
        except Exception:
            return None
    try:
        import pytz
        tz_ist = pytz.timezone(tz_name)
        if src.tzinfo is None:
            src = src.replace(tzinfo=dt.timezone.utc)
        return src.astimezone(tz_ist)
    except Exception:
        return None

def hhmm_from_ist(dt_obj: dt.datetime) -> str:
    return f"{dt_obj.hour:02d}:{dt_obj.minute:02d}"

def _apply_weekday_update(path: str, tb: str, tot: float, config: AdvConfig) -> Tuple[int, WeekdayRow]:
    existing = read_weekday_master(path)
    prev = existing.get(tb)
    if prev:
        n = prev.n_tot + 1
        s = prev.sum_tot + tot
        mn = min(prev.min_tot, tot)
        mx = max(prev.max_tot, tot)
    else:
        n = 1
        s = tot
        mn = tot
        mx = tot
    avg = s / n if n > 0 else 0.0
    updated = WeekdayRow(
        time_bucket=tb,
        n_tot=n,
        sum_tot=s,
        avg_tot=avg,
        min_tot=mn,
        max_tot=mx,
        last_updated=to_utc_iso(dt.datetime.now(dt.timezone.utc)),
    )
    existing[tb] = updated
    write_weekday_master(path, existing, atomic=config.ATOMIC_CSV_WRITES)
    return n, updated

def _split_path_for(config: AdvConfig, index: str, expiry_code: str, strike_offset: str, date_str: str) -> str:
    root = os.getenv("MIRROR_SPLIT_ROOT", "").strip()
    if not root:
        return ""
    index_dir = SPLIT_INDEX_DIR.get(index.strip().upper(), index.strip().upper())
    return os.path.join(root, index_dir, expiry_code, strike_offset, f"{date_str}.csv")

def _pick_first_present(d: dict, keys: Iterable[str]) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return None

def _read_split_csv_totals(
    config: AdvConfig,
    path: str,
    tz_name: str,
    start_hhmm: str,
    end_hhmm: str,
) -> Dict[str, float]:
    out: Dict[str, Tuple[dt.datetime, float]] = {}
    if not path or not os.path.exists(path):
        # keep concise, no spam for missing paths
        return {}

    import time
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                headers = rdr.fieldnames or []
                if not headers:
                    if attempt < max_retries - 1:
                        time.sleep(0.15 * (attempt + 1))
                        continue
                    return {}
                for row in rdr:
                    try:
                        ts_val = _pick_first_present(row, COL_TS_PRIMARY)
                        if not ts_val:
                            continue
                        ist = to_ist(ts_val, tz_name)
                        if not ist or not in_session_ist(ist, start_hhmm, end_hhmm):
                            continue

                        prem_val = _pick_first_present(row, COL_PREMIUM_PRIMARY)
                        tot: Optional[float] = None
                        if prem_val not in (None, ""):
                            try:
                                tot = float(prem_val)
                            except Exception:
                                tot = None
                        if tot is None:
                            call_v = row.get(COL_CALL); put_v = row.get(COL_PUT)
                            if call_v not in (None, "") and put_v not in (None, ""):
                                try:
                                    tot = float(call_v) + float(put_v)
                                except Exception:
                                    tot = None
                        if tot is None:
                            continue

                        tb = hhmm_from_ist(ist)
                        prev = out.get(tb)
                        if (prev is None) or (ist >= prev[0]):
                            out[tb] = (ist, tot)
                    except Exception:
                        continue
            break
        except PermissionError:
            if attempt < max_retries - 1:
                time.sleep(0.2 * (attempt + 1))
                continue
            return {}

    return {tb: tot for tb, (_t, tot) in out.items()}

def _build_totals_split_for_index_day(
    config: AdvConfig,
    index: str,
    date_str: str,
    expiry_codes: Iterable[str],
    offsets: Iterable[str],
) -> Dict[Tuple[str, str, str], float]:
    result: Dict[Tuple[str, str, str], float] = {}
    for exp in expiry_codes:
        for off in offsets:
            path = _split_path_for(config, index, exp, off, date_str)
            per_min = _read_split_csv_totals(
                config=config,
                path=path,
                tz_name=config.MARKET_TIMEZONE,
                start_hhmm=config.SESSION_START_HHMM,
                end_hhmm=config.SESSION_END_HHMM,
            )
            if not per_min:
                continue
            for tb, tot in per_min.items():
                result[(exp, off, tb)] = tot
    return result

def _apply_pairs_from_base(
    base: Dict[Tuple[str, str, str], float]
) -> Dict[Tuple[str, str, str], float]:
    by_exp_tb: DefaultDict[Tuple[str, str], Dict[str, float]] = defaultdict(dict)
    for (exp, off, tb), tot in base.items():
        by_exp_tb[(exp, tb)][off] = tot
    out: Dict[Tuple[str, str, str], float] = {}
    for (exp, tb), vals in by_exp_tb.items():
        if ("atm_p1" in vals) and ("atm_m1" in vals):
            out[(exp, "atm_p1m1", tb)] = (vals["atm_p1"] + vals["atm_m1"]) / 2.0
        if ("atm_p2" in vals) and ("atm_m2" in vals):
            out[(exp, "atm_p2m2", tb)] = (vals["atm_p2"] + vals["atm_m2"]) / 2.0
    return out

def _read_rep_strike_from_split_csv(path: str) -> Optional[float]:
    if not path or not os.path.exists(path):
        return None
    rep = None
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            cols = rdr.fieldnames or []
            candidates = [c for c in ("atm_strike","call_strike","put_strike") if c in cols]
            if not candidates:
                return None
            last_val = None
            for row in rdr:
                for c in candidates:
                    v = row.get(c)
                    if v not in (None, ""):
                        try:
                            last_val = float(v)
                        except Exception:
                            continue
            rep = last_val
    except Exception:
        return None
    return rep

def _strike_summary_for_expiry(
    config: AdvConfig,
    index: str,
    date_str: str,
    expiry_code: str,
    offsets: Iterable[str],
) -> List[Tuple[str, Optional[float]]]:
    out: List[Tuple[str, Optional[float]]] = []
    for off in offsets:
        path = _split_path_for(config, index, expiry_code, off, date_str)
        rep = _read_rep_strike_from_split_csv(path)
        out.append((off, rep))
    return out

def _format_ladder(label_vals: List[Tuple[str, Optional[float]]]) -> str:
    parts = []
    for off, rep in label_vals:
        if rep is None:
            parts.append(f"{off}:-")
        else:
            parts.append(f"{off}:{int(rep) if abs(rep - int(rep)) < 1e-6 else f'{rep:.2f}'}")
    return " ".join(parts)

def stream_update_for_latest_minute(
    config: AdvConfig,
    date_str: str,
    indices: Iterable[str],
    expiry_codes: Iterable[str],
) -> int:
    if not config.ENABLE_STREAMING:
        debug("[CFG ] Streaming disabled via ADV_ENABLE_STREAMING=false")
        return 0

    updated = 0

    for index in indices:
        # Collect base and pairs
        base = _build_totals_split_for_index_day(
            config=config,
            index=index,
            date_str=date_str,
            expiry_codes=tuple(expiry_codes),
            offsets=BASE_OFFSETS,
        )
        pairs_from_files = _build_totals_split_for_index_day(
            config=config,
            index=index,
            date_str=date_str,
            expiry_codes=tuple(expiry_codes),
            offsets=PAIR_OFFSETS,
        )
        if not pairs_from_files:
            pairs_from_files = _apply_pairs_from_base(base)

        # Optional terse counters
        base_count_by_exp: Dict[str, int] = defaultdict(int)
        pair_count_by_exp: Dict[str, int] = defaultdict(int)
        last_tb_by_exp: Dict[str, str] = {}

        # Base updates (suppress per-minute prints if summary-only)
        for (exp, off, tb), tot in base.items():
            wcode = weekday_code_from_date(dt.datetime.strptime(f"{date_str} {tb}", "%Y-%m-%d %H:%M").date())
            if wcode not in ("mon", "tue", "wed", "thu", "fri"):
                continue
            path = config.WEEKDAY_MASTER_PATH.format(
                ADV_ROOT=config.ADV_ROOT,
                index=index,
                expiry_code=exp,
                strike_offset=off,
                weekday=wcode,
            )
            _n, _ = _apply_weekday_update(path, tb, tot, config)
            updated += 1
            base_count_by_exp[exp] += 1
            last_tb_by_exp[exp] = tb
            if not config.LOG_SUMMARY_ONLY:
                debug(f"[UPD ] {index} {exp} {off} {wcode} {tb}: tot={tot:.2f}")

        # Pair updates
        for (exp, poff, tb), tot in pairs_from_files.items():
            wcode = weekday_code_from_date(dt.datetime.strptime(f"{date_str} {tb}", "%Y-%m-%d %H:%M").date())
            path = config.WEEKDAY_MASTER_PATH.format(
                ADV_ROOT=config.ADV_ROOT,
                index=index,
                expiry_code=exp,
                strike_offset=poff,
                weekday=wcode,
            )
            _n, _ = _apply_weekday_update(path, tb, tot, config)
            updated += 1
            pair_count_by_exp[exp] += 1
            last_tb_by_exp[exp] = tb
            if not config.LOG_SUMMARY_ONLY:
                debug(f"[PAIR] {index} {exp} {poff} {wcode} {tb}: tot={tot:.2f}")

        # Concise per-expiry summaries
        for exp in expiry_codes:
            ladder = _strike_summary_for_expiry(config, index, date_str, exp, list(BASE_OFFSETS) + list(PAIR_OFFSETS))
            ladder_txt = _format_ladder(ladder)
            last_tb = last_tb_by_exp.get(exp, "-")
            bcnt = base_count_by_exp.get(exp, 0)
            pcnt = pair_count_by_exp.get(exp, 0)

            if config.LOG_LAST_TOTALS:
                # Compute last totals snapshot per offset for context (best effort)
                last_totals: List[str] = []
                for off in list(BASE_OFFSETS) + list(PAIR_OFFSETS):
                    # Look up last minute in our processed maps
                    candidates = [(tb, v) for (e,o,tb), v in {**base, **pairs_from_files}.items() if e == exp and o == off]
                    if candidates:
                        tb_latest, val = sorted(candidates, key=lambda x: x[0])[-1]
                        last_totals.append(f"{off}={val:.2f}")
                lt_txt = " | " + " ".join(last_totals) if last_totals else ""
            else:
                lt_txt = ""

            debug(f"[SUM ] {index} {exp} last={last_tb} base={bcnt} pair={pcnt} | {ladder_txt}{lt_txt}")

    return updated

def aggregate_day_for_key(
    config: AdvConfig,
    agg_key: AggregationKey,
    rows_iter: Iterable[dict],
    influx: Optional[InfluxWeekdayUpdater] = None,
    dry_run: bool = False,
    date_str: Optional[str] = None,
) -> None:
    path = config.WEEKDAY_MASTER_PATH.format(
        ADV_ROOT=config.ADV_ROOT,
        index=agg_key.index,
        expiry_code=agg_key.expiry_code,
        strike_offset=agg_key.strike_offset,
        weekday=agg_key.weekday,
    )

    if date_str:
        ledger = load_ledger(path)
        if date_str in ledger:
            print(f"[SKIP] {agg_key.index} {agg_key.expiry_code} {agg_key.strike_offset} already merged for {date_str}")
            return

    existing_all = read_weekday_master(path)
    updates: Dict[str, WeekdayRow] = {}

    for row in rows_iter:
        try:
            if (row.get("index", "").strip().upper() != agg_key.index.upper()):
                continue
            if (row.get("expiry_code", "").strip().lower() != agg_key.expiry_code.lower()):
                continue
            if normalize_offset_name(row.get("strike_offset", "")) != agg_key.strike_offset:
                continue

            ce_raw, pe_raw = row.get("ce_ltp"), row.get("pe_ltp")
            if not ce_raw or not pe_raw:
                continue
            ce_v = float(ce_raw); pe_v = float(pe_raw)
        except Exception:
            continue

        ts_raw = row.get("ts")
        if not ts_raw:
            continue
        ts_ist = to_ist(ts_raw, config.MARKET_TIMEZONE)
        if ts_ist is None or not in_session_ist(ts_ist, config.SESSION_START_HHMM, config.SESSION_END_HHMM):
            continue

        tb = hhmm_from_ist(ts_ist)
        tot = ce_v + pe_v

        prev = existing_all.get(tb)
        if prev:
            n = prev.n_tot + 1
            s = prev.sum_tot + tot
            mn = min(prev.min_tot, tot)
            mx = max(prev.max_tot, tot)
        else:
            n = 1
            s = tot
            mn = tot
            mx = tot
        avg = s / n if n > 0 else 0.0

        updated = WeekdayRow(
            time_bucket=tb,
            n_tot=n,
            sum_tot=s,
            avg_tot=avg,
            min_tot=mn,
            max_tot=mx,
            last_updated=to_utc_iso(dt.datetime.now(dt.timezone.utc)),
        )
        existing_all[tb] = updated
        updates[tb] = updated

    if not dry_run:
        write_weekday_master(path, existing_all, atomic=config.ATOMIC_CSV_WRITES)
        if date_str:
            append_ledger(path, date_str)

    if influx and not dry_run and updates:
        ist_date = dt.date.fromisoformat(date_str) if date_str else None
        influx.write_weekday_updates(
            config=config,
            agg_index=agg_key.index,
            agg_expiry_code=agg_key.expiry_code,
            agg_strike_offset=agg_key.strike_offset,
            agg_weekday=agg_key.weekday,
            rows=updates,
            ist_date=ist_date,
        )

def aggregate_eod_paired(
    config: AdvConfig,
    index: str,
    date_str: str,
    expiry_codes: Iterable[str],
) -> int:
    base = _build_totals_split_for_index_day(
        config=config,
        index=index,
        date_str=date_str,
        expiry_codes=tuple(expiry_codes),
        offsets=BASE_OFFSETS,
    )
    pairs_from_files = _build_totals_split_for_index_day(
        config=config,
        index=index,
        date_str=date_str,
        expiry_codes=tuple(expiry_codes),
        offsets=PAIR_OFFSETS,
    )
    if not pairs_from_files:
        pairs_from_files = _apply_pairs_from_base(base)

    count = 0
    for (exp, poff, tb), tot in pairs_from_files.items():
        wcode = weekday_code_from_date(dt.datetime.strptime(f"{date_str} {tb}", "%Y-%m-%d %H:%M").date())
        path = config.WEEKDAY_MASTER_PATH.format(
            ADV_ROOT=config.ADV_ROOT,
            index=index,
            expiry_code=exp,
            strike_offset=poff,
            weekday=wcode,
        )
        _n, _ = _apply_weekday_update(path, tb, tot, config)
        count += 1
    return count

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Advanced weekday masters updater (stream or EOD).")
    ap.add_argument("--mode", choices=["stream", "eod"], default="stream", help="stream: minute updates; eod: reconciliation")
    ap.add_argument("--date", help="YYYY-MM-DD (IST date). Default: today", default=None)
    ap.add_argument("--indices", nargs="*", default=["NIFTY 50","SENSEX","NIFTY BANK"], help="Canonical indices")
    ap.add_argument("--expiries", nargs="*", default=["this_week","next_week","this_month","next_month"], help="Expiry codes")
    args = ap.parse_args()

    cfg = AdvConfig.from_env()
    ist_now = to_ist(dt.datetime.now(dt.timezone.utc).isoformat(), cfg.MARKET_TIMEZONE) or dt.datetime.now()
    date_str = args.date or ist_now.date().isoformat()

    if args.mode == "stream":
        total = stream_update_for_latest_minute(cfg, date_str, args.indices, args.expiries)
        print(f"[OK] Stream updated {total} buckets for {date_str}")
    else:
        total = 0
        for idx in args.indices:
            total += aggregate_eod_paired(cfg, idx, date_str, args.expiries)
        print(f"[OK] EOD paired updates applied: {total} entries for {date_str}")
