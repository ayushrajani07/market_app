#!/usr/bin/env python3

import os
import sys
import time
from pathlib import Path
from datetime import datetime, time as dtime
import socket

import pytz
from dotenv import load_dotenv

# Ensure project root (folder containing app/) is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

from socket import gethostname
from app.monitors.health_writer import write_broker_health
from app.brokers import kite_helpers as KH
from app.brokers.kite_client import get_kite_client, _oauth_login
from app.collectors.atm_option_collector import ATMOptionCollector
from app.collectors.overview_collector import OverviewCollector
from app.utils.time_utils import rounded_half_minute
from app.storage.influx_writer import InfluxWriter
from app.monitors.health_writer import write_monitor_status, write_pipeline_tick
from app.collectors.minute_merge import merge_call_put_to_rows
from app.collectors.csv_daily_split_writer import append_rows

# Debug: confirm which influx_writer is used
import app.storage.influx_writer as influx_writer_mod, inspect
print("[INF-MOD] influx_writer file:", inspect.getfile(influx_writer_mod), flush=True)

print("[ENV] DEBUG =", os.getenv("INFLUX_WRITE_STATS_DEBUG"))
print("[ENV] ENABLE=", os.getenv("INFLUX_WRITE_STATS_ENABLE"))
print("[ENV] ALWAYS=", os.getenv("INFLUX_WRITE_STATS_ALWAYS_EMIT"))

IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

# Root for daily split CSVs
CSV_SPLIT_ROOT = os.getenv("CSV_SPLIT_ROOT", os.path.join("data", "csv_data"))

def parse_offsets_env() -> tuple[int, ...]:
    raw = os.getenv("LOGGER_OFFSETS", "0")
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    vals = []
    for p in parts:
        try:
            vals.append(int(p))
        except Exception:
            continue
    return tuple(vals) if vals else (0,)

def ts_now() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def fmt_int(n):
    try:
        return f"{int(n):,}"
    except Exception:
        return "-"

def print_init(loop_interval_s: int):
    print(f"{ts_now()} | INIT  | interval={loop_interval_s}s tz=IST open=09:15 close=15:30 host={gethostname()}")

def print_sys(loop_ms: float, atm_ms: float, idx_ms: float, legs_count: int, ov_count: int):
    print(f"{ts_now()} | SYS   | loop={loop_ms:.0f}ms atm={atm_ms:.0f}ms idx={idx_ms:.0f}ms legs={legs_count} ov={ov_count}")

def print_idx(name: str, atm, week_counts=None, month_counts=None):
    tag = (name[:6]).ljust(6)
    atm_s = fmt_int(atm) if atm is not None else "-"
    wk = f" Wk:{week_counts[0]}/{week_counts[1]}" if week_counts else ""
    mo = f" Mo:{month_counts}/{month_counts[1]}" if month_counts else ""
    print(f"{ts_now()} | {tag} | atm={atm_s}{wk}{mo}")

def print_csv(groups: int, rows: int):
    print(f"{ts_now()} | CSV   | groups={groups} rows={rows}")

def print_error(e: Exception, where: str = ""):
    where_s = f" in {where}" if where else ""
    print(f"{ts_now()} | ERROR | {type(e).__name__}{where_s}: {e}")

def market_is_open() -> bool:
    now = datetime.now(IST)
    return now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE

def wait_until_open():
    while True:
        now = datetime.now(IST)
        if now.weekday() >= 5:
            time.sleep(60)
            continue
        if now.time() >= MARKET_OPEN:
            break
        time.sleep(30)

def main():
    try:
        kite = get_kite_client()

        def ensure_token():
            fresh = _oauth_login(kite)
            return getattr(fresh, "_KiteConnect__access_token", "")

        writer = InfluxWriter()
        broker_env = os.getenv("ENV", "local")
        HOSTNAME = socket.gethostname()

        _broker_state = {"latencies": [], "429s": 0, "errors": 0}

        def _on_quote_success(latency_ms: float):
            try:
                _broker_state["latencies"].append(float(latency_ms))
            except Exception:
                pass

        def _on_quote_429():
            _broker_state["429s"] += 1

        def _on_quote_error():
            _broker_state["errors"] += 1

        KH.on_quote_success = _on_quote_success
        KH.on_quote_429 = _on_quote_429
        KH.on_quote_error = _on_quote_error

        atm_collector = ATMOptionCollector(kite_client=kite, ensure_token=ensure_token, influx_writer=writer)
        overview_collector = OverviewCollector(
            kite_client=kite,
            ensure_token=ensure_token,
            atm_collector=atm_collector,
            influx_writer=writer,
        )

        loop_interval = int(os.getenv("LOGGER_LOOP_INTERVAL_SEC", "30"))

        wait_until_open()
        print_init(loop_interval)

        # shared counters dict passed into collectors every loop
        counters = {"legs_written_this_loop": 0, "ov_written_this_loop": 0}

        try:
            while True:
                if not market_is_open():
                    if datetime.now(IST).time() >= MARKET_CLOSE:
                        break
                    time.sleep(5)
                    continue

                loop_start = time.time()

                # ATM legs
                t0 = time.time()
                offsets = parse_offsets_env()  # e.g., (-2,-1,0,1,2)
                legs_result = atm_collector.collect(offsets=offsets, counters=counters)
                atm_collect_ms = (time.time() - t0) * 1000.0
                leg_recs = legs_result.get("legs", []) or []
                legs_count = len(leg_recs)

                # per-index ATM and bucket counts for console
                idx_atm = {"NIFTY": None, "SENSEX": None, "BANKNIFTY": None}
                bucket_counts = {"NIFTY": {}, "SENSEX": {}, "BANKNIFTY": {}}
                for leg in leg_recs:
                    idx = leg.get("index")
                    bucket = leg.get("bucket")
                    if not idx or not bucket:
                        continue
                    if idx_atm.get(idx) is None and isinstance(leg.get("atm_strike"), (int, float)):
                        try:
                            idx_atm[idx] = int(leg.get("atm_strike"))
                        except Exception:
                            idx_atm[idx] = None
                    bucket_counts[idx][bucket] = bucket_counts[idx].get(bucket, 0) + 1

                # Overview (index snapshot)
                t1 = time.time()
                overview_data = overview_collector.collect(counters=counters)
                overview_collect_ms = (time.time() - t1) * 1000.0
                overview_count = len(overview_data)

                # Append to daily CSVs
                csv_groups = 0
                csv_rows = 0
                try:
                    if leg_recs:
                        merged = merge_call_put_to_rows(
                            leg_recs,
                            step_hint_by_index={"NIFTY": 50, "SENSEX": 100, "BANKNIFTY": 100},
                        )

                        merged_map = merged.get("merged_map", {})
                        merged_map_p1m1 = merged.get("merged_map__p1m1", {})
                        merged_map_p2m2 = merged.get("merged_map__p2m2", {})

                        today_str = datetime.now().astimezone().date().isoformat()

                        # Existing symmetric file layout
                        groups = {}
                        for (_ts_ist, idx, expc, off), row in merged_map.items():
                            groups.setdefault((idx, expc, off), []).append(row)
                        for (idx, expc, off), rows in groups.items():
                            append_rows(CSV_SPLIT_ROOT, idx, expc, off, today_str, rows)

                        # New asymmetric files: atm_p1m1 and atm_p2m2
                        groups_p1m1 = {}
                        if merged_map_p1m1:
                            for (_ts_ist, idx, expc, off), row in merged_map_p1m1.items():
                                groups_p1m1.setdefault((idx, expc, off), []).append(row)
                            for (idx, expc, off), rows in groups_p1m1.items():
                                append_rows(CSV_SPLIT_ROOT, idx, expc, off, today_str, rows)

                        groups_p2m2 = {}
                        if merged_map_p2m2:
                            for (_ts_ist, idx, expc, off), row in merged_map_p2m2.items():
                                groups_p2m2.setdefault((idx, expc, off), []).append(row)
                            for (idx, expc, off), rows in groups_p2m2.items():
                                append_rows(CSV_SPLIT_ROOT, idx, expc, off, today_str, rows)

                        # Throughput counters (sum of all three)
                        csv_groups = len(groups) + len(groups_p1m1) + len(groups_p2m2)
                        csv_rows = (
                            sum(len(v) for v in groups.values())
                            + sum(len(v) for v in groups_p1m1.values())
                            + sum(len(v) for v in groups_p2m2.values())
                        )
                except Exception as e:
                    print_error(e, "CSV append")

                loop_duration_ms = (time.time() - loop_start) * 1000.0

                # Terminal prints
                print_sys(loop_duration_ms, atm_collect_ms, overview_collect_ms, legs_count, overview_count)

                def two(cmap, a, b):
                    return (cmap.get(a, 0), cmap.get(b, 0))

                print_idx(
                    "NIFTY",
                    idx_atm["NIFTY"],
                    week_counts=two(bucket_counts["NIFTY"], "this_week", "next_week"),
                    month_counts=two(bucket_counts["NIFTY"], "this_month", "next_month"),
                )
                print_idx(
                    "SENSEX",
                    idx_atm["SENSEX"],
                    week_counts=two(bucket_counts["SENSEX"], "this_week", "next_week"),
                    month_counts=two(bucket_counts["SENSEX"], "this_month", "next_month"),
                )
                print_idx(
                    "BANKN",
                    idx_atm["BANKNIFTY"],
                    week_counts=None,
                    month_counts=two(bucket_counts["BANKNIFTY"], "this_month", "next_month"),
                )

                if csv_groups or csv_rows:
                    print_csv(csv_groups, csv_rows)

                # Heartbeat
                try:
                    writer.write_point(
                        measurement="pipeline_health",
                        tags={"app": "logger", "scope": "heartbeat", "env": os.getenv("ENV", "local"), "host": gethostname()},
                        fields={"ok": 1},
                    )
                except Exception:
                    pass

                # Health metrics
                try:
                    write_pipeline_tick(
                        writer,
                        env=os.getenv("ENV", "local"),
                        app="logger",
                        loop_duration_ms=loop_duration_ms,
                        atm_collect_ms=atm_collect_ms,
                        overview_collect_ms=overview_collect_ms,
                        records_written_index_overview=overview_count,
                        records_written_atm_legs=legs_count,
                        json_files_written=None,
                        influx_points_written=None,
                        retry_count=None,
                        backoff_applied=None,
                    )

                    write_monitor_status(
                        writer,
                        env=os.getenv("ENV", "local"),
                        app="logger",
                        status="ok",
                        loop_duration_ms=loop_duration_ms,
                        legs_count=legs_count,
                        overview_count=overview_count,
                        lag_sec_index_overview=0,
                        lag_sec_atm_option_quote=0,
                        errors_in_tick=0,
                        influx_points_attempted=None,
                        influx_points_written=None,
                        influx_points_rejected=None,
                        json_write_errors=None,
                    )

                    if csv_groups or csv_rows:
                        writer.write_point(
                            measurement="csv_throughput",
                            tags={"app": "logger", "env": os.getenv("ENV", "local"), "host": gethostname()},
                            fields={"csv_groups_written": int(csv_groups), "csv_rows_written": int(csv_rows)},
                        )
                except Exception as e:
                    print_error(e, "monitor writes")

                # -------- Emit pipeline_activity from loop numbers + counters --------
                legs_expected_this_loop = int(legs_count)
                ov_expected_this_loop = 3  # or dynamic len of indices attempted

                legs_written_this_loop = int(counters.get("legs_written_this_loop", 0))
                ov_written_this_loop = int(counters.get("ov_written_this_loop", 0))

                try:
                    writer.write_point(
                        measurement="pipeline_activity",
                        tags={"app": "logger", "host": HOSTNAME},
                        fields={
                            "loop_ms": int(loop_duration_ms),
                            "atm_ms": int(atm_collect_ms),
                            "idx_ms": int(overview_collect_ms),
                            "legs_processed": int(legs_count),
                            "ov_processed": int(overview_count),
                            "legs_expected": legs_expected_this_loop,
                            "legs_written": legs_written_this_loop,
                            "ov_expected": ov_expected_this_loop,
                            "ov_written": ov_written_this_loop,
                        },
                    )
                except Exception as e:
                    print_error(e, "pipeline_activity write")

                # Flush write stats
                try:
                    writer.flush_stats()
                except Exception:
                    pass

                # Reset per-loop counters
                counters["legs_written_this_loop"] = 0
                counters["ov_written_this_loop"] = 0

                # Pace the loop
                time.sleep(loop_interval)

                # Broker health publishing
                try:
                    if _broker_state["latencies"] or _broker_state["429s"] or _broker_state["errors"]:
                        avg_lat = (
                            sum(_broker_state["latencies"]) / max(1, len(_broker_state["latencies"]))
                            if _broker_state["latencies"] else None
                        )
                        write_broker_health(
                            writer,
                            env=broker_env,
                            api="kite",
                            quote_latency_ms=avg_lat,
                            http_429_count=_broker_state["429s"] or None,
                            error_rate_percent=(100.0 if _broker_state["errors"] > 0 else 0.0),
                            tokens_refresh_count=None,
                        )
                        _broker_state["latencies"].clear()
                        _broker_state["429s"] = 0
                        _broker_state["errors"] = 0
                except Exception:
                    pass

        except KeyboardInterrupt:
            print(f"{ts_now()} | STOP  | logger interrupted")

        finally:
            try:
                writer.close()
            except Exception as e:
                print_error(e, "InfluxWriter.close")

    except Exception as e:
        print_error(e, "startup")
        raise

if __name__ == "__main__":
    main()
