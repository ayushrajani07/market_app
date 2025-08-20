#!/usr/bin/env python3
from __future__ import annotations

import sys
import os
import time
import datetime as dt
import subprocess
from pathlib import Path

# Ensure project root (folder containing app/) is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # two levels up from advanced/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.advanced.session_config import SessionConfig
from app.advanced.session_time import now_in_tz, is_in_session, seconds_until_hhmm, today_date_str
from app.advanced.session_tasks import preflight_check, run_subprocess
from app.advanced.minute_scheduler import run_minutely
from app.advanced.adv_metrics_writer import write_options_analytics_adv
from app.advanced.adv_config import AdvConfig

# --------------------------------------------------------------------------------------
# Helpers to manage the system_metrics_writer as a child process
# --------------------------------------------------------------------------------------

def _start_system_metrics_writer() -> subprocess.Popen | None:
    # Skip starting if psutil isn't installed to avoid crash loops
    try:
        import psutil  # noqa: F401
    except Exception:
        print("[SESSION] system_metrics_writer skipped (psutil not installed).")
        return None

    python_exe = sys.executable
    cmd = [python_exe, "-m", "app.advanced.system_metrics_writer"]

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
        print("[SESSION] system_metrics_writer started")
        return proc
    except Exception as e:
        print(f"[SESSION] Failed to start system_metrics_writer: {e}")
        return None

def _stop_process(proc: subprocess.Popen | None, name: str, timeout: float = 5.0) -> None:
    if not proc:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
        print(f"[SESSION] {name} stopped")
    except Exception as e:
        print(f"[SESSION] Failed to stop {name}: {e}")

# --------------------------------------------------------------------------------------
# Optional utilities
# --------------------------------------------------------------------------------------

def _run_env_doctor(date_str: str, indexes: list[str], sources: list[str]) -> int:
    try:
        from .adv_env_doctor import main as env_doctor_main
    except Exception:
        print("[SESSION] Env doctor module not available; skipping.", file=sys.stderr)
        return 0
    idxs = ",".join(indexes)
    srcs = ",".join(sources)
    return env_doctor_main(["--date", date_str, "--indexes", idxs, "--sources", srcs])

def _run_bulk_eod(date_str: str, csv_root: str, weekday_root: str, use_ledger: bool = True) -> int:
    # Invoke the bulk EOD CLI once; it scans csv_root for all INDEX/EXPIRY/STRIKE daily CSVs for date_str
    cmd = [
        sys.executable, "-m", "app.advanced.weekday_master_bulk_cli",
        "--csv-root", csv_root,
        "--weekday-root", weekday_root,
        "--date", date_str,
    ]
    if not use_ledger:
        cmd.append("--no-ledger")
    return run_subprocess(cmd)

def _print_summary(
    cfg: SessionConfig,
    date_str: str,
    metrics_ran: bool,
    eod_enabled: bool,
    eod_rc: int,
    started_at: dt.datetime,
    finished_at: dt.datetime,
) -> None:
    tz = cfg.MARKET_TIMEZONE
    win = f"{cfg.SESSION_START_HHMM}-{cfg.SESSION_END_HHMM}"
    metrics = "on" if metrics_ran else "off"
    eod_state = "on" if eod_enabled else "off"
    rc = eod_rc
    elapsed_s = int((finished_at - started_at).total_seconds())
    print(
        f"[SESSION] summary | date={date_str} tz={tz} window={win} metrics={metrics} "
        f"bulk_eod={eod_state} rc={rc} elapsed={elapsed_s}s"
    )

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    session_start_wall = dt.datetime.now(dt.timezone.utc)
    cfg = SessionConfig.from_env()

    print("[SESSION] Organizer starting")

    # Start system metrics writer upfront; keep reference to manage lifecycle
    sys_metrics_proc = _start_system_metrics_writer()

    # If started before market open, wait until configured start with optional pre-open heartbeats
    now_ist = now_in_tz(cfg.MARKET_TIMEZONE)
    if not is_in_session(now_ist, cfg.SESSION_START_HHMM, cfg.SESSION_END_HHMM):
        secs = seconds_until_hhmm(now_ist, cfg.SESSION_START_HHMM)
        if secs > 0:
            print(f"[SESSION] Waiting {secs}s until market open ({cfg.SESSION_START_HHMM} {cfg.MARKET_TIMEZONE})")
            try:
                # Emit pre-open heartbeats via advanced writer (best-effort)
                adv_cfg = AdvConfig.from_env()
                def emit_preopen_hb(remaining: int):
                    try:
                        write_options_analytics_adv(
                            config=adv_cfg,
                            tags={"scope": "session_preopen"},
                            fields={"ok": 1, "seconds_to_open": int(max(0, remaining))},
                            ts_utc=dt.datetime.now(dt.timezone.utc),
                        )
                    except Exception:
                        pass

                emit_preopen_hb(secs)
                while secs > 0:
                    time.sleep(min(30, secs))
                    now_ist = now_in_tz(cfg.MARKET_TIMEZONE)
                    secs = seconds_until_hhmm(now_ist, cfg.SESSION_START_HHMM)
                    emit_preopen_hb(secs)

            except KeyboardInterrupt:
                print("[SESSION] Interrupted while waiting for open")
                _print_summary(
                    cfg=cfg,
                    date_str=today_date_str(now_in_tz(cfg.MARKET_TIMEZONE), cfg.DATE_FMT),
                    metrics_ran=False,
                    eod_enabled=False,
                    eod_rc=130,
                    started_at=session_start_wall,
                    finished_at=dt.datetime.now(dt.timezone.utc),
                )
                _stop_process(sys_metrics_proc, "system_metrics_writer")
                return 130

    # Market open or already in session
    now_ist = now_in_tz(cfg.MARKET_TIMEZONE)
    date_str = today_date_str(now_ist, cfg.DATE_FMT)

    # Preflight checks
    if cfg.ENABLE_PREFLIGHT:
        ok = preflight_check()
        if not ok:
            print("[SESSION] Preflight failed; exiting")
            _print_summary(
                cfg=cfg,
                date_str=date_str,
                metrics_ran=False,
                eod_enabled=False,
                eod_rc=1,
                started_at=session_start_wall,
                finished_at=dt.datetime.now(dt.timezone.utc),
            )
            _stop_process(sys_metrics_proc, "system_metrics_writer")
            return 1

    # Optional: run env doctor
    if getattr(cfg, "RUN_ENV_DOCTOR", False):
        env_rc = _run_env_doctor(date_str, cfg.INDEX_LIST, cfg.SOURCES)
        if env_rc != 0:
            print("[SESSION] Env doctor reported missing inputs. Continuing; EOD may skip some combos.")

    metrics_ran = False

    # Minute-boundary advanced metrics during session (optional heartbeat)
    if cfg.ENABLE_ADV_METRICS:
        metrics_ran = True
        adv_cfg = AdvConfig.from_env()

        def should_continue() -> bool:
            t_ist = now_in_tz(cfg.MARKET_TIMEZONE)
            return is_in_session(t_ist, cfg.SESSION_START_HHMM, cfg.SESSION_END_HHMM)

        def on_tick(boundary_ist: dt.datetime) -> None:
            # Per-minute session heartbeat (IST aligned, written in UTC)
            ts_utc = boundary_ist.astimezone(dt.timezone.utc)
            for index in cfg.INDEX_LIST:
                tags = {
                    "index": index,
                    "scope": "session_heartbeat",
                }
                fields = {
                    "heartbeat": 1,
                }
                write_options_analytics_adv(
                    config=adv_cfg,
                    tags=tags,
                    fields=fields,
                    ts_utc=ts_utc,
                )
            print(f"[ADV-METRICS] minute boundary tick @ {ts_utc.isoformat().replace('+00:00','Z')}")

        print("[SESSION] Running minute-boundary advanced metrics")
        try:
            run_minutely(cfg.MARKET_TIMEZONE, should_continue, on_tick, align_immediately=True)
        except KeyboardInterrupt:
            print("[SESSION] Interrupted minute-boundary metrics")
    else:
        # Lightweight idle until close, also keep system_metrics_writer alive
        print(f"[SESSION] Live until close ({cfg.SESSION_END_HHMM} {cfg.MARKET_TIMEZONE}). Press Ctrl+C to stop early.")
        try:
            last_check = time.time()
            while True:
                now_ist = now_in_tz(cfg.MARKET_TIMEZONE)
                if not is_in_session(now_ist, cfg.SESSION_START_HHMM, cfg.SESSION_END_HHMM):
                    break

                # Every 30s, ensure system_metrics_writer is alive; restart if needed
                if time.time() - last_check >= 30:
                    if sys_metrics_proc and sys_metrics_proc.poll() is not None:
                        print("[SESSION] system_metrics_writer died; restarting")
                        sys_metrics_proc = _start_system_metrics_writer()
                    elif sys_metrics_proc is None:
                        # Try starting if it wasn't started earlier (e.g., psutil installed later)
                        sys_metrics_proc = _start_system_metrics_writer()
                    last_check = time.time()

                time.sleep(5)
        except KeyboardInterrupt:
            print("[SESSION] Interrupted during session; proceeding to EOD if enabled")

    # At/after session end: run Bulk EOD weekday masters
    eod_rc = 0
    if cfg.EOD_RUN_ENABLED:
        print("[SESSION] Running Bulk EOD (weekday masters)")
        eod_rc = _run_bulk_eod(
            date_str=date_str,
            csv_root=cfg.CSV_SPLIT_ROOT,
            weekday_root=cfg.WEEKDAY_ROOT,
            use_ledger=cfg.EOD_USE_LEDGER,
        )
        if eod_rc != 0:
            print("[SESSION] Bulk EOD completed with non-zero exit code")
        else:
            print("[SESSION] Bulk EOD completed successfully")
    else:
        print("[SESSION] EOD run disabled via config")

    print("[SESSION] Organizer done")

    # Final single-line summary
    session_end_wall = dt.datetime.now(dt.timezone.utc)
    _print_summary(
        cfg=cfg,
        date_str=date_str,
        metrics_ran=metrics_ran,
        eod_enabled=cfg.EOD_RUN_ENABLED,
        eod_rc=eod_rc,
        started_at=session_start_wall,
        finished_at=session_end_wall,
    )

    # Stop system metrics writer on exit
    _stop_process(sys_metrics_proc, "system_metrics_writer")

    return eod_rc

if __name__ == "__main__":
    raise SystemExit(main())
