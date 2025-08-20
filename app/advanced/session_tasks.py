#!/usr/bin/env python3
from __future__ import annotations
import os
import subprocess
from typing import List, Optional

def run_subprocess(cmd: List[str]) -> int:
    try:
        print("[RUN]", " ".join(cmd))
        proc = subprocess.run(cmd, check=False)
        return proc.returncode
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        print(f"[ERROR] Subprocess failed: {e}")
        return 1

def preflight_check() -> bool:
    # Minimal preflight: environment sanity checks. Extend as needed.
    required_env = [
        "ADV_ROOT",
        "INFLUXDB_URL",
        "INFLUXDB_ORG",
        "INFLUXDB_BUCKET",
        "INFLUXDB_TOKEN",
        "MARKET_TIMEZONE",
    ]
    missing = [k for k in required_env if not os.getenv(k)]
    if missing:
        print(f"[WARN] Missing env vars: {', '.join(missing)} (proceeding anyway)")
    return True

def start_adv_metrics_loop(enabled: bool, tick_seconds: int) -> Optional["AdvMetricsLoop"]:
    if not enabled:
        print("[INFO] Advanced metrics loop disabled via config")
        return None
    loop = AdvMetricsLoop(tick_seconds=tick_seconds)
    loop.start()
    return loop

class AdvMetricsLoop:
    """
    Placeholder simple loop that you can extend to:
      - load features from Influx or raw files
      - compute IV-related metrics
      - write to options_analytics_adv (new measurement)
    For now, it just logs a heartbeat at the configured interval.
    """
    def __init__(self, tick_seconds: int = 30) -> None:
        self.tick_seconds = tick_seconds
        self._stop = False

    def start(self) -> None:
        import threading
        t = threading.Thread(target=self._run, name="adv-metrics-loop", daemon=True)
        t.start()
        print(f"[INFO] Advanced metrics loop started (tick={self.tick_seconds}s)")

    def stop(self) -> None:
        self._stop = True
        print("[INFO] Advanced metrics loop stop requested")

    def _run(self) -> None:
        import time, datetime as dt
        while not self._stop:
            try:
                print(f"[ADV-METRICS] heartbeat {dt.datetime.utcnow().isoformat()}Z")
                # TODO: insert metrics computation call here
                time.sleep(self.tick_seconds)
            except Exception as e:
                print(f"[ERROR] adv-metrics-loop: {e}")
                time.sleep(self.tick_seconds)
