from datetime import datetime, UTC
from typing import Optional, Dict, Any
import os
from dotenv import load_dotenv

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions

load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name, "")
    if val == "" or val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


class InfluxWriter:
    def __init__(self, on_success=None, on_error=None, on_retry=None):
        url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        token = os.getenv("INFLUXDB_TOKEN", "")
        org = os.getenv("INFLUXDB_ORG", "")
        bucket = os.getenv("INFLUXDB_BUCKET", "")
        batch_size = int(os.getenv("INFLUXDB_WRITE_BATCH_SIZE", "0"))
        flush_ms = int(os.getenv("INFLUXDB_WRITE_FLUSH_MS", "1000"))

        if not url or not token or not org or not bucket:
            raise RuntimeError(
                "Missing InfluxDB credentials in environment. "
                "Please set INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET"
            )

        self._track_stats = _env_bool("INFLUX_WRITE_STATS_ENABLE", False)
        self._stats_always_emit = _env_bool("INFLUX_WRITE_STATS_ALWAYS_EMIT", False)
        self._stats_debug = _env_bool("INFLUX_WRITE_STATS_DEBUG", False)

        print(
            f"[INF-CONFIG] url={url} org={org} bucket={bucket} "
            f"batch_size={batch_size} flush_ms={flush_ms} "
            f"stats_enable={self._track_stats} always_emit={self._stats_always_emit} debug={self._stats_debug}"
        )

        self.bucket = bucket
        self.client = InfluxDBClient(url=url, token=token, org=org)

        if batch_size > 0:
            write_options = WriteOptions(
                batch_size=batch_size,
                flush_interval=flush_ms,
                jitter_interval=500,
                retry_interval=2000,
                max_retries=5,
                max_retry_delay=60000,
                exponential_base=2,
            )
            self.write_api = self.client.write_api(
                write_options=write_options,
                success_callback=on_success,
                error_callback=on_error,
                retry_callback=on_retry,
            )
        else:
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

        self._points_written = 0
        self._points_rejected = 0

    def health_check(self):
        p = (
            Point("pipeline_health")
            .tag("check", "startup")
            .field("ok", 1)
            .time(datetime.now(UTC), WritePrecision.NS)
        )
        self.write_api.write(bucket=self.bucket, record=p)

    def write_point(
        self,
        measurement: str,
        tags: Optional[Dict[str, str]] = None,
        fields: Optional[Dict[str, Any]] = None,
        ts_utc: Optional[datetime] = None
    ):
        ts = ts_utc or datetime.now(UTC)
        p = Point(measurement)
        if tags:
            for k, v in tags.items():
                if v is not None:
                    p = p.tag(k, str(v))
        if fields:
            for k, v in fields.items():
                if v is None:
                    continue
                try:
                    if isinstance(v, (int, float, bool)):
                        p = p.field(k, v)
                    else:
                        p = p.field(k, float(v))
                except Exception:
                    p = p.field(k, str(v))
        p = p.time(ts, WritePrecision.NS)

        try:
            self.write_api.write(bucket=self.bucket, record=p)
            if self._track_stats:
                self._points_written += 1
        except Exception:
            if self._track_stats:
                self._points_rejected += 1
            raise

    def flush_stats(self):
        if not self._track_stats:
            return

        if not self._stats_always_emit and self._points_written == 0 and self._points_rejected == 0:
            if self._stats_debug:
                print("[INF-STATS] nothing to emit (0/0)")
            return

        pt = (
            Point("influx_write_stats")
            .field("points_written", int(self._points_written))
            .field("points_rejected", int(self._points_rejected))
            .time(datetime.now(UTC), WritePrecision.NS)
        )
        try:
            self.write_api.write(bucket=self.bucket, record=pt)
            if self._stats_debug:
                print(f"[INF-STATS] EMIT written={self._points_written} rejected={self._points_rejected}")
        finally:
            self._points_written = 0
            self._points_rejected = 0

    def close(self):
        try:
            self.flush_stats()
        except Exception:
            if self._stats_debug:
                print("[INF-STATS] final flush failed; continuing close")
        self.client.close()
