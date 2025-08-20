#!/usr/bin/env python3
import time
import os
from datetime import datetime, timezone
import psutil

from influxdb_client import InfluxDBClient, Point, WritePrecision

def main():
    url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token = os.getenv("INFLUXDB_TOKEN", "")
    org = os.getenv("INFLUXDB_ORG", "")
    bucket = os.getenv("INFLUXDB_BUCKET", "")
    interval = int(os.getenv("SYSTEM_METRICS_INTERVAL_SEC", "10"))

    if not (url and token and org and bucket):
        print("[SYS-METRICS] InfluxDB env not set; exiting")
        return 1

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api()

    last_net = psutil.net_io_counters()
    last_time = time.time()

    # Prime CPU percent
    psutil.cpu_percent(interval=None)

    try:
        while True:
            now = datetime.now(timezone.utc)
            cpu = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory().percent
            disk = psutil.disk_usage(os.getenv("SYSTEM_METRICS_DISK_PATH", "/")).percent

            cur_net = psutil.net_io_counters()
            cur_time = time.time()
            dt = max(0.001, cur_time - last_time)
            tx_mb_s = (cur_net.bytes_sent - last_net.bytes_sent) / dt / (1024 * 1024)
            rx_mb_s = (cur_net.bytes_recv - last_net.bytes_recv) / dt / (1024 * 1024)
            last_net = cur_net
            last_time = cur_time

            p = (
                Point("system_metrics")
                .field("cpu_percent", float(cpu))
                .field("mem_percent", float(mem))
                .field("disk_used_percent", float(disk))
                .field("net_tx_mb_s", float(tx_mb_s))
                .field("net_rx_mb_s", float(rx_mb_s))
                .time(now, WritePrecision.NS)
            )
            try:
                write_api.write(bucket=bucket, record=p)
            except Exception as e:
                print(f"[SYS-METRICS] write failed: {e}")

            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.close()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
