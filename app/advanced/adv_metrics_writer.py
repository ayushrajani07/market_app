#!/usr/bin/env python3
from __future__ import annotations
from typing import Dict, Any

from .adv_config import AdvConfig

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
except Exception:
    InfluxDBClient = None
    Point = None
    WritePrecision = None

def write_options_analytics_adv(config: AdvConfig, tags: Dict[str, str], fields: Dict[str, Any], ts_utc=None) -> None:
    """
    Writes derived metrics to the advanced-only measurement:
      measurement = options_analytics_adv
      tags: must include app=advanced, env, host + domain tags (index, expiry_code, strike_offset as applicable)
      fields: floats/ints for metrics (no duplication of core fields)
    """
    if InfluxDBClient is None or Point is None or not config.ENABLE_INFLUX_WRITES:
        return

    client = InfluxDBClient(url=config.INFLUX_URL, token=config.INFLUX_TOKEN, org=config.INFLUX_ORG)
    write_api = client.write_api()
    p = Point(config.MEAS_ADV_ANALYTICS)
    p = p.tag("app", config.APP_TAG).tag("env", config.ENV).tag("host", config.HOST)

    for k, v in tags.items():
        p = p.tag(k, str(v))

    for k, v in fields.items():
        if isinstance(v, bool) or v is None:
            continue
        if isinstance(v, int):
            p = p.field(k, int(v))
        else:
            p = p.field(k, float(v))

    if ts_utc is not None:
        p = p.time(ts_utc, WritePrecision.NS)

    write_api.write(bucket=config.INFLUX_BUCKET, record=p)
    client.close()
 