#!/usr/bin/env python3
from __future__ import annotations
import datetime as dt
from typing import Dict, Optional

from .adv_config import AdvConfig
from .adv_io import WeekdayRow

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
except Exception:
    InfluxDBClient = None
    Point = None
    WritePrecision = None

def ist_hhmm_to_utc_ts(hhmm: str, ist_date: dt.date, tz_name: str) -> Optional[dt.datetime]:
    try:
        import pytz
        h, m = map(int, hhmm.split(":"))
        ist = pytz.timezone(tz_name)
        d_ist = ist.localize(dt.datetime(ist_date.year, ist_date.month, ist_date.day, h, m, 0))
        return d_ist.astimezone(dt.timezone.utc)
    except Exception:
        return None

class InfluxWeekdayUpdater:
    def write_weekday_updates(
        self,
        config: AdvConfig,
        agg_index: str,
        agg_expiry_code: str,
        agg_strike_offset: str,
        agg_weekday: str,
        rows: Dict[str, WeekdayRow],
        ist_date: Optional[dt.date] = None,
    ) -> None:
        """
        Write updates to measurement=config.MEAS_WEEKDAY_UPDATES with standard tags.
        Avoids importing AggregationKey to prevent circular imports.
        """
        if not config.ENABLE_INFLUX_WRITES:
            return
        if InfluxDBClient is None or Point is None:
            return

        client = InfluxDBClient(url=config.INFLUX_URL, token=config.INFLUX_TOKEN, org=config.INFLUX_ORG)
        write_api = client.write_api()

        if ist_date is None:
            ist_date = dt.datetime.now(dt.timezone.utc).astimezone().date()

        points = []
        for hhmm, wr in rows.items():
            ts_utc = ist_hhmm_to_utc_ts(hhmm, ist_date, config.MARKET_TIMEZONE)
            if ts_utc is None:
                continue
            p = (
                Point(config.MEAS_WEEKDAY_UPDATES)
                .tag("app", config.APP_TAG)
                .tag("env", config.ENV)
                .tag("host", config.HOST)
                .tag("index", agg_index)
                .tag("expiry_code", agg_expiry_code)
                .tag("strike_offset", agg_strike_offset)
                .tag("weekday", agg_weekday)
                .field("avg_tot", float(wr.avg_tot))
                .field("min_tot", float(wr.min_tot))
                .field("max_tot", float(wr.max_tot))
                .field("n_tot", int(wr.n_tot))
                .time(ts_utc, WritePrecision.NS)
            )
            points.append(p)

        if points:
            write_api.write(bucket=config.INFLUX_BUCKET, record=points)
        client.close()
