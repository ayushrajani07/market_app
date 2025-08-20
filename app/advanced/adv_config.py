#!/usr/bin/env python3
from __future__ import annotations

import os
from dataclasses import dataclass

def _get_env_bool(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).strip().lower() in {"1", "true", "yes", "on"}

WEEKDAY_CODE = {0: "mon", 1: "tue", 2: "wed", 3: "thu", 4: "fri", 5: "sat", 6: "sun"}
BASE_OFFSETS = ("atm_m2", "atm_m1", "atm", "atm_p1", "atm_p2")
PAIR_OFFSETS = ("atm_p1m1", "atm_p2m2")

@dataclass(frozen=True)
class AdvConfig:
    ADV_ROOT: str

    INFLUX_URL: str
    INFLUX_ORG: str
    INFLUX_BUCKET: str
    INFLUX_TOKEN: str

    ENV: str = "prod"
    HOST: str = os.getenv("HOST", os.getenv("HOSTNAME", "unknown-host"))
    APP_TAG: str = "advanced"
    MARKET_TIMEZONE: str = "Asia/Kolkata"
    SESSION_START_HHMM: str = "09:15"
    SESSION_END_HHMM: str = "15:30"

    ENABLE_INFLUX_WRITES: bool = False
    ATOMIC_CSV_WRITES: bool = True
    ALIGN_TO_MINUTE: bool = True

    ENABLE_STREAMING: bool = False

    # NEW: concise output controls
    LOG_SUMMARY_ONLY: bool = False
    LOG_LAST_TOTALS: bool = False

    STRIKE_STEP_NIFTY: int = 50
    STRIKE_STEP_BANKNIFTY: int = 100
    STRIKE_STEP_SENSEX: int = 100

    WEEKDAY_MASTER_PATH: str = "{ADV_ROOT}/weekday_masters/{index}/{expiry_code}/{strike_offset}/{weekday}.csv"

    MEAS_WEEKDAY_UPDATES: str = "weekday_master_updates"
    MEAS_ADV_ANALYTICS: str = "options_analytics_adv"
    MEAS_ADV_DIAGNOSTICS: str = "advanced_diagnostics"

    RAW_OVERVIEW_ROOT: str = ""
    RAW_OPTIONS_THIS_WEEK_ROOT: str = ""
    RAW_OPTIONS_NEXT_WEEK_ROOT: str = ""
    RAW_OPTIONS_THIS_MONTH_ROOT: str = ""
    RAW_OPTIONS_NEXT_MONTH_ROOT: str = ""

    RAW_FILE_NAME_PATTERN: str = "{index}/{date}.csv"

    @staticmethod
    def from_env() -> "AdvConfig":
        try:
            from dotenv import load_dotenv
            import pathlib
            load_dotenv(dotenv_path=pathlib.Path(__file__).resolve().parents[2] / ".env", override=False)
        except Exception:
            pass

        return AdvConfig(
            ADV_ROOT=os.getenv("ADV_ROOT", "./data_adv"),
            INFLUX_URL=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
            INFLUX_ORG=os.getenv("INFLUXDB_ORG", ""),
            INFLUX_BUCKET=os.getenv("INFLUXDB_BUCKET", ""),
            INFLUX_TOKEN=os.getenv("INFLUXDB_TOKEN", ""),
            ENV=os.getenv("ENV", "prod"),
            HOST=os.getenv("HOST", os.getenv("HOSTNAME", "unknown-host")),
            APP_TAG="advanced",
            MARKET_TIMEZONE=os.getenv("MARKET_TIMEZONE", "Asia/Kolkata"),
            SESSION_START_HHMM=os.getenv("SESSION_START_HHMM", "09:15"),
            SESSION_END_HHMM=os.getenv("SESSION_END_HHMM", "15:30"),
            ENABLE_INFLUX_WRITES=_get_env_bool("ADV_ENABLE_INFLUX_WRITES", "false"),
            ATOMIC_CSV_WRITES=_get_env_bool("ADV_ATOMIC_CSV_WRITES", "true"),
            ALIGN_TO_MINUTE=_get_env_bool("ALIGN_TO_MINUTE", "true"),
            ENABLE_STREAMING=_get_env_bool("ADV_ENABLE_STREAMING", "false"),
            LOG_SUMMARY_ONLY=_get_env_bool("ADV_LOG_SUMMARY_ONLY", "true"),
            LOG_LAST_TOTALS=_get_env_bool("ADV_LOG_LAST_TOTALS", "false"),
            STRIKE_STEP_NIFTY=int(os.getenv("STRIKE_STEP_NIFTY", "50")),
            STRIKE_STEP_BANKNIFTY=int(os.getenv("STRIKE_STEP_BANKNIFTY", "100")),
            STRIKE_STEP_SENSEX=int(os.getenv("STRIKE_STEP_SENSEX", "100")),
            WEEKDAY_MASTER_PATH=os.getenv(
                "WEEKDAY_MASTER_PATH",
                "{ADV_ROOT}/weekday_masters/{index}/{expiry_code}/{strike_offset}/{weekday}.csv",
            ),
            MEAS_WEEKDAY_UPDATES=os.getenv("MEAS_WEEKDAY_UPDATES", "weekday_master_updates"),
            MEAS_ADV_ANALYTICS=os.getenv("MEAS_ADV_ANALYTICS", "options_analytics_adv"),
            MEAS_ADV_DIAGNOSTICS=os.getenv("MEAS_ADV_DIAGNOSTICS", "advanced_diagnostics"),
            RAW_OVERVIEW_ROOT=os.getenv("RAW_OVERVIEW_ROOT", ""),
            RAW_OPTIONS_THIS_WEEK_ROOT=os.getenv("RAW_OPTIONS_THIS_WEEK_ROOT", ""),
            RAW_OPTIONS_NEXT_WEEK_ROOT=os.getenv("RAW_OPTIONS_NEXT_WEEK_ROOT", ""),
            RAW_OPTIONS_THIS_MONTH_ROOT=os.getenv("RAW_OPTIONS_THIS_MONTH_ROOT", ""),
            RAW_OPTIONS_NEXT_MONTH_ROOT=os.getenv("RAW_OPTIONS_NEXT_MONTH_ROOT", ""),
            RAW_FILE_NAME_PATTERN=os.getenv("RAW_FILE_NAME_PATTERN", "{index}/{date}.csv"),
        )
