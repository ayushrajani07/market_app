#!/usr/bin/env python3
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import List

def _get_bool(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).strip().lower() in {"1", "true", "yes", "on"}

@dataclass(frozen=True)
class SessionConfig:
    # Session orchestration
    MARKET_TIMEZONE: str
    SESSION_START_HHMM: str
    SESSION_END_HHMM: str
    INDEX_LIST: List[str]
    SOURCES: List[str]
    OFFSETS: List[str]
    DATE_FMT: str

    # Controls
    ENABLE_PREFLIGHT: bool
    ENABLE_ADV_METRICS: bool
    ADV_METRICS_TICK_SECONDS: int
    EOD_RUN_ENABLED: bool

    @staticmethod
    def from_env() -> "SessionConfig":
        # Lazy load .env if available
        try:
            from dotenv import load_dotenv
            import pathlib
            load_dotenv(pathlib.Path(__file__).resolve().parents[2] / ".env", override=False)
        except Exception:
            pass

        indexes = [x.strip() for x in os.getenv("SESSION_INDEXES", "NIFTY 50,NIFTY BANK,SENSEX").split(",") if x.strip()]
        sources = [x.strip() for x in os.getenv("SESSION_SOURCES", "this_week,this_month").split(",") if x.strip()]
        offsets = [x.strip() for x in os.getenv("SESSION_OFFSETS", "atm_m2,atm_m1,atm,atm_p1,atm_p2").split(",") if x.strip()]

        return SessionConfig(
            MARKET_TIMEZONE=os.getenv("MARKET_TIMEZONE", "Asia/Kolkata"),
            SESSION_START_HHMM=os.getenv("SESSION_START_HHMM", "09:15"),
            SESSION_END_HHMM=os.getenv("SESSION_END_HHMM", "15:30"),
            INDEX_LIST=indexes,
            SOURCES=sources,
            OFFSETS=offsets,
            DATE_FMT=os.getenv("SESSION_DATE_FMT", "%Y-%m-%d"),
            ENABLE_PREFLIGHT=_get_bool("SESSION_ENABLE_PREFLIGHT", "true"),
            ENABLE_ADV_METRICS=_get_bool("SESSION_ENABLE_ADV_METRICS", "false"),
            ADV_METRICS_TICK_SECONDS=int(os.getenv("ADV_METRICS_TICK_SECONDS", "30")),
            EOD_RUN_ENABLED=_get_bool("SESSION_EOD_RUN_ENABLED", "true"),
        )
