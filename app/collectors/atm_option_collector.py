#!/usr/bin/env python3

import json
from datetime import date
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Iterable

import pytz

from app.brokers.kite_helpers import safe_call, parse_expiry, get_now
from app.brokers.expiry_discovery import (
    discover_weeklies_for_index,
    discover_monthlies_for_index,
)
from app.utils.time_utils import rounded_half_minute
from app.sinks.influx_sink import write_atm_leg  # centralized Influx mapping
from app.collectors.csv_sidecar import append_leg_csv  # CSV sidecar per leg

IST = pytz.timezone("Asia/Kolkata")

SPOT_SYMBOL = {
    "NIFTY": "NSE:NIFTY 50",
    "SENSEX": "BSE:SENSEX",
    "BANKNIFTY": "NSE:NIFTY BANK",
}

STEP = {"NIFTY": 50, "SENSEX": 100, "BANKNIFTY": 100}


def _derive_offset_by_steps(k: int) -> str:
    if k <= -2:
        return "atm_m2"
    if k == -1:
        return "atm_m1"
    if k == 0:
        return "atm"
    if k == 1:
        return "atm_p1"
    return "atm_p2"


class ATMOptionCollector:
    def __init__(
        self,
        kite_client,
        ensure_token,
        raw_dir: str = "data/raw_snapshots/options",
        use_dynamic_expiries: bool = True,
        influx_writer=None,
    ):
        self.kite = kite_client
        self.ensure_token = ensure_token
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.use_dynamic_expiries = use_dynamic_expiries
        self.influx_writer = influx_writer  # can be None

        # Preload instruments from NFO/BFO
        try:
            self.insts_nfo = self.kite.instruments(exchange="NFO") or []
        except Exception:
            self.insts_nfo = []
        try:
            self.insts_bfo = self.kite.instruments(exchange="BFO") or []
        except Exception:
            self.insts_bfo = []

        # Opening snapshots to compute diffs later in the day
        self.oi_open_map: Dict[str, int] = {}
        self.iv_open_map: Dict[str, float] = {}

    def _spot_price(self, idx: str) -> Optional[float]:
        q = safe_call(self.kite, self.ensure_token, "quote", [SPOT_SYMBOL[idx]]) or {}
        return q.get(SPOT_SYMBOL[idx], {}).get("last_price")

    def _pool_for_idx(self, idx: str) -> list:
        if idx == "SENSEX":
            return (self.insts_bfo or []) + (self.insts_nfo or [])
        return (self.insts_nfo or []) + (self.insts_bfo or [])

    def _discover_expiries_for_idx(self, idx: str, atm: int) -> Dict[str, List[date]]:
        weeklies = discover_weeklies_for_index(self.insts_nfo, self.insts_bfo, idx, atm)
        m_this, m_next = discover_monthlies_for_index(self.insts_nfo, self.insts_bfo, idx, atm)
        monthly = [d for d in [m_this, m_next] if d]
        return {"weekly": weeklies, "monthly": monthly}

    @staticmethod
    def _normalize_discovery(exps: dict) -> dict:
        def flat_list(x):
            if not x:
                return []
            if isinstance(x, (list, tuple)):
                out = []
                for v in x:
                    if isinstance(v, (list, tuple)):
                        out.extend(v)
                    else:
                        out.append(v)
                return out
            return [x]

        return {
            "weekly": flat_list(exps.get("weekly")),
            "monthly": flat_list(exps.get("monthly")),
        }

    @staticmethod
    def _as_date(exp_val):
        try:
            if hasattr(exp_val, "year") and hasattr(exp_val, "month") and hasattr(exp_val, "day"):
                return exp_val
            if isinstance(exp_val, (list, tuple)) and exp_val:
                return ATMOptionCollector._as_date(exp_val[0])
        except Exception:
            pass
        return None

    def _find_ce_pe_for_strike_and_exp(
        self, pool: list, idx: str, strike: int, exp
    ) -> Optional[Tuple[Tuple[str, dict], Tuple[str, dict]]]:
        """
        Returns ((ce_token, ce_inst), (pe_token, pe_inst)) for a given strike & expiry, else None.
        """
        UIDX = idx.upper()
        opts = [
            i
            for i in pool
            if str(i.get("segment", "")).endswith("-OPT")
            and i.get("strike") == strike
            and UIDX in str(i.get("tradingsymbol", "")).upper()
        ]
        for o in opts:
            try:
                o["_exp"] = parse_expiry(o)
            except Exception:
                o["_exp"] = None
        ce = next((o for o in opts if o.get("_exp") == exp and o.get("instrument_type") == "CE"), None)
        pe = next((o for o in opts if o.get("_exp") == exp and o.get("instrument_type") == "PE"), None)
        if ce and pe:
            return ((str(ce["instrument_token"]), ce), (str(pe["instrument_token"]), pe))
        return None

    def _gather_offset_pairs_for_bucket(
        self, pool: list, idx: str, atm: int, exp, offsets: Iterable[int]
    ) -> List[Tuple[str, dict, str, dict, int, str]]:
        """
        For an expiry bucket, collect CE/PE token+inst for each requested offset.
        Returns list of tuples: (ce_token, ce_inst, pe_token, pe_inst, strike, strike_offset_label)
        """
        out = []
        step = STEP.get(idx, 100)
        for k in offsets:
            strike = atm + int(k) * step
            found = self._find_ce_pe_for_strike_and_exp(pool, idx, strike, exp)
            if not found:
                continue
            (ce_tkn, ce_inst), (pe_tkn, pe_inst) = found
            out.append((ce_tkn, ce_inst, pe_tkn, pe_inst, strike, _derive_offset_by_steps(int(k))))
        return out

    def collect(self, offsets: Iterable[int] = (0,), counters: dict | None = None) -> Dict[str, Any]:
        """
        Collect option legs for each index/expiry and for each requested strike offset.
        offsets are in strike steps relative to ATM, e.g., (-2,-1,0,1,2).
        If counters is provided, increments counters['legs_written_this_loop'] for each successful Influx write.
        """
        snapshot_time = get_now().strftime("%Y%m%d_%H%M%S")
        legs: List[Dict[str, Any]] = []
        overview_aggs: Dict[str, Any] = {}

        for idx in ["NIFTY", "SENSEX", "BANKNIFTY"]:
            spot = self._spot_price(idx)
            if spot is None:
                continue
            atm = round(spot / STEP[idx]) * STEP[idx]
            pool = self._pool_for_idx(idx)
            exps = self._discover_expiries_for_idx(idx, atm) if self.use_dynamic_expiries else {}
            if self.use_dynamic_expiries:
                exps = self._normalize_discovery(exps)

            if idx == "BANKNIFTY":
                pairs = [
                    ("this_month", exps["monthly"][0] if len(exps.get("monthly") or []) > 0 else None),
                    ("next_month", exps["monthly"][1] if len(exps.get("monthly") or []) > 1 else None),
                ]
            else:
                pairs = [
                    ("this_week", exps["weekly"] if (exps.get("weekly") and len(exps["weekly"]) > 0) else None),
                    ("next_week", exps["weekly"][1] if (exps.get("weekly") and len(exps["weekly"]) > 1) else None),
                    ("this_month", exps["monthly"] if (exps.get("monthly") and len(exps["monthly"]) > 0) else None),
                    ("next_month", exps["monthly"][1] if (exps.get("monthly") and len(exps["monthly"]) > 1) else None),
                ]

            overview_aggs[idx] = {}
            for label, exp in pairs:
                if not exp:
                    continue
                exp_norm = self._as_date(exp)
                if not exp_norm:
                    continue

                # Build CE/PE pairs for all requested offsets, then quote them batched
                offset_pairs = self._gather_offset_pairs_for_bucket(pool, idx, atm, exp_norm, offsets)
                if not offset_pairs:
                    continue

                # Batch all tokens into one quote call for this index×bucket
                tokens = []
                for ce_tkn, _ce_inst, pe_tkn, _pe_inst, _strike, _offset_lbl in offset_pairs:
                    tokens.append(ce_tkn)
                    tokens.append(pe_tkn)
                qdata = safe_call(self.kite, self.ensure_token, "quote", tokens) or {}

                # Overview aggregate for ATM only (offset 0) if present
                agg_tp = None
                agg_iv = None
                agg_oi_c = None
                agg_oi_p = None
                for ce_tkn, _ce_inst, pe_tkn, _pe_inst, strike, off_lbl in offset_pairs:
                    if off_lbl == "atm":
                        ce_q = qdata.get(str(ce_tkn), {}) or {}
                        pe_q = qdata.get(str(pe_tkn), {}) or {}
                        agg_tp = sum(x for x in [ce_q.get("last_price"), pe_q.get("last_price")] if isinstance(x, (int, float)))
                        agg_oi_c = ce_q.get("oi")
                        agg_oi_p = pe_q.get("oi")
                        if isinstance(ce_q.get("iv"), (int, float)) and isinstance(pe_q.get("iv"), (int, float)):
                            agg_iv = (ce_q.get("iv") + pe_q.get("iv")) / 2.0
                        break

                iv_key = f"{idx}_{label}_iv_open"
                if agg_iv is not None and iv_key not in self.iv_open_map:
                    self.iv_open_map[iv_key] = agg_iv
                iv_open_val = self.iv_open_map.get(iv_key)
                iv_day_change = (
                    (iv_open_val - agg_iv)
                    if (isinstance(iv_open_val, (int, float)) and isinstance(agg_iv, (int, float)))
                    else None
                )

                overview_aggs[idx][label] = {
                    "TP": agg_tp,
                    "OI_CALL": agg_oi_c,
                    "OI_PUT": agg_oi_p,
                    "PCR": ((agg_oi_p / agg_oi_c) if isinstance(agg_oi_p, int) and isinstance(agg_oi_c, int) and agg_oi_c != 0 else None),
                    "atm_iv": agg_iv,
                    "iv_open": iv_open_val,
                    "iv_day_change": iv_day_change,
                    "days_to_expiry": (exp_norm - date.today()).days,
                }

                # Emit legs for all offsets, using batch quotes
                for ce_tkn, ce_inst, pe_tkn, pe_inst, strike, off_lbl in offset_pairs:
                    ce_q = qdata.get(str(ce_tkn), {}) or {}
                    pe_q = qdata.get(str(pe_tkn), {}) or {}

                    # Seed opening OI/IV per token
                    for tkn, q in [(ce_tkn, ce_q), (pe_tkn, pe_q)]:
                        if tkn not in self.oi_open_map and isinstance(q.get("oi"), int):
                            self.oi_open_map[tkn] = q.get("oi")
                        if tkn not in self.iv_open_map and isinstance(q.get("iv"), (int, float)):
                            self.iv_open_map[tkn] = q.get("iv")

                    for side, tkn, inst, q in [
                        ("CALL", ce_tkn, ce_inst, ce_q),
                        ("PUT", pe_tkn, pe_inst, pe_q),
                    ]:
                        oi_open = self.oi_open_map.get(tkn)
                        oi_curr = q.get("oi")
                        oi_change = ((oi_open - oi_curr) if (isinstance(oi_open, int) and isinstance(oi_curr, int)) else None)

                        strike_val = None
                        try:
                            if isinstance(inst, dict):
                                strike_val = inst.get("strike")
                        except Exception:
                            strike_val = None

                        rec = {
                            "timestamp": rounded_half_minute(get_now()),  # aware dt, rounded
                            "index": idx,
                            "bucket": label,  # expiry_code bucket
                            "side": side,  # CALL/PUT
                            "expiry": exp_norm.isoformat(),  # YYYY-MM-DD
                            "atm_strike": atm,
                            "strike": strike_val,
                            "strike_offset": off_lbl,
                            "last_price": q.get("last_price"),
                            "days_to_expiry": (exp_norm - date.today()).days,
                            "average_price": q.get("average_price"),
                            "volume": q.get("volume"),
                            "oi": oi_curr,
                            "oi_open": oi_open,
                            "oi_change": oi_change,
                            "ohlc.open": q.get("ohlc", {}).get("open"),
                            "ohlc.high": q.get("ohlc", {}).get("high"),
                            "ohlc.low": q.get("ohlc", {}).get("low"),
                            "ohlc.close": q.get("ohlc", {}).get("close"),
                            "net_change": (
                                (q.get("last_price") - q.get("ohlc", {}).get("close"))
                                if isinstance(q.get("last_price"), (int, float))
                                and isinstance(q.get("ohlc", {}).get("close"), (int, float))
                                else None
                            ),
                            "net_change_percent": (
                                ((q.get("last_price") - q.get("ohlc", {}).get("close")) / q.get("ohlc", {}).get("close") * 100.0)
                                if isinstance(q.get("last_price"), (int, float))
                                and isinstance(q.get("ohlc", {}).get("close"), (int, float))
                                and q.get("ohlc", {}).get("close") != 0
                                else None
                            ),
                            "day_change": (
                                (q.get("last_price") - q.get("ohlc", {}).get("open"))
                                if isinstance(q.get("last_price"), (int, float))
                                and isinstance(q.get("ohlc", {}).get("open"), (int, float))
                                else None
                            ),
                            "day_change_percent": (
                                ((q.get("last_price") - q.get("ohlc", {}).get("open")) / q.get("ohlc", {}).get("open") * 100.0)
                                if isinstance(q.get("last_price"), (int, float))
                                and isinstance(q.get("ohlc", {}).get("open"), (int, float))
                                and q.get("ohlc", {}).get("open") != 0
                                else None
                            ),
                            "iv": q.get("iv"),
                        }

                        legs.append(rec)

                        # Persist JSON per leg (best-effort)
                        bdir = self.raw_dir / label
                        bdir.mkdir(parents=True, exist_ok=True)
                        fname = f"{idx}_{side}_{label}_{exp_norm.isoformat()}_{get_now().strftime('%Y%m%d_%H%M%S')}.json"
                        json_path = bdir / fname
                        try:
                            with open(json_path, "w") as f:
                                json.dump(rec, f, indent=2)
                        except Exception:
                            pass

                        # CSV sidecar (best-effort)
                        try:
                            append_leg_csv(str(json_path), rec)
                        except Exception:
                            pass

                        # Optional Influx write (best-effort)
                        try:
                            if self.influx_writer:
                                write_atm_leg(rec, self.influx_writer)
                                # NEW: increment counter for each successful write
                                if counters is not None:
                                    counters["legs_written_this_loop"] = counters.get("legs_written_this_loop", 0) + 1
                        except Exception:
                            pass

        return {"legs": legs, "overview_aggs": overview_aggs}
