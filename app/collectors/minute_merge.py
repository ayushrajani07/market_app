# app/collectors/minute_merge.py

from collections import defaultdict
from typing import Dict, Tuple, Any, List

# Existing function signature assumed; we extend output while keeping backward compatibility.
# Input: legs (list of leg dicts with index, bucket, strike_offset, side, timestamp, etc.)
# Output: existing merged_map plus two new maps: p1m1_map and p2m2_map.

def merge_call_put_to_rows(
    legs: List[Dict[str, Any]],
    step_hint_by_index: Dict[str, int] | None = None
) -> Dict[Tuple[str, str, str, str], Dict[str, Any]]:
    """
    Existing behavior:
      - Merge symmetric CALL+PUT legs by (timestamp_ist, index, expiry_code, strike_offset) into one row.
    New behavior:
      - Additionally prepare two asymmetric merges:
          atm_p1m1: +1 CALL with -1 PUT
          atm_p2m2: +2 CALL with -2 PUT
      - These are exposed via keys suffixed with special group labels so the caller can write to separate CSVs.

    Return:
      merged_map: {(ts_ist, idx, expiry_code, offset): row}
      merged_map__p1m1: {(ts_ist, idx, expiry_code, "atm_p1m1"): row}
      merged_map__p2m2: {(ts_ist, idx, expiry_code, "atm_p2m2"): row}
    """
    # 1) Existing symmetric merge (unchanged)
    by_key = defaultdict(dict)  # key -> {"CALL": leg, "PUT": leg}
    for leg in legs:
        ts_ist = leg.get("timestamp")  # aware dt rounded by collector
        idx = leg.get("index")
        expc = leg.get("bucket")
        off = leg.get("strike_offset")  # "atm","atm_p1","atm_m1",...
        side = leg.get("side")  # "CALL"/"PUT"
        if not (ts_ist and idx and expc and off and side):
            continue
        key = (ts_ist, idx, expc, off)
        by_key[key][side] = leg

    merged_map = {}
    for key, sides in by_key.items():
        call = sides.get("CALL")
        put = sides.get("PUT")
        if not call or not put:
            continue
        ts_ist, idx, expc, off = key
        row = _build_row_pair(ts_ist, idx, expc, off, call, put, label=None)
        merged_map[key] = row

    # 2) New asymmetric pairs: p1m1 and p2m2
    # Index → (timestamp, expiry) → offset → leg_by_side
    grid = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    for leg in legs:
        ts_ist = leg.get("timestamp")
        idx = leg.get("index")
        expc = leg.get("bucket")
        off = leg.get("strike_offset")
        side = leg.get("side")
        if not (ts_ist and idx and expc and off and side):
            continue
        grid[(ts_ist, idx)][expc][off][side] = leg

    merged_map__p1m1 = {}
    merged_map__p2m2 = {}

    def get_pair(expc_map, call_off: str, put_off: str):
        call_leg = (expc_map.get(call_off) or {}).get("CALL")
        put_leg = (expc_map.get(put_off) or {}).get("PUT")
        return call_leg, put_leg

    for (ts_ist, idx), expc_map in grid.items():
        for expc, by_off in expc_map.items():
            # p1m1: atm_p1 CALL + atm_m1 PUT
            c1, p1 = get_pair(by_off, "atm_p1", "atm_m1")
            if c1 and p1:
                key = (ts_ist, idx, expc, "atm_p1m1")
                merged_map__p1m1[key] = _build_row_pair(ts_ist, idx, expc, "atm_p1m1", c1, p1, label="atm_p1m1")

            # p2m2: atm_p2 CALL + atm_m2 PUT
            c2, p2 = get_pair(by_off, "atm_p2", "atm_m2")
            if c2 and p2:
                key = (ts_ist, idx, expc, "atm_p2m2")
                merged_map__p2m2[key] = _build_row_pair(ts_ist, idx, expc, "atm_p2m2", c2, p2, label="atm_p2m2")

    # Attach new maps to result container for callers that expect only merged_map
    # To preserve compatibility, return a dict-like with extra keys; callers that iterate items() still work.
    # If the original caller expects a plain dict of rows, adapt there to pick merged_map only.
    result = {
        "merged_map": merged_map,
        "merged_map__p1m1": merged_map__p1m1,
        "merged_map__p2m2": merged_map__p2m2,
    }
    return result


def _build_row_pair(ts_ist, idx, expc, off_label, call_leg, put_leg, label: str | None):
    """Build one CSV row combining CALL+PUT legs."""
    def fnum(x):
        return float(x) if isinstance(x, (int, float)) else None
    def fint(x):
        return int(x) if isinstance(x, (int, float)) else None

    row = {
        "timestamp": ts_ist,              # aware dt
        "index": idx,
        "expiry_code": expc,
        "group": off_label if label else expc,  # keep original behavior; 'group' captures our custom name
        "strike_offset": off_label,       # e.g., "atm", "atm_p1m1"
        # CALL fields
        "call_strike": call_leg.get("strike"),
        "call_last_price": fnum(call_leg.get("last_price")),
        "call_iv": fnum(call_leg.get("iv")),
        "call_volume": fint(call_leg.get("volume")),
        "call_oi": fint(call_leg.get("oi")),
        # PUT fields
        "put_strike": put_leg.get("strike"),
        "put_last_price": fnum(put_leg.get("last_price")),
        "put_iv": fnum(put_leg.get("iv")),
        "put_volume": fint(put_leg.get("volume")),
        "put_oi": fint(put_leg.get("oi")),
        # Totals
        "tp_sum": fnum(
            (call_leg.get("last_price") if isinstance(call_leg.get("last_price"), (int, float)) else 0)
            + (put_leg.get("last_price") if isinstance(put_leg.get("last_price"), (int, float)) else 0)
        ),
    }
    return row
