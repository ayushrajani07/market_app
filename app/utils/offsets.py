#!/usr/bin/env python3
from __future__ import annotations

def offset_label(delta_steps: int) -> str:
    # Map integer steps to labels; extend as needed
    if delta_steps <= -2: return "atm_m2"
    if delta_steps == -1: return "atm_m1"
    if delta_steps == 0:  return "atm"
    if delta_steps == 1:  return "atm_p1"
    return "atm_p2"

def derive_offset(strike: int | None, atm: int | None, step: int) -> str:
    if strike is None or atm is None:
        return "atm"
    try:
        k = round((int(strike) - int(atm)) / int(step))
        return offset_label(k)
    except Exception:
        return "atm"
