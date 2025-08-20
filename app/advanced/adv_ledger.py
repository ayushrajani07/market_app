#!/usr/bin/env python3
from __future__ import annotations
import os
from typing import Set

def ledger_path_for(weekday_csv_path: str) -> str:
    # Ledger sits next to the weekday CSV with .ledger extension
    return weekday_csv_path + ".ledger"

def load_ledger(weekday_csv_path: str) -> Set[str]:
    path = ledger_path_for(weekday_csv_path)
    if not os.path.exists(path):
        return set()
    out: Set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                out.add(s)
    return out

def append_ledger(weekday_csv_path: str, date_str: str) -> None:
    path = ledger_path_for(weekday_csv_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(date_str + "\n")
