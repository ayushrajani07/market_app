#!/usr/bin/env python3
from __future__ import annotations
import os
from .adv_config import AdvConfig

def raw_path_for(config: AdvConfig, source_code: str, index: str, expiry_date: str, option: str = "PUT", stamp: str = "") -> str:
    """
    Resolve path using RAW_FILE_NAME_PATTERN with placeholders:
      {index}, {expiry}, {date}, {option}, {stamp}
    - source_code -> {expiry} (this_week/this_month/...)
    - expiry_date -> {date} (e.g., 2025-08-19)
    - option -> {option} (CALL/PUT)
    - stamp -> {stamp} (e.g., 20250818_125430) optional
    """
    sc = source_code.strip().lower()
    if sc == "overview":
        root = config.RAW_OVERVIEW_ROOT
    elif sc == "this_week":
        root = config.RAW_OPTIONS_THIS_WEEK_ROOT
    elif sc == "next_week":
        root = config.RAW_OPTIONS_NEXT_WEEK_ROOT
    elif sc == "this_month":
        root = config.RAW_OPTIONS_THIS_MONTH_ROOT
    elif sc == "next_month":
        root = config.RAW_OPTIONS_NEXT_MONTH_ROOT
    else:
        raise ValueError(f"Unknown source_code: {source_code}")

    relative = config.RAW_FILE_NAME_PATTERN.format(
        index=index,
        expiry=source_code,
        date=expiry_date,
        option=option,
        stamp=stamp,
    )
    return os.path.join(root, relative)
