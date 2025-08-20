#!/usr/bin/env python3
"""
Path Hardcoding Audit

Scans the repository for:
- Hardcoded absolute paths (Windows + Unix)
- Risky Python patterns (os.chdir, sys.path.append)
- open()/Path() with absolute-looking string literals
- YAML/JSON keys that likely point to absolute host paths

Skips noisy/generated dirs:
- data/raw_snapshots
- __pycache__
- venv/.venv

Usage:
  python tools/path_audit.py
  python tools/path_audit.py --json > path_audit_report.json

Exit code:
  0 if no findings, 1 if any findings are reported.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Iterable, List, Dict, Any, Tuple

# ----------------------------
# Configuration
# ----------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]

EXCLUDE_DIRS = {
    "data/raw_snapshots",
    "__pycache__",
    "venv",
    ".venv",
    ".git",
    ".idea",
    ".vscode",
}

# File globs to check; None means "all files"
INCLUDE_EXTENSIONS = None  # e.g., {".py",".yml",".yaml",".json",".sh",".ps1"}

# Patterns
ABS_PATH_PATTERNS = [
    re.compile(r"[A-Z]:\\", re.IGNORECASE),         # Windows drive paths
    re.compile(r"\\\\\\\\"),                        # Windows UNC "\\server\share" (escaped)
    re.compile(r"/home/"),                          # Unix home
    re.compile(r"/Users/"),                         # macOS home
    re.compile(r"/var/"),                           # Unix system path
    re.compile(r"/opt/"),                           # Unix optional software
]

PY_RISKY_PATTERNS = [
    re.compile(r"\bos\.chdir\s*\("),               # changing CWD
    re.compile(r"\bsys\.path\.append\s*\("),       # path hacks
]

# open() or Path() with absolute-like literal
PY_OPEN_ABS_PATTERN = re.compile(
    r"""(?:\bopen\s*\(\s*|Path\s*\(\s*)        # open(  or Path(
        [rRuU]?                                # optional raw/unicode prefixes
        (["'])                                 # quote
        (\/|[A-Z]:\\)                          # starts with / or C:\ 
        """,
    re.VERBOSE,
)

# YAML/JSON lines like: path: "/abs" or file: "C:\abs"
YAML_JSON_PATH_KEYS = re.compile(
    r"""^\s*(path|file|root|dir|directory|mount|source)\s*:\s*["']?(\/|[A-Z]:\\)""",
    re.IGNORECASE | re.VERBOSE,
)

# ----------------------------
# Helpers
# ----------------------------

def is_excluded(path: Path) -> bool:
    try:
        rel = path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return True
    parts = rel.split("/")
    # Exclude if any segment matches an excluded dir
    for i in range(len(parts)):
        sub = "/".join(parts[: i + 1])
        for ex in EXCLUDE_DIRS:
            if sub == ex or sub.startswith(ex + "/"):
                return True
    return False


def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file() and not is_excluded(p):
            if INCLUDE_EXTENSIONS is None or p.suffix in INCLUDE_EXTENSIONS:
                yield p


def scan_file(path: Path) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        return [{
            "file": str(path.relative_to(REPO_ROOT)),
            "line": None,
            "category": "read_error",
            "pattern": None,
            "snippet": str(e),
            "suggestion": "Check file encoding/permissions."
        }]

    is_py = path.suffix == ".py"
    is_yaml_json = path.suffix.lower() in {".yml", ".yaml", ".json"}

    lines = text.splitlines()
    for i, line in enumerate(lines, start=1):
        # Broad absolute path patterns (any file)
        for pat in ABS_PATH_PATTERNS:
            if pat.search(line):
                findings.append({
                    "file": str(path.relative_to(REPO_ROOT)),
                    "line": i,
                    "category": "absolute_path",
                    "pattern": pat.pattern,
                    "snippet": line.strip(),
                    "suggestion": "Replace hardcoded absolute path with repo_path(...), env var, or container-relative path."
                })
                break  # avoid duplicate categories on same line

        # Python-specific risky usage
        if is_py:
            for pat in PY_RISKY_PATTERNS:
                if pat.search(line):
                    suggestion = "Prefer repo_path(...) and package imports. Avoid os.chdir/sys.path hacks."
                    findings.append({
                        "file": str(path.relative_to(REPO_ROOT)),
                        "line": i,
                        "category": "python_risky",
                        "pattern": pat.pattern,
                        "snippet": line.strip(),
                        "suggestion": suggestion
                    })
            if PY_OPEN_ABS_PATTERN.search(line):
                findings.append({
                    "file": str(path.relative_to(REPO_ROOT)),
                    "line": i,
                    "category": "python_open_path_literal",
                    "pattern": "open()/Path() with absolute-like literal",
                    "snippet": line.strip(),
                    "suggestion": "Do not hardcode absolute literals in open()/Path(); use repo_path(...) or env_path(...)."
                })

        # YAML/JSON path-ish keys
        if is_yaml_json and YAML_JSON_PATH_KEYS.search(line):
            findings.append({
                "file": str(path.relative_to(REPO_ROOT)),
                "line": i,
                "category": "yaml_json_abs_path",
                "pattern": YAML_JSON_PATH_KEYS.pattern,
                "snippet": line.strip(),
                "suggestion": "Use relative repo paths (./path) or Docker/container paths; avoid host-specific absolutes."
            })

    return findings


def summarize(findings: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_cat: Dict[str, int] = {}
    by_file: Dict[str, int] = {}
    for f in findings:
        by_cat[f["category"]] = by_cat.get(f["category"], 0) + 1
        by_file[f["file"]] = by_file.get(f["file"], 0) + 1
    return {"total": len(findings), "by_category": by_cat, "by_file": by_file}


def print_human(findings: List[Dict[str, Any]]) -> None:
    summary = summarize(findings)
    print("== Path Audit Report ==")
    print(f"Repo: {REPO_ROOT}")
    print(f"Total findings: {summary['total']}")
    print("\nBy category:")
    for k, v in sorted(summary["by_category"].items(), key=lambda x: x[0]):
        print(f"  - {k}: {v}")
    print("\nFindings:")
    for f in findings:
        loc = f"{f['file']}:{f['line']}" if f['line'] is not None else f['file']
        print(f"- [{f['category']}] {loc}")
        if f.get("pattern"):
            print(f"  pattern: {f['pattern']}")
        print(f"  line: {f['snippet']}")
        print(f"  suggestion: {f['suggestion']}")


def main(argv: List[str]) -> int:
    as_json = "--json" in argv
    findings: List[Dict[str, Any]] = []
    for f in iter_files(REPO_ROOT):
        findings.extend(scan_file(f))

    # Deduplicate identical entries (same file:line:snippet:category)
    seen: set[Tuple[str, Any, str, str]] = set()
    uniq: List[Dict[str, Any]] = []
    for f in findings:
        key = (f["file"], f["line"], f["snippet"], f["category"])
        if key not in seen:
            seen.add(key)
            uniq.append(f)

    if as_json:
        print(json.dumps({
            "repo": str(REPO_ROOT),
            "summary": summarize(uniq),
            "findings": uniq
        }, indent=2))
    else:
        print_human(uniq)

    return 0 if len(uniq) == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
