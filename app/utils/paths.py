from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Optional, Union

# Type alias for convenience
PathLike = Union[str, Path]


def _resolve_repo_root() -> Path:
    """
    Resolve the repository root by walking up from this file
    to the directory that contains your top-level files/folders.

    Assumes this file lives at: app/utils/paths.py
    So repo root = parents[2] (.. -> utils, .. -> app, .. -> repo root)

    If your layout changes, adjust the parents index accordingly.
    """
    here = Path(__file__).resolve()
    root = here.parents  # repo/
    return root


# Compute once; safe to import anywhere
REPO_ROOT: Path = _resolve_repo_root()


def repo_root() -> Path:
    """
    Return the resolved repository root path (Path object).
    """
    return REPO_ROOT


def repo_path(*parts: PathLike) -> Path:
    """
    Join one or more path components to the repository root. Always returns a Path.
    Example:
        repo_path("data", "raw_snapshots") -> <repo>/data/raw_snapshots
    """
    return REPO_ROOT.joinpath(*map(str, parts))


def ensure_dir(path: PathLike, exist_ok: bool = True) -> Path:
    """
    Ensure a directory exists, creating parents if needed. Returns the Path.
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=exist_ok)
    return p


def env_path(var_name: str, default_rel: Optional[Iterable[PathLike]] = None) -> Path:
    """
    Read a path from an environment variable. If unset, fall back to a repo-relative default.

    Example:
        env_path("SNAPSHOT_DIR", default_rel=("data", "raw_snapshots"))

    Behavior:
      - If ENV is set, expanduser/vars and return as Path.
      - If ENV is unset and default_rel is provided, return repo_path(*default_rel).
      - If both unset/None, raise ValueError.
    """
    value = os.getenv(var_name)
    if value:
        return Path(os.path.expandvars(os.path.expanduser(value))).resolve()
    if default_rel is not None:
        return repo_path(*default_rel)
    raise ValueError(f"Environment variable {var_name} not set and no default provided.")


def set_cwd_to_repo_root() -> None:
    """
    Optionally change the current working directory to the repo root.
    Useful for scripts that assume relative paths.
    Prefer using repo_path() instead of relying on cwd.
    """
    os.chdir(REPO_ROOT)


# Common convenience getters (customize as needed)
def snapshots_dir() -> Path:
    """
    Directory for raw JSON snapshots.
    Override with SNAPSHOT_DIR env var if desired.
    """
    return ensure_dir(env_path("SNAPSHOT_DIR", ("data", "raw_snapshots")))


def processed_dir() -> Path:
    """
    Directory for processed outputs/exports.
    Override with PROCESSED_DIR env var if desired.
    """
    return ensure_dir(env_path("PROCESSED_DIR", ("data", "processed")))


def logs_dir() -> Path:
    """
    Directory for application logs if you choose to log to files.
    Override with LOGS_DIR env var if desired.
    """
    return ensure_dir(env_path("LOGS_DIR", ("data", "logs")))


def grafana_dir() -> Path:
    """
    Repo location of Grafana artifacts (dashboards/provisioning).
    """
    return repo_path("grafana")


def docker_dir() -> Path:
    """
    Repo location of Docker artifacts.
    """
    return repo_path("docker")
