"""Resolution of user data directories.

All filesystem locations the GUI reads or writes (clones, .env, instrument
packages) flow through this module so tests can monkeypatch a single
seam and so a future user-configurable Settings tab has one place to hook.

Resolution order (highest first):
  1. METIS_DATA_DIR — relocate the whole tree
  2. Per-asset env vars (METIS_SIMULATIONS_DIR, METIS_INST_PKGS)
  3. platformdirs default (~/.local/share/metis-test-runner on Linux)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from platformdirs import user_data_dir


def data_dir() -> Path:
    """Top-level user data directory. Created on first call."""
    override = os.environ.get("METIS_DATA_DIR")
    d = Path(override) if override else Path(user_data_dir("metis-test-runner"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def pipeline_dir() -> Path:
    return data_dir() / "METIS_Pipeline"


def simulations_dir() -> Path:
    override = os.environ.get("METIS_SIMULATIONS_DIR")
    return Path(override) if override else data_dir() / "METIS_Simulations"


def inst_pkgs_dir() -> Path:
    override = os.environ.get("METIS_INST_PKGS")
    return Path(override) if override else data_dir() / "inst_pkgs"


def env_file() -> Path:
    return data_dir() / ".env"


def venv_python() -> Path:
    """Python interpreter that hosts MTR (the pipx/venv MTR was installed into)."""
    return Path(sys.executable)
