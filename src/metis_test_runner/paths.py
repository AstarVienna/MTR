"""Resolution of user data directories.

All filesystem locations the GUI reads or writes (clones, .env, instrument
packages) flow through this module so tests can monkeypatch a single
seam and so a future user-configurable Settings tab has one place to hook.

Resolution order (highest first):
  1. METIS_DATA_DIR — relocate the whole tree
  2. Per-asset env vars (METIS_PIPELINE_DIR, METIS_SIMULATIONS_DIR, METIS_INST_PKGS)
  3. platformdirs default (~/.local/share/metis-test-runner on Linux)

A repo set via METIS_PIPELINE_DIR / METIS_SIMULATIONS_DIR is the user's own
checkout: MTR never clones, updates or deletes it (see user_owned_repos()).
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from platformdirs import user_data_dir


def data_dir() -> Path:
    """Top-level user data directory. Created on first call."""
    override = os.environ.get("METIS_DATA_DIR")
    d = Path(override) if override else Path(user_data_dir("metis-test-runner"))
    d.mkdir(parents=True, exist_ok=True)
    return d


PIPELINE_ENV = "METIS_PIPELINE_DIR"
SIMULATIONS_ENV = "METIS_SIMULATIONS_DIR"


def pipeline_dir() -> Path:
    override = os.environ.get(PIPELINE_ENV)
    return Path(override) if override else data_dir() / "METIS_Pipeline"


def simulations_dir() -> Path:
    override = os.environ.get(SIMULATIONS_ENV)
    return Path(override) if override else data_dir() / "METIS_Simulations"


def user_owned_repos() -> dict[Path, str]:
    """Repo targets set via an env var, mapped to that variable's name.

    These are the user's own checkouts: the installer never runs git in them
    and the uninstaller never deletes them.
    """
    owned = {}
    if os.environ.get(PIPELINE_ENV):
        owned[pipeline_dir()] = PIPELINE_ENV
    if os.environ.get(SIMULATIONS_ENV):
        owned[simulations_dir()] = SIMULATIONS_ENV
    return owned


def pipeline_present() -> bool:
    """True when there is a pipeline checkout to run against.

    A user-owned checkout need not be a git repo, so it only has to exist.
    """
    d = pipeline_dir()
    if os.environ.get(PIPELINE_ENV):
        return d.is_dir()
    return (d / ".git").exists()


def inst_pkgs_dir() -> Path:
    override = os.environ.get("METIS_INST_PKGS")
    return Path(override) if override else data_dir() / "inst_pkgs"


def env_file() -> Path:
    return data_dir() / ".env"


def examples_dir() -> Path:
    """Directory holding the bundled example YAML/CSV inputs.

    The examples ship inside the package, so after a ``pipx install`` there is
    no ``examples/`` in the user's working directory — ``mtr-cli
    examples/LMS_RAD_06.yaml`` (as the README long advised) could not work.
    Resolved via importlib.resources so it is correct for a wheel, an editable
    install and a source checkout alike.
    """
    from importlib.resources import files

    return Path(str(files("metis_test_runner") / "examples"))


def write_text_atomic(path: Path, text: str, *, mode: int | None = None) -> None:
    """Write *text* to *path* atomically, optionally chmod-ing to *mode*.

    A plain ``write_text`` truncates first, so an interrupted write leaves a
    corrupted file. That matters for the two config files MTR rewrites — the
    EDPS ``application.properties`` (whose only backup MTR has just renamed)
    and ``~/.awe/Environment.cfg`` (which also holds keys MTR wants to
    preserve). Writing a sibling temp file and renaming it means readers see
    either the old content or the new, never a half-written file.

    *mode* is applied to the temp file **before** the rename, so the final
    path is never briefly world-readable — which matters when the content is
    a password.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        if mode is not None:
            os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
