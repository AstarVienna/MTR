"""Resolution of user data directories.

All filesystem locations the GUI reads or writes (clones, .env, instrument
packages) flow through this module so tests can monkeypatch a single
seam and so a future user-configurable Settings tab has one place to hook.

Resolution order (highest first):
  1. METIS_DATA_DIR — relocate the whole tree
  2. Per-asset env vars (METIS_PIPELINE_DIR, METIS_SIMULATIONS_DIR, METIS_INST_PKGS)
  3. For the two repos: the Install tab's checkout fields (settings.json)
  4. platformdirs default (~/.local/share/metis-test-runner on Linux and macOS)

A repo set via 2. or 3. is the user's own checkout: MTR never clones, updates
or deletes it (see user_owned_repos()).
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

from platformdirs import user_data_dir
from platformdirs.unix import Unix

APP_NAME = "metis-test-runner"

_legacy_warned = False


def legacy_macos_data_dir() -> Path:
    """MTR's former macOS default, which EDPS cannot use: it contains a space."""
    return Path.home() / "Library" / "Application Support" / APP_NAME


def _default_data_dir() -> Path:
    # macOS's own default (~/Library/Application Support) contains a space, and
    # EDPS cannot handle whitespace in its input paths, so use the XDG location.
    if sys.platform == "darwin":
        return Path(Unix(APP_NAME).user_data_dir)
    return Path(user_data_dir(APP_NAME))


def _warn_legacy_data_dir(d: Path) -> None:
    global _legacy_warned
    legacy = legacy_macos_data_dir()
    if _legacy_warned or sys.platform != "darwin" or not legacy.is_dir() or legacy == d:
        return
    _legacy_warned = True
    print(
        f"Note: MTR now keeps its data in {d} instead of {legacy}, because EDPS "
        "cannot handle the space in that path. Run Install again, then delete "
        "the old folder.",
        file=sys.stderr,
    )


def data_dir() -> Path:
    """Top-level user data directory. Created on first call."""
    override = os.environ.get("METIS_DATA_DIR")
    d = Path(override) if override else _default_data_dir()
    if not override:
        _warn_legacy_data_dir(d)
    d.mkdir(parents=True, exist_ok=True)
    return d


PIPELINE_ENV = "METIS_PIPELINE_DIR"
SIMULATIONS_ENV = "METIS_SIMULATIONS_DIR"
#: Repo key (as in installer.REPOS) → env var that overrides it.
REPO_ENV = {"pipeline": PIPELINE_ENV, "simulations": SIMULATIONS_ENV}
#: user_owned_repos() label for a checkout chosen on the Install tab.
INSTALL_TAB = "the Install tab"


def settings_file() -> Path:
    """Settings shared by the GUI and the CLI tools (unlike QSettings)."""
    return data_dir() / "settings.json"


def load_repo_dirs() -> dict[str, Path]:
    """Repo key → own checkout chosen on the Install tab; {} if none or unreadable."""
    try:
        data = json.loads(settings_file().read_text())
        dirs = data.get("repo_dirs", {})
        return {k: Path(v) for k, v in dirs.items() if k in REPO_ENV and isinstance(v, str) and v}
    except (OSError, ValueError, AttributeError):
        return {}


def save_repo_dir(key: str, path: Path | None) -> None:
    """Store (or with None, clear) the Install tab's own checkout for repo *key*."""
    if key not in REPO_ENV:
        raise ValueError(f"unknown repo key {key!r}")
    try:
        data = json.loads(settings_file().read_text())
        if not isinstance(data, dict):
            data = {}
    except (OSError, ValueError):
        data = {}
    dirs = {k: str(v) for k, v in load_repo_dirs().items()}
    if path is None:
        dirs.pop(key, None)
    else:
        dirs[key] = str(Path(path).expanduser())
    data["repo_dirs"] = dirs
    write_text_atomic(settings_file(), json.dumps(data, indent=2) + "\n")


def _repo_override(key: str) -> tuple[Path, str] | None:
    """(own checkout, where it was set) for repo *key*, or None if MTR manages it."""
    env = os.environ.get(REPO_ENV[key])
    if env:
        return Path(env), REPO_ENV[key]
    chosen = load_repo_dirs().get(key)
    return (chosen, INSTALL_TAB) if chosen else None


def pipeline_dir() -> Path:
    own = _repo_override("pipeline")
    return own[0] if own else data_dir() / "METIS_Pipeline"


def simulations_dir() -> Path:
    own = _repo_override("simulations")
    return own[0] if own else data_dir() / "METIS_Simulations"


def user_owned_repos() -> dict[Path, str]:
    """Repo targets the user chose, mapped to where: an env var or INSTALL_TAB.

    These are the user's own checkouts: the installer never runs git in them
    and the uninstaller never deletes them.
    """
    return {own[0]: own[1] for key in REPO_ENV if (own := _repo_override(key))}


def change_hint(source: str) -> str:
    """How to change or drop an own checkout set via *source* (see user_owned_repos)."""
    if source == INSTALL_TAB:
        return "Change or clear it on the Install tab"
    return f"Point {source} at an existing checkout, or unset it to let MTR clone the repository"


def pipeline_present() -> bool:
    """True when there is a pipeline checkout to run against.

    A user-owned checkout need not be a git repo, so it only has to exist.
    """
    d = pipeline_dir()
    if _repo_override("pipeline"):
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
