"""Find newer MTR releases, upgrade in place, and pick the notes to show afterwards.

Free of Qt, so all of this can be tested without a display. The pipeline is
pip-installed into MTR's own venv, so an upgrade must never recreate that venv.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from importlib import metadata, resources
from pathlib import Path
from typing import Any

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import (
    InvalidSdistFilename,
    InvalidWheelFilename,
    parse_sdist_filename,
    parse_wheel_filename,
)
from packaging.version import InvalidVersion, Version

from . import __version__, paths
from .env import ensurepip_command_if_needed, resolve_runtime_env
from .installer import stream_subprocess

PACKAGE = "metis-test-runner"
INDEX_URL = f"https://pypi.org/simple/{PACKAGE}/"
CHANGELOG_PAGE = "https://github.com/AstarVienna/MTR/blob/main/CHANGELOG.md"
ACTION_HEADING = "### Action required"
DISABLE_ENV = "MTR_NO_UPDATE_CHECK"
CHECK_INTERVAL_S = 24 * 3600
# A release that moves the Qt pin downloads about 100 MB.
UPGRADE_TIMEOUT_S = 1800

_SECTION = re.compile(r"^## \[?(\d+\.\d+\.\d+)[^\n]*$", re.M)
_SUBSECTION = re.compile(r"^(### .*)$", re.M)


# ---------------------------------------------------------------------------
# Finding a newer release
# ---------------------------------------------------------------------------


def _file_version(filename: str) -> Version | None:
    try:
        if filename.endswith(".whl"):
            return parse_wheel_filename(filename)[1]
        return parse_sdist_filename(filename)[1]
    except (InvalidWheelFilename, InvalidSdistFilename, InvalidVersion):
        return None


def _supports(requires_python: str | None, python: str) -> bool:
    if not requires_python:
        return True
    try:
        return SpecifierSet(requires_python).contains(python, prereleases=True)
    except InvalidSpecifier:
        return False


def latest_release(files: list[dict[str, Any]], python: str) -> str | None:
    """Highest stable, non-yanked version in Index API *files* that runs on *python*."""
    versions = []
    for entry in files:
        version = _file_version(entry["filename"])
        if version is None or version.is_prerelease or version.is_devrelease or entry.get("yanked"):
            continue
        if _supports(entry.get("requires-python"), python):
            versions.append(version)
    return str(max(versions)) if versions else None


def fetch_latest(timeout: float = 5) -> str | None:
    """Latest usable release on PyPI, or None on any failure.

    Being offline or behind a firewall is normal on institute machines, not an
    error worth showing. ``urllib`` honours ``https_proxy``.
    """
    request = urllib.request.Request(
        INDEX_URL,
        headers={
            "Accept": "application/vnd.pypi.simple.v1+json",
            "User-Agent": f"{PACKAGE}/{__version__}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return latest_release(json.load(response)["files"], platform.python_version())
    except Exception:
        return None


def is_newer(latest: str | None, current: str) -> bool:
    if not latest:
        return False
    try:
        return Version(latest) > Version(current)
    except InvalidVersion:
        return False


def _cache_path() -> Path:
    return paths.data_dir() / "update-check.json"


def _read_cache() -> dict[str, Any]:
    try:
        cache = json.loads(_cache_path().read_text())
    except (OSError, ValueError):
        return {}
    return cache if isinstance(cache, dict) else {}


def cached_latest() -> str | None:
    latest = _read_cache().get("latest")
    return latest if isinstance(latest, str) else None


def check_due(now: float | None = None) -> bool:
    checked = _read_cache().get("checked")
    if not isinstance(checked, int | float):
        return True
    return (time.time() if now is None else now) - checked >= CHECK_INTERVAL_S


def remember_latest(latest: str) -> None:
    try:
        paths.write_text_atomic(_cache_path(), json.dumps({"checked": time.time(), "latest": latest}))
    except OSError:
        pass  # the next start simply checks again


# ---------------------------------------------------------------------------
# How this copy of MTR was installed
# ---------------------------------------------------------------------------


def _is_editable() -> bool:
    try:
        raw = metadata.distribution(PACKAGE).read_text("direct_url.json")
        return bool(raw and json.loads(raw).get("dir_info", {}).get("editable"))
    except metadata.PackageNotFoundError:
        return True  # a source tree without an install: also a developer's copy
    except ValueError:
        return False


def install_kind() -> str:
    """Return "editable", "pipx", "venv", or "other" for what MTR must not upgrade itself."""
    if _is_editable():
        return "editable"
    prefix = Path(sys.prefix)
    if not os.access(prefix, os.W_OK):
        return "other"
    if (prefix / "pipx_metadata.json").is_file():
        return "pipx"
    if sys.prefix == sys.base_prefix or (prefix / "uv-receipt.toml").is_file():
        return "other"
    return "venv"


def checks_enabled() -> bool:
    return not os.environ.get(DISABLE_ENV) and install_kind() != "editable"


# ---------------------------------------------------------------------------
# Upgrading
# ---------------------------------------------------------------------------


def upgrade_commands(version: str) -> list[list[str]] | None:
    """Commands that upgrade this install, or None if the user must do it by hand.

    Never ``pipx reinstall``: it recreates the venv, which wipes the pipeline.
    A pipx install without pipx on PATH is left to the user because upgrading
    it with pip leaves ``pipx list`` showing the old version.
    """
    kind = install_kind()
    if kind == "pipx":
        pipx = shutil.which("pipx")
        return [[pipx, "upgrade", Path(sys.prefix).name]] if pipx else None
    if kind == "venv":
        boot = ensurepip_command_if_needed()
        upgrade = [sys.executable, "-m", "pip", "install", "--upgrade", f"{PACKAGE}=={version}"]
        return [boot, upgrade] if boot else [upgrade]
    return None


def manual_command() -> str:
    """The command to give the user when MTR cannot upgrade itself."""
    prefix = Path(sys.prefix)
    if (prefix / "pipx_metadata.json").is_file():
        return f"pipx upgrade {prefix.name}"
    if (prefix / "uv-receipt.toml").is_file():
        # Not `uv tool upgrade`: it syncs the venv to the receipt, which wipes the pipeline.
        return f"uv pip install --python {sys.executable} --upgrade-package {PACKAGE} {PACKAGE}"
    return f"{sys.executable} -m pip install --upgrade {PACKAGE}"


def _upgrade_env() -> dict[str, str]:
    # "native" drops archive secrets but adds no pipeline PYTHONPATH, which
    # could shadow pipx's own imports. PIPX_HOME targets this venv, not the default.
    env = resolve_runtime_env("native")
    if (Path(sys.prefix) / "pipx_metadata.json").is_file():
        env["PIPX_HOME"] = str(Path(sys.prefix).parents[1])
    return env


def installed_version() -> str | None:
    """Version on disk, which differs from ``__version__`` right after an upgrade."""
    code = f"from importlib.metadata import version; print(version({PACKAGE!r}))"
    try:
        done = subprocess.run(
            [sys.executable, "-I", "-c", code],
            capture_output=True,
            text=True,
            timeout=30,
            env=resolve_runtime_env("native"),
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout.strip() or None if done.returncode == 0 else None


def perform_upgrade(commands: list[list[str]], on_line: Callable[[str, str], None]) -> str:
    """Run *commands* and return the new version; raise if nothing actually changed.

    A zero exit is not proof: pipx also exits 0 for a package pinned with ``pipx pin``.
    """
    env = _upgrade_env()
    for command in commands:
        stream_subprocess(command, on_line=on_line, env=env, timeout=UPGRADE_TIMEOUT_S)
    installed = installed_version()
    if installed is None or not is_newer(installed, __version__):
        raise RuntimeError(
            f"The upgrade finished, but the installed version is {installed or 'unknown'}, "
            f"not newer than {__version__}. A pipx-pinned install stays put until `pipx unpin`."
        )
    return installed


def restart() -> None:
    """Replace this process with the new MTR, after the Qt event loop has ended."""
    for stream in (sys.stdout, sys.stderr):
        if stream is not None:
            stream.flush()  # execve drops whatever is still buffered
    command = [sys.executable, "-m", "metis_test_runner", *sys.argv[1:]]
    os.execve(sys.executable, command, resolve_runtime_env("native"))


# ---------------------------------------------------------------------------
# Notes shown after an update
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WhatsNew:
    title: str
    actions: str  # markdown; empty when no skipped release needs user action
    notes: str  # markdown


def load_changelog() -> str | None:
    """The changelog shipped in the wheel; editable installs don't have this copy."""
    try:
        return (resources.files("metis_test_runner") / "CHANGELOG.md").read_text(encoding="utf-8")
    except OSError:
        return None


def _sections(changelog: str) -> list[tuple[str, str, str]]:
    """(version, heading, body) per release; ``## Unreleased`` is skipped."""
    matches = list(_SECTION.finditer(changelog))
    ends = [m.start() for m in matches[1:]] + [len(changelog)]
    return [(m[1], m[0], changelog[m.end() : end]) for m, end in zip(matches, ends, strict=False)]


def _split_actions(body: str) -> tuple[str, str]:
    """Separate a release's *Action required* text from the rest of its notes."""
    first, *parts = _SUBSECTION.split(body)
    actions, rest = [], [first]
    for heading, text in zip(parts[::2], parts[1::2], strict=True):
        if heading.strip() == ACTION_HEADING:
            actions.append(text.strip())
        else:
            rest += [heading, text]
    return "\n\n".join(actions), "".join(rest).strip()


def whats_new(
    last_seen: str | None, *, has_history: bool, changelog: str | None, current: str
) -> WhatsNew | None:
    """Notes to show for *current*, or None if the user has nothing to read.

    With no *last_seen*, *has_history* tells an existing user (first release
    with this feature, so only the current notes are known to apply) from a
    fresh install, which shows nothing.
    """
    if changelog is None:
        return None
    now = Version(current)
    try:
        since = Version(last_seen) if last_seen else None
    except InvalidVersion:
        since = None
    if since is None:
        if not has_history:
            return None
        title = f"What's new in {current}"
        wanted = lambda version: version == now
    elif now <= since:
        return None
    else:
        title = f"What's new since {last_seen}"
        wanted = lambda version: since < version <= now

    actions, notes = [], []
    for version, heading, body in _sections(changelog):
        if not wanted(Version(version)):
            continue
        action, rest = _split_actions(body)
        if action:
            actions.append(f"**{version}**\n\n{action}")
        notes.append(f"{heading}\n\n{rest}")
    if not notes:
        return None
    return WhatsNew(title, "\n\n".join(actions), "\n\n".join(notes))
