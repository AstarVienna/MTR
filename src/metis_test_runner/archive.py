"""archive.py — METIS remote archive integration module

Thin client wrapper around the MetisWISE Python API.  Provides:
  - helpers for pip-installing MetisWISE into the project venv,
  - injecting the five user-supplied DB fields into the process
    environment for commonwise (stored in the OS keyring, see
    ``credentials.py``; all other archive settings inherit from the
    MetisWISE-packaged default, which already points at the remote METIS
    AIT archive),
  - query / download operations against the configured remote archive,
  - auto-detection and bulk download of master calibrations missing
    from a pipeline input set.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from . import credentials, paths
from .indexes import ESO_INDEX, PYCPL_INDEX

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Module-level alias kept for back-compat with tests / external callers.
# Now resolves to the platformdirs user data directory.
REPO_ROOT = paths.data_dir()

# ---------------------------------------------------------------------------
# Section A — MetisWISE availability & installation
# ---------------------------------------------------------------------------


def metiswise_available() -> bool:
    """Return ``True`` if the ``metiswise`` package is importable."""
    try:
        import metiswise  # noqa: F401
        return True
    except ImportError:
        return False


# metiswise 0.0.4 is the first release that imports the ``metis_drld`` pip
# package (instead of a git-cloned ``codes.drld_parser``).  It is not yet
# published to the entropynaut index (which still tops out at 0.0.3) and its
# GitHub release ships no wheel asset, so we install it from the public GitHub
# tag source tarball.
#
# TODO: once metiswise 0.0.4+ lands on the entropynaut index, drop the tarball
# URL and use a plain ``"metiswise"`` / ``"metiswise>=0.0.4"`` requirement.
METISWISE_REQUIREMENT = (
    "metiswise @ https://github.com/AstarVienna/MetisWISE/archive/refs/tags/v0.0.4.tar.gz"
)

# metiswise 0.0.4's runtime dependencies, MINUS ``eso-pymetis`` (provided as an
# editable install of the cloned pymetis by the Install tab) and MINUS its
# jupyter/sphinx/pytest/coverage/mock/build dev/notebook deps (which the
# archive client does not need).  We install these first, with normal
# resolution, then install metiswise itself with ``--no-deps`` — see the long
# comment in install_metiswise_command for why.
_METISWISE_RUNTIME_DEPS: tuple[str, ...] = (
    "commonwise",
    "metis-drld",
    "psycopg2-binary",
    "astropy",
    "numpy",
    "scipy",
    "matplotlib",
    "pooch",
    "docutils",
    "httpx",
    "httpcore",
    "lxml",
)


def install_metiswise_command(
    pip_credentials: str,
) -> tuple[list[list[str]], dict[str, str]]:
    """Return ``(pip_commands, env_overrides)`` to install MetisWISE 0.0.4.

    Installs into the same interpreter that hosts MTR (sys.executable points at
    pipx's isolated venv or whatever venv the user installed MTR into).
    *pip_credentials* should be ``"username:password"`` for the OmegaCEN pip
    channel; the credentialed entropynaut index also serves MetisWISE's
    ``commonwise`` and ``metis-drld`` dependencies.

    The index URLs (including the credentialed one) travel in
    ``env_overrides["PIP_EXTRA_INDEX_URL"]`` (space-separated, the env form
    of ``--extra-index-url``), never in argv: a command line is readable by
    every local user via ``/proc/<pid>/cmdline`` while pip runs, and gets
    echoed into logs.  pip redacts the password in its own "Looking in
    indexes" output (the username may still appear).  Run the commands with
    ``env=os.environ | env_overrides``.

    Two commands, run in order:

      1. Install metiswise's runtime deps (``_METISWISE_RUNTIME_DEPS``) with
         normal resolution.  None of these depend on ``pycpl`` or
         ``eso-pymetis``, so this neither downgrades pycpl nor pulls a second
         pymetis copy.
      2. Install metiswise itself with ``--no-deps``.

    Why ``--no-deps`` for metiswise: metiswise 0.0.4 declares ``eso-pymetis``,
    and the cloned eso-pymetis the Install tab installs editable pins
    ``pycpl==1.0.3.post4``.  A normal ``pip install metiswise`` would let that
    pin downgrade our ``pycpl==1.0.3.post10`` — which we must keep, because
    post10 is where ivh's index ships prebuilt wheels for the macOS versions
    our users run (post4 would force a source build that fails there).
    ``--no-deps`` keeps pip from ever seeing eso-pymetis's pycpl pin.

    TODO: once pymetis (eso-pymetis) bumps its pycpl pin to >= post10, the
    downgrade risk is gone — collapse this back into a single
    ``pip install <metiswise>`` with normal dependency resolution.
    """
    env_overrides = {
        "PIP_EXTRA_INDEX_URL": " ".join((
            ESO_INDEX,
            PYCPL_INDEX,
            f"https://{pip_credentials}@pip.entropynaut.com/packages/",
        )),
    }
    deps_cmd = [
        sys.executable, "-m", "pip", "install",
        *_METISWISE_RUNTIME_DEPS,
    ]
    metiswise_cmd = [
        sys.executable, "-m", "pip", "install", "--no-deps",
        METISWISE_REQUIREMENT,
    ]
    return [deps_cmd, metiswise_cmd], env_overrides


def _ensure_awetarget() -> None:
    """Ensure ``AWETARGET=metiswise`` is set in the environment.

    MetisWISE / commonwise use this variable to locate the correct
    ``Environment.cfg`` shipped with the package.
    """
    os.environ.setdefault("AWETARGET", "metiswise")


_metiswise_imports_done = False


def _ensure_metiswise_imports() -> None:
    """Import the full MetisWISE class hierarchy.

    ``metiswise.main.aweimports`` pulls in ``raw``/``pro``/``drld``.  With
    metiswise 0.0.4 those resolve against pip-installed packages:
      - ``metiswise.main.drld`` imports ``metis_drld`` (a declared metiswise
        dependency, installed from the entropynaut index), and
      - ``raw``/``pro`` import ``pymetis`` (provided by the editable
        ``eso-pymetis`` the Install tab installs from the METIS_Pipeline
        clone).
    So this is now a plain import — no git clone, no ``sys.path`` munging.

    Pre-0.0.4 this used to git-clone METIS_DRLD and alias ``codes.drld_parser``;
    that is no longer needed.  If metiswise's packaging changes again, this is
    the spot to revisit.
    """
    global _metiswise_imports_done
    if _metiswise_imports_done:
        return

    try:
        import metiswise.main.aweimports  # noqa: F401
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "MetisWISE and its dependencies (metis_drld, pymetis) are not "
            "fully installed — run Install, then (re)install MetisWISE from "
            "the Archive tab."
        ) from exc

    _metiswise_imports_done = True


# ---------------------------------------------------------------------------
# Section B — DB connection & credentials injection
# ---------------------------------------------------------------------------

_thread_local = threading.local()

# Per-process credential state: the keyring is read at most once per process
# (concurrent worker threads serialize on the lock), and only when an archive
# operation actually needs the DB — never at import or GUI startup.
_db_creds_lock = threading.Lock()
_db_creds_applied = False


def _ensure_db_connection() -> None:
    """Create a MetisWISE database profile and connection for this thread.

    MetisWISE uses thread-local storage; each new thread must create its
    own profile and database connection.  Repeated calls on the same
    thread are no-ops.
    """
    if getattr(_thread_local, "db_ready", False):
        return
    _ensure_awetarget()
    try:
        # Cheap probe: common/__init__.py is docstring-only and does NOT pull
        # in common.config. Keeping the no-op-when-missing behavior here lets
        # query_archive raise its friendlier "MetisWISE is not installed"
        # error instead of a credentials error.
        import common  # noqa: F401
    except ImportError:
        _thread_local.db_ready = True
        return
    # Must run BEFORE the first common.config import: commonwise snapshots
    # os.environ into its module-global Env dict at import time.
    _ensure_credentials_applied()
    from common.config.Profile import profiles
    profiles.create_profile()
    from common.database.Database import database
    database.connect()
    _thread_local.db_ready = True


def reset_db_connection() -> None:
    """Force the next archive operation to re-establish the DB connection.

    Call this after ``apply_db_credentials`` so that the next
    ``query_archive`` / ``download_file`` call on this thread picks up
    the updated settings.
    """
    _thread_local.db_ready = False


def apply_db_credentials(fields: dict[str, str]) -> None:
    """Inject the five DB fields into this process for commonwise.

    commonwise builds a module-global ``Env`` dict the first time
    ``common.config.Environment`` is imported; process environment
    variables override any key already present in a packaged config file
    (all five of ours are).  If the module is already imported, its
    ``Env`` is additionally patched in place — ``Profile`` binds the dict
    by name, so it must never be reassigned.

    Raises ``RuntimeError`` if any field is empty or the password is
    ``"undefined"``: commonwise would fall back to a blocking
    ``getpass()`` prompt, which would hang a GUI worker thread.
    """
    global _db_creds_applied

    missing = [k for k in ENV_CFG_FIELDS if not fields.get(k)]
    if fields.get("database_password") == "undefined":
        missing.append("database_password")
    if missing:
        raise RuntimeError(
            "Incomplete archive credentials — missing: " + ", ".join(missing)
        )

    _ensure_awetarget()
    injected = {k: fields[k] for k in ENV_CFG_FIELDS}
    # Suppress commonwise's getpass() prompt for administrator accounts
    # (the packaged default is the truthy string "True").
    injected["ask_administrator_password"] = ""

    os.environ.update(injected)
    env_module = sys.modules.get("common.config.Environment")
    if env_module is not None:
        env_module.Env.update(injected)
    _db_creds_applied = True


def _ensure_credentials_applied() -> None:
    """Load DB credentials lazily, once per process.

    Order: OS keyring first; then the legacy ``~/.awe/Environment.cfg``
    (pre-migration installs, or keyring-less headless machines) — but only
    if it holds all five fields.  Raises ``RuntimeError`` with guidance
    when neither source has usable credentials.
    """
    with _db_creds_lock:
        if _db_creds_applied:
            return
        try:
            fields = credentials.get_db_credentials()
        except credentials.CredentialsUnavailable:
            fields = None
        if fields is None or any(not fields.get(k) for k in ENV_CFG_FIELDS):
            legacy = read_env_cfg()
            if all(legacy.values()):
                fields = legacy
            else:
                raise RuntimeError(
                    "No archive credentials configured.  Open the Archive "
                    "tab and run “Save & Test Connection” to store them in "
                    "your OS keyring."
                )
        apply_db_credentials(fields)


ENV_CFG_FIELDS: tuple[str, ...] = (
    "database_user",
    "database_password",
    "project",
    "database_tablespacename",
    "database_name",
)


def env_cfg_path() -> Path:
    """Return the path to ``~/.awe/Environment.cfg``."""
    return Path.home() / ".awe" / "Environment.cfg"


def read_env_cfg() -> dict[str, str]:
    """Return the current values of the five credential fields.

    Keys missing from the file (or the file being absent) map to ``""``.
    Only the ``[global]`` section is inspected.
    """
    values = {name: "" for name in ENV_CFG_FIELDS}
    cfg = env_cfg_path()
    if not cfg.exists():
        return values
    try:
        text = cfg.read_text()
    except OSError:
        return values

    in_global = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            in_global = line == "[global]"
            continue
        if not in_global:
            continue
        m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*[:=]\s*(.*)$", line)
        if not m:
            continue
        key, val = m.group(1), m.group(2).strip()
        if key in values:
            values[key] = val
    return values


def write_env_cfg(
    database_user: str,
    database_password: str,
    project: str,
    database_tablespacename: str,
    database_name: str,
) -> Path:
    """Write the five ``[global]`` fields into ``~/.awe/Environment.cfg``.

    Legacy/headless path: the GUI no longer calls this (credentials live
    in the OS keyring and are injected via ``apply_db_credentials``), but
    it remains for keyring-less setups where the file is the only store.

    All other keys — ``data_server``, ``data_port``, ``data_protocol``,
    etc. — are intentionally left out so they inherit from the
    MetisWISE-packaged default (``metis-ds.hpc.rug.nl:8013``, https).

    When the file already exists, existing keys are patched in place,
    preserving surrounding comments and unrelated keys.  Missing keys
    are appended to the ``[global]`` section (which is created if absent).
    """
    values = {
        "database_user": database_user,
        "database_password": database_password,
        "project": project,
        "database_tablespacename": database_tablespacename,
        "database_name": database_name,
    }

    cfg = env_cfg_path()
    cfg.parent.mkdir(mode=0o700, exist_ok=True)

    if not cfg.exists():
        lines = ["[global]"] + [f"{k} : {v}" for k, v in values.items()]
        cfg.write_text("\n".join(lines) + "\n")
        os.chmod(cfg, 0o600)
        return cfg

    text = cfg.read_text()
    lines = text.splitlines()

    # Locate [global] section boundaries.
    global_start = -1
    global_end = len(lines)
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "[global]":
            global_start = idx
            continue
        if (global_start >= 0
                and stripped.startswith("[")
                and stripped.endswith("]")):
            global_end = idx
            break

    if global_start < 0:
        # No [global] section — append one.
        if lines and lines[-1] != "":
            lines.append("")
        lines.append("[global]")
        for k, v in values.items():
            lines.append(f"{k} : {v}")
        cfg.write_text("\n".join(lines) + "\n")
        return cfg

    # Patch keys that exist inside [global]; remember which ones we handled.
    seen: set[str] = set()
    for idx in range(global_start + 1, global_end):
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped or stripped.startswith(("#", ";")):
            continue
        m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*([:=])\s*(.*)$", stripped)
        if not m:
            continue
        key, sep = m.group(1), m.group(2)
        if key in values:
            lines[idx] = f"{key} {sep} {values[key]}"
            seen.add(key)

    # Append any still-missing keys at the end of [global].
    missing = [k for k in values if k not in seen]
    if missing:
        insertion = [f"{k} : {values[k]}" for k in missing]
        lines = lines[:global_end] + insertion + lines[global_end:]

    cfg.write_text("\n".join(lines) + "\n")
    return cfg


def scrub_env_cfg() -> Path | None:
    """Remove the five managed keys from ``~/.awe/Environment.cfg``.

    One-time migration helper: once credentials live in the OS keyring,
    the plaintext copies in the legacy file are deleted.  Comments and
    unrelated keys (``data_server`` etc.) are preserved — commonwise still
    reads the file, and surviving keys must keep overriding the packaged
    defaults.  The file itself is left in place even if ``[global]`` ends
    up empty.

    Returns the path if the file was modified, ``None`` if it is absent
    or contains none of the managed keys.
    """
    cfg = env_cfg_path()
    if not cfg.exists():
        return None
    try:
        text = cfg.read_text()
    except OSError:
        return None
    lines = text.splitlines()

    # Locate [global] section boundaries (same scan as write_env_cfg).
    global_start = -1
    global_end = len(lines)
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "[global]":
            global_start = idx
            continue
        if (global_start >= 0
                and stripped.startswith("[")
                and stripped.endswith("]")):
            global_end = idx
            break
    if global_start < 0:
        return None

    kept: list[str] = []
    removed = False
    for idx, raw in enumerate(lines):
        if global_start < idx < global_end:
            m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*[:=]", raw.strip())
            if m and m.group(1) in ENV_CFG_FIELDS:
                removed = True
                continue
        kept.append(raw)

    if not removed:
        return None
    cfg.write_text("\n".join(kept) + "\n")
    return cfg


# ---------------------------------------------------------------------------
# Section C — Archive client (direct MetisWISE API calls)
# ---------------------------------------------------------------------------


def _resolve_dataitem_class(name: str, dataitem_cls: type) -> type | None:
    """Walk DataItem subclasses recursively to find one matching *name*."""
    for sub in dataitem_cls.__subclasses__():
        if sub.__name__ == name:
            return sub
        found = _resolve_dataitem_class(name, sub)
        if found is not None:
            return found
    return None


def query_archive(
    category: str | None = None,
    on_log: Callable[[str], None] | None = None,
) -> list[dict]:
    """Query the remote archive database for available files.

    If *category* is given it is resolved as a ``DataItem`` subclass name
    (e.g. ``"LINEARITY_2RG"``, ``"IFU_SCI_RAW"``) and ``.select_all()``
    is called on that class.  Returns a list of dicts with ``filename``,
    ``pro_catg``, and ``class_name`` keys.
    """
    _ensure_db_connection()
    _ensure_metiswise_imports()

    try:
        from metiswise.main.dataitem import DataItem
    except ImportError as exc:
        raise RuntimeError(
            "MetisWISE is not installed.  Use the Archive tab to install it."
        ) from exc

    if on_log:
        on_log("Querying archive…")

    if category:
        cls = _resolve_dataitem_class(category, DataItem)
        if cls is None:
            if on_log:
                on_log(f"Unknown category: {category}")
            return []
        results = cls.select_all()
    else:
        results = DataItem.select_all()

    # ``results`` may be a MetisWISE Select query object whose __bool__ raises
    # "Select object cannot be used without an operator in conditional
    # statements", so never evaluate it in a boolean context — just iterate.
    items = []
    if results is not None:
        for r in results:
            # Skip malformed / empty rows (e.g. a None sentinel returned by the
            # MetisWISE ORM when the archive has no data) rather than aborting
            # the whole query with an AttributeError.
            if r is None:
                continue
            filename = getattr(r, "filename", None)
            if filename is None:
                continue
            items.append({
                "filename": filename,
                "pro_catg": getattr(r, "pro_catg", ""),
                "class_name": type(r).__name__,
            })
    return items


def download_file(
    filename: str,
    dest_dir: Path,
    on_log: Callable[[str], None] | None = None,
) -> Path | None:
    """Download a file from the remote archive to *dest_dir*.

    Returns the path to the downloaded file, or ``None`` on failure.
    """
    _ensure_db_connection()

    try:
        from metiswise.main.dataitem import DataItem
    except ImportError as exc:
        raise RuntimeError(
            "MetisWISE is not installed.  Use the Archive tab to install it."
        ) from exc

    if on_log:
        on_log(f"Retrieving {filename} from archive…")

    try:
        results = (DataItem.filename == filename)
        if len(results) == 0:
            if on_log:
                on_log(f"File not found in archive: {filename}")
            return None

        di = results[0]
        di.retrieve()

        src = Path(di.pathname) / di.filename
        if not src.exists():
            src = Path(di.filename)
        if not src.exists():
            if on_log:
                on_log(f"Retrieved file not found on disk: {filename}")
            return None

        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / filename
        # MetisWISE may retrieve directly into *dest_dir* (e.g. cwd),
        # in which case copy2() would error with "same file".
        if src.resolve() != dest.resolve():
            shutil.copy2(str(src), str(dest))
        if on_log:
            on_log(f"Downloaded {filename} → {dest}")
        return dest

    except Exception as exc:
        if on_log:
            on_log(f"Download failed: {exc}")
        return None


def _build_pro_dataitem(path: Path):
    """Header-driven construction of a ``Pro`` DataItem.

    Replaces ``Pro(filename=path)`` for processed products.  Upstream
    ``Pro.__init__`` does ``self.raws = [DataItem-or-None, …]`` after
    resolving each provenance raw filename in the database — when any
    of those raws hasn't been ingested, the entry is ``None`` and the
    typed-list assignment raises a bare ``TypeError`` (see
    ``common/database/typed_list.py:134``).  This helper does the same
    header-driven initialisation but filters ``None`` out of the
    ``raws`` / ``calibs`` list assignments, so masters can be uploaded
    before their raws.

    Raises ``ValueError`` if the file lacks ``ESO PRO CATG`` or the
    PRO.CATG isn't registered in ``Pro.class_from_procatg``.
    """
    from astropy.io import fits
    from metiswise.main.pro import (
        Pro,
        get_provenance_from_header,
        get_optional_dataitem_from_filename,
    )

    with fits.open(str(path)) as hdus:
        header = hdus[0].header

    pro_catg = header.get("ESO PRO CATG")
    if not pro_catg:
        raise ValueError(f"{path.name}: no 'ESO PRO CATG' header")
    cls = Pro.class_from_procatg.get(pro_catg)
    if cls is None:
        raise ValueError(
            f"{path.name}: PRO.CATG {pro_catg!r} not registered in "
            "Pro.class_from_procatg"
        )

    di = cls()
    di.pathname = str(path)

    provenance = get_provenance_from_header(header)
    if provenance:
        prov_raws, prov_calibs, _params = provenance[-1]
        names_raws = [fn for fn, *_ in prov_raws]
        names_calibs = [fn for fn, *_ in prov_calibs]

        raws = [get_optional_dataitem_from_filename(fn) for fn in names_raws]
        di.raws = [r for r in raws if r is not None]
        padded = raws + [None] * 9
        (di.raw1, di.raw2, di.raw3, di.raw4, di.raw5,
         di.raw6, di.raw7, di.raw8, di.raw9) = padded[:9]

        calibs = [get_optional_dataitem_from_filename(fn) for fn in names_calibs]
        di.calibs = [c for c in calibs if c is not None]
        padded = calibs + [None] * 9
        (di.calib1, di.calib2, di.calib3, di.calib4, di.calib5,
         di.calib6, di.calib7, di.calib8, di.calib9) = padded[:9]

    for prop_name in cls.get_persistent_properties():
        prop = getattr(cls, prop_name)
        fits_key = f"ESO {prop.__doc__}".replace(".", " ")
        if fits_key in header:
            setattr(di, prop_name, header[fits_key])

    return di


def upload_file(
    path: Path,
    class_name: str | None = None,
    on_log: Callable[[str], None] | None = None,
) -> bool:
    """Ingest a local FITS file into the remote archive.

    Header-driven dispatch (mirrors MetisWISE's ``tools/ingest_file.py``):

    * duplicate-check via ``DataItem.filename == path.name`` — an existing
      match is treated as success (returns ``True`` without re-uploading);
    * ``ESO PRO CATG`` → :func:`_build_pro_dataitem` (tolerates
      not-yet-ingested raw-provenance files);
    * ``ESO DPR CATG`` → ``Raw(path)``;
    * neither header + *class_name* provided → manual override via
      :func:`_resolve_dataitem_class`;
    * ``.store()`` uploads the FITS payload, ``.commit()`` persists metadata.

    Returns ``True`` on success (including "already present"), ``False``
    on failure.
    """
    _ensure_db_connection()
    _ensure_metiswise_imports()

    try:
        from astropy.io import fits
        from metiswise.main.dataitem import DataItem
        from metiswise.main.raw import Raw
    except ImportError as exc:
        raise RuntimeError(
            "MetisWISE is not installed.  Use the Archive tab to install it."
        ) from exc

    if not path.exists():
        if on_log:
            on_log(f"File not found: {path}")
        return False

    try:
        existing = (DataItem.filename == path.name)
        if len(existing):
            if on_log:
                on_log(f"{path.name}: already in archive — skipping")
            return True

        if on_log:
            on_log(f"Ingesting {path.name}…")

        with fits.open(str(path)) as hdus:
            header = hdus[0].header

        if "ESO PRO CATG" in header:
            di = _build_pro_dataitem(path)
        elif "ESO DPR CATG" in header:
            di = Raw(str(path))
        elif class_name:
            cls = _resolve_dataitem_class(class_name, DataItem)
            if cls is None:
                if on_log:
                    on_log(f"Unknown DataItem class: {class_name}")
                return False
            di = cls(filename=str(path))
        else:
            if on_log:
                on_log(
                    f"Cannot classify {path.name}: neither "
                    "ESO DPR CATG nor ESO PRO CATG in header"
                )
            return False

        di.store()
        di.commit()
        if on_log:
            on_log(f"Uploaded {path.name} → {type(di).__name__}")
        return True

    except Exception as exc:
        if on_log:
            msg = str(exc) or f"{type(exc).__name__} (no message)"
            on_log(f"Upload failed for {path.name}: {msg}")
        return False


# ---------------------------------------------------------------------------
# Section D — Master calibration auto-download
# ---------------------------------------------------------------------------

# Canonical source of truth for what each EDPS task produces and consumes
# lives in METIS_Pipeline/metisp/workflows/metis/metis_*_wkf.py (see
# `.with_main_input(...)` / `.with_associated_input(...)` calls). The mapping
# below mirrors that graph for the subset of tasks whose master products we
# can fetch from the archive. Static reference files (PERSISTENCE_MAP,
# ATM_PROFILE, REF_STD_CAT, etc.) are intentionally absent: they are
# regenerated locally in run_metis.py's pre-pipeline prelude via
# metis_simulations.makeCalibPrototypes.generateStaticCalibs().


@dataclass(frozen=True)
class TaskProducts:
    """Archive-relevant I/O for a single EDPS task.

    ``produces`` are the ``PRO.CATG`` names of the master products the task
    writes; all must be present for the task to be considered covered by
    local files. ``consumes`` lists non-static ancillary masters that are
    required as input but are *not* produced by another task in the chain —
    today this stays empty for every task (all such inputs are either
    chain outputs or locally generated static calibs), and is retained as a
    schema slot for the rare future case that needs it.
    """

    produces: tuple[str, ...]
    consumes: tuple[str, ...] = ()


TASK_PRODUCTS: dict[str, TaskProducts] = {
    # LM IMG
    "metis_lm_img_lingain":          TaskProducts(produces=("LINEARITY_2RG", "GAIN_MAP_2RG")),
    "metis_lm_img_dark":             TaskProducts(produces=("MASTER_DARK_2RG",)),
    "metis_lm_img_flat":             TaskProducts(produces=("MASTER_IMG_FLAT_LAMP_LM",)),
    "metis_lm_img_distortion":       TaskProducts(produces=("LM_DISTORTION_TABLE",)),
    # N IMG
    "metis_n_img_lingain":           TaskProducts(produces=("LINEARITY_GEO", "GAIN_MAP_GEO")),
    "metis_n_img_dark":              TaskProducts(produces=("MASTER_DARK_GEO",)),
    "metis_n_img_flat":              TaskProducts(produces=("MASTER_IMG_FLAT_LAMP_N",)),
    "metis_n_img_distortion":        TaskProducts(produces=("N_DISTORTION_TABLE",)),
    # IFU
    "metis_ifu_lingain":             TaskProducts(produces=("LINEARITY_IFU", "GAIN_MAP_IFU")),
    "metis_ifu_dark":                TaskProducts(produces=("MASTER_DARK_IFU",)),
    "metis_ifu_distortion":          TaskProducts(produces=("IFU_DISTORTION_TABLE",)),
    "metis_ifu_wavecal":             TaskProducts(produces=("IFU_WAVECAL",)),
    "metis_ifu_rsrf":                TaskProducts(produces=("MASTER_IFU_RSRF",)),
    # LM LSS
    "metis_lm_lss_lingain":          TaskProducts(produces=("LINEARITY_2RG", "GAIN_MAP_2RG")),
    "metis_lm_lss_dark":             TaskProducts(produces=("MASTER_DARK_2RG",)),
    "metis_lm_lss_rsrf":             TaskProducts(produces=("LM_LSS_MASTER_RSRF",)),
    "metis_lm_lss_trace":            TaskProducts(produces=("LM_LSS_TRACE_TABLE",)),
    "metis_lm_lss_wave":             TaskProducts(produces=("LM_LSS_WAVECAL",)),
    # N LSS
    "metis_n_lss_lingain":           TaskProducts(produces=("LINEARITY_GEO", "GAIN_MAP_GEO")),
    "metis_n_lss_dark":              TaskProducts(produces=("MASTER_DARK_GEO",)),
    "metis_n_lss_rsrf":              TaskProducts(produces=("N_LSS_MASTER_RSRF",)),
    "metis_n_lss_trace":             TaskProducts(produces=("N_LSS_TRACE_TABLE",)),
    "metis_n_lss_wave":              TaskProducts(produces=("N_LSS_WAVECAL",)),
}


def _get_task_chain(workflow: str) -> list[tuple[str, str, str | None]]:
    """Import and return the task chain for *workflow* from ``run_metis``."""
    from .run_metis import WORKFLOW_TASK_CHAIN
    return WORKFLOW_TASK_CHAIN.get(workflow, [])


def _task_covered(task_name: str, raw_tag: str, data_tags: set[str]) -> bool:
    """Return True if *task_name* is satisfied by files the user already has.

    A task is covered when either its raw-input classification tag is in
    *data_tags*, or **every** ``PRO.CATG`` the task produces is in
    *data_tags*.  Requiring all products (not any) matters for multi-output
    tasks: e.g. if ``LINEARITY_2RG`` is on disk but its sibling
    ``GAIN_MAP_2RG`` is not, downstream tasks still cannot run and the
    missing sibling must be fetched.
    """
    if raw_tag in data_tags:
        return True
    entry = TASK_PRODUCTS.get(task_name)
    if entry is None or not entry.produces:
        return False
    return all(catg in data_tags for catg in entry.produces)


def identify_missing_calibrations(
    workflow: str,
    data_tags: set[str],
    has_science: bool,
) -> list[tuple[str, str]]:
    """Identify master calibrations needed but not available locally.

    Walks the ``WORKFLOW_TASK_CHAIN`` for *workflow*.  For each non-science
    task that is **not** covered but is upstream of one that **is**, every
    ``PRO.CATG`` the task produces that is missing from *data_tags* is
    returned as its own entry.

    Returns a list of ``(task_name, master_pro_catg)`` pairs; a single task
    may appear multiple times when it produces more than one master.
    """
    chain = _get_task_chain(workflow)
    if not chain:
        return []

    deepest_present_idx = -1
    for idx, (task_name, tag, meta) in enumerate(chain):
        if meta == "science":
            continue
        if _task_covered(task_name, tag, data_tags):
            deepest_present_idx = idx

    if deepest_present_idx < 0:
        return []

    missing: list[tuple[str, str]] = []
    for idx, (task_name, tag, meta) in enumerate(chain):
        if idx > deepest_present_idx:
            break
        if meta == "science":
            continue
        if _task_covered(task_name, tag, data_tags):
            continue
        entry = TASK_PRODUCTS.get(task_name)
        if entry is None:
            continue
        for pro_catg in entry.produces:
            if pro_catg not in data_tags:
                missing.append((task_name, pro_catg))
    return missing


def fetch_missing_calibrations(
    workflow: str,
    data_tags: set[str],
    has_science: bool,
    dest_dir: Path,
    on_log: Callable[[str], None] | None = None,
) -> list[Path]:
    """Download missing master calibration files from the remote archive.

    1. Calls :func:`identify_missing_calibrations` to find gaps.
    2. Queries the archive for each missing master ``PRO.CATG``.
    3. Downloads matching files into *dest_dir*.

    Returns a list of downloaded file paths.  Gracefully handles the case
    where no master files are available yet (returns an empty list).
    """
    missing = identify_missing_calibrations(workflow, data_tags, has_science)

    if not missing:
        if on_log:
            on_log("No missing calibrations identified")
        return []

    if on_log:
        on_log(f"Missing calibrations: {', '.join(pc for _, pc in missing)}")

    downloaded: list[Path] = []
    for task_name, pro_catg in missing:
        if on_log:
            on_log(f"Searching archive for {pro_catg}…")
        items = query_archive(category=pro_catg, on_log=on_log)
        if not items:
            if on_log:
                on_log(f"  No {pro_catg} found in archive — skipping")
            continue

        target = items[-1]["filename"]
        path = download_file(target, dest_dir, on_log=on_log)
        if path:
            downloaded.append(path)
    return downloaded
