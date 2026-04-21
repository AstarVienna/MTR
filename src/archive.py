"""archive.py — METIS remote archive integration module

Thin client wrapper around the MetisWISE Python API.  Provides:
  - helpers for pip-installing MetisWISE into the project venv,
  - writing the five user-supplied fields into ``~/.awe/Environment.cfg``
    (all other archive settings inherit from the MetisWISE-packaged
    default, which already points at the remote METIS AIT archive),
  - query / download operations against the configured remote archive,
  - auto-detection and bulk download of master calibrations missing
    from a pipeline input set.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent

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


def install_metiswise_command(credentials: str) -> list[str]:
    """Return the ``uv pip install`` command to install MetisWISE.

    Installs into the project ``.venv`` (the same virtual environment
    created by the Install tab via ``uv sync``).  *credentials* should be
    ``"username:password"`` for the OmegaCEN pip channel.
    """
    return [
        "uv", "pip", "install",
        "--python", str(REPO_ROOT / ".venv" / "bin" / "python"),
        "--index-strategy", "unsafe-best-match",
        "--extra-index-url", "https://ftp.eso.org/pub/dfs/pipelines/libraries",
        "--extra-index-url", "https://ivh.github.io/pycpl/simple/",
        "--extra-index-url",
        f"https://{credentials}@pip.entropynaut.com/packages/",
        "metiswise",
    ]


def _ensure_awetarget() -> None:
    """Ensure ``AWETARGET=metiswise`` is set in the environment.

    MetisWISE / commonwise use this variable to locate the correct
    ``Environment.cfg`` shipped with the package.
    """
    os.environ.setdefault("AWETARGET", "metiswise")


DRLD_DIR = REPO_ROOT / "METIS_DRLD"
DRLD_REPO_URL = "https://github.com/AstarVienna/METIS_DRLD.git"

_metiswise_imports_done = False


def _ensure_metiswise_imports() -> None:
    """Ensure the ``codes`` package (METIS_DRLD) is importable, then
    import the full MetisWISE class hierarchy.

    ``metiswise.main.raw`` imports ``from metiswise.main.drld import drld``
    at module scope, which in turn does
    ``from codes.drld_parser.data_reduction_library_design import
    DataReductionLibraryDesign`` and instantiates it.  ``codes`` is **not**
    a MetisWISE pip dependency — the upstream MetisWISE Containerfile
    clones ``METIS_DRLD`` and adds it to ``PYTHONPATH``.  We do the same:
    clone on first call and stick it on ``sys.path``.
    """
    global _metiswise_imports_done
    if _metiswise_imports_done:
        return

    import importlib
    import sys

    drld_path = str(DRLD_DIR)
    if drld_path not in sys.path:
        sys.path.insert(0, drld_path)

    # Clone METIS_DRLD on first use.  Must happen before the import so that
    # Python's finder sees a real ``codes`` package on sys.path; a failed
    # import populates a negative cache that persists for the process, which
    # is why invalidate_caches() is called after the clone.
    if not (DRLD_DIR / "codes").is_dir():
        subprocess.run(
            ["git", "clone", "--depth", "1", DRLD_REPO_URL, str(DRLD_DIR)],
            check=True,
        )
        importlib.invalidate_caches()

    from codes.drld_parser.data_reduction_library_design import (
        DataReductionLibraryDesign,  # noqa: F401
    )
    import metiswise.main.aweimports  # noqa: F401

    _metiswise_imports_done = True


# ---------------------------------------------------------------------------
# Section B — DB connection & Environment.cfg credentials writer
# ---------------------------------------------------------------------------

_thread_local = threading.local()


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
        from common.config.Profile import profiles
        profiles.create_profile()
        from common.database.Database import database
        database.connect()
    except ImportError:
        pass
    _thread_local.db_ready = True


def reset_db_connection() -> None:
    """Force the next archive operation to re-establish the DB connection.

    Call this after writing new credentials to ``~/.awe/Environment.cfg``
    so that the next ``query_archive`` / ``download_file`` call picks up
    the updated settings.
    """
    _thread_local.db_ready = False


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


def list_available_masters(
    workflow: str,
    on_log: Callable[[str], None] | None = None,
) -> dict[str, list[str]]:
    """Query the archive for master calibration files relevant to *workflow*.

    Returns ``{pro_catg: [filename, …]}`` for each master product type
    available in the archive.
    """
    chain = _get_task_chain(workflow)
    result: dict[str, list[str]] = {}
    for task_name, _tag, _meta in chain:
        pro_catg = TASK_TO_MASTER_PROCATG.get(task_name)
        if not pro_catg:
            continue
        items = query_archive(category=pro_catg, on_log=on_log)
        if items:
            result[pro_catg] = [it["filename"] for it in items]
    return result


# ---------------------------------------------------------------------------
# Section D — Master calibration auto-download
# ---------------------------------------------------------------------------

# Maps pipeline task names to the PRO.CATG of the master product they create.
TASK_TO_MASTER_PROCATG: dict[str, str] = {
    # LM IMG
    "metis_lm_img_lingain":          "LINEARITY_2RG",
    "metis_lm_img_dark":             "MASTER_DARK_2RG",
    "metis_lm_img_flat":             "MASTER_IMG_FLAT_LAMP_LM",
    "metis_lm_img_distortion":       "LM_DISTORTION_TABLE",
    # N IMG
    "metis_n_img_lingain":           "LINEARITY_GEO",
    "metis_n_img_dark":              "MASTER_DARK_GEO",
    "metis_n_img_flat":              "MASTER_IMG_FLAT_LAMP_N",
    "metis_n_img_distortion":        "N_DISTORTION_TABLE",
    # IFU
    "metis_ifu_lingain":             "LINEARITY_IFU",
    "metis_ifu_dark":                "MASTER_DARK_IFU",
    "metis_ifu_distortion":          "IFU_DISTORTION_TABLE",
    "metis_ifu_wavecal":             "IFU_WAVECAL",
    "metis_ifu_rsrf":                "MASTER_IFU_RSRF",
    # LM LSS
    "metis_lm_lss_lingain":          "LINEARITY_2RG",
    "metis_lm_lss_dark":             "MASTER_DARK_2RG",
    "metis_lm_lss_rsrf":             "LM_LSS_MASTER_RSRF",
    "metis_lm_lss_trace":            "LM_LSS_TRACE_TABLE",
    "metis_lm_lss_wave":             "LM_LSS_WAVECAL",
    # N LSS
    "metis_n_lss_lingain":           "LINEARITY_GEO",
    "metis_n_lss_dark":              "MASTER_DARK_GEO",
    "metis_n_lss_rsrf":              "N_LSS_MASTER_RSRF",
    "metis_n_lss_trace":             "N_LSS_TRACE_TABLE",
    "metis_n_lss_wave":              "N_LSS_WAVECAL",
}


def _get_task_chain(workflow: str) -> list[tuple[str, str, str | None]]:
    """Import and return the task chain for *workflow* from ``run_metis``."""
    from run_metis import WORKFLOW_TASK_CHAIN
    return WORKFLOW_TASK_CHAIN.get(workflow, [])


def _task_covered(task_name: str, raw_tag: str, data_tags: set[str]) -> bool:
    """Return True if *task_name* is satisfied by files the user already has.

    A task is covered when either its raw-input classification tag or its
    master ``PRO.CATG`` appears in *data_tags*.  This lets pre-computed
    master files on disk short-circuit an archive download.
    """
    if raw_tag in data_tags:
        return True
    pro_catg = TASK_TO_MASTER_PROCATG.get(task_name)
    return bool(pro_catg and pro_catg in data_tags)


def identify_missing_calibrations(
    workflow: str,
    data_tags: set[str],
    has_science: bool,
) -> list[tuple[str, str]]:
    """Identify master calibrations needed but not available locally.

    Walks the ``WORKFLOW_TASK_CHAIN`` for *workflow*.  A task is considered
    covered when either its raw-input classification tag or its master
    ``PRO.CATG`` is present in *data_tags*.  For each non-science task that
    is **not** covered but is upstream of one that **is**, the master product
    category is returned.

    Returns a list of ``(task_name, master_pro_catg)`` pairs.
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
        if not _task_covered(task_name, tag, data_tags):
            pro_catg = TASK_TO_MASTER_PROCATG.get(task_name)
            if pro_catg:
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
