"""archive.py — METIS archive integration module

Provides MetisWISE installation helpers, archive data server client
operations, and master-calibration auto-download logic.  Archive operations
call the MetisWISE Python API directly (the package must be pip-installed
into the project venv).

The MetisWISE package ships with a production ``Environment.cfg`` that
points to the METIS AIT archive servers.  A user-local override can be
placed in ``~/.awe/Environment.cfg`` if needed.
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).parent.resolve()

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


def check_stale_environment_cfg() -> str | None:
    """Return a warning message if ``~/.awe/Environment.cfg`` overrides the
    production config with stale container-era settings, else ``None``.
    """
    cfg = Path.home() / ".awe" / "Environment.cfg"
    if not cfg.exists():
        return None
    try:
        text = cfg.read_text()
        if re.search(r"data_server\s*[:=]\s*dataserver\b", text):
            return (
                f"Warning: {cfg} still points data_server to 'dataserver' "
                f"(the old container hostname).  This overrides the "
                f"production config shipped with MetisWISE.  Consider "
                f"removing or updating {cfg}."
            )
    except OSError:
        pass
    return None


# ---------------------------------------------------------------------------
# Section B — Archive client (direct MetisWISE API calls)
# ---------------------------------------------------------------------------


def upload_files(
    filepaths: list[Path],
    on_log: Callable[[str], None] | None = None,
) -> list[str]:
    """Upload FITS files to the archive.

    For each file, reads the FITS header to determine whether it is a raw
    or processed data item, then stores and commits it via MetisWISE.

    Returns the list of successfully ingested filenames.
    """
    _ensure_awetarget()

    try:
        from astropy.io import fits
        from metiswise.main.raw import Raw
        from metiswise.main.pro import Pro
    except ImportError as exc:
        raise RuntimeError(
            "MetisWISE is not installed.  Use the Archive tab to install it."
        ) from exc

    ingested: list[str] = []
    total = len(filepaths)

    for i, fp in enumerate(filepaths, 1):
        if on_log:
            on_log(f"[{i}/{total}] Ingesting {fp.name}…")

        try:
            hdus = fits.open(str(fp))
            header = hdus[0].header

            if "ESO DPR CATG" in header:
                di = Raw(str(fp))
            elif "ESO PRO CATG" in header:
                di = Pro(str(fp))
            else:
                if on_log:
                    on_log(
                        f"[{i}/{total}] Skipping {fp.name}: "
                        f"no DPR.CATG or PRO.CATG header"
                    )
                continue

            di.store()
            di.commit()
            ingested.append(fp.name)
            if on_log:
                on_log(f"[{i}/{total}] {fp.name} ingested successfully")

        except Exception as exc:
            if on_log:
                on_log(f"[{i}/{total}] Failed to ingest {fp.name}: {exc}")

    return ingested


def query_archive(
    pro_catg: str | None = None,
    on_log: Callable[[str], None] | None = None,
) -> list[dict]:
    """Query the archive database for available files.

    If *pro_catg* is given, filters by ``PRO.CATG`` value (master product
    category).  Returns a list of dicts with ``filename``, ``pro_catg``,
    and ``class_name`` keys.
    """
    _ensure_awetarget()

    try:
        import metiswise.main.aweimports  # noqa: F401 — registers DataItem subclasses
        from metiswise.main.dataitem import DataItem
        from metiswise.main.pro import Pro
    except ImportError as exc:
        raise RuntimeError(
            "MetisWISE is not installed.  Use the Archive tab to install it."
        ) from exc

    if on_log:
        on_log("Querying archive…")

    try:
        if pro_catg:
            results = (Pro.pro_catg == pro_catg)
        else:
            results = DataItem.select_all()

        items = []
        for r in results:
            items.append({
                "filename": r.filename,
                "pro_catg": getattr(r, "pro_catg", ""),
                "class_name": type(r).__name__,
            })
        return items

    except Exception as exc:
        if on_log:
            on_log(f"Query failed: {exc}")
        return []


def download_file(
    filename: str,
    dest_dir: Path,
    on_log: Callable[[str], None] | None = None,
) -> Path | None:
    """Download a file from the archive to *dest_dir*.

    Returns the path to the downloaded file, or ``None`` on failure.
    """
    _ensure_awetarget()

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

        # Locate the retrieved file.
        src = Path(di.pathname) / di.filename
        if not src.exists():
            src = Path(di.filename)
        if not src.exists():
            if on_log:
                on_log(f"Retrieved file not found on disk: {filename}")
            return None

        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / filename
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
        items = query_archive(pro_catg=pro_catg, on_log=on_log)
        if items:
            result[pro_catg] = [it["filename"] for it in items]
    return result


# ---------------------------------------------------------------------------
# Section C — Master calibration auto-download
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
    # Late import to avoid circular dependency with run_metis.py.
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

    # Find the deepest task that is covered (the target).
    deepest_present_idx = -1
    for idx, (task_name, tag, meta) in enumerate(chain):
        if meta == "science":
            continue
        if _task_covered(task_name, tag, data_tags):
            deepest_present_idx = idx

    if deepest_present_idx < 0:
        return []

    # All tasks upstream of (and including) the deepest covered task
    # that are NOT themselves covered need their master products.
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
    """Download missing master calibration files from the archive.

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
        items = query_archive(pro_catg=pro_catg, on_log=on_log)
        if not items:
            if on_log:
                on_log(f"  No {pro_catg} found in archive — skipping")
            continue

        # Take the most recent file (last in list).
        target = items[-1]["filename"]
        path = download_file(target, dest_dir, on_log=on_log)
        if path:
            downloaded.append(path)
    return downloaded
