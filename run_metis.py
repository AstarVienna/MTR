#!/usr/bin/env python3
"""
run_metis.py — METIS observation simulation + pipeline wrapper

Reads YAML observation-block files, uses ScopeSim to generate synthetic
FITS frames, then runs the matching EDPS workflow on those frames.

Usage:
    python run_metis.py [OPTIONS] yaml1.yaml [yaml2.yaml ...]

Three execution modes are supported via --runner (or METIS_RUNNER env var):

  metapkg (default)
      Uses uv + metis-meta-package. Requires bootstrap.sh to have been run;
      looks for ~/metis-meta-package and ~/METIS_Simulations.

  native
      Calls edps / python directly from PATH. Use this when running inside
      a Docker/Podman container or on a bare-metal install.

  docker / podman
      Wraps every command with ``docker exec`` / ``podman exec`` into a named
      container. Requires --container NAME (or METIS_CONTAINER env var).
      The output directory must be bind-mounted into the container.

The workflow (lm_img / n_img / ifu / lm_lss / n_lss / …) is inferred
automatically from the DPR.TECH / mode values in the YAML blocks.
The pipeline target task is inferred from the data types present in the YAML
(or from FITS headers when --no-sim is used): it targets the deepest task in
the workflow chain whose main-input classification tag is present in the data.
If any block has catg="SCIENCE", the pipeline is run with -m science.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import yaml
from pathlib import Path
from datetime import datetime


# ---------------------------------------------------------------------------
# Workflow lookup tables
# ---------------------------------------------------------------------------

# Primary key: properties.tech value in YAML block
TECH_TO_WORKFLOW = {
    "IMAGE,LM": "metis.metis_lm_img_wkf",
    "IMAGE,N":  "metis.metis_n_img_wkf",
    "LMS":      "metis.metis_ifu_wkf",
    "IFU":      "metis.metis_ifu_wkf",    # name after updateHeaders()
    "LSS,LM":   "metis.metis_lm_lss_wkf",
    "LSS,N":    "metis.metis_n_lss_wkf",
    "RAVC,LM":  "metis.metis_lm_ravc_wkf",
    "RAVC,IFU": "metis.metis_ifu_wkf",
    "APP,LM":   "metis.metis_lm_app_wkf",
    "PUP,LM":   "metis.metis_pupil_imaging_wkf",
    "PUP,N":    "metis.metis_pupil_imaging_wkf",
}

# Fallback key: mode value in YAML block
MODE_TO_WORKFLOW = {
    "img_lm":     "metis.metis_lm_img_wkf",
    "wcu_img_lm": "metis.metis_lm_img_wkf",
    "img_n":      "metis.metis_n_img_wkf",
    "wcu_img_n":  "metis.metis_n_img_wkf",
    "lss_l":      "metis.metis_lm_lss_wkf",
    "lss_m":      "metis.metis_lm_lss_wkf",
    "lss_n":      "metis.metis_n_lss_wkf",
    "lms":        "metis.metis_ifu_wkf",
    "wcu_lms":    "metis.metis_ifu_wkf",
}

# ---------------------------------------------------------------------------
# Task chain tables
# ---------------------------------------------------------------------------

# Ordered task chain for each workflow: (task_name, main_input_tag, meta_target).
# Listed from most upstream (first calibration step) to most downstream.
# meta_target matches the EDPS meta-target that gates the task:
#   None       — task has no with_meta_targets(); always eligible
#   "qc1calib" — task gated behind QC1_CALIB meta-target
#   "science"  — task gated behind SCIENCE meta-target
#
# Only tasks with a raw-file main input are listed here; intermediate tasks
# whose main input is a previous task's product are omitted because they run
# automatically when their upstream task is targeted.
WORKFLOW_TASK_CHAIN = {
    "metis.metis_ifu_wkf": [
        ("metis_ifu_lingain",    "DETLIN_IFU_RAW",      None),
        ("metis_ifu_dark",       "DARK_IFU_RAW",         None),
        ("metis_ifu_distortion", "IFU_DISTORTION_RAW",  None),
        ("metis_ifu_wavecal",    "IFU_WAVE_RAW",         None),
        ("metis_ifu_rsrf",       "IFU_RSRF_RAW",         None),
        ("metis_ifu_std_reduce", "IFU_STD_RAW",          None),
        ("metis_ifu_sci_reduce", "IFU_SCI_RAW",          "science"),
    ],
    "metis.metis_lm_img_wkf": [
        ("metis_lm_img_lingain",          "DETLIN_2RG_RAW",   None),
        ("metis_lm_img_dark",             "DARK_2RG_RAW",     None),
        ("metis_lm_img_flat",             "LM_FLAT_LAMP_RAW", None),
        ("metis_lm_img_distortion",       "LM_DISTORTION_RAW", None),
        ("metis_lm_img_basic_reduce_sci", "LM_IMAGE_SCI_RAW", "science"),
        ("metis_lm_img_basic_reduce_std", "LM_IMAGE_STD_RAW", "science"),
    ],
    "metis.metis_n_img_wkf": [
        ("metis_n_img_lingain",    "DETLIN_GEO_RAW",   None),
        ("metis_n_img_dark",       "DARK_GEO_RAW",     None),
        ("metis_n_img_flat",       "N_FLAT_LAMP_RAW",  None),
        ("metis_n_img_distortion", "N_DISTORTION_RAW", None),
        ("metis_n_img_chopnod_sci", "N_IMAGE_SCI_RAW", "science"),
        ("metis_n_img_chopnod_std", "N_IMAGE_STD_RAW", "science"),
    ],
    # LSS calibration tasks are all gated behind QC1_CALIB.
    "metis.metis_lm_lss_wkf": [
        ("metis_lm_lss_lingain",      "DETLIN_2RG_RAW",       "qc1calib"),
        ("metis_lm_lss_dark",         "DARK_2RG_RAW",         "qc1calib"),
        ("metis_lm_lss_adc_slitloss", "LM_ADC_SLITLOSS_RAW", "qc1calib"),
        ("metis_lm_lss_rsrf",         "LM_LSS_RSRF_RAW",      "qc1calib"),
        ("metis_lm_lss_trace",        "LM_LSS_RSRF_PINH_RAW", "qc1calib"),
        ("metis_lm_lss_wave",         "LM_LSS_WAVE_RAW",      "qc1calib"),
        ("metis_lm_lss_std",          "LM_LSS_STD_RAW",       "science"),
        ("metis_lm_lss_sci",          "LM_LSS_SCI_RAW",       "science"),
    ],
    "metis.metis_n_lss_wkf": [
        ("metis_n_lss_lingain",  "DETLIN_GEO_RAW",       "qc1calib"),
        ("metis_n_lss_dark",     "DARK_GEO_RAW",         "qc1calib"),
        ("metis_n_adc_slitloss", "N_ADC_SLITLOSS_RAW",  "qc1calib"),
        ("metis_n_lss_rsrf",     "N_LSS_RSRF_RAW",       "qc1calib"),
        ("metis_n_lss_trace",    "N_LSS_RSRF_PINH_RAW",  "qc1calib"),
        ("metis_n_lss_wave",     "N_LSS_WAVE_RAW",       "qc1calib"),
        ("metis_n_lss_std",      "N_LSS_STD_RAW",        "science"),
        ("metis_n_lss_sci",      "N_LSS_SCI_RAW",        "science"),
    ],
    # RAVC and APP extend the LM IMG workflow with a single extra science task.
    "metis.metis_lm_ravc_wkf": [
        ("metis_lm_img_lingain",          "DETLIN_2RG_RAW",    None),
        ("metis_lm_img_dark",             "DARK_2RG_RAW",      None),
        ("metis_lm_img_flat",             "LM_FLAT_LAMP_RAW",  None),
        ("metis_lm_img_distortion",       "LM_DISTORTION_RAW", None),
        ("metis_lm_img_basic_reduce_sci", "LM_IMAGE_SCI_RAW",  "science"),
        ("metis_lm_img_basic_reduce_std", "LM_IMAGE_STD_RAW",  "science"),
    ],
    "metis.metis_lm_app_wkf": [
        ("metis_lm_img_lingain",          "DETLIN_2RG_RAW",    None),
        ("metis_lm_img_dark",             "DARK_2RG_RAW",      None),
        ("metis_lm_img_flat",             "LM_FLAT_LAMP_RAW",  None),
        ("metis_lm_img_distortion",       "LM_DISTORTION_RAW", None),
        ("metis_lm_img_basic_reduce_sci", "LM_IMAGE_SCI_RAW",  "science"),
        ("metis_lm_img_basic_reduce_std", "LM_IMAGE_STD_RAW",  "science"),
    ],
    # Pupil imaging reuses LM IMG calibration tasks.
    "metis.metis_pupil_imaging_wkf": [
        ("metis_lm_img_lingain", "DETLIN_2RG_RAW",   None),
        ("metis_lm_img_dark",    "DARK_2RG_RAW",     None),
        ("metis_lm_img_flat",    "LM_FLAT_LAMP_RAW", None),
        ("metis_pupil_imaging",  "LM_PUPIL_RAW",     "science"),
    ],
}

# Reverse lookup: (dpr.catg, dpr.type, dpr.tech) → EDPS classification tag.
# Used to classify FITS files by their ESO DPR headers when --no-sim is given.
# Derived from metis_classification.py.
DPR_TO_TAG = {
    # LM IMG
    ("CALIB",     "DETLIN",          "IMAGE,LM"): "DETLIN_2RG_RAW",
    ("CALIB",     "DARK",            "IMAGE,LM"): "DARK_2RG_RAW",
    ("CALIB",     "DISTORTION",      "IMAGE,LM"): "LM_DISTORTION_RAW",
    ("CALIB",     "DARK,WCUOFF",     "IMAGE,LM"): "LM_WCU_OFF_RAW",
    ("CALIB",     "FLAT,LAMP",       "IMAGE,LM"): "LM_FLAT_LAMP_RAW",
    ("SCIENCE",   "OBJECT",          "IMAGE,LM"): "LM_IMAGE_SCI_RAW",
    ("SCIENCE",   "SKY",             "IMAGE,LM"): "LM_IMAGE_SKY_RAW",
    ("CALIB",     "STD",             "IMAGE,LM"): "LM_IMAGE_STD_RAW",
    # N IMG
    ("CALIB",     "DETLIN",          "IMAGE,N"):  "DETLIN_GEO_RAW",
    ("CALIB",     "DARK",            "IMAGE,N"):  "DARK_GEO_RAW",
    ("CALIB",     "DISTORTION",      "IMAGE,N"):  "N_DISTORTION_RAW",
    ("CALIB",     "DARK,WCUOFF",     "IMAGE,N"):  "N_WCU_OFF_RAW",
    ("CALIB",     "FLAT,LAMP",       "IMAGE,N"):  "N_FLAT_LAMP_RAW",
    ("SCIENCE",   "OBJECT",          "IMAGE,N"):  "N_IMAGE_SCI_RAW",
    ("SCIENCE",   "SKY",             "IMAGE,N"):  "N_IMAGE_SKY_RAW",
    ("CALIB",     "STD",             "IMAGE,N"):  "N_IMAGE_STD_RAW",
    # IFU
    ("CALIB",     "DETLIN",          "IFU"):      "DETLIN_IFU_RAW",
    ("CALIB",     "DARK",            "IFU"):      "DARK_IFU_RAW",
    ("CALIB",     "DISTORTION",      "IFU"):      "IFU_DISTORTION_RAW",
    ("CALIB",     "WAVE",            "IFU"):      "IFU_WAVE_RAW",
    ("CALIB",     "RSRF",            "IFU"):      "IFU_RSRF_RAW",
    ("CALIB",     "DARK,WCUOFF",     "IFU"):      "IFU_WCU_OFF_RAW",
    ("CALIB",     "STD",             "IFU"):      "IFU_STD_RAW",
    ("CALIB",     "SKY",             "IFU"):      "IFU_SKY_RAW",
    ("SCIENCE",   "OBJECT",          "IFU"):      "IFU_SCI_RAW",
    # LM LSS
    ("CALIB",     "SLITLOSS",        "LSS,LM"):   "LM_ADC_SLITLOSS_RAW",
    ("CALIB",     "FLAT,LAMP",       "LSS,LM"):   "LM_LSS_RSRF_RAW",
    ("CALIB",     "FLAT,LAMP,PINH",  "LSS,LM"):   "LM_LSS_RSRF_PINH_RAW",
    ("CALIB",     "WAVE",            "LSS,LM"):   "LM_LSS_WAVE_RAW",
    ("CALIB",     "STD",             "LSS,LM"):   "LM_LSS_STD_RAW",
    ("SCIENCE",   "OBJECT",          "LSS,LM"):   "LM_LSS_SCI_RAW",
    # N LSS
    ("CALIB",     "SLITLOSS",        "LSS,N"):    "N_ADC_SLITLOSS_RAW",
    ("CALIB",     "FLAT,LAMP",       "LSS,N"):    "N_LSS_RSRF_RAW",
    ("CALIB",     "FLAT,LAMP,PINH",  "LSS,N"):    "N_LSS_RSRF_PINH_RAW",
    ("CALIB",     "WAVE",            "LSS,N"):    "N_LSS_WAVE_RAW",
    ("CALIB",     "STD",             "LSS,N"):    "N_LSS_STD_RAW",
    ("SCIENCE",   "OBJECT",          "LSS,N"):    "N_LSS_SCI_RAW",
    # Pupil
    ("TECHNICAL", "PUPIL",           "PUP,LM"):   "LM_PUPIL_RAW",
}


def read_edps_port(default: int = 5000) -> int:
    """Read the EDPS server port from ~/.edps/application.properties."""
    props = Path.home() / ".edps" / "application.properties"
    if props.exists():
        for line in props.read_text().splitlines():
            line = line.strip()
            if line.startswith("port="):
                raw = line.split("=", 1)[1]
                try:
                    return int(raw)
                except ValueError:
                    print(
                        f"warning: {props} has malformed port value "
                        f"{raw!r}; falling back to default {default}",
                        file=sys.stderr,
                    )
    return default


def _normalize_tech(tech: str) -> str:
    """Normalise a tech string for lookup: upper-case, trim whitespace,
    collapse spaces around commas. Keys in TECH_TO_WORKFLOW use this form."""
    return ",".join(part.strip() for part in tech.upper().split(","))


def _normalize_mode(mode: str) -> str:
    """Normalise a mode string for lookup: lower-case and trim whitespace.
    Keys in MODE_TO_WORKFLOW use this form."""
    return mode.strip().lower()


def infer_workflow(yaml_files):
    """Return (workflow, has_science, data_tags) by scanning all YAML blocks.

    Checks ``properties.tech`` first, then ``mode`` as a fallback.
    ``has_science`` is True when any block has ``properties.catg == "SCIENCE"``.
    ``data_tags`` is the set of ``do.catg`` values found across all blocks;
    these equal the EDPS classification tag names for the generated FITS files.
    Raises ValueError when no recognised tech/mode value is found.
    """
    techs = []
    modes = []
    has_science = False
    data_tags = set()

    for path in yaml_files:
        with open(path) as fh:
            data = yaml.safe_load(fh)
        if not isinstance(data, dict):
            continue
        for block in data.values():
            if not isinstance(block, dict):
                continue
            tag = block.get("do.catg", "")
            if tag:
                data_tags.add(tag)
            props = block.get("properties", {})
            tech = props.get("tech", "")
            if tech and tech not in techs:
                techs.append(tech)
            mode = block.get("mode", "")
            if mode and mode not in modes:
                modes.append(mode)
            if props.get("catg", "").upper() == "SCIENCE":
                has_science = True

    for t in techs:
        key = _normalize_tech(t)
        if key in TECH_TO_WORKFLOW:
            return TECH_TO_WORKFLOW[key], has_science, data_tags
    for m in modes:
        key = _normalize_mode(m)
        if key in MODE_TO_WORKFLOW:
            return MODE_TO_WORKFLOW[key], has_science, data_tags

    raise ValueError(
        "Cannot determine workflow from YAML content.\n"
        f"  Found tech values : {techs}\n"
        f"  Found mode values : {modes}\n"
        f"  Known tech values : {list(TECH_TO_WORKFLOW)}\n"
        f"  Known mode values : {list(MODE_TO_WORKFLOW)}"
    )


def collect_tags_from_fits(fits_dir):
    """Return the set of EDPS classification tags present in a FITS directory.

    For each ``.fits`` file, reads ``HIERARCH ESO DPR CATG/TYPE/TECH`` and
    maps the triple to a raw-file tag name via ``DPR_TO_TAG``.  If no DPR
    headers are present, falls back to ``HIERARCH ESO PRO CATG`` and adds
    that value as-is (e.g. ``MASTER_DARK_2RG``).  This lets pre-computed
    master calibration files register as "covered" for downstream gap
    detection.  Files whose headers don't match any known rule are silently
    skipped.  Requires astropy.
    """
    try:
        from astropy.io import fits as afits
    except ImportError:
        return set()

    tags = set()
    for f in Path(fits_dir).glob("*.fits"):
        try:
            with afits.open(f, memmap=True) as hdul:
                hdr = hdul[0].header
                catg = hdr.get("HIERARCH ESO DPR CATG", "").strip()
                typ  = hdr.get("HIERARCH ESO DPR TYPE", "").strip()
                tech = hdr.get("HIERARCH ESO DPR TECH", "").strip()
                pro_catg = hdr.get("HIERARCH ESO PRO CATG", "").strip()
            if catg:
                tag = DPR_TO_TAG.get((catg, typ, tech))
                if tag:
                    tags.add(tag)
            elif pro_catg:
                tags.add(pro_catg)
        except Exception:
            continue
    return tags


def infer_workflow_from_fits(fits_dir):
    """Infer the EDPS workflow from DPR TECH headers in a FITS directory.

    Used when --no-sim is given without any YAML files.
    """
    try:
        from astropy.io import fits as afits
    except ImportError:
        raise ValueError("astropy is required to infer workflow from FITS headers.")

    techs = []
    for f in Path(fits_dir).glob("*.fits"):
        try:
            with afits.open(f, memmap=True) as hdul:
                tech = hdul[0].header.get("HIERARCH ESO DPR TECH", "").strip()
            if tech and tech not in techs:
                techs.append(tech)
        except Exception:
            continue

    for t in techs:
        if t in TECH_TO_WORKFLOW:
            return TECH_TO_WORKFLOW[t]

    raise ValueError(
        "Cannot determine workflow from FITS headers.\n"
        f"  Found DPR.TECH values : {techs}\n"
        f"  Known DPR.TECH values : {list(TECH_TO_WORKFLOW)}\n"
        "Pass YAML files or ensure FITS headers contain a recognised DPR.TECH value."
    )


def infer_edps_target(workflow, data_tags, has_science):
    """Return the EDPS flags needed to target the right pipeline task(s).

    Walks the workflow's task chain (deepest last) and finds all tasks whose
    main-input classification tag is present in ``data_tags``.  The deepest
    matching non-science task determines the calibration target; science data
    is handled separately via ``-m science``.

    Returns a list of extra flags to append to the edps command, e.g.:
        ["-t", "metis_ifu_lingain"]
        ["-m", "qc1calib"]
        ["-m", "science"]
        ["-m", "qc1calib", "-m", "science"]   # both calib and science data
        []                                     # no matching task found
    """
    chain = WORKFLOW_TASK_CHAIN.get(workflow, [])
    flags = []

    # Find the deepest non-science task whose main input tag is present.
    calib_task = None
    calib_meta = None
    for task_name, tag, meta in chain:
        if meta != "science" and tag in data_tags:
            calib_task = task_name
            calib_meta = meta

    if calib_task is not None:
        if calib_meta == "qc1calib":
            flags += ["-m", "qc1calib"]
        else:
            flags += ["-t", calib_task]

    if has_science:
        flags += ["-m", "science"]

    return flags


# ---------------------------------------------------------------------------
# Simulation driver script builder
# ---------------------------------------------------------------------------

def _build_sim_script(out_dir, do_calib, do_static, n_cores, yaml_list,
                      inst_pkgs_path=None, sims_root=None,
                      static_calibs_dir=None):
    """Return the simulation driver script as a string.

    When *inst_pkgs_path* is given (metapkg and native runners) the script
    overrides ScopeSim's local_packages_path and auto-downloads the instrument
    packages into that directory if the METIS package is not yet present.

    When *static_calibs_dir* is given (and *do_static* is true) the script
    generates static calibration prototypes (PERSISTENCE_MAP, ATM_PROFILE, …)
    into that directory instead of the simulation output, reusing any files
    that already exist there.  This avoids regenerating ~200 MB of FITS files
    on every run.
    """
    # Static calibration generation is handled separately (see end of script)
    # so we never let runSimulationBlock() do it via doStatic.  The upstream
    # code re-parses its third arg with argparse where --doStatic defaults to
    # False, silently overwriting the params dict — but that's moot now.
    path_entry = str(sims_root) if sims_root is not None else "python"
    # metis_simulations submodules read DEFAULT_IRDB_LOCATION at import time;
    # it must be set in the environment before the package is imported.
    default_irdb = f"{path_entry}/inst_pkgs" if sims_root is not None else "./inst_pkgs"
    lines = [
        "import sys",
        "import os as _os",
        "import tempfile as _tempfile",
        # Redirect scipy's pooch-backed datasets cache to a guaranteed-writable
        # path; some environments (CI, sandboxed users) have a read-only
        # ~/.cache. metis_simulations.sources calls scipy.datasets.face() at
        # import time, which would otherwise fail with PermissionError.
        "_os.environ.setdefault("
        "'SCIPY_DATASETS_DIR', "
        "_os.path.join(_tempfile.gettempdir(), 'scipy-data'))",
        f"sys.path.insert(0, {path_entry!r})",
        "",
    ]
    if inst_pkgs_path is not None:
        lines += [
            f"_os.environ['DEFAULT_IRDB_LOCATION'] = {inst_pkgs_path!r}",
            "",
        ]
    else:
        lines += [
            "if 'DEFAULT_IRDB_LOCATION' not in _os.environ:",
            f"    _os.environ['DEFAULT_IRDB_LOCATION'] = {default_irdb!r}",
            "",
        ]
    lines += [
        "import scopesim as sim",
    ]
    if inst_pkgs_path is not None:
        lines += [
            "# Override ScopeSim's inst_pkgs path.",
            f'sim.rc.__config__["!SIM.file.local_packages_path"] = {inst_pkgs_path!r}',
            "",
            "# Auto-download instrument packages if not present.",
            "from pathlib import Path as _Path",
            f"_inst_dir = _Path({inst_pkgs_path!r})",
            "if not (_inst_dir / 'METIS').is_dir():",
            f"    print('Instrument packages not found at {inst_pkgs_path}. Downloading \u2026')",
            "    _inst_dir.mkdir(parents=True, exist_ok=True)",
            "    sim.download_packages('METIS', release='2026-02-18')",
            "    sim.download_packages('ELT', release='2025-10-26')",
            "    sim.download_packages('Armazones', release='2023-07-11')"
        ]
    lines += [
        "",
        "# --- skycalc cache race guard ----------------------------------------",
        "# scopesim's skycalc_ipy backend writes its shared cache file at",
        "# ~/.astar/skycalc_ipy/skymodel_<hash>.fits via HDUList.writeto() with",
        "# no overwrite=True. When METIS_Simulations.setupSimulations.calculateDarks",
        "# fans out simulate() across a multiprocessing.Pool(nCores), the first",
        "# worker to reach a cold cache wins the write; every other worker crashes",
        "# the pool with OSError('File ... already exists'). We cannot patch",
        "# METIS_Simulations (vendored, out of scope), so we patch the underlying",
        "# skycalc_ipy method here. This patch lives at module level (outside the",
        "# if __name__ guard) so it is picked up both by forked workers on Linux",
        "# and by spawn-mode workers on macOS (which re-import this script).",
        "try:",
        "    import skycalc_ipy.core as _skc_core",
        "    _skc_orig_call = _skc_core.SkyModel.__call__",
        "    def _skc_safe_call(self, *args, **kwargs):",
        "        try:",
        "            return _skc_orig_call(self, *args, **kwargs)",
        "        except OSError as _exc:",
        "            if 'already exists' not in str(_exc):",
        "                raise",
        "            # Another Pool worker already populated the cache; retry so",
        "            # skycalc_ipy reads it back instead of re-downloading.",
        "            return _skc_orig_call(self, *args, **kwargs)",
        "    _skc_core.SkyModel.__call__ = _skc_safe_call",
        "except ImportError:",
        "    # skycalc_ipy not installed in this environment; nothing to patch.",
        "    pass",
        "# ---------------------------------------------------------------------",
        "",
        "from metis_simulations import runSimulationBlock as rsb",
        "",
        'if __name__ == "__main__":',
        "    params = dict(",
        f"        outputDir = {out_dir!r},",
        "        small     = False,",
        "        doStatic  = False,",
        f"        doCalib   = {do_calib!r},",
        "        sequence  = False,",
        "        startMJD  = None,",
        "        calibFile = None,",
        f"        nCores    = {n_cores!r},",
        "        testRun   = False,",
        "    )",
        "    try:",
        f"        rsb.runSimulationBlock({yaml_list!r}, params, [])",
        "    except ValueError as _exc:",
        "        if 'Package could not be found' in str(_exc):",
        "            import sys as _sys",
        "            print('', file=_sys.stderr)",
        "            print('HINT: ScopeSim could not find the instrument packages.', file=_sys.stderr)",
    ]
    if inst_pkgs_path is not None:
        lines.append(
            f"            print('  Instrument packages path: {inst_pkgs_path}', file=_sys.stderr)"
        )
    else:
        lines.append(
            "            print('  No instrument packages path was configured.', file=_sys.stderr)"
        )
    lines += [
        "            print('  In the GUI: set the Instrument packages field in the Run tab.', file=_sys.stderr)",
        "            print('  On the command line: pass --inst-pkgs <path>.', file=_sys.stderr)",
        "        raise",
    ]

    # --- Static calibration prototypes (cached) -----------------------------
    # Generate PERSISTENCE_MAP, ATM_PROFILE, REF_STD_CAT, etc. into a shared
    # cache directory so they survive across runs.  Skip if already present.
    if do_static and static_calibs_dir is not None:
        lines += [
            "",
            "    # --- Generate static calibration prototypes (cached) ---",
            f"    _static_dir = {static_calibs_dir!r}",
            "    if not _os.path.isfile(_os.path.join(_static_dir, 'PERSISTENCE_MAP_LM.fits')):",
            "        _os.makedirs(_static_dir, exist_ok=True)",
            "        from metis_simulations import makeCalibPrototypes as _mcp",
            "        _mcp.generateStaticCalibs(_static_dir)",
            "        print(f'Generated static calibration prototypes in {_static_dir}')",
            "    else:",
            "        print(f'Static calibration prototypes already cached in {_static_dir}')",
        ]

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Runner-aware subprocess helpers
# ---------------------------------------------------------------------------

def _check_metapkg_env(runner, meta_pkg):
    """Raise FileNotFoundError if metapkg runner is configured but .env is missing.

    uv's own error message when ``--env-file`` points at a missing file is
    technically correct but unhelpful. Fail fast with a pointer at the Install
    tab / METIS_META_PKG.
    """
    if runner != "metapkg":
        return
    env_file = meta_pkg / ".env"
    if not env_file.exists():
        raise FileNotFoundError(
            f"{env_file} not found — run the Install tab or check METIS_META_PKG"
        )


def _run_simulation(runner, container, sim_code, sims_cwd, meta_pkg=None):
    """Execute the simulation script in the appropriate environment.

    - metapkg : wrap with ``uv run --project <meta_pkg>``
    - native  : call ``python`` directly (tools must be on PATH)
    - docker/podman : pipe script via stdin into ``<runtime> exec -i -w <cwd>
                      <container> python -``

    Returns the subprocess exit code.
    """
    if runner in ("docker", "podman"):
        return subprocess.run(
            [runner, "exec", "-i", "-w", str(sims_cwd), container,
             "python", "-"],
            input=sim_code.encode(),
        ).returncode

    _check_metapkg_env(runner, meta_pkg)

    # For metapkg and native, write to a temp file and run it.
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix="_run_sim.py",
                                     delete=False)
    tmp.write(sim_code)
    tmp.close()
    try:
        if runner == "metapkg":
            cmd = [
                "uv", "run", "--no-sync",
                "--project", str(meta_pkg),
                "--env-file", str(meta_pkg / ".env"),
                "python", tmp.name,
            ]
            cwd = str(sims_cwd)
        else:  # native
            cmd = ["python", tmp.name]
            cwd = str(sims_cwd)
        return subprocess.run(cmd, cwd=cwd).returncode
    finally:
        os.unlink(tmp.name)


def _edps_base_cmd(runner, container, edps_port, meta_pkg=None):
    """Return the command prefix list up to and including the edps port flag."""
    base = ["edps", "-P", str(edps_port)]
    if runner == "metapkg":
        _check_metapkg_env(runner, meta_pkg)
        return ["uv", "run", "--no-sync", "--project", str(meta_pkg),
                "--env-file", str(meta_pkg / ".env")] + base
    if runner in ("docker", "podman"):
        return [runner, "exec", container] + base
    return base  # native


def _edps_cwd(runner, meta_pkg=None):
    """Return the working directory for EDPS subprocess calls, or None."""
    return str(meta_pkg) if runner == "metapkg" else None


# ---------------------------------------------------------------------------
# EDPS association_preference runtime override
# ---------------------------------------------------------------------------

def _set_association_preference(value: str) -> str | None:
    """Patch ``association_preference`` in ``~/.edps/application.properties``.

    Returns the original value so it can be restored, or ``None`` if the
    file doesn't exist (no patching needed).
    """
    props = Path.home() / ".edps" / "application.properties"
    if not props.exists():
        return None
    text = props.read_text()
    match = re.search(r"^association_preference=(.*)$", text, re.MULTILINE)
    original = match.group(1) if match else None
    new_text = re.sub(
        r"^association_preference=.*",
        f"association_preference={value}",
        text,
        flags=re.MULTILINE,
    )
    props.write_text(new_text)
    return original


def _restore_association_preference(original: str | None) -> None:
    """Restore the original ``association_preference`` value."""
    if original is not None:
        _set_association_preference(original)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "yaml_files", nargs="*", metavar="YAML",
        help="One or more observation-block YAML files (not required with --no-sim)",
    )
    p.add_argument(
        "-o", "--output", metavar="DIR",
        help="Root output directory [default: ./output/<timestamp>]",
    )
    p.add_argument(
        "--calib", type=int, nargs="?", const=1, default=1, metavar="N",
        help="Auto-generate N calibration frames (dark/flat) per unique config, "
             "inferred from YAML content. Bare --calib = 1; --calib 0 disables. "
             "[default: 1]",
    )
    p.add_argument(
        "--static", type=int, nargs="?", const=1, default=1, metavar="N",
        help="Ensure static calibration prototypes (PERSISTENCE_MAP_*, "
             "ATM_PROFILE, REF_STD_CAT, …) exist in a shared cache directory "
             "(output/static_calibs/) and pass it to EDPS. Files are generated "
             "once and reused across runs. "
             "Bare --static = 1; --static 0 disables. [default: 1]",
    )
    p.add_argument(
        "--cores", type=int, default=4, metavar="N",
        help="CPU cores for parallel simulations [default: 4]",
    )
    p.add_argument(
        "--no-sim", action="store_true",
        help="Skip simulations; run pipeline on existing FITS data. "
             "The FITS source defaults to <output>/sim/ but can be overridden "
             "with --pipeline-input.",
    )
    p.add_argument(
        "--pipeline-input", metavar="DIR", action="append",
        help="Directory containing FITS files to use as pipeline input. "
             "May be specified multiple times. Only used with --no-sim. "
             "When omitted, defaults to <output>/sim/.",
    )
    p.add_argument(
        "--no-pipeline", action="store_true",
        help="Run simulations only; skip EDPS pipeline",
    )
    p.add_argument(
        "--runner",
        choices=["metapkg", "native", "docker", "podman"],
        default=os.environ.get("METIS_RUNNER", "metapkg"),
        help="Execution mode: metapkg (default) uses uv + metis-meta-package; "
             "native calls tools directly from PATH (bare-metal or inside a "
             "container); docker/podman exec commands into a running container "
             "(env: METIS_RUNNER)",
    )
    p.add_argument(
        "--container", metavar="NAME",
        default=os.environ.get("METIS_CONTAINER"),
        help="Container name or ID for --runner=docker/podman "
             "(env: METIS_CONTAINER)",
    )
    p.add_argument(
        "--meta-pkg", metavar="DIR",
        help="Path to the metis-meta-package directory "
             "[default: ./metis-meta-package] (metapkg runner only)",
    )
    p.add_argument(
        "--simulations-dir", metavar="DIR",
        help="Path to the METIS_Simulations repository. For docker/podman "
             "runners this must be the path *inside* the container "
             "[default: ./METIS_Simulations for native/metapkg, "
             "/home/metis/METIS_Simulations for docker/podman]",
    )
    p.add_argument(
        "--inst-pkgs", metavar="DIR",
        help="Path to the ScopeSim instrument packages directory "
             "(Armazones, ELT, METIS, …). "
             "For the metapkg runner this defaults to <meta-pkg>/inst_pkgs. "
             "For the native runner this defaults to ./inst_pkgs relative to "
             "the current working directory — ScopeSim will download packages "
             "there on first use. "
             "For docker/podman runners supply the container-internal path; "
             "if omitted ScopeSim resolves ./inst_pkgs inside the container.",
    )
    p.add_argument(
        "--auto-fetch-calibrations", action="store_true",
        help="Automatically download missing master calibration files from "
             "the archive data server before running the pipeline. "
             "Requires the archive stack to be running (see Archive tab).",
    )
    p.add_argument(
        "--archive-host", default="localhost",
        help="Archive data server hostname [default: localhost]",
    )
    p.add_argument(
        "--archive-port", type=int, default=8013,
        help="Archive data server port [default: 8013]",
    )
    p.add_argument(
        "--prefer-masters", action="store_true",
        help="Set EDPS association_preference to 'master_per_quality_level' "
             "for this run, preferring master calibrations over reduced raw data.",
    )
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    runner = args.runner

    if args.no_sim and args.no_pipeline:
        sys.exit("Error: --no-sim and --no-pipeline both set; nothing to do.")

    if not args.no_sim and not args.yaml_files:
        p.error("YAML files are required unless --no-sim is given")

    if runner in ("docker", "podman") and not args.container:
        sys.exit(
            f"Error: --container NAME is required when --runner={runner}\n"
            f"You can also set the METIS_CONTAINER environment variable."
        )

    # Resolve and validate YAML paths
    yaml_files = []
    for raw in args.yaml_files:
        p = Path(raw).resolve()
        if not p.exists():
            sys.exit(f"Error: YAML file not found: {p}")
        yaml_files.append(p)

    # Locate the pipeline environment directory (metapkg runner only).
    # Prefers the repo root (consolidated install via the MTR Install tab),
    # then ./pipeline, then ./metis-meta-package (legacy standalone bootstrap).
    meta_pkg = None
    if runner == "metapkg":
        if args.meta_pkg:
            meta_pkg = Path(args.meta_pkg).resolve()
        else:
            _env_pkg = os.environ.get("METIS_META_PKG")
            candidates = (
                ([Path(_env_pkg)] if _env_pkg else []) +
                [Path.cwd(),
                 Path.cwd() / "pipeline",
                 Path.cwd() / "metis-meta-package"]
            )
            for candidate in candidates:
                if (candidate / ".env").exists():
                    meta_pkg = candidate
                    break
            else:
                meta_pkg = Path(_env_pkg) if _env_pkg else Path.cwd()
        if not (meta_pkg / ".env").exists():
            sys.exit(
                f"Error: pipeline environment not found at {meta_pkg}\n"
                "Run the Install tab in the MTR GUI, or pass --meta-pkg to point\n"
                "to an existing metis-meta-package directory."
            )

    # Locate METIS_Simulations
    # For docker/podman the path is resolved inside the container, so we skip
    # the existence check and default to the upstream image layout.
    if runner in ("docker", "podman"):
        sims_root = Path(args.simulations_dir) if args.simulations_dir \
                    else Path("/home/metis/METIS_Simulations")
        sims_cwd = sims_root
    else:
        sims_root = Path(args.simulations_dir).resolve() if args.simulations_dir \
                    else Path.cwd() / "METIS_Simulations"
        sims_cwd = sims_root
        if not (sims_root / "metis_simulations").is_dir():
            sys.exit(
                f"Error: METIS_Simulations not found at {sims_root}\n"
                "Pass --simulations-dir if it is installed elsewhere."
            )

    # Infer workflow and collect data tags from YAML (if provided)
    print(f"  Runner    : {runner}"
          + (f" (container: {args.container})" if args.container else ""))
    if yaml_files:
        print("Analysing YAML file(s) …")
        try:
            workflow, has_science, yaml_tags = infer_workflow(yaml_files)
        except ValueError as exc:
            sys.exit(f"Error: {exc}")
        print(f"  Workflow  : {workflow}")
        print(f"  Data tags : {sorted(yaml_tags) or '(none found)'}")
    else:
        # Pipeline-only with no YAML: workflow will be inferred from FITS headers
        workflow = None
        has_science = False
        yaml_tags = set()
        print("  Workflow  : (will be inferred from FITS headers)")

    # Create output directories
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_root = Path(args.output).resolve() if args.output \
                  else Path.cwd() / "output" / ts
    sim_out  = output_root / "sim"
    pipe_out = output_root / "pipeline"

    # When pipeline-only with an explicit input directory, use that as the
    # FITS source instead of <output>/sim/.
    if args.no_sim and args.pipeline_input:
        for d in args.pipeline_input:
            p = Path(d).resolve()
            if not p.is_dir():
                sys.exit(f"Error: pipeline input directory not found: {p}")
        sim_out = Path(args.pipeline_input[0]).resolve()

    if not args.no_sim:
        sim_out.mkdir(parents=True, exist_ok=True)
    pipe_out.mkdir(parents=True, exist_ok=True)

    # Shared directory for cached static calibration prototypes.
    # Placed alongside the per-run output directories so they persist.
    static_calibs_dir = output_root.parent / "static_calibs"

    print(f"\nOutput root      : {output_root}")
    if args.no_sim and args.pipeline_input and len(args.pipeline_input) > 1:
        for i, d in enumerate(args.pipeline_input):
            label = "  Pipeline input : " if i == 0 else "                   "
            print(f"{label}{Path(d).resolve()}")
    else:
        print(f"  Pipeline input : {sim_out}")
    print(f"  Pipeline output: {pipe_out}")
    if args.static:
        print(f"  Static calibs  : {static_calibs_dir}")
    print()

    # -----------------------------------------------------------------------
    # Step 1: Simulations
    # -----------------------------------------------------------------------
    if not args.no_sim:
        if args.inst_pkgs:
            # Explicit override: use as-is for docker/podman (container path),
            # resolve to absolute for metapkg/native.
            inst_pkgs_path = args.inst_pkgs if runner in ("docker", "podman") \
                             else str(Path(args.inst_pkgs).resolve())
        elif runner == "metapkg":
            inst_pkgs_path = str(Path(__file__).parent.resolve() / "inst_pkgs")
        elif runner == "native":
            inst_pkgs_path = str(Path.cwd() / "inst_pkgs")
        else:
            # docker/podman without explicit --inst-pkgs: ScopeSim resolves
            # ./inst_pkgs relative to sims_cwd inside the container.
            inst_pkgs_path = None
        sim_code = _build_sim_script(
            out_dir            = str(sim_out),
            do_calib           = args.calib,
            do_static          = args.static,
            n_cores            = args.cores,
            yaml_list          = [str(p) for p in yaml_files],
            inst_pkgs_path     = inst_pkgs_path,
            sims_root          = sims_root,
            static_calibs_dir  = str(static_calibs_dir),
        )

        print("=== Running simulations ===")
        rc = _run_simulation(runner, args.container, sim_code, sims_cwd,
                             meta_pkg=meta_pkg)
        if rc != 0:
            sys.exit(f"Error: simulation step failed (exit code {rc}).")

    # -----------------------------------------------------------------------
    # Step 1.5: Auto-fetch missing master calibrations (optional)
    # -----------------------------------------------------------------------
    if not args.no_pipeline and args.auto_fetch_calibrations:
        from archive import fetch_missing_calibrations

        fetch_tags = yaml_tags
        if args.no_sim:
            fetch_tags = yaml_tags | collect_tags_from_fits(sim_out)

        print("=== Checking for missing calibrations ===")
        try:
            fetched = fetch_missing_calibrations(
                workflow=workflow,
                data_tags=fetch_tags,
                has_science=has_science,
                dest_dir=sim_out,
                on_log=lambda msg: print(f"  {msg}"),
            )
            if fetched:
                print(f"  Downloaded {len(fetched)} master calibration file(s)")
            else:
                print("  No missing calibrations to fetch")
        except Exception as exc:
            print(f"  Warning: auto-fetch failed ({exc}); continuing without")

    # -----------------------------------------------------------------------
    # Step 2: EDPS pipeline
    # -----------------------------------------------------------------------
    if not args.no_pipeline:
        # When re-using existing FITS (--no-sim), classify them from headers
        # to determine which pipeline tasks apply.
        if args.no_sim:
            # Scan all pipeline input directories for FITS tags/workflow.
            input_dirs = ([str(Path(d).resolve()) for d in args.pipeline_input]
                          if args.pipeline_input else [str(sim_out)])
            fits_tags = set()
            for d in input_dirs:
                fits_tags |= collect_tags_from_fits(d)
            if fits_tags:
                print(f"  FITS tags found : {sorted(fits_tags)}")
            data_tags = yaml_tags | fits_tags
            if workflow is None:
                last_err = None
                for d in input_dirs:
                    try:
                        workflow = infer_workflow_from_fits(d)
                        print(f"  Workflow  : {workflow}")
                        break
                    except ValueError as exc:
                        last_err = exc
                if workflow is None and last_err is not None:
                    sys.exit(f"Error: {last_err}")
        else:
            data_tags = yaml_tags

        target_flags = infer_edps_target(workflow, data_tags, has_science)
        if target_flags:
            print(f"  EDPS target     : {' '.join(target_flags)}")
        else:
            print("  EDPS target     : (none inferred; EDPS will use workflow default)")

        edps_port = read_edps_port()
        edps_cmd  = _edps_base_cmd(runner, args.container, edps_port, meta_pkg)
        edps_cwd  = _edps_cwd(runner, meta_pkg)
        # EDPS and PyEsorex write log files to their cwd.  For local runners,
        # override cwd to pipe_out (host-accessible via the MTR bind mount) so
        # the logs land there.  uv finds the virtualenv via --project, so cwd
        # no longer needs to be the meta_pkg directory.
        if runner not in ("docker", "podman"):
            edps_cwd = str(pipe_out)

        # If --prefer-masters, temporarily patch EDPS config
        original_pref = None
        if args.prefer_masters:
            print("  Overriding association_preference → master_per_quality_level")
            original_pref = _set_association_preference("master_per_quality_level")

        # Warm up: start the EDPS server and confirm it is ready before
        # submitting the reduction job.
        print("=== Starting EDPS server ===")
        print("=== Listing Workflows    ===")
        rc = subprocess.run(edps_cmd + ["-lw"], cwd=edps_cwd).returncode
        if rc != 0:
            _restore_association_preference(original_pref)
            sys.exit(f"Error: EDPS server failed to start (exit code {rc}).")

        # Build EDPS input directories: sim output + any extra dirs + static
        # calibs cache.  EDPS uses nargs='*' for -i, so all paths must follow
        # a single -i flag (a second -i would replace the first, not append).
        edps_inputs = ["-i", str(sim_out)]
        if args.no_sim and args.pipeline_input:
            for d in args.pipeline_input[1:]:
                edps_inputs.append(str(Path(d).resolve()))
        if args.static and static_calibs_dir.is_dir():
            edps_inputs.append(str(static_calibs_dir))

        pipeline_rc = 1
        try:
            print("=== Running EDPS pipeline ===")
            pipeline_rc = subprocess.run(
                edps_cmd + [
                    "-w", workflow,
                ] + edps_inputs + [
                    "-o", str(pipe_out),
                ] + target_flags,
                cwd=edps_cwd,
            ).returncode
        finally:
            print("=== Stopping EDPS server ===")
            subprocess.run(edps_cmd + ["-s"], cwd=edps_cwd,
                           capture_output=True, timeout=15)
            _restore_association_preference(original_pref)
        if pipeline_rc != 0:
            sys.exit(f"Error: pipeline step failed (exit code {pipeline_rc}).")

    print(f"\nDone. Pipeline products are in: {pipe_out}")


if __name__ == "__main__":
    main()
