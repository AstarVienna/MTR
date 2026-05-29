#!/usr/bin/env python3
"""
run_metis.py — METIS observation simulation + pipeline wrapper

Reads YAML observation-block files or AIT-format CSV test sequences, uses
ScopeSim to generate synthetic FITS frames, then runs the matching EDPS
workflow on those frames. YAML and CSV inputs may be mixed in a single run.

Usage:
    python run_metis.py [OPTIONS] input1.yaml [input2.csv ...]

Four execution modes are supported via --runner (or METIS_RUNNER env var):

  default
      Runs subprocesses inside MTR's own pipx/venv interpreter (sys.executable)
      and loads the .env written by the GUI Install tab into the subprocess
      environment. This is the standard mode after a fresh pipx install.

  native
      Calls edps / python directly from PATH. Use this when running inside
      a Docker/Podman container or on a bare-metal install.

  docker / podman
      Wraps every command with ``docker exec`` / ``podman exec`` into a named
      container. Requires --container NAME (or METIS_CONTAINER env var).
      The output directory must be bind-mounted into the container.

The workflow (lm_img / n_img / ifu / lm_lss / n_lss / …) is inferred
automatically from the DPR.TECH / mode values in YAML blocks. CSV content is
not parsed on the MTR side, so for CSV-only runs the workflow is auto-detected
from the simulated FITS headers instead (after Step 1); --workflow NAME is then
an optional override.
The pipeline target task is inferred from the data types present in the YAML
(or from FITS headers when --no-sim or all inputs are CSV): it targets the
deepest task in the workflow chain whose main-input classification tag is
present in the data. If any block has catg="SCIENCE", the pipeline is run
with -m science.
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
import yaml
from collections import Counter
from pathlib import Path
from datetime import datetime

from . import paths

# Recognised input file extensions
INPUT_EXTS = (".yaml", ".yml", ".csv")


# ---------------------------------------------------------------------------
# Workflow lookup tables
# ---------------------------------------------------------------------------

# Umbrella workflow that imports every METIS sub-workflow.  When mixed inputs
# span multiple sub-workflows we invoke this one and pass per-sub-workflow
# -t / -m targeting flags so EDPS can pick the right tasks from a single run.
UMBRELLA_WORKFLOW = "metis.metis_wkf"

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


def scan_yaml_inputs(yaml_files):
    """Return ``(data_tags, has_science, sub_workflows)`` by scanning YAML.

    Unlike :func:`infer_workflow`, this does *not* pick a single sub-workflow.
    Instead it walks every block and records the set of sub-workflows
    implied by the per-block ``properties.tech`` / ``mode`` value.  Blocks
    whose tech/mode are unrecognised are silently skipped (their tags are
    still collected); the caller is expected to handle an empty
    *sub_workflows* set explicitly.
    """
    has_science = False
    data_tags = set()
    sub_workflows = set()

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
            if props.get("catg", "").upper() == "SCIENCE":
                has_science = True
            wf = None
            tech = props.get("tech", "")
            if tech:
                wf = TECH_TO_WORKFLOW.get(_normalize_tech(tech))
            if wf is None:
                mode = block.get("mode", "")
                if mode:
                    wf = MODE_TO_WORKFLOW.get(_normalize_mode(mode))
            if wf:
                sub_workflows.add(wf)

    return data_tags, has_science, sub_workflows


def scan_fits_inputs(fits_dir):
    """Return ``(data_tags, sub_workflows)`` from FITS headers in *fits_dir*.

    Walks ``*.fits`` recursively (same as :func:`collect_tags_from_fits`).
    Each file's ``HIERARCH ESO DPR TECH`` is mapped via ``TECH_TO_WORKFLOW``
    to identify which sub-workflow it belongs to.  Files whose tech is
    unrecognised contribute their classification tag (if any) but do not
    activate a sub-workflow.
    """
    data_tags = set()
    sub_workflows = set()
    try:
        from astropy.io import fits as afits
    except ImportError:
        return data_tags, sub_workflows

    for f in Path(fits_dir).rglob("*.fits"):
        try:
            with afits.open(f, memmap=True) as hdul:
                hdr = hdul[0].header
                catg = hdr.get("HIERARCH ESO DPR CATG", "").strip()
                typ = hdr.get("HIERARCH ESO DPR TYPE", "").strip()
                tech = hdr.get("HIERARCH ESO DPR TECH", "").strip()
                pro_catg = hdr.get("HIERARCH ESO PRO CATG", "").strip()
        except Exception:
            continue

        if catg:
            tag = DPR_TO_TAG.get((catg, typ, tech))
        else:
            tag = pro_catg or None
        if tag:
            data_tags.add(tag)

        if tech:
            wf = TECH_TO_WORKFLOW.get(_normalize_tech(tech))
            if wf:
                sub_workflows.add(wf)

    return data_tags, sub_workflows


def infer_workflow(input_files):
    """Return (workflow, has_science, data_tags) by scanning YAML blocks.

    CSV inputs are skipped here: their AIT format does not carry enough
    information for MTR to map to an EDPS workflow without parsing the full
    csvParser logic. When the input list contains *no* YAML files this
    function raises ValueError; callers should then require an explicit
    --workflow choice (or fall back to FITS header inference).

    For YAML files, checks ``properties.tech`` first, then ``mode`` as a
    fallback. ``has_science`` is True when any block has
    ``properties.catg == "SCIENCE"``. ``data_tags`` is the set of
    ``do.catg`` values found across all YAML blocks; these equal the EDPS
    classification tag names for the generated FITS files.
    """
    yaml_files = [p for p in input_files
                  if Path(p).suffix.lower() in (".yaml", ".yml")]
    if not yaml_files:
        raise ValueError(
            "No YAML files provided; workflow cannot be inferred from CSV.\n"
            "  Pass --workflow NAME explicitly, or skip the pipeline with --no-pipeline."
        )

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


def known_workflows():
    """Return the sorted set of workflow names recognised by infer_workflow.

    Exposed so the GUI workflow dropdown stays in sync with the CLI choices.
    """
    return sorted(set(TECH_TO_WORKFLOW.values()) | set(MODE_TO_WORKFLOW.values()))


def classify_fits_file(path):
    """Return the EDPS classification tag for a single FITS file, or None.

    Reads ``HIERARCH ESO DPR CATG/TYPE/TECH`` and maps the triple via
    ``DPR_TO_TAG``.  Falls back to ``HIERARCH ESO PRO CATG`` when no DPR
    headers are present.  Returns ``None`` on any failure (missing astropy,
    unreadable file, unmatched headers).
    """
    try:
        from astropy.io import fits as afits
    except ImportError:
        return None

    try:
        with afits.open(path, memmap=True) as hdul:
            hdr = hdul[0].header
            catg = hdr.get("HIERARCH ESO DPR CATG", "").strip()
            typ  = hdr.get("HIERARCH ESO DPR TYPE", "").strip()
            tech = hdr.get("HIERARCH ESO DPR TECH", "").strip()
            pro_catg = hdr.get("HIERARCH ESO PRO CATG", "").strip()
    except Exception:
        return None

    if catg:
        return DPR_TO_TAG.get((catg, typ, tech))
    if pro_catg:
        return pro_catg
    return None


def collect_tags_from_fits(fits_dir):
    """Return the set of EDPS classification tags present in a FITS directory.

    Walks *fits_dir* recursively so that ``--pipeline-input`` pointing at an
    output-root (containing ``sim/``, ``pipeline/``, …) still finds raw FITS.
    For each ``.fits`` file, delegates to :func:`classify_fits_file`.  Files
    whose headers don't match any known rule are silently skipped.
    """
    tags = set()
    for f in Path(fits_dir).rglob("*.fits"):
        tag = classify_fits_file(f)
        if tag:
            tags.add(tag)
    return tags


def infer_workflow_from_fits(fits_dir):
    """Infer the EDPS workflow from DPR TECH headers in a FITS directory.

    Used when --no-sim is given without any YAML files, or when all inputs
    are CSV (CSV cannot be auto-classified in MTR).
    """
    try:
        from astropy.io import fits as afits
    except ImportError:
        raise ValueError("astropy is required to infer workflow from FITS headers.")

    techs = []
    for f in Path(fits_dir).rglob("*.fits"):
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


def infer_edps_targets_for_workflows(data_tags, has_science, sub_workflows):
    """Combine per-sub-workflow EDPS target flags for an umbrella-workflow run.

    For each sub-workflow in *sub_workflows*, picks the deepest non-science
    task whose main-input tag is present in *data_tags* (same logic as
    :func:`infer_edps_target`).  Deduplicates so that:

    - A given ``-t <task>`` appears at most once even if multiple sub-workflows
      share the same task name (e.g. LM IMG / RAVC / APP all share the
      ``metis_lm_img_*`` calibration tasks).
    - A single ``-m qc1calib`` is emitted if any sub-workflow contributes one.
    - ``-m science`` is appended once when *has_science* is true.

    Sub-workflow iteration order follows ``WORKFLOW_TASK_CHAIN`` for
    deterministic flag ordering.
    """
    flags = []
    seen_tasks = set()
    seen_qc1 = False

    for wf in WORKFLOW_TASK_CHAIN:
        if wf not in sub_workflows:
            continue
        # Reuse the single-workflow logic without its has_science handling
        # so that "-m science" appears at most once at the end.
        sub_flags = infer_edps_target(wf, data_tags, has_science=False)
        i = 0
        while i < len(sub_flags):
            kind, val = sub_flags[i], sub_flags[i + 1]
            if kind == "-t":
                if val not in seen_tasks:
                    flags += ["-t", val]
                    seen_tasks.add(val)
            elif kind == "-m" and val == "qc1calib":
                if not seen_qc1:
                    flags += ["-m", "qc1calib"]
                    seen_qc1 = True
            i += 2

    if has_science:
        flags += ["-m", "science"]

    return flags


# ---------------------------------------------------------------------------
# Simulation driver script builder
# ---------------------------------------------------------------------------

def _build_sim_script(out_dir, do_calib, do_static, n_cores, input_list,
                      inst_pkgs_path=None, sims_root=None,
                      static_calibs_dir=None, do_sim=True):
    """Return the simulation driver script as a string.

    When *inst_pkgs_path* is given (default and native runners) the script
    overrides ScopeSim's local_packages_path and auto-downloads the instrument
    packages into that directory if the METIS package is not yet present.

    When *static_calibs_dir* is given (and *do_static* is true) the script
    generates static calibration prototypes (PERSISTENCE_MAP, ATM_PROFILE, …)
    into that directory instead of the simulation output, reusing any files
    that already exist there.  This avoids regenerating ~200 MB of FITS files
    on every run.

    When *do_sim* is False, the runSimulationBlock() call is omitted so the
    script only runs the static calibration generation block.  Used by the
    pipeline-only (--no-sim) path to populate the static calibs cache when
    it is missing, without re-running any of the sim blocks.
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
            "    sim.download_packages('METIS', release='2026-04-17')",
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
        'if __name__ == "__main__":',
        "    pass",
    ]
    if do_sim:
        lines += [
            "    from metis_simulations import runSimulationBlock as rsb",
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
            f"        rsb.runSimulationBlock({input_list!r}, params, [])",
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

def _check_default_env(runner):
    """Validate that the default runner can be used.

    Confirms the .env file written by the GUI Install tab exists at
    ``paths.env_file()``; otherwise points the user at the Install tab.
    """
    if runner != "default":
        return
    env_file = paths.env_file()
    if not env_file.exists():
        raise FileNotFoundError(
            f"{env_file} not found — run the Install tab in the MTR GUI"
        )


def _default_subprocess_env() -> dict[str, str]:
    """Build the subprocess env for the default runner.

    Merges the parent process env with the parsed .env file written by the
    Install tab, and prepends the MTR pipx/venv ``bin/`` to ``PATH`` so
    ``edps`` and ``pyesorex`` resolve to the venv copies even when the
    parent's PATH does not include them.
    """
    from dotenv import dotenv_values
    env = os.environ.copy()
    env.update({k: v for k, v in dotenv_values(paths.env_file()).items()
                if v is not None})
    venv_bin = str(Path(sys.executable).parent)
    env["PATH"] = venv_bin + os.pathsep + env.get("PATH", "")
    return env


def _run_simulation(runner, container, sim_code, sims_cwd):
    """Execute the simulation script in the appropriate environment.

    - default : run with ``sys.executable`` and the merged Install-tab env
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

    _check_default_env(runner)

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix="_run_sim.py",
                                     delete=False)
    tmp.write(sim_code)
    tmp.close()
    try:
        if runner == "default":
            return subprocess.run(
                [sys.executable, tmp.name],
                cwd=str(sims_cwd),
                env=_default_subprocess_env(),
            ).returncode
        else:  # native
            return subprocess.run(
                ["python", tmp.name],
                cwd=str(sims_cwd),
            ).returncode
    finally:
        os.unlink(tmp.name)


def _edps_base_cmd(runner, container, edps_port):
    """Return the command prefix list up to and including the edps port flag."""
    base = ["edps", "-P", str(edps_port)]
    if runner in ("docker", "podman"):
        return [runner, "exec", container] + base
    return base  # default / native


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
        "input_files", nargs="*", metavar="INPUT",
        help="One or more observation-block files (.yaml, .yml, or .csv); "
             "YAML and CSV may be mixed. Not required with --no-sim.",
    )
    p.add_argument(
        "--workflow", metavar="NAME", default=None,
        choices=known_workflows(),
        help="Force EDPS workflow name (e.g. metis.metis_lm_img_wkf). "
             "Optional: the workflow is auto-detected from YAML content, or "
             "from the simulated FITS headers for CSV-only runs. Pass this to "
             "override the auto-detection.",
    )
    p.add_argument(
        "-o", "--output", metavar="DIR",
        default=os.environ.get("METIS_OUTPUT_DIR"),
        help="Root output directory [default: ./output/<timestamp>] "
             "(env: METIS_OUTPUT_DIR)",
    )
    p.add_argument(
        "--calib", type=int, nargs="?", const=1, default=1, metavar="N",
        help="Auto-generate N calibration frames (dark/flat) per unique config, "
             "inferred from input content. Bare --calib = 1; --calib 0 disables. "
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
        default=([os.environ["METIS_PIPELINE_INPUT"]]
                 if os.environ.get("METIS_PIPELINE_INPUT") else None),
        help="Directory containing FITS files to use as pipeline input. "
             "May be specified multiple times. Only used with --no-sim. "
             "When omitted, defaults to <output>/sim/. "
             "(env: METIS_PIPELINE_INPUT)",
    )
    p.add_argument(
        "--no-pipeline", action="store_true",
        help="Run simulations only; skip EDPS pipeline",
    )
    p.add_argument(
        "--runner",
        choices=["default", "native", "docker", "podman"],
        default=os.environ.get("METIS_RUNNER", "default"),
        help="Execution mode: default runs subprocesses inside MTR's own "
             "pipx/venv and loads the Install-tab .env; native calls tools "
             "directly from PATH (bare-metal or inside a container); "
             "docker/podman exec commands into a running container "
             "(env: METIS_RUNNER)",
    )
    p.add_argument(
        "--container", metavar="NAME",
        default=os.environ.get("METIS_CONTAINER"),
        help="Container name or ID for --runner=docker/podman "
             "(env: METIS_CONTAINER)",
    )
    p.add_argument(
        "--simulations-dir", metavar="DIR",
        default=os.environ.get("METIS_SIMULATIONS_DIR"),
        help="Path to the METIS_Simulations repository. For docker/podman "
             "runners this must be the path *inside* the container "
             "[default: ./METIS_Simulations for default/native, "
             "/home/metis/METIS_Simulations for docker/podman] "
             "(env: METIS_SIMULATIONS_DIR)",
    )
    p.add_argument(
        "--inst-pkgs", metavar="DIR",
        default=os.environ.get("METIS_INST_PKGS"),
        help="Path to the ScopeSim instrument packages directory "
             "(Armazones, ELT, METIS, …). "
             "For the default runner this defaults to the user data dir "
             "(~/.local/share/metis-test-runner/inst_pkgs). "
             "For the native runner this defaults to ./inst_pkgs relative to "
             "the current working directory — ScopeSim will download packages "
             "there on first use. "
             "For docker/podman runners supply the container-internal path; "
             "if omitted ScopeSim resolves ./inst_pkgs inside the container. "
             "(env: METIS_INST_PKGS)",
    )
    p.add_argument(
        "--auto-fetch-calibrations", action="store_true",
        help="Automatically download missing master calibration files from "
             "the remote METIS archive before running the pipeline. "
             "Requires MetisWISE to be installed and ~/.awe/Environment.cfg "
             "to hold valid credentials (see Archive tab).",
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

    if not args.no_sim and not args.input_files:
        p.error("Input files (YAML or CSV) are required unless --no-sim is given")

    if runner in ("docker", "podman") and not args.container:
        sys.exit(
            f"Error: --container NAME is required when --runner={runner}\n"
            f"You can also set the METIS_CONTAINER environment variable."
        )

    # Resolve and validate input paths (YAML and/or CSV)
    input_files = []
    for raw in args.input_files:
        p = Path(raw).resolve()
        if not p.exists():
            sys.exit(f"Error: input file not found: {p}")
        if p.suffix.lower() not in INPUT_EXTS:
            sys.exit(
                f"Error: unsupported input extension: {p.suffix} ({p})\n"
                f"  Supported extensions: {', '.join(INPUT_EXTS)}"
            )
        input_files.append(p)

    # For the default runner, confirm the Install tab has produced a .env
    # under paths.data_dir(). Other runners don't need this check.
    if runner == "default" and not paths.env_file().exists():
        sys.exit(
            f"Error: {paths.env_file()} not found.\n"
            "Run the Install tab in the MTR GUI before invoking the default runner."
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
                    else paths.simulations_dir()
        sims_cwd = sims_root
        if not (sims_root / "metis_simulations").is_dir():
            sys.exit(
                f"Error: METIS_Simulations not found at {sims_root}\n"
                "Pass --simulations-dir if it is installed elsewhere."
            )

    # Collect data tags and the set of sub-workflows implied by the YAML
    # blocks (if provided).  We always run EDPS against the umbrella workflow
    # ``metis.metis_wkf`` (which imports every sub-workflow), so we never need
    # to pick a single sub-workflow up front — but knowing which sub-workflows
    # are present lets us compute the right -t / -m flags and drive the
    # auto-fetch step per sub-workflow.
    # In --no-sim mode YAML/CSV content is ignored — tags and sub-workflows
    # come from the FITS headers in --pipeline-input, so a leftover input from
    # a previous simulate+run cannot mislead the pipeline step here.
    print(f"  Runner    : {runner}"
          + (f" (container: {args.container})" if args.container else ""))
    workflow = UMBRELLA_WORKFLOW
    has_science = False
    yaml_tags = set()
    yaml_sub_workflows = set()

    if input_files and not args.no_sim:
        by_ext = Counter(p.suffix.lower() for p in input_files)
        breakdown = ", ".join(
            f"{n} {ext.lstrip('.').upper()}"
            for ext, n in sorted(by_ext.items())
        )
        print(f"Analysing input file(s)  ({breakdown}) …")
        for p in input_files:
            kind = "YAML" if p.suffix.lower() in (".yaml", ".yml") else "CSV"
            print(f"    [{kind:<4}] {p}")

        yaml_subset = [p for p in input_files
                       if p.suffix.lower() in (".yaml", ".yml")]
        csv_only = all(p.suffix.lower() == ".csv" for p in input_files)

        if args.no_pipeline:
            # Sim-only: workflow / sub-workflow values are unused downstream;
            # skip inference so a CSV-only set never trips on it here.
            print("  Workflow      : (not needed in --no-pipeline mode)")
        elif csv_only:
            # CSV content is not parsed on the MTR side. --workflow is optional:
            # if given, use it as an explicit override; otherwise the active
            # sub-workflow(s) and data tags are inferred from the simulated FITS
            # headers after Step 1 (see the pipeline step below). EDPS always
            # runs the umbrella workflow, so a single sub-workflow is never
            # needed up front.
            if args.workflow:
                yaml_sub_workflows = {args.workflow}
                print(f"  Workflow      : {workflow}")
                print(f"  Sub-workflows : {sorted(yaml_sub_workflows)}  "
                      "(explicit override; CSV-only run)")
            else:
                print("  Workflow      : (auto-detected from simulated FITS; "
                      "CSV-only run)")
            print("  Data tags     : (will be inferred from FITS headers)")
        else:
            yaml_tags, has_science, yaml_sub_workflows = scan_yaml_inputs(yaml_subset)
            if not yaml_sub_workflows:
                sys.exit(
                    "Error: could not identify any METIS sub-workflow from the "
                    "YAML input(s). Check that properties.tech or mode is set "
                    "to one of the known values."
                )
            if args.workflow and args.workflow not in yaml_sub_workflows:
                print(f"  Sub-workflows : {{{args.workflow}}}  "
                      f"(override; inferred was {sorted(yaml_sub_workflows)})")
                yaml_sub_workflows = {args.workflow}
            else:
                print(f"  Workflow      : {workflow}")
                print(f"  Sub-workflows : {sorted(yaml_sub_workflows)}")
            print(f"  Data tags     : {sorted(yaml_tags) or '(none found)'}")
    else:
        if input_files:
            print(f"  Note          : ignoring {len(input_files)} input file(s) "
                  "in --no-sim mode")
        print(f"  Workflow      : {workflow}")
        print("  Sub-workflows : (will be inferred from FITS headers)")

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
            # resolve to absolute for default/native.
            inst_pkgs_path = args.inst_pkgs if runner in ("docker", "podman") \
                             else str(Path(args.inst_pkgs).resolve())
        elif runner == "default":
            inst_pkgs_path = str(paths.inst_pkgs_dir())
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
            input_list         = [str(p) for p in input_files],
            inst_pkgs_path     = inst_pkgs_path,
            sims_root          = sims_root,
            static_calibs_dir  = str(static_calibs_dir),
        )

        print("=== Running simulations ===")
        rc = _run_simulation(runner, args.container, sim_code, sims_cwd)
        if rc != 0:
            sys.exit(f"Error: simulation step failed (exit code {rc}).")

    # -----------------------------------------------------------------------
    # Step 1.25: Static calibration prototypes (pipeline-only path)
    # -----------------------------------------------------------------------
    # When --no-sim is set the simulation step is skipped entirely, so the
    # statics that would normally be produced alongside the sim are never
    # generated. Run the same sim script with do_sim=False (which keeps only
    # the static calib block) when the shared cache is empty.
    elif not args.no_pipeline and args.static \
            and not (static_calibs_dir / "PERSISTENCE_MAP_LM.fits").is_file():
        sim_code = _build_sim_script(
            out_dir            = str(sim_out),
            do_calib           = 0,
            do_static          = args.static,
            n_cores            = args.cores,
            input_list         = [],
            inst_pkgs_path     = None,
            sims_root          = sims_root,
            static_calibs_dir  = str(static_calibs_dir),
            do_sim             = False,
        )
        print("=== Generating static calibration prototypes ===")
        rc = _run_simulation(runner, args.container, sim_code, sims_cwd)
        if rc != 0:
            sys.exit(f"Error: static calibration generation failed (exit code {rc}).")

    # -----------------------------------------------------------------------
    # Step 1.5: Auto-fetch missing master calibrations (optional)
    # -----------------------------------------------------------------------
    if not args.no_pipeline and args.auto_fetch_calibrations:
        from .archive import (
            fetch_missing_calibrations,
            identify_missing_calibrations,
        )

        fetch_tags = yaml_tags
        fetch_sub_workflows = set(yaml_sub_workflows)
        if args.no_sim:
            fits_tags_now, fits_wfs_now = scan_fits_inputs(sim_out)
            fetch_tags = yaml_tags | fits_tags_now
            fetch_sub_workflows = fetch_sub_workflows | fits_wfs_now

        print("=== Checking for missing calibrations ===")
        all_missing = []
        all_fetched = []
        for wf in sorted(fetch_sub_workflows):
            try:
                missing = identify_missing_calibrations(
                    workflow=wf,
                    data_tags=fetch_tags,
                    has_science=has_science,
                )
                if not missing:
                    continue
                all_missing.extend(missing)
                fetched = fetch_missing_calibrations(
                    workflow=wf,
                    data_tags=fetch_tags,
                    has_science=has_science,
                    dest_dir=sim_out,
                    on_log=lambda msg: print(f"  {msg}"),
                )
                all_fetched.extend(fetched)
            except Exception as exc:
                print(f"  Warning: auto-fetch for {wf} failed ({exc}); "
                      "continuing without")
        if not all_missing:
            print("  All required calibrations already present")
        elif all_fetched:
            print(f"  Downloaded {len(all_fetched)} master calibration file(s)")
        else:
            catgs = ", ".join(pc for _, pc in all_missing)
            print(f"  Missing masters not available in archive: {catgs}")

    # -----------------------------------------------------------------------
    # Step 2: EDPS pipeline
    # -----------------------------------------------------------------------
    if not args.no_pipeline:
        # When re-using existing FITS (--no-sim), classify them from headers
        # to determine which sub-workflows' tasks apply.
        if args.no_sim:
            # Scan all pipeline input directories for FITS tags and the set
            # of sub-workflows their DPR.TECH headers imply.  EDPS itself
            # picks the right files per workflow from the shared input dir,
            # so we just need to cover every workflow whose raws are present.
            input_dirs = ([str(Path(d).resolve()) for d in args.pipeline_input]
                          if args.pipeline_input else [str(sim_out)])
            fits_tags = set()
            active_sub_workflows = set()
            for d in input_dirs:
                tags, wfs = scan_fits_inputs(d)
                fits_tags |= tags
                active_sub_workflows |= wfs
            if fits_tags:
                print(f"  FITS tags found : {sorted(fits_tags)}")
            if active_sub_workflows:
                print(f"  Sub-workflows   : {sorted(active_sub_workflows)}")
            else:
                sys.exit(
                    "Error: could not identify any METIS sub-workflow from "
                    "FITS headers in: " + ", ".join(input_dirs)
                )
            data_tags = fits_tags
            # Re-derive has_science from the FITS tags + active workflow
            # chains so the -m science flag is added when science raws are
            # present.
            has_science = any(
                meta == "science" and tag in data_tags
                for wf in active_sub_workflows
                for _, tag, meta in WORKFLOW_TASK_CHAIN.get(wf, [])
            )
        else:
            data_tags = yaml_tags
            active_sub_workflows = yaml_sub_workflows
            # CSV inputs carry no MTR-side tags or sub-workflow (their content
            # is not parsed), so these are empty for a CSV-only run.  Without
            # them no EDPS -t target is inferred and EDPS falls back to the
            # workflow's science products, which a calibration-only run cannot
            # satisfy -> 0 jobs created.  Recover both from the FITS we just
            # simulated, mirroring the --no-sim path, so the correct calibration
            # target gets scheduled even when --workflow was omitted.
            if not data_tags or not active_sub_workflows:
                fits_tags, fits_wfs = scan_fits_inputs(sim_out)
                if not data_tags:
                    data_tags = fits_tags
                if not active_sub_workflows:
                    active_sub_workflows = fits_wfs
                if data_tags:
                    print(f"  Data tags     : {sorted(data_tags)}  "
                          "(inferred from simulated FITS)")
                if active_sub_workflows:
                    print(f"  Sub-workflows : {sorted(active_sub_workflows)}  "
                          "(inferred from simulated FITS)")
                has_science = any(
                    meta == "science" and tag in data_tags
                    for wf in active_sub_workflows
                    for _, tag, meta in WORKFLOW_TASK_CHAIN.get(wf, [])
                )
            if not active_sub_workflows:
                sys.exit(
                    "Error: could not identify any METIS sub-workflow from the "
                    "simulated FITS headers.\n"
                    "  Pass --workflow NAME to set it explicitly."
                )

        target_flags = infer_edps_targets_for_workflows(
            data_tags, has_science, active_sub_workflows,
        )
        if target_flags:
            print(f"  EDPS target     : {' '.join(target_flags)}")
        else:
            print("  EDPS target     : (none inferred; EDPS will use workflow default)")

        edps_port = read_edps_port()
        edps_cmd  = _edps_base_cmd(runner, args.container, edps_port)
        # EDPS and PyEsorex write log files to their cwd.  For local runners
        # cwd to pipe_out (host-accessible via the MTR bind mount) so the logs
        # land there; for docker/podman the container resolves cwd internally.
        edps_cwd = None if runner in ("docker", "podman") else str(pipe_out)
        edps_env = _default_subprocess_env() if runner == "default" else None

        # If --prefer-masters, temporarily patch EDPS config
        original_pref = None
        if args.prefer_masters:
            print("  Overriding association_preference → master_per_quality_level")
            original_pref = _set_association_preference("master_per_quality_level")

        # Warm up: start the EDPS server and confirm it is ready before
        # submitting the reduction job.
        print("=== Starting EDPS server ===")
        print("=== Listing Workflows    ===")
        rc = subprocess.run(edps_cmd + ["-lw"], cwd=edps_cwd, env=edps_env).returncode
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
                env=edps_env,
            ).returncode
        finally:
            print("=== Stopping EDPS server ===")
            subprocess.run(edps_cmd + ["-s"], cwd=edps_cwd, env=edps_env,
                           capture_output=True, timeout=15)
            _restore_association_preference(original_pref)
        if pipeline_rc != 0:
            sys.exit(f"Error: pipeline step failed (exit code {pipeline_rc}).")

    print(f"\nDone. Pipeline products are in: {pipe_out}")


if __name__ == "__main__":
    main()
