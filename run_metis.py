#!/usr/bin/env python3
"""
run_metis.py — METIS observation simulation + pipeline wrapper

Reads YAML observation-block files, uses ScopeSim to generate synthetic
FITS frames, then runs the matching EDPS workflow on those frames.

Prerequisite: the metis-meta-package bootstrap has been run on this machine.
All commands are executed via uv (the package manager installed by bootstrap.sh).

Usage:
    python run_metis.py [OPTIONS] yaml1.yaml [yaml2.yaml ...]

The workflow (lm_img / n_img / ifu / lm_lss / n_lss / …) is inferred
automatically from the DPR.TECH / mode values in the YAML blocks.
The pipeline target task is inferred from the data types present in the YAML
(or from FITS headers when --no-sim is used): it targets the deepest task in
the workflow chain whose main-input classification tag is present in the data.
If any block has catg="SCIENCE", the pipeline is run with -m science.
"""

import argparse
import os
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
                try:
                    return int(line.split("=", 1)[1])
                except ValueError:
                    pass
    return default


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
        if t in TECH_TO_WORKFLOW:
            return TECH_TO_WORKFLOW[t], has_science, data_tags
    for m in modes:
        if m in MODE_TO_WORKFLOW:
            return MODE_TO_WORKFLOW[m], has_science, data_tags

    raise ValueError(
        "Cannot determine workflow from YAML content.\n"
        f"  Found tech values : {techs}\n"
        f"  Found mode values : {modes}\n"
        f"  Known tech values : {list(TECH_TO_WORKFLOW)}\n"
        f"  Known mode values : {list(MODE_TO_WORKFLOW)}"
    )


def collect_tags_from_fits(fits_dir):
    """Return the set of EDPS classification tags present in a FITS directory.

    Reads ``HIERARCH ESO DPR CATG/TYPE/TECH`` from each ``.fits`` file and
    maps the triple to a tag name via ``DPR_TO_TAG``.  Files whose headers
    don't match any known rule are silently skipped.  Requires astropy.
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
            if catg:
                tag = DPR_TO_TAG.get((catg, typ, tech))
                if tag:
                    tags.add(tag)
        except Exception:
            continue
    return tags


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
# Simulation driver script (written to a temp file, then executed)
# ---------------------------------------------------------------------------

_SIM_SCRIPT = """\
import sys
sys.path.insert(0, "python")

# Override ScopeSim's default relative inst_pkgs path so it resolves to the
# copy bundled inside metis-meta-package regardless of the working directory.
import scopesim as sim
sim.rc.__config__["!SIM.file.local_packages_path"] = {inst_pkgs_path!r}

import runSimulationBlock as rsb

params = dict(
    outputDir = {out_dir!r},
    small     = {small!r},
    doStatic  = False,
    doCalib   = {do_calib!r},
    sequence  = False,
    startMJD  = None,
    calibFile = None,
    nCores    = {n_cores!r},
    testRun   = False,
)
rsb.runSimulationBlock({yaml_list!r}, params)
"""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "yaml_files", nargs="+", metavar="YAML",
        help="One or more observation-block YAML files",
    )
    p.add_argument(
        "-o", "--output", metavar="DIR",
        help="Root output directory [default: ./output/<timestamp>]",
    )
    p.add_argument(
        "-s", "--small", action="store_true",
        help="Use 32×32 pixel detectors (fast mode for testing)",
    )
    p.add_argument(
        "--calib", action="store_true",
        help="Auto-generate calibration frames (dark/flat) inferred from YAML content",
    )
    p.add_argument(
        "--cores", type=int, default=4, metavar="N",
        help="CPU cores for parallel simulations [default: 4]",
    )
    p.add_argument(
        "--no-sim", action="store_true",
        help="Skip simulations; run pipeline on existing <output>/sim/ directory",
    )
    p.add_argument(
        "--no-pipeline", action="store_true",
        help="Run simulations only; skip EDPS pipeline",
    )
    p.add_argument(
        "--meta-pkg", metavar="DIR",
        help="Path to the metis-meta-package directory "
             "[default: ~/metis-meta-package]",
    )
    p.add_argument(
        "--simulations-dir", metavar="DIR",
        help="Path to the METIS_Simulations repository "
             "[default: ~/METIS_Simulations]",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    if args.no_sim and args.no_pipeline:
        sys.exit("Error: --no-sim and --no-pipeline both set; nothing to do.")

    # Resolve and validate YAML paths
    yaml_files = []
    for raw in args.yaml_files:
        p = Path(raw).resolve()
        if not p.exists():
            sys.exit(f"Error: YAML file not found: {p}")
        yaml_files.append(p)

    # Locate metis-meta-package
    meta_pkg = Path(args.meta_pkg).resolve() if args.meta_pkg \
               else Path.home() / "metis-meta-package"
    if not (meta_pkg / ".env").exists():
        sys.exit(
            f"Error: metis-meta-package not found at {meta_pkg}\n"
            "Run metis-meta-package/bootstrap.sh first, or pass --meta-pkg."
        )

    # Locate METIS_Simulations
    sims_root = Path(args.simulations_dir).resolve() if args.simulations_dir \
                else Path.home() / "METIS_Simulations"
    sims_cwd = sims_root / "Simulations"
    if not sims_cwd.is_dir():
        sys.exit(
            f"Error: METIS_Simulations not found at {sims_root}\n"
            "Pass --simulations-dir if it is installed elsewhere."
        )

    # Infer workflow and collect data tags from YAML
    print("Analysing YAML file(s) …")
    try:
        workflow, has_science, yaml_tags = infer_workflow(yaml_files)
    except ValueError as exc:
        sys.exit(f"Error: {exc}")

    print(f"  Workflow  : {workflow}")
    print(f"  Data tags : {sorted(yaml_tags) or '(none found)'}")

    # Create output directories
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_root   = Path(args.output).resolve() if args.output \
                    else Path.cwd() / "output" / ts
    sim_out  = output_root / "sim"
    pipe_out = output_root / "pipeline"
    sim_out.mkdir(parents=True, exist_ok=True)
    pipe_out.mkdir(parents=True, exist_ok=True)

    print(f"\nOutput root      : {output_root}")
    print(f"  Simulated FITS : {sim_out}")
    print(f"  Pipeline output: {pipe_out}\n")

    # -----------------------------------------------------------------------
    # Step 1: Simulations
    # -----------------------------------------------------------------------
    if not args.no_sim:
        sim_code = _SIM_SCRIPT.format(
            inst_pkgs_path = str(meta_pkg / "inst_pkgs"),
            out_dir        = str(sim_out),
            small          = args.small,
            do_calib       = args.calib,
            n_cores        = args.cores,
            yaml_list      = [str(p) for p in yaml_files],
        )
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix="_run_sim.py", delete=False)
        tmp.write(sim_code)
        tmp.close()

        print("=== Running simulations ===")
        try:
            rc = subprocess.run(
                [
                    "uv", "run",
                    "--project", str(meta_pkg),
                    "--env-file", str(meta_pkg / ".env"),
                    "python", tmp.name,
                ],
                cwd=str(sims_cwd),
            ).returncode
        finally:
            os.unlink(tmp.name)

        if rc != 0:
            sys.exit(f"Error: simulation step failed (exit code {rc}).")

    # -----------------------------------------------------------------------
    # Step 2: EDPS pipeline
    # -----------------------------------------------------------------------
    if not args.no_pipeline:
        # When re-using existing FITS (--no-sim), classify them from headers
        # to determine which pipeline tasks apply.
        if args.no_sim:
            fits_tags = collect_tags_from_fits(sim_out)
            if fits_tags:
                print(f"  FITS tags found : {sorted(fits_tags)}")
            data_tags = yaml_tags | fits_tags
        else:
            data_tags = yaml_tags

        target_flags = infer_edps_target(workflow, data_tags, has_science)
        if target_flags:
            print(f"  EDPS target     : {' '.join(target_flags)}")
        else:
            print("  EDPS target     : (none inferred; EDPS will use workflow default)")

        edps_port = read_edps_port()
        uv_edps = ["uv", "run", "--env-file", str(meta_pkg / ".env"), "edps", "-P", str(edps_port)]

        # Warm up: start the EDPS server and confirm it is ready before
        # submitting the reduction job.
        print("=== Starting EDPS server ===")
        rc = subprocess.run(uv_edps + ["-lw"], cwd=str(meta_pkg)).returncode
        if rc != 0:
            sys.exit(f"Error: EDPS server failed to start (exit code {rc}).")

        edps_run = uv_edps + [
            "-w", workflow,
            "-i", str(sim_out),
            "-o", str(pipe_out),
        ] + target_flags

        print("=== Running EDPS pipeline ===")
        rc = subprocess.run(edps_run, cwd=str(meta_pkg)).returncode

        if rc != 0:
            sys.exit(f"Error: pipeline step failed (exit code {rc}).")

    print(f"\nDone. Pipeline products are in: {pipe_out}")


if __name__ == "__main__":
    main()
