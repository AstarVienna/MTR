# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains the **METIS Pipeline** — a Python-based astronomical data reduction pipeline for the Mid-infrared E-ELT Imager and Spectrograph (METIS) instrument at ESO. The main source code lives in `METIS_Pipeline/metisp/pymetis/`.

## Common Commands

All commands assume the working directory is `METIS_Pipeline/`.

### Environment Setup

```bash
export PYTHONPATH="$(pwd)/metisp/pymetis/src/"
export PYCPL_RECIPE_DIR="$(pwd)/metisp/pyrecipes/"
export PYESOREX_PLUGIN_DIR="$(pwd)/metisp/pyrecipes/"
export SOF_DATA="$(pwd)/METIS_Pipeline_Test_Data/small202402/outputSmall/"
export SOF_DIR="$(pwd)/METIS_Pipeline_Test_Data/small202402/sofFiles/"
```

### Running Tests

```bash
# Run all unit tests (excluding external/integration tests)
python -m pytest -m "not external and not inputset and not pyesorex and not recipe"

# Run a single test file
python -m pytest metisp/pymetis/src/pymetis/tests/recipes/test_metis_det_dark.py

# Run tests by marker
python -m pytest -m "recipe"
python -m pytest -m "dataitem"
```

### Available Test Markers
- `edps` — EDPS integration tests (long-running)
- `inputset` — InputSet tests
- `external` — requires external test data
- `dataitem` — DataItem tests
- `product` — product tests
- `recipe` — recipe tests (require pyesorex)
- `pyesorex` — tests needing pyesorex
- `slow` — full-size data tests
- `metadata` — recipe metadata tests

### Running Recipes

```bash
# List available recipes
pyesorex --recipes

# Run a recipe with a SOF file
pyesorex metis_det_lingain "${SOF_DIR}/metis_det_lingain.lm.sof"

# Run via EDPS workflow
edps -w metis.metis_lm_img_wkf -i $SOF_DATA -c   # dry run / check
edps -w metis.metis_lm_img_wkf -i $SOF_DATA        # process
```

### Installation

```bash
# Install system dependencies (Ubuntu 24.04)
./toolbox/install_dependencies_ubuntu.sh

# Install EDPS and PyEsoRex
./toolbox/install_edps.sh

# Configure EDPS
./toolbox/create_config.sh
```

## Code Architecture

The pipeline is built around the ESO CPL/PyEsoRex/EDPS frameworks and follows a **decoupled recipe pattern**:

```
MetisRecipe  (cpl.ui.PyRecipe subclass — public interface registered with pyesorex)
    └── MetisRecipeImpl  (holds all processing logic)
            ├── InputSet  (declares required/optional input frames by tag)
            ├── DataItem  (typed wrappers for FITS frames/tables with metadata)
            └── Mixins    (orthogonal capabilities: band, detector, mode)
```

### Key Directories under `metisp/pymetis/src/pymetis/`

- **`classes/recipes/`** — `MetisRecipe` (base) and `MetisRecipeImpl` (implementation base); all recipes inherit from these.
- **`classes/dataitems/`** — `DataItem` hierarchy: base classes for images, tables, HDUs, product sets.
- **`classes/inputs/`** — `PipelineInput` and `InputSet`; define what frames a recipe consumes.
- **`classes/mixins/`** — Composable traits (e.g., `BandLmMixin`, `Detector2rgMixin`) mixed into recipe implementations.
- **`classes/prefab/`** — Reusable recipe implementation templates (e.g., `MetisBaseImgFlatImpl`).
- **`dataitems/`** — Concrete `DataItem` subclasses organized by data type: `masterdark/`, `masterflat/`, `background/`, `distortion/`, `img/`, `lss/`, `ifu/`, etc.
- **`recipes/`** — Concrete recipe implementations organized by observing mode:
  - `det/` — detector-level (dark, linearity/gain)
  - `lm_img/`, `n_img/` — L/M-band and N-band imaging
  - `lm_lss/`, `n_lss/` — long-slit spectroscopy
  - `ifu/` — integral field unit
  - `hci/` — high-contrast imaging (ADI, CGraPH)
  - `cal/` — chop/home calibration
- **`tests/`** — Mirror of the recipe/class structure; uses `conftest.py` fixtures.
- **`workflows/`** — EDPS workflow definitions (orchestrate sequences of recipes).

### Adding a New Recipe

1. Create a `DataItem` subclass in `dataitems/` for any new frame types.
2. Create an `InputSet` subclass in the recipe's module declaring required tags.
3. Implement `MetisRecipeImpl` subclass with `process()` logic; apply mixins as needed.
4. Wrap it in a `MetisRecipe` subclass (thin dispatcher) and place it in `recipes/<mode>/`.
5. Register the recipe module in `metisp/pyrecipes/` so pyesorex discovers it.
6. Add tests mirroring the recipe path under `tests/`.

### C Extension (`metism/`)

Low-level image processing library written in C/C++ with an Autotools build system. Depends on HDRL and IRPlib. Build separately before running recipes that require native CPL routines.

## Simulation + Pipeline Wrapper

`run_metis.py` is a CLI wrapper intended for end users who have run `metis-meta-package/bootstrap.sh` on their machine. It generates synthetic FITS data via ScopeSim and runs the matching EDPS workflow, all on the host.

**Prerequisite**: `metis-meta-package/bootstrap.sh` must have been run. The wrapper looks for `~/metis-meta-package` and `~/METIS_Simulations` by default (the locations bootstrap creates them).

```bash
# Basic usage – infers workflow from YAML content
python run_metis.py LMS_RAD_06.yaml

# Multiple YAML files, custom output dir, with auto-calibration frames
python run_metis.py -o /tmp/myrun --calib obs1.yaml obs2.yaml

# Fast mode (32×32 detectors) for testing
python run_metis.py --small LMS_RAD_06.yaml

# Only run simulations (no pipeline)
python run_metis.py --no-pipeline LMS_RAD_06.yaml

# Only run pipeline on previously simulated data
python run_metis.py --no-sim -o /tmp/myrun LMS_RAD_06.yaml

# Override install locations if bootstrap used non-default paths
python run_metis.py --meta-pkg /opt/metis-meta-package --simulations-dir /opt/METIS_Simulations obs.yaml
```

Output is written to `./output/<timestamp>/sim/` (FITS frames) and `./output/<timestamp>/pipeline/` (pipeline products).

**Workflow inference**: reads `properties.tech` (primary) or `mode` (fallback) from each YAML block to select the EDPS workflow.

**Pipeline task targeting**: EDPS defaults to the last (leaf) task in a workflow, which is always a SCIENCE task. Without an explicit target this causes 0 jobs to be scheduled when only calibration data is present. `run_metis.py` avoids this by inspecting `do.catg` values in the YAML (the EDPS classification tag name for each block's output FITS) and matching them against `WORKFLOW_TASK_CHAIN` — an ordered list of `(task_name, main_input_tag, meta_target)` per workflow. It targets the deepest task whose main-input tag is present in the data:
- Tasks with no `meta_target` (IFU/IMG calibrations) → `-t <task_name>`
- Tasks gated by `QC1_CALIB` (all LSS calibrations) → `-m qc1calib`
- Science data present → also appends `-m science`

When `--no-sim` is used, FITS headers (`HIERARCH ESO DPR CATG/TYPE/TECH`) are read via `collect_tags_from_fits()` and mapped through `DPR_TO_TAG` to get classification tags.

## EDPS Internals (Key Findings)

These findings are non-obvious from the EDPS docs and took significant debugging to discover.

### Classification rules (`metis_classification.py`)
`classification_rule()` uses `FitsUtils.long_keyword()` to map short keyword names (e.g. `dpr.catg`) to `HIERARCH ESO DPR CATG` in FITS headers. The keyword `instrume` maps to plain `INSTRUME`. Classification is an exact-equality check on all keys in the rule dict.

### EDPS task targeting vs. meta-targets
- **`-t <task>`** — directly targets a named task; EDPS processes it plus all ancestor nodes. `get_unique_jobs()` only counts jobs for the named task itself.
- **`-m <meta_target>`** — activates a meta-target (e.g. `science`, `qc1calib`); selects all tasks marked `with_meta_targets([SCIENCE])` / `with_meta_targets([QC1_CALIB])`.
- Without either flag, EDPS defaults to the **leaf node** of the workflow DAG (the last task defined), which is always a SCIENCE task → 0 jobs scheduled for calibration-only data.

### IFU workflow task chain (`metis_ifu_wkf.py`)
All tasks except `metis_ifu_postprocess` have no `meta_target`. `metis_ifu_postprocess` has `with_meta_targets([SCIENCE])` and is the only leaf → default target. The calib chain is: `metis_ifu_lingain` → `metis_ifu_dark` → `metis_ifu_distortion` → `metis_ifu_wavecal` → `metis_ifu_rsrf` → `metis_ifu_{std,sci}_reduce` → `metis_ifu_{sci,std}_telluric` → `metis_ifu_calibrate` → `metis_ifu_postprocess`.

### LSS workflows
All calibration tasks in `metis_lm_lss_wkf.py` and `metis_n_lss_wkf.py` are gated behind `QC1_CALIB`. Use `-m qc1calib` to run them; `-m science` alone does not trigger them.

### IMG workflows (LM/N)
Calibration tasks (`lingain`, `dark`, `flat`, `distortion`) have no `meta_target`. Science tasks have `SCIENCE`. Use `-t <deepest_calib_task>` for calib-only runs.

### EDPS configuration
- Server port is read from `~/.edps/application.properties` (key `port=`). Default is 5000; this install uses **4444**.
- EDPS is invoked via `uv run --env-file <meta-pkg>/.env edps -P <port>`.
- Start/confirm server: `edps -P <port> -lw`. Dry-run classification check: `edps -P <port> -w <workflow> -i <dir> -c`.

### ScopeSim output filenames vs. EDPS tags
ScopeSim names files `METIS.<INTERNAL_NAME>.<timestamp>.fits` — this internal name does NOT always match the EDPS classification tag. Use FITS `HIERARCH ESO DPR CATG/TYPE/TECH` headers (mapped via `DPR_TO_TAG`) to determine the EDPS tag reliably.

## Repository Layout

This directory is a git repository containing the runner script and reference YAML. External repos and local data are gitignored.

```
test_runner/                 # git root (this repo)
├── run_metis.py             # Main CLI wrapper
├── LMS_RAD_06.yaml          # Reference IFU observation sequence
├── podman-compose.yml       # Container-based environment
├── README.md                # User-facing documentation
├── .gitignore               # Excludes external repos, data/, output/, test_*.yaml
│
│   # gitignored — cloned/installed separately via metis-meta-package bootstrap
├── METIS_Pipeline/          # Main pipeline (Python + C)
│   ├── metisp/pymetis/      # Core Python package
│   ├── metisp/pyrecipes/    # pyesorex plugin entry points
│   ├── metism/              # C/C++ image processing library
│   ├── toolbox/             # Install/setup scripts
│   └── .github/workflows/   # CI: run_edps.yaml (daily), edps_runner.yaml
├── METIS_Simulations/       # Simulation scripts (scopesim-based)
├── metis-meta-package/      # UV/pip meta-installer
├── data/                    # Local test data (gitignored)
└── output/                  # Pipeline run outputs (gitignored)
```
