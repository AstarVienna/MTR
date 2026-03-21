# METIS Pipeline Runner

A CLI wrapper for end-to-end testing of the [METIS instrument pipeline](https://github.com/AstarVienna/METIS_Pipeline). It generates synthetic FITS observations via [ScopeSim](https://scopesim.readthedocs.io/) and then runs the matching [EDPS](https://www.eso.org/sci/software/edps/) reduction workflow, all in one command.

## Prerequisites

The `metis-meta-package` bootstrap must have been run on your machine. It installs `uv`, ScopeSim, EDPS, PyEsoRex, and all Python dependencies, and creates the directory layout the runner expects:

```bash
git clone <metis-meta-package-url> ~/metis-meta-package
cd ~/metis-meta-package
bash bootstrap.sh
```

By default the runner looks for:
- `~/metis-meta-package/` — the meta-package install
- `~/METIS_Simulations/` — the ScopeSim simulation scripts
- `~/METIS_Pipeline/` — the pipeline source (for `PYTHONPATH` etc.)

Pass `--meta-pkg` / `--simulations-dir` to override these paths.

## Installation

No extra install step is needed beyond the bootstrap above. Clone this repo anywhere:

```bash
git clone <this-repo-url>
cd metis-pipeline-runner
```

## Usage

```bash
python run_metis.py [OPTIONS] yaml1.yaml [yaml2.yaml ...]
```

The workflow (`lm_img`, `n_img`, `ifu`, `lm_lss`, `n_lss`, …) and the deepest pipeline target task are inferred automatically from the YAML content.

### Options

| Flag | Default | Description |
|---|---|---|
| `-o / --output-dir` | `./output/<timestamp>` | Root directory for all outputs |
| `--calib N` | `0` | Prepend N auto-generated calibration blocks |
| `--small` | off | Use 32×32 detector cutouts for fast testing |
| `--no-sim` | off | Skip simulation; run pipeline on existing data |
| `--no-pipeline` | off | Run simulation only; skip pipeline |
| `--meta-pkg PATH` | `~/metis-meta-package` | Path to the meta-package install |
| `--simulations-dir PATH` | `~/METIS_Simulations` | Path to ScopeSim scripts |

### Examples

```bash
# Full run: simulate + reduce a complete IFU observation sequence
python run_metis.py LMS_RAD_06.yaml

# Multiple YAML files, custom output dir, with 2 auto-calibration frames prepended
python run_metis.py -o /tmp/myrun --calib 2 obs1.yaml obs2.yaml

# Fast mode (32×32 detectors) for quick iteration
python run_metis.py --small LMS_RAD_06.yaml

# Only simulate, inspect the FITS files manually
python run_metis.py --no-pipeline LMS_RAD_06.yaml

# Only run the pipeline on previously simulated data
python run_metis.py --no-sim -o /tmp/myrun LMS_RAD_06.yaml
```

Output is written to:
- `<output-dir>/sim/` — synthetic FITS frames from ScopeSim
- `<output-dir>/pipeline/` — reduced data products from EDPS

## YAML Format

Each top-level key in the YAML is one *observation block*. The workflow is inferred from `properties.tech` (primary) or `mode` (fallback). Required fields per block:

```yaml
BLOCK_NAME:
  do.catg: <EDPS classification tag>   # e.g. DETLIN_IFU_RAW, IFU_SCI_RAW
  mode: <scopesim mode>                 # e.g. wcu_lms, lms
  source:
    name: <scopesim source name>        # e.g. empty_sky, star
    kwargs: {}
  properties:
    dit: <float>          # detector integration time (s)
    ndit: <int>           # number of integrations
    catg: <CALIB|SCIENCE>
    tech: <LMS|IMAGE,LM|LSS,LM|…>
    type: <DETLIN|DARK|FLAT|…>
    tplname: <ESO template name>
    nObs: <int>           # number of exposures to simulate
```

See `LMS_RAD_06.yaml` for a complete IFU example covering the full calibration + science chain.

## Supported Workflows

| EDPS Workflow | `tech` values |
|---|---|
| `metis_lm_img_wkf` | `IMAGE,LM` |
| `metis_n_img_wkf` | `IMAGE,N` |
| `metis_ifu_wkf` | `LMS`, `IFU`, `RAVC,IFU` |
| `metis_lm_lss_wkf` | `LSS,LM` |
| `metis_n_lss_wkf` | `LSS,N` |
| `metis_lm_ravc_wkf` | `RAVC,LM` |
| `metis_lm_app_wkf` | `APP,LM` |
| `metis_pupil_imaging_wkf` | `PUP,LM`, `PUP,N` |

## Repository Layout

```
metis-pipeline-runner/
├── run_metis.py            # Main CLI script
├── LMS_RAD_06.yaml         # Full IFU observation sequence (reference example)
└── podman-compose.yml      # Container environment for isolated runs
```

## Related Repositories

This runner is designed to work alongside the following repos, which are installed via the `metis-meta-package` bootstrap:

- **[METIS_Pipeline](https://github.com/AstarVienna/METIS_Pipeline)** — the core Python/C pipeline, EDPS workflows, and PyEsoRex recipes
- **[METIS_Simulations](https://github.com/AstarVienna/METIS_Simulations)** — ScopeSim scripts that generate synthetic FITS observations for each observing mode
- **[metis-meta-package](https://github.com/AstarVienna/metis-meta-package)** — meta-installer that sets up `uv`, EDPS, PyEsoRex, and all dependencies in one step
