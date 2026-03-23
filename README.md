# METIS Test Runner

<p align="center">
  <a href="https://github.com/eiseleb47/MTR/actions/workflows/unit_tests.yaml"><img src="https://img.shields.io/github/actions/workflow/status/eiseleb47/MTR/unit_tests.yaml?branch=main&label=unit%20tests&style=for-the-badge&labelColor=1e1e2e&color=a6e3a1&logo=github&logoColor=cdd6f4" alt="Unit Tests"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3-89b4fa?style=for-the-badge&labelColor=1e1e2e&logo=python&logoColor=cdd6f4" alt="Python 3"></a>
  <a href="https://github.com/eiseleb47/MTR/commits/main"><img src="https://img.shields.io/github/last-commit/eiseleb47/MTR?style=for-the-badge&labelColor=1e1e2e&color=cba6f7&logo=git&logoColor=cdd6f4" alt="Last Commit"></a>
  <a href="https://github.com/eiseleb47/MTR"><img src="https://img.shields.io/badge/platform-linux-fab387?style=for-the-badge&labelColor=1e1e2e&logo=linux&logoColor=cdd6f4" alt="Platform"></a>
</p>

A CLI wrapper for end-to-end testing of the [METIS instrument pipeline](https://github.com/AstarVienna/METIS_Pipeline). It generates synthetic FITS observations via [ScopeSim](https://scopesim.readthedocs.io/) and then runs the matching [EDPS](https://www.eso.org/sci/software/edps/) reduction workflow, all in one command.

## Prerequisites

The runner supports three installation layouts. Choose the one that matches how you have the pipeline installed:

**Option A — metis-meta-package** (`--runner metapkg`, default)

Run the bootstrap on your machine. It installs `uv`, ScopeSim, EDPS, PyEsoRex, and all Python dependencies:

```bash
git clone <metis-meta-package-url> ~/metis-meta-package
cd ~/metis-meta-package
bash bootstrap.sh
```

The runner looks for `~/metis-meta-package/` and `~/METIS_Simulations/` by default. Pass `--meta-pkg` / `--simulations-dir` to override.

**Option B — Docker or Podman container** (`--runner docker` / `--runner podman`)

Build and start the pipeline container from [METIS_Pipeline/toolbox/](https://github.com/AstarVienna/METIS_Pipeline/tree/main/toolbox):

```bash
cd METIS_Pipeline/toolbox
docker build -t metispipeline .
docker run -d --name metis-pipeline --net=host \
  --mount type=bind,source=/path/to/output,target=/output \
  metispipeline
```

Then pass `--runner docker --container metis-pipeline` (or set `METIS_RUNNER`/`METIS_CONTAINER`). The output directory must be bind-mounted into the container.

**Option C — bare-metal or inside a container** (`--runner native`)

If the pipeline tools (`edps`, `python`, ScopeSim) are already on your PATH — either because you are running the script *inside* a container or have installed everything directly — no additional setup is needed. Pass `--runner native`.

## Installation

No extra install step is needed beyond the bootstrap above. Clone this repo anywhere:

```bash
git clone <this-repo-url>
cd MTR
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
| `--runner {metapkg,native,docker,podman}` | `metapkg` | Execution mode (see below) |
| `--container NAME` | — | Container name/ID for `docker`/`podman` runners (env: `METIS_CONTAINER`) |
| `--calib` | off | Auto-generate calibration frames (dark/flat) inferred from YAML content |
| `--small` | off | Use 32×32 detector cutouts for fast testing |
| `--no-sim` | off | Skip simulation; run pipeline on existing data |
| `--no-pipeline` | off | Run simulation only; skip pipeline |
| `--meta-pkg PATH` | `~/metis-meta-package` | Path to the meta-package install (`metapkg` runner only) |
| `--simulations-dir PATH` | `~/METIS_Simulations` | Path to ScopeSim scripts (host path for `native`/`metapkg`; container-internal path for `docker`/`podman`) |

### Runner modes

| Mode | When to use |
|---|---|
| `metapkg` (default) | You ran `metis-meta-package/bootstrap.sh`. Tools are managed by `uv` inside the meta-package. |
| `native` | Tools (`edps`, `python`, ScopeSim) are installed directly on PATH — e.g. you are running **inside** a Docker/Podman container, or have a bare-metal install. |
| `docker` / `podman` | Tools live inside a container and you are running the script **outside** it. The runner wraps every command with `docker exec` / `podman exec`. |

The runner can also be set via the `METIS_RUNNER` environment variable.

> **Note for `docker`/`podman` runners:** the output directory (`-o`) must be bind-mounted into the container so EDPS can write pipeline products to it. The `--simulations-dir` flag should point to the path of `METIS_Simulations/Simulations` *inside* the container (default: `/home/metis/METIS_Simulations`).

### Examples

```bash
# Full run with metis-meta-package (default)
python run_metis.py LMS_RAD_06.yaml

# Inside a container or bare-metal install (tools on PATH)
python run_metis.py --runner native LMS_RAD_06.yaml

# Exec into a running Docker container from the host
python run_metis.py --runner docker --container metis-pipeline LMS_RAD_06.yaml

# Exec into a running Podman container; set runner via env var
METIS_RUNNER=podman METIS_CONTAINER=metis-pipeline python run_metis.py LMS_RAD_06.yaml

# Multiple YAML files, custom output dir, with auto-calibration frames
python run_metis.py -o /tmp/myrun --calib obs1.yaml obs2.yaml

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
MTR/
├── run_metis.py            # Main CLI script
├── LMS_RAD_06.yaml         # Full IFU observation sequence (reference example)
└── podman-compose.yml      # Container environment for isolated runs
```

## Related Repositories

This runner is designed to work alongside the following repos, which are installed via the `metis-meta-package` bootstrap:

- **[METIS_Pipeline](https://github.com/AstarVienna/METIS_Pipeline)** — the core Python/C pipeline, EDPS workflows, and PyEsoRex recipes
- **[METIS_Simulations](https://github.com/AstarVienna/METIS_Simulations)** — ScopeSim scripts that generate synthetic FITS observations for each observing mode
- **[metis-meta-package](https://github.com/eiseleb47/metis-meta-package)** — meta-installer that sets up `uv`, EDPS, PyEsoRex, and all dependencies in one step
