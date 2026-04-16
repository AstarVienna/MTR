# METIS Test Runner

<p align="center">
  <a href="https://github.com/eiseleb47/MTR/actions/workflows/unit_tests.yaml"><img src="https://img.shields.io/github/actions/workflow/status/eiseleb47/MTR/unit_tests.yaml?branch=main&label=unit%20tests&style=for-the-badge&labelColor=1e1e2e&color=a6e3a1&logo=github&logoColor=cdd6f4" alt="Unit Tests"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3-89b4fa?style=for-the-badge&labelColor=1e1e2e&logo=python&logoColor=cdd6f4" alt="Python 3"></a>
  <a href="https://github.com/eiseleb47/MTR/commits/main"><img src="https://img.shields.io/github/last-commit/eiseleb47/MTR?style=for-the-badge&labelColor=1e1e2e&color=cba6f7&logo=git&logoColor=cdd6f4" alt="Last Commit"></a>
  <a href="https://github.com/eiseleb47/MTR"><img src="https://img.shields.io/badge/platform-linux-fab387?style=for-the-badge&labelColor=1e1e2e&logo=linux&logoColor=cdd6f4" alt="Platform"></a>
</p>

A graphical front-end for end-to-end testing of the [METIS instrument pipeline](https://github.com/AstarVienna/METIS_Pipeline). It generates synthetic FITS observations via [ScopeSim](https://scopesim.readthedocs.io/) and then runs the matching [EDPS](https://www.eso.org/sci/software/edps/) reduction workflow — all from a single, self-contained GUI. A command-line interface (`run_metis.py`) is also shipped as a fallback for scripted or headless use.

## Quick Start

```bash
git clone <this-repo-url>
cd MTR
./launch.sh
```

`launch.sh` boots the GUI via `uv`. If `uv` is not already installed, it will prompt you once to install it, then proceed. Always use `launch.sh` to start the GUI — it uses `uv sync --inexact` to preserve pipeline dependencies that the Install tab adds later.

From there, everything — installing the pipeline, selecting a runner, picking YAML inputs, and watching live pipeline output — is available as point-and-click controls.

## The GUI

The GUI is the recommended way to drive the test runner. It exposes every CLI flag through labelled controls, remembers your settings between sessions, and streams colour-coded live output from the pipeline.

Launch it with:

```bash
./launch.sh         # installs uv if missing, then launches the GUI
```

A **Light / Dark theme** button lives in the toolbar and toggles on the fly.

### Install tab

The Install tab performs the full pipeline bootstrap non-interactively. Use it if you do **not** already have the pipeline installed. Clicking **Install / Update** will:

1. Clone (or update, if already present) `METIS_Pipeline` and `METIS_Simulations` into the repo root
2. Run `uv sync --group pipeline` to install all pipeline Python dependencies into the main virtual environment
3. Write `.env` in the repo root (environment variables for PYTHONPATH, plugin directories, etc.)
4. Initialise and configure EDPS on port 4444

Re-running is safe — existing repositories are updated in place rather than re-cloned.

**Skip this tab** if you already have the pipeline installed via one of these paths — jump straight to the Run tab instead:

- **metis-meta-package** — choose runner `metapkg` and set *Meta-package dir* to your `metis-meta-package` folder
- **Bare-metal / ESO docs install** — choose runner `native`
- **Pipeline container** (Docker / Podman) — choose runner `docker` or `podman` and supply the container name

### Run tab

The Run tab wraps `run_metis.py` in a file-picker UI. All CLI options are exposed as form controls; runner-specific fields (container name, meta-package path) show and hide based on the selected runner.

Workflow:

1. **Add YAML input files** via the file browser (the list supports multi-select removal)
2. **Tune options** — output directory, CPU cores, auto-calibration, runner mode, pipeline mode (simulate + run, simulate only, pipeline only), simulations directory, instrument packages directory
3. **Click Run** — the Run button becomes Stop, and pipeline output streams into the log view with ANSI colouring stripped and stderr highlighted
4. **Inspect output** — the pane below the option form shows exactly where simulation frames and pipeline products will be written, updating live as you edit the output path

Settings are persisted via `QSettings` and restored on next launch, so you can re-run the last configuration with two clicks.

## Prerequisites (runner modes)

Regardless of whether you drive the runner from the GUI or the CLI, the underlying pipeline tools have to live *somewhere*. Three layouts are supported — pick the one that matches your install:

**Option A — consolidated install** (runner `metapkg`, default)

The Install tab takes care of this automatically. It installs all pipeline
dependencies into the main `.venv` alongside the GUI dependencies and writes a
`.env` file in the repo root. The runner looks for `.env` in the current
working directory first, then falls back to `./pipeline/` and
`./metis-meta-package/` for backwards compatibility with standalone installs.

Use the GUI's *Meta-package dir* field (or `--meta-pkg` / `--simulations-dir`)
to point at an external installation if needed.

**Option B — Docker or Podman container** (runner `docker` / `podman`)

Build and start the pipeline container from [METIS_Pipeline/toolbox/](https://github.com/AstarVienna/METIS_Pipeline/tree/main/toolbox):

```bash
cd METIS_Pipeline/toolbox
docker build -t metispipeline .
docker run -d --name metis-pipeline --net=host \
  --mount type=bind,source=/path/to/output,target=/output \
  metispipeline
```

Then in the GUI, set runner to `docker` (or `podman`) and enter the container name. The output directory must be bind-mounted into the container so EDPS can write products back to the host.

**Option C — bare-metal or inside a container** (runner `native`)

If the pipeline tools (`edps`, `python`, ScopeSim) are already on your PATH — either because you are running *inside* a container, or have installed everything directly — no extra setup is needed. Select runner `native`.

ScopeSim instrument packages (Armazones, ELT, METIS) will be downloaded into `./inst_pkgs/` in your current working directory on first use. Set the GUI's *Instrument packages* field (or `--inst-pkgs PATH`) to download or reuse packages from a fixed location instead.

> **Tip:** always launch the GUI (or invoke `run_metis.py`) from the same directory — otherwise ScopeSim will download a fresh copy of the instrument packages into every new directory, cluttering your filesystem.

## YAML Format

Each top-level key in the YAML is one *observation block*. The workflow (`lm_img`, `n_img`, `ifu`, `lm_lss`, `n_lss`, …) and the deepest pipeline target task are inferred automatically from the YAML content — primarily from `properties.tech`, falling back to `mode`.

Required fields per block:

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

## Output Layout

Output is written under the chosen output directory (default: `./output/<timestamp>/`):

- `<output-dir>/sim/` — synthetic FITS frames from ScopeSim
- `<output-dir>/pipeline/` — reduced data products from EDPS

The GUI displays the resolved paths live under the *Output directory* field so you can see exactly where products will land before you hit Run.

## Command-Line Fallback

`run_metis.py` is the headless interface that the GUI drives under the hood. It is useful for scripting, CI jobs, and SSH sessions without a display. It accepts the same options as the GUI.

```bash
python run_metis.py [OPTIONS] yaml1.yaml [yaml2.yaml ...]
```

### Options

| Flag | Default | Description |
|---|---|---|
| `-o / --output` | `./output/<timestamp>` | Root directory for all outputs |
| `--runner {metapkg,native,docker,podman}` | `metapkg` | Execution mode (see below; env: `METIS_RUNNER`) |
| `--container NAME` | — | Container name/ID for `docker` / `podman` runners (env: `METIS_CONTAINER`) |
| `--calib [N]` | `1` | Auto-generate N calibration frames (dark/flat) per unique config, inferred from YAML. Pass `--calib 0` to disable. |
| `--cores N` | `4` | CPU cores used for parallel simulations |
| `--no-sim` | off | Skip simulation; run pipeline on existing FITS data (source defaults to `<output>/sim/` — override with `--pipeline-input`) |
| `--pipeline-input DIR` | `<output>/sim/` | Directory containing FITS files to feed the pipeline (only with `--no-sim`) |
| `--no-pipeline` | off | Run simulation only; skip EDPS pipeline |
| `--meta-pkg PATH` | `.` (repo root) | Path to the pipeline environment directory (`metapkg` runner only). Falls back to `./pipeline/` then `./metis-meta-package/`. |
| `--simulations-dir PATH` | `./METIS_Simulations` (host) or `/home/metis/METIS_Simulations` (container) | Path to ScopeSim scripts |
| `--inst-pkgs PATH` | see below | Path to ScopeSim instrument packages (Armazones, ELT, METIS). Defaults to `./inst_pkgs` for `metapkg`/`native`, and container-resolved `./inst_pkgs` for `docker`/`podman` |

### Runner modes

| Mode | When to use |
|---|---|
| `metapkg` (default) | You used the GUI's Install tab (or an external `metis-meta-package` install). Tools are managed by `uv` in the project's virtual environment. |
| `native` | Tools (`edps`, `python`, ScopeSim) are installed directly on PATH — e.g. you are running **inside** a Docker/Podman container, or have a bare-metal install. |
| `docker` / `podman` | Tools live inside a container and you are running the script **outside** it. The runner wraps every command with `docker exec` / `podman exec`. |

> **Note for `docker` / `podman` runners:** the output directory (`-o`) must be bind-mounted into the container so EDPS can write pipeline products to it. The `--simulations-dir` flag should point to the path of `METIS_Simulations/Simulations` *inside* the container (default: `/home/metis/METIS_Simulations`).

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

# Crank up parallelism for big simulation batches
python run_metis.py --cores 12 LMS_RAD_06.yaml

# Only simulate, inspect the FITS files manually
python run_metis.py --no-pipeline LMS_RAD_06.yaml

# Only run the pipeline on previously simulated data
python run_metis.py --no-sim -o /tmp/myrun LMS_RAD_06.yaml

# Pipeline-only with FITS files from a custom location
python run_metis.py --no-sim --pipeline-input /data/sim_fits -o /tmp/myrun
```

## Repository Layout

```
MTR/
├── gui.py                  # Graphical front-end (PyQt6) — primary entry point
├── launch.sh               # GUI launcher (installs uv if missing, then runs gui.py)
├── run_metis.py            # Headless CLI (used directly or wrapped by the GUI)
├── LMS_RAD_06.yaml         # Full IFU observation sequence (reference example)
└── podman-compose.yml      # Container environment for isolated runs
```

## Related Repositories

This runner is designed to work alongside the following repos, which are installed via the GUI's Install tab:

- **[METIS_Pipeline](https://github.com/AstarVienna/METIS_Pipeline)** — the core Python/C pipeline, EDPS workflows, and PyEsoRex recipes
- **[METIS_Simulations](https://github.com/AstarVienna/METIS_Simulations)** — ScopeSim scripts that generate synthetic FITS observations for each observing mode
- **[metis-meta-package](https://github.com/eiseleb47/metis-meta-package)** — legacy standalone meta-installer (still supported as a fallback; the Install tab now installs pipeline dependencies directly into the main virtual environment)
