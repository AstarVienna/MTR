# METIS Test Runner

<p align="center">
  <a href="https://github.com/eiseleb47/MTR/actions/workflows/unit_tests.yaml"><img src="https://img.shields.io/github/actions/workflow/status/eiseleb47/MTR/unit_tests.yaml?branch=main&label=unit%20tests&style=for-the-badge&labelColor=1e1e2e&color=a6e3a1&logo=github&logoColor=cdd6f4" alt="Unit Tests"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3-89b4fa?style=for-the-badge&labelColor=1e1e2e&logo=python&logoColor=cdd6f4" alt="Python 3"></a>
  <a href="https://github.com/eiseleb47/MTR/commits/main"><img src="https://img.shields.io/github/last-commit/eiseleb47/MTR?style=for-the-badge&labelColor=1e1e2e&color=cba6f7&logo=git&logoColor=cdd6f4" alt="Last Commit"></a>
  <a href="https://github.com/eiseleb47/MTR"><img src="https://img.shields.io/badge/platform-linux-fab387?style=for-the-badge&labelColor=1e1e2e&logo=linux&logoColor=cdd6f4" alt="Platform"></a>
</p>

A graphical front-end for end-to-end testing of the [METIS instrument pipeline](https://github.com/AstarVienna/METIS_Pipeline). It generates synthetic FITS observations via [ScopeSim](https://scopesim.readthedocs.io/) and then runs the matching [EDPS](https://www.eso.org/sci/software/edps/) reduction workflow — all from a single, self-contained GUI. A command-line interface (`mtr-cli`) is also shipped as a fallback for scripted or headless use.

## Install

MTR is distributed on PyPI. The recommended installer is **pipx**, which creates an isolated venv per application and never touches your system Python.

**Step 1 — install pipx** (one-time, skip if you already have it):

```bash
# Debian / Ubuntu
sudo apt install pipx && pipx ensurepath

# Fedora
sudo dnf install pipx && pipx ensurepath

# macOS (Homebrew)
brew install pipx && pipx ensurepath

# Any platform with Python ≥ 3.12 (fallback)
python -m pip install --user pipx && pipx ensurepath
```

**Step 2 — install MTR**:

```bash
pipx install metis-test-runner
mtr                                  # launches the GUI from anywhere
mtr-cli examples/LMS_RAD_06.yaml     # CLI equivalent
```

If you'd rather not install pipx, use Python's built-in venv module:

```bash
python -m venv ~/.venvs/mtr
~/.venvs/mtr/bin/pip install metis-test-runner
~/.venvs/mtr/bin/mtr                 # or: source ~/.venvs/mtr/bin/activate && mtr
```

On first launch, open the **Install** tab and click *Install / Update* to fetch the METIS_Pipeline, METIS_Simulations, and ESO pipeline dependencies into `~/.local/share/metis-test-runner/` (override with `METIS_DATA_DIR=/path`). All pipeline dependencies install into the same isolated venv that MTR itself lives in — they never leak into your system Python.

## System Dependencies

The GUI requires a handful of system libraries for Qt6 / OpenGL rendering. Python, Git, and basic CLI tools (`curl`, `tar`, …) are assumed to already be present.

**Debian / Ubuntu**

```bash
sudo apt install \
  libgl1 libegl1 libglib2.0-0 libxkbcommon0 \
  libfontconfig1 libfreetype6 libdbus-1-3 \
  libxkbcommon-x11-0 libxcb-cursor0 libxcb-keysyms1 libxcb-icccm4 libxcb-shape0
```

**Fedora**

```bash
sudo dnf install \
  mesa-libGL mesa-libEGL glib2 libxkbcommon \
  fontconfig freetype dbus-libs \
  libxkbcommon-x11 xcb-util-cursor xcb-util-keysyms xcb-util-wm
```

**macOS (Homebrew)**

On macOS, Qt uses the native Cocoa backend so X11/xcb packages are not needed:

```bash
brew install freetype fontconfig glib
```

> **Tip:** if the GUI crashes at startup with *"could not find or load the Qt platform plugin"*, set `QT_DEBUG_PLUGINS=1` to get detailed diagnostics:
> ```bash
> QT_DEBUG_PLUGINS=1 mtr
> ```
> The output will list exactly which shared library failed to load.

## The GUI

The GUI is the recommended way to drive the test runner. It exposes every CLI flag through labelled controls, remembers your settings between sessions, and streams colour-coded live output from the pipeline.

Launch it with:

```bash
mtr
```

A **Light / Dark theme** button lives in the toolbar and toggles on the fly.

### Install tab

The Install tab performs the full pipeline bootstrap non-interactively. Use it if you do **not** already have the pipeline installed. Clicking **Install / Update** will:

1. Clone (or update, if already present) `METIS_Pipeline` and `METIS_Simulations` into the user data directory (`~/.local/share/metis-test-runner/` by default)
2. `pip install` all ESO pipeline Python dependencies — `pycpl`, `edps`, `pyesorex`, `adari_core`, `scopesim`, `scopesim_templates` — into the same isolated venv that hosts MTR (via `--extra-index-url` against the ESO mirrors). Out-of-band installs such as MetisWISE from the Archive tab are preserved.
3. Write `.env` into the user data directory (environment variables for PYTHONPATH, plugin directories, etc.)
4. Initialise and configure EDPS on port 4444

Re-running is safe — existing repositories are updated in place rather than re-cloned.

**Skip this tab** if you already have the pipeline installed via one of these paths — jump straight to the Run tab instead:

- **Bare-metal / ESO docs install** — choose runner `native`
- **Pipeline container** (Docker / Podman) — choose runner `docker` or `podman` and supply the container name

### Archive tab

The Archive tab connects to the remote METIS AIT archive via the
[MetisWISE](https://github.com/AstarVienna/MetisWISE) client. It has two pages:

1. **Install & Configure** — paste the OmegaCEN credentials from the
   [METIS wiki](https://metis.strw.leidenuniv.nl/wiki/doku.php?id=ait:archive)
   and click **Install MetisWISE** to pip-install the package into the same
   isolated venv that hosts MTR. Then fill in the five `[global]` fields (`database_user`,
   `database_password`, `project`, `database_tablespacename`, `database_name`)
   and click **Save & Test Connection**. The values are written to
   `~/.awe/Environment.cfg`; `data_server`, port and protocol are inherited
   from the MetisWISE-packaged default (`metis-ds.hpc.rug.nl:8013`, https).

2. **Query & Download** — filter by raw classification tag or master
   `PRO.CATG`, click **Search**, select files and download them to a local
   directory.

To auto-pull missing master calibrations during a pipeline run, add
`--auto-fetch-calibrations` to the Run tab's options (or the CLI).

### Run tab

The Run tab wraps `mtr-cli` in a file-picker UI. All CLI options are exposed as form controls; runner-specific fields (container name) show and hide based on the selected runner.

Workflow:

1. **Add input files** via the file browser — YAML observation blocks (`*.yaml`, `*.yml`) and AIT-format CSV test sequences (`*.csv`) may be added freely and mixed in a single run. A live tally (e.g. `2 YAML  ·  1 CSV`) appears below the list.
2. **Tune options** — output directory, CPU cores, auto-calibration, runner mode, pipeline mode (simulate + run, simulate only, pipeline only), simulations directory, instrument packages directory, or **Translate CSV → YAML** (a dry run that converts a CSV test sheet to YAML without simulating)
3. **Pick a workflow** (only for CSV-only runs that include the pipeline) — workflow auto-detection reads YAML content, so when the list contains only CSV files and you intend to run the pipeline, the *Workflow* dropdown appears and must be set; otherwise it stays hidden
4. **Click Run** — the Run button becomes Stop, and pipeline output streams into the log view with ANSI colouring stripped and stderr highlighted
5. **Inspect output** — the pane below the option form shows exactly where simulation frames and pipeline products will be written, updating live as you edit the output path

Settings are persisted via `QSettings` and restored on next launch, so you can re-run the last configuration with two clicks.

## Prerequisites (runner modes)

Regardless of whether you drive the runner from the GUI or the CLI, the underlying pipeline tools have to live *somewhere*. Three layouts are supported — pick the one that matches your install:

**Option A — pipx install** (runner `default`, the default)

The Install tab takes care of this automatically. It pip-installs all pipeline
dependencies (`pycpl`, `edps`, `pyesorex`, `adari_core`, `scopesim`,
`scopesim_templates`) into the same isolated venv that hosts MTR, clones
`METIS_Pipeline` and `METIS_Simulations` into the user data directory
(`~/.local/share/metis-test-runner/` by default), and writes a `.env` file
there. The `default` runner reads that `.env` automatically and invokes every
subprocess with the MTR venv's Python interpreter.

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

> **Tip:** always launch the GUI (or invoke `mtr-cli`) from the same directory — otherwise ScopeSim will download a fresh copy of the instrument packages into every new directory, cluttering your filesystem.

## Input Formats

Two input formats are supported and may be mixed in a single run:

- **YAML observation blocks** (`*.yaml`, `*.yml`) — the primary, human-authored format. Workflow auto-detection works on YAML content.
- **AIT-format CSV test sequences** (`*.csv`) — the AIT performance-test sheet exported as CSV. Parsed by `metis_simulations.csvParser` at simulation time. MTR does **not** inspect CSV content itself, so for CSV-only runs the workflow is auto-detected from the **simulated FITS headers** (after the simulation step); `--workflow NAME` (or the GUI dropdown, hidden by default) is an optional override.

See [`examples/small_test_img_lm.csv`](examples/small_test_img_lm.csv) (IMAGE,LM) and [`examples/small_test_img_n.csv`](examples/small_test_img_n.csv) (IMAGE,N) for minimal CSVs; the rest of this section covers YAML.

### YAML Format

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

See `examples/LMS_RAD_06.yaml` for a complete IFU example covering the full calibration + science chain.

The small per-mode examples — `examples/small_test.yaml` (IFU),
`examples/small_test_img_lm.yaml` / `examples/small_test_img_lm.csv` (IMAGE,LM), and
`examples/small_test_img_n.yaml` / `examples/small_test_img_n.csv` (IMAGE,N) —
are minimal inputs that exercise the detector linearity + gain step
(`metis_det_lingain`) and the master-dark step (`metis_det_dark`). Each provides
the DETLIN frames that recipe requires: **six distinct DITs**, each with two
illuminated (ON) and two dark (OFF) frames (the OFF frames use
`filter_name: "closed"`), plus ≥2 dark frames for the dark step. Six DITs are
used because `metis_det_lingain`'s gain fit — `np.polyfit(deg=1, cov=True)` —
needs at least **four** DIT points below the linearity limit (`len > order+2`);
fewer raises *"the number of data points must exceed order to scale the
covariance matrix."*

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

`mtr-cli` is the headless interface that the GUI drives under the hood. It is useful for scripting, CI jobs, and SSH sessions without a display. It accepts the same options as the GUI.

```bash
mtr-cli [OPTIONS] input1.yaml [input2.csv ...]
```

YAML and CSV inputs may be mixed in any combination.

### Options

| Flag | Default | Description |
|---|---|---|
| `-o / --output` | `./output/<timestamp>` | Root directory for all outputs (env: `METIS_OUTPUT_DIR`) |
| `--runner {default,native,docker,podman}` | `default` | Execution mode (see below; env: `METIS_RUNNER`) |
| `--container NAME` | — | Container name/ID for `docker` / `podman` runners (env: `METIS_CONTAINER`) |
| `--calib [N]` | `1` | Auto-generate N calibration frames (dark/flat) per unique config, inferred from input content. Pass `--calib 0` to disable. |
| `--workflow NAME` | auto-detect | Force EDPS workflow name (e.g. `metis.metis_lm_img_wkf`). Optional: the workflow is auto-detected from YAML content, or from the simulated FITS headers for CSV-only runs. Pass this to override. |
| `--cores N` | `4` | CPU cores used for parallel simulations |
| `--no-sim` | off | Skip simulation; run pipeline on existing FITS data (source defaults to `<output>/sim/` — override with `--pipeline-input`) |
| `--pipeline-input DIR` | `<output>/sim/` | Directory containing FITS files to feed the pipeline (only with `--no-sim`; env: `METIS_PIPELINE_INPUT`) |
| `--no-pipeline` | off | Run simulation only; skip EDPS pipeline |
| `--csv-to-yaml` | off | Dry run: translate CSV input(s) to YAML via METIS_Simulations (`testRun`+`writeYaml`) and stop — writes `<name>.yaml` next to each CSV, with no simulation and no pipeline. Only CSV inputs are affected. Note: the YAML is written next to the source CSV, so don't point it at a bundled `examples/*.csv` whose `.yaml` you want to keep. |
| `--simulations-dir PATH` | `./METIS_Simulations` (host) or `/home/metis/METIS_Simulations` (container) | Path to ScopeSim scripts (env: `METIS_SIMULATIONS_DIR`) |
| `--inst-pkgs PATH` | see below | Path to ScopeSim instrument packages (Armazones, ELT, METIS). Defaults to the user data dir for the `default` runner, `./inst_pkgs` for `native`, and container-resolved `./inst_pkgs` for `docker`/`podman` (env: `METIS_INST_PKGS`) |
| `--auto-fetch-calibrations` | off | Before running the pipeline, query the remote METIS archive (via MetisWISE) for any master calibrations the input set is missing and download them into the pipeline input directory. Requires MetisWISE to be installed and `~/.awe/Environment.cfg` to hold valid credentials — see the Archive tab. |
| `--prefer-masters` | off | Set EDPS `association_preference` to `master_per_quality_level` for this run, preferring master calibrations over reduced raw data. |

### Runner modes

| Mode | When to use |
|---|---|
| `default` | You used the GUI's Install tab. Pipeline tools are pip-installed alongside MTR in the same isolated pipx/venv. Subprocesses run with that venv's Python interpreter and load the Install-tab `.env` from `~/.local/share/metis-test-runner/.env`. No external dependencies. |
| `native` | Tools (`edps`, `python`, ScopeSim) are installed directly on PATH — e.g. you are running **inside** a Docker/Podman container, or have a bare-metal install. |
| `docker` / `podman` | Tools live inside a container and you are running the script **outside** it. The runner wraps every command with `docker exec` / `podman exec`. |

> **Note for `docker` / `podman` runners:** the output directory (`-o`) must be bind-mounted into the container so EDPS can write pipeline products to it. The `--simulations-dir` flag should point to the path of `METIS_Simulations/Simulations` *inside* the container (default: `/home/metis/METIS_Simulations`).

### Examples

```bash
# Full run with the pipx-installed pipeline (default runner)
mtr-cli examples/LMS_RAD_06.yaml

# Inside a container or bare-metal install (tools on PATH)
mtr-cli --runner native examples/LMS_RAD_06.yaml

# Exec into a running Docker container from the host
mtr-cli --runner docker --container metis-pipeline examples/LMS_RAD_06.yaml

# Exec into a running Podman container; set runner via env var
METIS_RUNNER=podman METIS_CONTAINER=metis-pipeline mtr-cli examples/LMS_RAD_06.yaml

# Multiple YAML files, custom output dir, with auto-calibration frames
mtr-cli -o /tmp/myrun --calib obs1.yaml obs2.yaml

# Crank up parallelism for big simulation batches
mtr-cli --cores 12 examples/LMS_RAD_06.yaml

# Only simulate, inspect the FITS files manually
mtr-cli --no-pipeline examples/LMS_RAD_06.yaml

# Only run the pipeline on previously simulated data
mtr-cli --no-sim -o /tmp/myrun examples/LMS_RAD_06.yaml

# Pipeline-only with FITS files from a custom location
mtr-cli --no-sim --pipeline-input /data/sim_fits -o /tmp/myrun

# CSV-only input, simulate only (workflow auto-detection not needed)
mtr-cli --no-pipeline examples/small_test_img_lm.csv

# CSV-only input, full simulate + pipeline run (workflow auto-detected from the
# simulated FITS; pass --workflow only to override)
mtr-cli examples/small_test_img_lm.csv

# Dry run: translate a CSV test sheet to YAML (writes my_sheet.yaml next to it),
# no simulation and no pipeline
mtr-cli --csv-to-yaml /path/to/my_sheet.csv

# Mixed YAML + CSV in a single run
mtr-cli examples/small_test.yaml examples/small_test_img_lm.csv
```

## Repository Layout

```
MTR/
├── src/metis_test_runner/
│   ├── gui.py              # Graphical front-end (PyQt6) — primary entry point
│   ├── run_metis.py        # Headless CLI (used directly or wrapped by the GUI)
│   ├── archive.py          # MetisWISE archive integration
│   ├── paths.py            # User data directory resolution (platformdirs)
│   ├── indexes.py          # ESO mirror pip-index URL constants
│   └── examples/           # Bundled YAML / CSV example inputs
├── container/
│   ├── Dockerfile          # Ubuntu 24.04 GUI container (Qt6 / Wayland)
│   └── compose.yml         # Podman / Docker Compose for the GUI service
├── tests/                  # Unit tests (pytest)
└── pyproject.toml          # Project metadata (hatchling build backend)
```

## Related Repositories

This runner is designed to work alongside the following repos, which are installed via the GUI's Install tab:

- **[METIS_Pipeline](https://github.com/AstarVienna/METIS_Pipeline)** — the core Python/C pipeline, EDPS workflows, and PyEsoRex recipes
- **[METIS_Simulations](https://github.com/AstarVienna/METIS_Simulations)** — ScopeSim scripts that generate synthetic FITS observations for each observing mode
