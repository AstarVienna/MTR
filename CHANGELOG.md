# Changelog

## 0.4.5

### Changed
- Temporarily **unpinned** `pycpl` — ivh's index is churning (0.4.4's
  `1.0.4.post6` has since been withdrawn from it), so the Install tab now takes
  whatever is newest. Added `--upgrade` so re-installs actually move; this also
  lets `edps`/`pyesorex`/`adari_core` advance on re-install.

## 0.4.4

### Changed
- Bumped `pycpl` pin from `1.0.3.post11` to `1.0.4.post6` — the newest release
  on ivh's index. Note it sorts *above* `1.0.3.post11` under PEP 440 despite the
  lower post number, and ships the same cp312/cp313/cp314 Linux+macOS wheels.
  Upstream `pymetis` still pins `1.0.3.post11`, so MTR is deliberately ahead of
  it; the `--no-deps` pymetis/metiswise installs keep that pin out of pip's
  resolver, so nothing can downgrade us. ESO's own 1.0.4 changelog lists only
  additive API changes, but ivh's wheel also bundles PyHDRL 1.0.0 (was 0.2.0),
  which drops `hdrl.core.Parameter` and reworks `Spectrum1D`/`Efficiency` — the
  METIS recipes use none of those, only `hdrl.core.Image`/`ImageList` and
  `hdrl.func.Flat`/`Collapse`/`BPM`.

## 0.4.3

### Fixed
- **pip bootstrap for pipx installs.** pipx creates each application's venv
  *without* pip (it installs the app via an external/shared pip), so the Install
  tab, the Uninstall button, and the Archive (MetisWISE) installer — all of which
  run `python -m pip …` in MTR's own interpreter — failed immediately with
  `No module named pip` on every pipx-installed MTR, i.e. the recommended install
  method. MTR now probes for pip and, when it is absent, bootstraps it with
  `python -m ensurepip --upgrade` (stdlib, bundled wheels, no network) before its
  pip calls. A no-op when pip already exists (e.g. a plain-`venv` install), so an
  existing, possibly newer pip is never touched.

### Changed
- Install-instructions note in the README: MTR supports Python 3.12–3.13; on a
  newer interpreter (e.g. 3.14) the ESO pipeline dependencies have no wheels, so
  pin MTR with `pipx install metis-test-runner --python python3.12`.

## 0.4.2

### Added
- **Uninstall button** on the Install tab (red, next to Install / Update).
  Clicking it shows a confirmation dialog, then reverses every change the
  installer made: pip-uninstalls the pipeline packages (`pycpl`, `edps`,
  `pyesorex`, `adari_core`, `scopesim`, `scopesim_templates`, `eso-pymetis`,
  `metis_simulations`) and the Archive-tab MetisWISE packages (`metiswise` plus
  its runtime deps), deletes the entire METIS data directory
  (`~/.local/share/metis-test-runner`), cleans up the EDPS configuration
  (restores a pre-existing config from backup if one exists, otherwise removes
  `~/.edps` and the EDPS bookkeeping directory), and clears the stored archive
  credentials from the OS keyring. Both buttons are disabled during the
  operation. Uninstall is resilient — a failure in one step is logged but does
  not abort the remaining steps.
- **Clear buttons** on the Run-tab input lists — one click empties the file
  list or the pipeline-directory list (success role). In pipeline-only mode the
  pipeline list is top-aligned and sized to the button column so the list and
  button edges line up.

### Changed
- Bumped `pycpl` pin from `1.0.3.post10` to `1.0.3.post11`, matching the
  upstream `pymetis` `[project]` pin. post11 ships prebuilt macOS wheels on
  ivh's index; post10 remains compatible but post11 is now the upstream
  baseline.

## 0.4.1

### Changed
- **Archive credentials now live in the OS keyring** (macOS Keychain / Windows
  Credential Locker / Linux Secret Service via the `keyring` package) instead
  of plaintext files. Both the OmegaCEN pip credentials and the five database
  fields are stored under the `metis-test-runner` keyring service after a
  successful install / connection test, and are injected into the process
  environment for MetisWISE at connect time — `~/.awe/Environment.cfg` is no
  longer written. A legacy `Environment.cfg` is still read as a fallback
  (e.g. on keyring-less headless machines) and is scrubbed of credentials on
  the first successful **Save & Test Connection** after the upgrade. The
  keyring is only ever touched when installing MetisWISE or connecting to the
  archive, never at GUI startup, so local-only workflows see no unlock prompt.
  In the Archive tab, leave the credential fields blank to use the stored
  values.
- The MetisWISE pip install now passes the credentialed index URL via the
  `PIP_EXTRA_INDEX_URL` environment variable instead of the command line, so
  the OmegaCEN password no longer appears in `/proc/<pid>/cmdline` (visible
  to all local users while pip runs) or in the GUI log echo.

## 0.4.0

### Added
- `mtr-exec` and `mtr-shell` console scripts for direct, env-file-free access to
  the pipeline environment. `mtr-exec -- <cmd>` runs a single command (e.g.
  `mtr-exec -- edps -lw`) and `mtr-shell` opens an interactive shell with `edps`,
  `pyesorex`, and `python`+ScopeSim resolved. Both are runner-aware
  (`default`/`native`/`docker`/`podman`); container runners wrap the command in
  `docker exec` / `podman exec`. These replace the old `uv run --env-file .env …`
  workflow.
- `--csv-lines START:END` (CLI flag + GUI Run-tab field): restrict CSV inputs to
  a range of 1-based file lines (`6:12`, `6:`, `:12`). The fixed 4-row AIT header
  block is always preserved; MTR writes a header-preserving sliced copy under
  `<output>/sliced_inputs/` and feeds that to the simulation.
- `metis_test_runner.env.resolve_runtime_env()` — a single source of truth for
  the runtime environment, shared by the GUI, `mtr-cli`, and `mtr-exec` /
  `mtr-shell`.

### Changed
- The `default` runner now **derives** its environment (`PYTHONPATH`, recipe
  directories, `METIS_INST_PKGS`, venv `bin/` on `PATH`) from the install
  locations instead of a generated `.env` file. An optional
  `~/.local/share/metis-test-runner/.env` is read only to *override* the derived
  defaults. The Install tab no longer writes `.env`, and the default-runner
  readiness checks now look for the pipeline clone rather than that file.

### Removed
- The `--workflow` override — both the CLI flag and the long-hidden GUI dropdown.
  The EDPS workflow is auto-detected for every input type (YAML pre-simulation,
  CSV from the simulated FITS headers); for manual control over the EDPS /
  pyesorex invocation, use `mtr-exec` / `mtr-shell`.

## 0.3.2

### Changed
- The Install tab now editable-installs the cloned `pymetis` as `eso-pymetis`
  (`--no-deps`) so MetisWISE reuses the same checkout instead of pulling a
  second `pymetis` copy from the index.
- The Archive tab installs MetisWISE in two steps — its runtime deps
  (`commonwise`, `metis-drld`, …) with normal resolution, then `metiswise`
  itself with `--no-deps` — so the cloned `eso-pymetis`'s `pycpl==1.0.3.post4`
  pin can't downgrade our `pycpl==1.0.3.post10` (post10 ships the prebuilt
  macOS wheels). This also skips MetisWISE's jupyter/sphinx/pytest dev deps.

### Fixed
- Archive connection no longer fails with `No module named 'codes.drld_parser'`.
  MetisWISE now installs at **0.0.4** (from its GitHub tag), which imports the
  `metis-drld` pip package — installed from the credentialed private index.
  Removed the `METIS_DRLD` `git clone` and the legacy `codes.drld_parser`
  import path.

## 0.3.1

### Added
- DETLIN ON/OFF examples (IFU/LM/N, YAML + CSV) so the det-lin/gain + dark steps complete.
- `--csv-to-yaml` dry-run (CLI flag + GUI checkbox): translate a CSV test sheet to YAML, no simulation/pipeline.

### Changed
- `--workflow` is now optional for CSV-only runs (auto-detected from the simulated FITS).
- EDPS products are hardlinked into the per-run output dir instead of copied (no disk duplication).
- GUI log renders carriage-return progress (download bars, FOV steps) in place.

### Fixed
- CSV-only combined runs no longer create 0 EDPS jobs (target inferred from simulated FITS).

## 0.3.0

### Removed
- `metapkg` runner mode, the `--meta-pkg` CLI flag, and the `METIS_META_PKG`
  env var. The new `default` runner runs subprocesses inside MTR's own pipx/venv
  (where the Install tab installs pipeline dependencies), making `uv` and the
  standalone `metis-meta-package` checkout obsolete.
- `paths.meta_pkg_dir()` helper and the "Meta-package dir" field from the Run
  tab. Existing GUI settings that saved `runner=metapkg` will silently fall
  back to `default` on first launch.

### Changed
- Default runner is now `default` (was `metapkg`). It runs simulations and
  EDPS with `sys.executable` (the MTR venv's interpreter) and merges the
  Install-tab `.env` from `~/.local/share/metis-test-runner/.env` into the
  subprocess environment, prepending the venv's `bin/` to `PATH` so `edps`
  and `pyesorex` resolve to the venv copies.

## 0.2.0

First PyPI release as `metis-test-runner`.

### Changed
- Restructured the project as a proper Python package under
  `src/metis_test_runner/`. Modules are now imported as
  `from metis_test_runner import gui, run_metis, archive`.
- Switched the build backend from `uv` to `hatchling`. The `[tool.uv]` and
  `[dependency-groups]` sections are gone. Pipeline dependencies that live on
  ESO mirrors (`pycpl`, `edps`, `pyesorex`, `adari_core`) are still installed
  at runtime by the GUI's Install tab — they are not in `[project.dependencies]`
  because they are not on PyPI.
- The GUI's Install tab and the Archive tab's MetisWISE installer now shell
  out to `pip` (via `sys.executable -m pip`) with `--extra-index-url` for the
  ESO and pycpl mirrors. `uv` is no longer required to use MTR.
- Runtime data (the `METIS_Pipeline` and `METIS_Simulations` clones, `.env`,
  instrument packages, DRLD) now lives in a user data directory resolved via
  `platformdirs` — `~/.local/share/metis-test-runner/` on Linux. Override the
  location with the `METIS_DATA_DIR` environment variable. The per-asset env
  vars (`METIS_SIMULATIONS_DIR`, `METIS_INST_PKGS`) still work for back-compat.
- The CLI is now invoked as `mtr-cli` (or `python -m metis_test_runner.run_metis`)
  instead of `python src/run_metis.py`.

### Added
- `mtr` and `mtr-cli` console-script entry points. After installation, both
  are on `PATH` and can be invoked from any directory.
- `metis_test_runner.paths` — a single seam for every filesystem location MTR
  reads or writes outside the package itself.
- `metis_test_runner.indexes` — ESO mirror URL constants.
- BSD-3-Clause `LICENSE` file.
- Trusted-publishing GitHub workflow (`.github/workflows/publish.yml`)
  publishes to PyPI on `v*.*.*` tag pushes.

### Removed
- `launch.sh` — superseded by the `mtr` console script. Developers now use
  `pip install -e .[dev]` to set up a working copy.

### Notes / known limitations
- `gui.REPO_ROOT` is kept as a back-compat alias pointing at `paths.data_dir()`
  so existing test monkeypatches continue to work. To be removed in a future
  release.

## 0.1.0

Internal pre-PyPI release. Distributed via `git clone` + `./launch.sh`.
