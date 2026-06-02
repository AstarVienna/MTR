# Changelog

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
