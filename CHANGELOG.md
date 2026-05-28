# Changelog

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
  vars (`METIS_META_PKG`, `METIS_SIMULATIONS_DIR`, `METIS_INST_PKGS`) still
  work for back-compat.
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
- The `--runner metapkg` mode of `mtr-cli` still shells out to `uv run --project
  <metis-meta-package>`. This is the only remaining `uv` dependency and is
  guarded with a clear error message if `uv` is not installed. It will be
  removed once `metis-meta-package` is published to PyPI.
- `gui.REPO_ROOT` is kept as a back-compat alias pointing at `paths.data_dir()`
  so existing test monkeypatches continue to work. To be removed in a future
  release.

## 0.1.0

Internal pre-PyPI release. Distributed via `git clone` + `./launch.sh`.
