# Changelog

## Unreleased

### Changed
- The Archive tab installs `metiswise>=0.0.4` from the entropynaut index
  instead of the v0.0.4 GitHub tag tarball, now that the index publishes it.

### Fixed
- Uninstall now removes the editable pymetis clone, which upstream renamed from
  `eso-pymetis` to `pymetis` on 2026-07-13. Both names are uninstalled.

## 0.5.0 — 2026-09-19

### Fixed
- **`mtr-cli` with no arguments crashed.** The "input files are required" check
  referenced the argparse parser from inside `main()`, where that name is not
  bound, so the most common first-run mistake produced an `UnboundLocalError`
  traceback instead of a usage message and exit 2. The check now lives in
  `parse_args()` where the parser is in scope. It went unnoticed because
  `main()` had no test coverage at all; it does now.
- **`--calib obs1.yaml` no longer eats the input file.** Both `--calib` and
  `--static` used `nargs="?"`, so a following positional was consumed as the
  option's value — including in the README's own example, which failed with
  `invalid int value: 'obs1.yaml'`. Both flags now take their value
  explicitly, and `--no-calib` / `--no-static` are the shorthand for `0`. A
  stray input file in that position gets an actionable error rather than an
  integer-parsing complaint. `--calib N` keeps its integer: it is forwarded to
  METIS_Simulations as `doCalib`, which sets `nObs` per calibration config.
  `--static` is documented as the `{0,1}` switch it always was — MTR only ever
  tested it for truthiness.
- **Re-installing no longer destroys the backup of your EDPS config.** On the
  second install the file in place is MTR's own, so backing it up again
  overwrote the pristine original — which Uninstall would then "restore". The
  first backup is now kept.
- **Uninstall refuses to delete implausible directories.** `METIS_DATA_DIR` was
  passed to `shutil.rmtree` with no validation, so a typo (or
  `METIS_DATA_DIR=$HOME`) could remove a home directory. Partial failures are
  now reported instead of being silently ignored.
- **The archive database password no longer reaches child processes.** It is
  injected into `os.environ` for commonwise's benefit, and every pipeline
  subprocess — `edps`, `pyesorex`, ScopeSim, and the interactive `mtr-shell` —
  inherited it. `env.resolve_runtime_env()` now strips it for every runner.
- **OmegaCEN pip credentials are percent-encoded.** A space in a password split
  `PIP_EXTRA_INDEX_URL` into two bogus indexes, and an `@` re-split the URL
  authority so pip would contact a host named by the password, carrying the
  rest of the credential. Credentials still never appear in `argv`.
- **`--auto-fetch-calibrations` works for science-only input, and stops
  claiming false success.** `has_science` was accepted by
  `identify_missing_calibrations` and never read, so an input set of only
  science raws — the case the feature exists for — fetched nothing and
  reported "All required calibrations already present". The caller also passed
  YAML-derived tags that are empty for CSV-only and stale under `--no-sim`; it
  now resolves them from the FITS actually on disk, and distinguishes "checked,
  nothing missing" from "could not check".
- **Install/uninstall timeouts now work.** The worker drained the subprocess's
  stdout to EOF *before* calling `wait(timeout=…)`, so the timeout could never
  fire: a hung pip download or a `git fetch` against a dead mirror hung the
  worker forever. A watchdog now enforces the deadline and kills the whole
  process group, so pip's and git's own children go too.
- **Stop no longer orphans the EDPS server.** The Run tab sent `SIGKILL`
  immediately, skipping `run_metis`'s cleanup; it now sends `SIGTERM`, waits,
  and only then kills. A user-initiated stop is reported as stopped rather than
  as a failure exit code.
- **Closing the window during a job is safe.** Only the ref workers were
  stopped, so quitting mid-install destroyed a tab out from under a live
  `QThread`. All workers are now tracked, the window asks before quitting on a
  running job, and the pipeline process is stopped.
- **A second archive action can no longer abort the first.** The Archive tab
  kept one worker slot and only disabled the button for the action in flight,
  so e.g. "Save & Test" during a MetisWISE install dropped the last reference
  to a running thread.
- **The Run tab recovers if the process fails to start.** `FailedToStart` never
  reaches `finished`, so Run stayed disabled and Stop enabled until restart.
- **The GUI respects the selected runner when building the environment.** It
  always resolved the `default` runner's environment, contradicting `env.py`,
  which deliberately returns the bare parent environment for
  `native`/`docker`/`podman`.
- **Ctrl-C no longer leaves EDPS patched and running.** The
  `association_preference` override and the daemon are now owned by a context
  manager entered before anything global is touched; the restore runs before
  the stop, and a hanging stop can no longer skip it or mask the real error.
  `mtr-cli` exits 130 on interrupt instead of printing a traceback.
- **Downloads are atomic.** Files were copied directly onto the final name, so
  an interrupted copy left a truncated `.fits` that the pipeline's FITS scan
  would accept as a valid master. Downloads now land on a temp name, are
  size-checked, and are renamed into place. Archive-supplied filenames can no
  longer escape the destination directory.
- **`~/.awe/Environment.cfg` is written atomically and always `0600`.** Only
  the creation path chmod-ed, so updating a pre-existing world-readable legacy
  file left the password readable by every local user. Values containing
  newlines are rejected — one could inject configuration lines and, for
  example, downgrade the archive transport to cleartext.
- **EDPS config patching handles backslashes.** The replacement text was passed
  to `re.subn` as a string, which interprets `\g<…>` and escapes, so a data
  directory containing a backslash produced a corrupted config.
- **Corrupt FITS files are reported instead of silently skipped**, so a
  truncated download no longer looks identical to a frame without DPR keywords.
- **Workers log a traceback for unexpected errors** rather than only
  `str(exc)`, which rendered a `KeyError` as `✗ Failed: 'foo'`.

### Added
- **Download and upload progress is shown.** Both workers already emitted a
  `progress` signal and nothing was ever connected to it.
- **`mtr-cli --examples-dir` and `--copy-examples DIR`.** The bundled examples
  ship inside the package, so after a `pipx install` the README's
  `mtr-cli examples/LMS_RAD_06.yaml` could not work — there is no `examples/`
  in the working directory.
- **`mtr-cli --version`**, and the EDPS command line is now echoed before the
  pipeline runs — it was the hardest part of a run to reproduce by hand and the
  one thing never printed.
- `SECURITY.md`, documenting how the two kinds of credential are handled and
  which invariants must hold when adding a new subprocess call.
- **Screenshots of all three tabs** (`docs/images/`), referenced by absolute URL
  so they render on GitHub and PyPI alike.
- Ruff configuration, a lint job, a `dependabot.yml`, `.editorconfig` and
  `.gitattributes`.

### Changed
- **`--prefer-masters` now says when it cannot do anything, and stays
  CLI-only.** The Install tab already pins
  `association_preference=master_per_quality_level`, so on a standard install
  the flag rewrites the value it already has — a complete no-op. For
  `--runner docker`/`podman` it is worse: it patches
  `~/.edps/application.properties` on the *host* while EDPS reads the
  container's own configuration. It now warns in both cases, and reports the
  real before/after when it does change something. The flag remains useful only
  where EDPS was configured outside MTR (typically `--runner native`), which is
  why it is deliberately *not* surfaced on the Run tab: that tab is
  overwhelmingly driven with the `default` runner, where it does nothing.
- **`__version__` is derived from the installed package metadata.** It was
  hardcoded and had been stale at `0.3.1` for four releases. `pyproject.toml`
  is now the single source of truth.
- **`astropy` is declared in the `dev` extra.** It is a real test dependency —
  `tests/test_archive.py` cannot even be imported without it — but was only
  installed ad hoc by CI, so a clean `pip install -e .[dev] && pytest` failed.
- **CI covers Python 3.13**, which the project advertises but never tested; adds
  a lint job, coverage, a packaging job that verifies the wheel actually
  contains the bundled examples and installs it, concurrency cancellation, pip
  caching, `permissions: contents: read` and job timeouts. All four console
  scripts are smoke-tested, including that bare `mtr-cli` fails cleanly.
- **Publishing is gated on the test suite** and on a check that the tag, the
  `pyproject.toml` version and the CHANGELOG heading agree. A tag push
  previously published even if tests were red. (`v0.4.1` was released in the
  changelog but never tagged, so it never reached PyPI.)
- `pyproject.toml`: SPDX license metadata, a `PyYAML>=6.0` floor, `py.typed`,
  explicit wheel `artifacts` for the examples, and an expanded `dev` extra. The
  `PyQt6==6.6.0` pin is kept, now with the reasoning, how to re-test it, and its
  known cost for Python 3.13 recorded next to it.
- `.gitignore` covers the test, lint and coverage caches.
- **The README leads with a Quickstart and folds the reference material into
  collapsible sections.** It had grown to 461 lines with no quickstart: a new
  user read ~85 lines of install variants and Qt system libraries, then 93 lines
  of prose describing a UI they could simply be shown, before learning what to
  actually do. Now ~148 visible lines, with system dependencies, runner modes,
  the input-format spec, the `mtr-cli` option table and the worked examples
  behind `<details>`. Nothing was deleted.
- **The README no longer documents a control that does not exist.** The Run tab
  walkthrough still described picking a workflow from a *Workflow* dropdown,
  removed in 0.4.0 along with `--workflow`.
- **Example paths in the README are reachable.** They pointed at a relative
  `examples/` directory that does not exist after a `pipx install`, and the two
  Markdown links to it 404'd on both GitHub and PyPI.

### Removed
- **`container/`.** The image's `CMD` referenced a `launch.sh` deleted four
  releases ago, and the image never installed MTR at all — its launcher was a
  `uv sync` against a `src/gui.py` that the PyPI restructure moved. Its original
  purpose (a local archive pod) disappeared when the archive moved to MetisWISE.
  Nothing documented it beyond a repository-layout line, and it is unrelated to
  the `docker`/`podman` *runner* modes, which target a pipeline container you
  build yourself from `METIS_Pipeline/toolbox/`.

### Added — Install tab
- **Pin a branch, tag or commit per repository in the Install tab.** A new
  *Repository version (advanced)* group gives `METIS_Pipeline` and
  `METIS_Simulations` an editable dropdown, populated in the background from
  `git ls-remote` (↻ reloads), plus a line showing what the local clone is
  currently on. Blank keeps the previous behaviour — track the default branch —
  so nothing changes for anyone who ignores the new fields. Intended for people
  developing *on* those repos, who until now could only install `main`.
  Selections persist in QSettings and are cleared by Uninstall.

  Clicking the field opens the list (an editable combo normally opens only from
  its arrow), except once something hand-typed is in it — a commit SHA stays
  cursor-editable. The list opens on button *release*, so it stays up after a
  normal click rather than only while the button is held.

  An existing clone is overwritten with the selection: a dirty working tree is
  itemised in a confirmation dialog first, and declining cancels the install.
  The reset is `git clean -fd` (never `-x`), so gitignored build artefacts,
  simulation `*.fits` output and `inst_pkgs/` survive. Branches become a real
  local branch tracking `origin`; tags and commits give a detached HEAD.
  Clearing a pinned field returns the clone to the remote's default branch —
  including from the detached HEAD a previous pin left behind.

### Fixed — Install tab
- **`git pull --ff-only` failures are no longer silently ignored.** The update
  path ran the pull through a bare `subprocess.run` whose return code was never
  checked, so a diverged branch, a dirty tree or a network error left the rest
  of the install running against the wrong commit while reporting success. It
  now raises with an actionable message. The same call also never passed
  `env=_child_env()`, unlike every other subprocess in the module.
- **The Install tab's settings were never saved.** `MainWindow` constructed
  `InstallTab()` inline without keeping a reference, so `closeEvent` could not
  reach it.
- The installer now verifies `metisp/pymetis`, `metisp/pyrecipes` and the
  simulations `pyproject.toml` exist after checkout. A ref predating the current
  layout previously failed deep inside `pip install --editable`, or — worse —
  silently produced a pipeline with zero recipes because `PYCPL_RECIPE_DIR`
  pointed at a directory that did not exist.
- A clone whose `origin` points somewhere other than the expected URL is now
  reported in the log instead of being fetched from silently.
- **Combo boxes had no visible dropdown arrow.** The theme styled
  `QComboBox::drop-down`, and styling that subcontrol at all suppresses Qt's
  native chevron. Harmless for the existing read-only combos (clicking anywhere
  opens those), but it left the new editable ref combos with no mouse affordance
  at all — the list was reachable only with the arrow keys. The rule is gone, so
  every combo in the app shows its arrow again.

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
