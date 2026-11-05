# Plan: update check, in-app update and post-update changelog

Status: proposal, not implemented.

## Goals

1. On startup, check PyPI for a newer release of `metis-test-runner`.
2. If one exists, show an **Update** button in the main window's toolbar.
3. After the user updates (by any means), show a window with the changelog
   since their previously used version, with any required actions (for example,
   a moved data-product location) shown first and prominently.

## Current state of the code

- `__version__` comes from the installed package metadata
  (`src/metis_test_runner/__init__.py`), so the running version is reliable.
- The top row is the `QToolBar` built in `MainWindow.__init__` (`gui.py`),
  which holds the *Theme* and *Data flow* buttons.
- Background work uses `QThread` workers; persistent GUI state uses
  `QSettings("METIS", "TestRunner")`.
- `installer.py` has `_ensure_pip()`, which bootstraps pip into pipx venvs
  (they ship without it).
- `CHANGELOG.md` uses `## X.Y.Z — date` sections, enforced by `publish.yml`.
  It is included in the sdist but **not in the wheel**.
- The pipeline (pymetis, edps, pycpl, …) is pip-installed into **the same venv
  as MTR**. An update must upgrade MTR in place and never recreate that venv.

## Part 1 — Startup check

### New module `update.py` (no Qt imports, so it is unit-testable)

- `fetch_latest(timeout=5) -> str | None`
  - `GET https://pypi.org/pypi/metis-test-runner/json`.
  - From `releases`, drop yanked releases and pre-releases, and drop releases
    whose files' `requires_python` excludes the running interpreter.
  - Return the highest remaining version, or `None`.
  - Any failure (offline, proxy, timeout, malformed JSON) returns `None`
    silently. Firewalled institute machines are a normal case, not an error.
    `urllib` honours `https_proxy` from the environment.
- Version comparison: add `packaging` as a dependency (small, correct). Since
  releases are only tagged `vX.Y.Z`, an int-tuple parser would also work.
- `install_kind() -> "editable" | "pipx" | "venv"`
  - Editable: `importlib.metadata.distribution("metis-test-runner")
    .read_text("direct_url.json")` contains `dir_info.editable` (PEP 610).
  - pipx: `sys.prefix` lies under pipx's `venvs` directory.
  - Otherwise: a plain venv.

### GUI wiring

- `UpdateCheckWorker(QThread)` started from `MainWindow.__init__`, emitting
  `update_available(str)`.
- Skip the check when:
  - `SMOKE_TEST` is set,
  - `MTR_NO_UPDATE_CHECK=1`,
  - the install is editable (developers update with git),
  - the last check was less than 24 h ago (timestamp in QSettings; cache the
    last result so the button still appears within that window).
- Toolbar: add a stretch spacer, then a hidden
  `QPushButton("Update to X.Y.Z")` with `role="accent"`. The tooltip shows the
  installed and latest versions. Shown when the signal arrives.

## Part 2 — Update button flow

1. **Refuse while jobs run.** Reuse `MainWindow._busy_jobs()` (as `closeEvent`
   does): replacing packages under a running pipeline run or archive operation
   is unsafe.
2. **Confirm.** Optionally fetch and show the upcoming release notes from
   `https://raw.githubusercontent.com/AstarVienna/MTR/main/CHANGELOG.md`.
3. **Upgrade in a worker**, streaming output to a small log dialog:
   - pipx install with `pipx` on `PATH`:
     `pipx upgrade metis-test-runner`. This upgrades in place, keeps the
     pipeline packages, and keeps pipx's own metadata consistent.
     **Never** `pipx reinstall`: it recreates the venv and wipes the pipeline.
   - Otherwise: `_ensure_pip()`, then
     `sys.executable -m pip install --upgrade metis-test-runner==X.Y.Z`.
     pip's default `only-if-needed` upgrade strategy leaves the pipeline's
     dependencies alone. Pinning the version makes the result match the button.
4. **Restart.** Ask "Restart now?". On yes, save settings and
   `os.execv(sys.executable, [sys.executable, "-m", "metis_test_runner", *sys.argv[1:]])`.
   A restart is required: old modules stay loaded in memory, and any lazy
   import after the swap would mix versions.

Risks:

- A new release may raise pins (PyQt6, Python range). The `requires_python`
  filter covers the interpreter; anything else goes in an *Action required*
  note (Part 3).
- A new release may need a newer pipeline checkout. Same mechanism: an
  *Action required* note telling the user to re-run *Install*.

Alternative (simpler, more robust, less convenient): the button only shows
the release notes and the command to copy (`pipx upgrade metis-test-runner`)
instead of running it.

## Part 3 — Post-update changelog window

### Ship the changelog in the wheel

```toml
[tool.hatch.build.targets.wheel.force-include]
"CHANGELOG.md" = "metis_test_runner/CHANGELOG.md"
```

Read it with `importlib.resources.files("metis_test_runner") / "CHANGELOG.md"`.

### Trigger

- QSettings key `last_seen_version`.
- On startup:
  - no stored value (fresh install): store the current version, show nothing;
  - installed version newer than stored: show the window, then store the
    current version;
  - installed version older or equal (downgrade / no change): show nothing.
- Independent of how the user upgraded, so a manual `pipx upgrade` also
  triggers it.

### Parsing

- Pure function: split on `^## \[?(\d+\.\d+\.\d+)`, keep sections with
  `last_seen < v <= current`, skip `Unreleased`.
- Unit-tested against the real `CHANGELOG.md`.

### Window

- `QDialog` with a `QTextBrowser` using `setMarkdown(...)` (available in the
  pinned Qt 6.6), an OK button and a link to the full changelog
  (`project.urls.Changelog`).
- Title: "What's new since X.Y.Z".

### Warnings and instructions: changelog convention

Keep one source of truth: a dedicated subsection in the changelog.

```markdown
## 0.8.0 — 2026-12-01

### Action required
- Data products now go to `~/.local/share/metis-test-runner/products`.
  Move old results from `<old path>`, or they will not appear in the Archive tab.

### Changed
- …
```

- The window collects every `### Action required` block from all skipped
  versions and shows them first, with a warning style, under the heading
  "Action required before you continue". The remaining notes follow.
- If any such block exists, closing requires ticking "I have read this".
- Extend the `publish.yml` consistency check so a misspelled heading
  (e.g. `### Actions required`) fails the release instead of silently hiding a
  warning.
- Later, if needed: migration hooks keyed by version
  (`MIGRATIONS = {"0.8.0": check_products_dir}`) for cases the code can detect
  itself, e.g. offering to move files from an old location. Add only when a
  real case appears.

## Extras

- `mtr-cli`: print a one-line "newer version available" notice to stderr,
  using only the cached 24 h result; never block on the network.
- README: document `MTR_NO_UPDATE_CHECK`, and add the *Action required*
  convention to the release steps.

## Tests

- `fetch_latest` against a PyPI JSON fixture: yanked releases, pre-releases,
  `requires_python` exclusions, network errors.
- Changelog section selection and *Action required* extraction.
- `install_kind` detection (editable, pipx, venv).
- `last_seen_version` logic: fresh install, upgrade, downgrade.
- GUI: update button stays hidden under `SMOKE_TEST`; no network access in
  tests.

## Estimate

About 300–400 lines: `update.py` (~120), GUI wiring (~150), tests (~100),
plus `pyproject.toml` and README changes. No new runtime dependency apart from
an optional `packaging`.

## Open question

Should the Update button run the upgrade itself (Part 2), or only show the
release notes and the command to copy?
