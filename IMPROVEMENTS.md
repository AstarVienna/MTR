# MTR — Findings and Implementation Plan

> **STATUS (2026-09-19): P0, P1, P3 and P5 have been implemented.** P2
> (structural refactors) and P4 (new features) were explicitly deferred and
> remain open — see *Remaining work* below. Items marked **[DONE]** are
> implemented and covered by tests; the descriptions are kept because they
> record *why* each change was made.
>
> Test suite went from 458 to 560 passing; coverage 71% -> 73%, with
> `archive.py` 85% -> 91% and `env.py` at 100%. `ruff check` is clean and
> wired into CI.

## Remaining work (not done)

- **P2 — structural refactors** (splitting `gui.py` and `run_metis.py`,
  removing the four dead functions, the shared option spec). Deferred by
  choice. The cheap part of P2-5 *was* done: `TestGuiArgsParseAsCli` now feeds
  the GUI's argv through the real `parse_args`.
- **P4 — features**: YAML schema validation, result diffing, `--json` output,
  a real dry run, run presets, cancel buttons for the remaining long
  operations, log ergonomics (no size cap, no save-to-file — `log_append` can
  still grow a `QTextEdit` unboundedly during a long run), a download cache,
  and per-stage timeouts in `run_metis`.
- **Action SHA pinning** in the workflows. `dependabot.yml` now manages the
  major tags, which is the practical equivalent; pinning by hand needs the
  current SHAs.
- **`PyQt6==6.6.0`** is deliberately still pinned, now with the reasoning and a
  re-test recipe recorded in `pyproject.toml`. This still blocks real 3.13
  wheel coverage.
- **The PyPI `environment:` block** in `publish.yml` is left commented out on
  purpose — enabling it without a matching Trusted Publisher config on PyPI
  breaks the OIDC exchange.


Audit date: **2026-09-17**, against commit `b9aeba1` (branch `main`, version `0.4.5`).

This document is written to be **self-contained**: an agent should be able to implement
any item below without re-exploring the repository first. Every item carries a file:line
anchor, the reason it matters, and a concrete fix.

---

## 0. Orientation — read this first

### 0.1 What this project is

MTR (`metis-test-runner`) is a GUI + CLI wrapper for end-to-end testing of the ESO METIS
instrument pipeline. A run has two stages:

1. **Simulate** — ScopeSim generates synthetic FITS observations from a YAML/CSV config.
2. **Reduce** — the EDPS workflow engine runs the METIS pipeline over those FITS files.

Published to PyPI; installed with `pipx`. The GUI (`mtr`) is the primary interface; the
CLI (`mtr-cli`) is the documented fallback. **The GUI does not import the CLI** — it
builds an argv list and shells out to `python -m metis_test_runner.run_metis`.

### 0.2 Module map (~6.2k source lines, ~5.1k test lines)

| File | Lines | Role |
|---|---:|---|
| `src/metis_test_runner/gui.py` | 3367 | All Qt. 3 tabs, 8 `QThread` workers, themes, git helpers, entry point |
| `src/metis_test_runner/run_metis.py` | 1535 | CLI core. Domain tables, workflow inference, orchestration in one `main()` |
| `src/metis_test_runner/archive.py` | 945 | MetisWISE/commonwise archive client + credential config |
| `src/metis_test_runner/direct.py` | 127 | `mtr-exec` / `mtr-shell` direct environment access |
| `src/metis_test_runner/credentials.py` | 118 | OS-keyring credential storage |
| `src/metis_test_runner/env.py` | 101 | **Shared env-resolution seam** (good design; keep it) |
| `src/metis_test_runner/paths.py` | 50 | **Shared path seam** (good design; keep it) |
| `src/metis_test_runner/indexes.py` | 6 | ESO pip index URLs |

Four "runner" modes thread through everything: `default` (MTR's own venv), `native`
(tools on PATH), `docker`, `podman`.

### 0.3 Architectural note: `archive.py` is NOT an ESO Science Archive client

It is a thin façade over the **MetisWISE / commonwise ORM** (Astro-WISE lineage) talking
to PostgreSQL plus a data server. **`archive.py` contains no HTTP code at all** — no
timeouts, retries, pagination, TLS options or checksums, because all of that lives inside
commonwise and is currently unreachable from MTR. Any plan that assumes a REST client is
wrong. `metiswise` is an optional dependency and is **not installed** in this environment,
so its runtime behaviour could not be verified.

### 0.4 Running the tests

```fish
# astropy is a HIDDEN test dependency — see item P1-6. Without it, collection FAILS.
pip install -e '.[dev]' astropy
env QT_QPA_PLATFORM=offscreen pytest tests/ -q
```

Verified baseline at audit time: **458 tests pass in ~1.3 s**, coverage **71 %**.
The suite is fast because *everything* is mocked; no test starts a real `QThread` and no
test runs a real subprocess.

Coverage by module (the gaps are the story):

| Module | Cover | Notable uncovered |
|---|---:|---|
| `run_metis.py` | 53 % | **lines 1108–1531 — all of `main()`, 0 %** |
| `gui.py` | 72 % | every `QThread.run()` body; all of `RunTab`'s process layer |
| `archive.py` | 85 % | `fetch_missing_calibrations` has **zero** tests |
| `env.py` | 100 % | — |

### 0.5 Verification status legend

Findings were produced by four parallel module audits and then independently
re-checked. Each item is tagged:

- **[VERIFIED]** — reproduced or confirmed directly against the source during this audit.
- **[REPORTED]** — from a module audit, consistent with the source, but not independently
  reproduced. Treat as high-confidence but confirm while fixing.
- **[UNCONFIRMED]** — investigated and **did not reproduce**; recorded so nobody re-files it.

---

## P0 — Confirmed bugs, fix first

### P0-1 **[DONE]** [VERIFIED] `mtr-cli` with no arguments crashes

`src/metis_test_runner/run_metis.py:1115`

```python
if not args.no_sim and not args.input_files:
    p.error("Input files (YAML or CSV) are required unless --no-sim is given")
```

`p` is the `ArgumentParser`, local to `parse_args()` (line 982). In `main()` the name `p`
is only bound later, at line 1126 (`p = Path(raw).resolve()`), which makes it a *local*
variable — so the reference raises before assignment.

Reproduced:

```
UnboundLocalError: cannot access local variable 'p' where it is not associated with a value
```

The most common first-run user error prints a traceback and exits 1 instead of a usage
message and exit 2. It survived because **`main()` has no test coverage at all**.

**Fix.** Move the check into `parse_args()` where the parser is in scope:

```python
# in parse_args(), just before `return p.parse_args(argv)`
args = p.parse_args(argv)
if not args.no_sim and not args.input_files:
    p.error("Input files (YAML or CSV) are required unless --no-sim is given")
return args
```

Then delete lines 1114–1115 from `main()`. While here, stop reusing the name `p` for
three different things in `main()` (1126, 1186, 1236) — rename the `Path` loop variable.

**Test.** `parse_args([])` should raise `SystemExit` with code 2. Add a `main()` smoke
test (see P1-1).

### P0-2 **[DONE]** [VERIFIED] The documented `--calib` invocation is a hard argparse error

`src/metis_test_runner/run_metis.py:1007-1011`, documented at `README.md:358`

`--calib` and `--static` use `nargs="?", type=int`, so a following positional is consumed
as the option's value. The README's own example fails:

```
$ mtr-cli -o /tmp/myrun --calib obs1.yaml obs2.yaml
error: argument --calib: invalid int value: 'obs1.yaml'    # exit 2 — reproduced
```

**Fix.** Preferred: drop `nargs="?"` and make the contract explicit — `--calib {0,1}` plus
a `--no-calib` convenience flag. Minimum: fix the README example and document that a bare
`--calib` must come last. Do not leave both the code and the docs as they are.

### P0-3 **[DONE]** [VERIFIED] Second install destroys the user's original EDPS config

`src/metis_test_runner/gui.py:1035-1044`

```python
def _backup_edps_config(self) -> None:
    props = Path.home() / ".edps" / "application.properties"
    if props.exists():
        backup = props.with_name("application.properties_backup")
        props.rename(backup)          # <-- unconditional
```

- Install #1: the user's real config → `..._backup`; MTR writes its own config.
- Install #2 (the UI actively encourages re-running): `props.exists()` is true — but it is
  now *MTR's own* file — so the rename **overwrites the pristine backup with MTR's config**.
  The user's original is gone permanently.
- `UninstallWorker._cleanup_edps` (`gui.py:1246-1258`) then "restores" MTR's config as if
  it were the user's.

`tests/test_gui.py:590-597` (`test_overwrites_previous_backup`) **asserts this
destructive behaviour** — it seeds a backup containing `port=1111` and asserts it has
become `port=9999`. The test must be changed along with the code.

**Fix.**

```python
backup = props.with_name("application.properties_backup")
if backup.exists():
    self.log.emit(f"Backup already exists at {backup} — leaving it untouched\n", "yellow")
    props.unlink()
else:
    props.rename(backup)
```

Rewrite `test_overwrites_previous_backup` into `test_preserves_existing_backup`.

### P0-4 **[DONE]** [VERIFIED] Uninstall can `rmtree` an arbitrary directory

`src/metis_test_runner/gui.py:1231-1244`

`shutil.rmtree(target)` guarded only by `target.exists()`. `REPO_ROOT` is
`paths.data_dir()`, which is `METIS_DATA_DIR` verbatim with **no validation**
(`paths.py:22-27`). `METIS_DATA_DIR=$HOME`, or a typo, and Uninstall deletes the home
directory. The confirmation dialog (`gui.py:1661-1671`) prints the path but nothing refuses.

**Fix.** Refuse to delete a path that is `/`, the user's home, or has fewer than three
components; require a marker (e.g. that it contains `METIS_Pipeline/` or was created by
MTR). Add `onerror=` so a partial failure is reported rather than silently continuing.

```python
def _assert_safe_to_remove(target: Path) -> None:
    resolved = target.resolve()
    if resolved == Path.home() or resolved == Path("/") or len(resolved.parts) < 3:
        raise RuntimeError(f"Refusing to delete {resolved} — not a plausible data dir")
```

### P0-5 **[DONE]** [VERIFIED] The archive DB password leaks into every child process

`src/metis_test_runner/archive.py:269`

```python
os.environ.update(injected)   # includes database_password
```

`env.py:55` does `env = os.environ.copy()`, and that env is handed to:

- `gui.py:756`, `gui.py:1221` — install/pipeline subprocesses
- `run_metis.py:830`, `:1496`, `:1514`, `:1525` — `edps`, `pyesorex`, ScopeSim
- `direct.py:81` (`mtr-exec`) and `direct.py:104` (`mtr-shell` — an **interactive bash**,
  where `env` prints the password and every command the user runs inherits it)

So the archive password reaches processes with no business seeing it, and any of them can
surface it in a crash dump or diagnostic. The injected names are also dangerously generic
(`project`, `database_user`, `database_password`).

**Fix (cheapest correct).** Strip the managed keys in `env.resolve_runtime_env()` before
returning, so every spawn path is covered by one change:

```python
_SECRET_KEYS = frozenset(ENV_CFG_FIELDS) | {"ask_administrator_password"}

def resolve_runtime_env(runner: str = "default") -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in _SECRET_KEYS}
    ...
```

Better long-term: don't write them to `os.environ` at all — commonwise reads
`sys.modules["common.config.Environment"].Env`, which `archive.py:270-272` already updates.

**Test.** Assert `resolve_runtime_env()` never contains `database_password` after
`apply_db_credentials()` has run.

### P0-6 **[DONE]** [VERIFIED] pip credentials are interpolated into a URL unescaped

`src/metis_test_runner/archive.py:125-131`

```python
"PIP_EXTRA_INDEX_URL": " ".join((
    ESO_INDEX, PYCPL_INDEX,
    f"https://{pip_credentials}@pip.entropynaut.com/packages/",
)),
```

`pip_credentials` is raw text from a GUI field (`gui.py:2054`). With no escaping:

- a **space** in the password splits the value into two index URLs — the real index is
  never queried and the error is unrelated to the cause;
- an **`@`** re-splits the authority, so pip connects to an attacker-chosen host **with
  the rest of the credential in the userinfo** — a credential-exfiltration primitive
  triggered by a merely awkward password;
- `/`, `#`, `?` truncate the authority; `%` is read as a percent-escape;
- no check that there is exactly one `:` — `"alice"` silently yields `https://alice@…`.

**Fix.** Split on the first `:`, percent-encode each half, reject whitespace, validate shape:

```python
from urllib.parse import quote

def _encode_pip_credentials(raw: str) -> str:
    if any(c.isspace() for c in raw):
        raise ValueError("Credentials must not contain whitespace")
    user, sep, password = raw.partition(":")
    if not sep or not user or not password:
        raise ValueError("Credentials must be in the form user:password")
    return f"{quote(user, safe='')}:{quote(password, safe='')}"
```

**Test.** Parametrise over passwords containing ` `, `@`, `/`, `%`, `#` and assert the
built URL has exactly one authority and that the credential never reaches argv. (The
existing argv-safety test at `tests/test_archive.py:124` only uses the benign `alice:p4ss`.)

**Note — the argv design itself is correct and should be preserved:** credentials travel
only in `PIP_EXTRA_INDEX_URL`, never in `argv`, which is why `gui.py:1747` can safely echo
the command. Don't regress that while fixing the encoding.

### P0-7 **[DONE]** [VERIFIED] `--auto-fetch-calibrations` reports false success and silently no-ops

Two independent defects that compound:

**(a) `has_science` is accepted and never used.** `archive.py:859-902` —
`identify_missing_calibrations(data_tags, has_science, sub_workflows)` never references
`has_science` in its body. Combined with the gate at `archive.py:885-886`:

```python
if deepest_present_idx < 0:
    return []
```

and the fact that science tasks are skipped when computing `deepest_present_idx`
(`:879-882`), a user whose inputs are **only science raws** — the canonical "fetch my
masters" case, and what the README promises at `README.md:316` — gets `[]`. Nothing is
fetched.

**(b) Ordering bug in the caller.** `run_metis.py:1355-1398` passes the *YAML-derived*
`has_science` (initialised `False` at `:1175`) to the fetch step, but the FITS-derived
recomputation only happens later, at `:1432`/`:1459` — **after** the fetch. So under
`--no-sim` the real values are never used. For a CSV-only run, `fetch_tags` and
`fetch_sub_workflows` are both empty, the loop at `:1371` never executes, and `:1392`
prints **"All required calibrations already present"** — which is false. The same false
message appears if every workflow raises into the `except Exception` at `:1389`.

**Fix.** Move the fetch step to after tag/workflow resolution; wire `has_science` into
`identify_missing_calibrations` (treat "science present" as making the whole calibration
chain required) or delete the parameter and correct the README. Distinguish "checked
nothing" from "checked, nothing missing" in the message.

### P0-8 **[DONE]** [VERIFIED] The container is dead

`container/Dockerfile:22` — `CMD ["bash", "launch.sh"]`, but `launch.sh` does not exist
anywhere in the tree. It was deleted in `5e86448` ("Restructure as PyPI package") and its
removal is recorded at `CHANGELOG.md:257`. Any `compose up` exits immediately.

Compounding, the image never installs MTR: it installs `python3` (`Dockerfile:8`) but no
pip/venv/uv, relying entirely on the `.venv` that `launch.sh` used to create.

**Fix.** Either repair it or delete it. **Correction to an earlier draft of this
document:** `README.md:199-211` documents Option B, the *pipeline* container the user
builds from `METIS_Pipeline/toolbox/` for the `docker`/`podman` **runner modes** — an
unrelated thing. The only reference to `container/` was the repository-layout listing
(`:424-426`), with no usage instructions anywhere.

Related, if repairing: `compose.yml:23,27` bind-mount `../logs` and `../EDPS_data`, both
gitignored and therefore absent on a fresh clone (Podman errors; Docker creates them as
**root**, breaking `userns_mode: keep-id`). `compose.yml:10` `userns_mode: keep-id` is
Podman-only and `docker compose` rejects it, contradicting the README's Docker path.
There is no `.dockerignore` despite `context: ..`.

---

## P1 — Correctness, safety and release integrity

### P1-1 **[DONE]** [VERIFIED] `main()` is 425 untested lines

`run_metis.py:1107-1531` is a single function with **0 % coverage**. It contains every
`sys.exit`, the three-way inference branches (`:1194-1223`, `:1406-1468`) and the EDPS
`try/finally`. This is the direct cause of P0-1 shipping.

**Fix, in order:**

1. Add a smoke test now: `main([])` exits 2; `main(["--no-sim", "--no-pipeline"])` exits
   non-zero with the documented message. (Requires making `main` accept `argv`.)
2. Then extract stages as separate functions — `analyse()`, `simulate()`, `fetch()`,
   `reduce()`, `report()` — each independently testable. Today none of them is reachable
   without running the whole pipeline.

### P1-2 **[DONE]** [VERIFIED] `InstallWorker._run`'s `timeout` is dead code

`gui.py:746-771` (and identically `gui.py:1213-1229`, `gui.py:1748-1754`):

```python
for line in proc.stdout:      # blocks until EOF
    self.log.emit(...)
proc.wait(timeout=timeout)    # process has already exited by now
```

The read loop runs to EOF first, so `wait()` never actually times out. Consequences:

- a hung pip download or a `git fetch` against a dead mirror hangs the worker **forever**,
  with no timeout and no cancel;
- every carefully chosen `timeout=900` / `timeout=600` (`:866`, `:922`, `:933`) is decorative.

Additionally, none of the three sites uses `with subprocess.Popen(...)`, so if the loop or
`wait` raises, the child is never killed and `proc.stdout` is never closed — orphaned
process plus leaked fd.

**Fix.** Enforce a deadline around the read loop (reader thread + `proc.kill()` on
expiry, or `subprocess.run(..., timeout=...)` where streaming isn't needed), and wrap in
`with Popen(...)`. Factor the three copies into one `CommandWorker._run` (see P2-1).

### P1-3 **[DONE]** [VERIFIED] Stop button orphans the pipeline; closing the window is unsafe

- `gui.py:3124-3126` — `_stop()` calls `QProcess.kill()` (SIGKILL) with no `terminate()`
  first. It kills only the direct child (`python -m …run_metis`); the **EDPS server on
  port 4444 and any ScopeSim children keep running**. It also defeats the CLI's own
  `finally` at `run_metis.py:1523-1527`, so `~/.edps/application.properties` stays patched
  and the temp sim script leaks.
- `gui.py:3335-3340` — `closeEvent` stops **only** the ref workers. The other 7 `QThread`
  subclasses (`InstallWorker`, `UninstallWorker`, `MetisWISEInstallWorker`,
  `TestConnectionWorker`, `QueryWorker`, `DownloadWorker`, `UploadWorker`) are never
  joined, and the running pipeline `QProcess` is never killed. Closing during a 10-minute
  install destroys the tab — and the `QTextEdit` its queued signal targets — under a live
  thread. `deleteLater` appears **zero** times in the file.

**Fix.** `terminate()` → wait ~5 s → `kill()`; start the child in its own process group and
signal the group. In `closeEvent`, warn if a job is running, then `requestInterruption()`
+ `wait()` on all live workers and kill the `QProcess`.

### P1-4 **[DONE]** [VERIFIED] Downloads are not atomic and never verified

`archive.py:607-612` — `shutil.copy2(src, dest)` writes **in place** onto the final name.
A crash, Ctrl-C or a full disk leaves a truncated `*.fits` at the destination, which
`run_metis`'s FITS scan then classifies as a present, valid master calibration. No
checksum or size check is performed, and `query_archive` (`:560-564`) does not even
surface the fields needed to do one.

**Fix.** Write to `dest.with_suffix(".part")` and `os.replace()` on success; compare size
(and a checksum if the ORM exposes one); skip the copy when a verified file already exists.

### P1-5 **[DONE]** [VERIFIED] Credentials file is written world-readable on the update path

`archive.py` — `os.chmod(cfg, 0o600)` exists **only** at line 387 (the create path).
Lines 414, 438 and 492 call `cfg.write_text(...)` with no chmod. If `~/.awe/Environment.cfg`
already exists as 0644 — plausible, it is a legacy Astro-WISE file — the password is
written world-readable. `cfg.parent.mkdir(mode=0o700, exist_ok=True)` (`:382`) does not
repair an existing 0755 `~/.awe`, and `mode` is masked by umask anyway.

All four writes are also non-atomic truncate-then-write, so an interrupted write destroys
the file — including the `data_server`/`data_port`/`data_protocol` keys the module goes
out of its way to preserve.

**Fix.** One helper: write to a `NamedTemporaryFile` in the same directory, `os.chmod(0o600)`,
`os.replace()`. Apply at all four sites. Also reject values containing `\r`/`\n` — an
embedded newline injects arbitrary config lines (e.g. `\ndata_protocol : http`, silently
downgrading the transport to cleartext).

**Test.** Assert `stat.S_IMODE(cfg.stat().st_mode) == 0o600` after updating a pre-existing
0644 file. No current test checks file mode.

### P1-6 **[DONE]** [VERIFIED] `astropy` is a hidden test dependency

Not declared in any extra (`pyproject.toml:37-38`); CI bolts it on with
`pip install -e .[dev] astropy` (`unit_tests.yaml:31`, added in commit `360fc60`).

A clean `pip install -e '.[dev]' && pytest` **fails at collection**:

```
tests/test_archive.py:21: ModuleNotFoundError: No module named 'astropy'
```

`astropy` is also imported at runtime (`archive.py:639`, `:714`).

**Fix.** Declare it: `[project.optional-dependencies] archive = ["astropy"]`, and add
`astropy` + `pytest-cov` to `dev`. Drop the ad-hoc install from CI.

### P1-7 **[DONE]** [VERIFIED] Version drift and a missing release tag

- `src/metis_test_runner/__init__.py:3` says `__version__ = "0.3.1"`; `pyproject.toml:7`
  says `0.4.5`. Frozen since the v0.3.1 tag, four releases stale. It is also referenced
  nowhere else in the codebase — dead *and* wrong.
- **`v0.4.1` was never tagged.** Tags present: `v0.3.0 v0.3.1 v0.3.2 v0.4.0 v0.4.2 v0.4.3
  v0.4.4 v0.4.5`. Commit `e96e104` ("Release 0.4.1") bumped the version and CHANGELOG
  (`CHANGELOG.md:118`) but the tag-triggered publish never ran, so 0.4.1 is almost
  certainly absent from PyPI. Same for `0.2.0` and `0.1.0`.

**Fix.** Make `__init__.py` derive the version instead of duplicating it:

```python
from importlib.metadata import version
__version__ = version("metis-test-runner")
```

Add a tag/version consistency gate to `publish.yml` comparing the tag, `pyproject.toml`
version and the top CHANGELOG heading. Decide whether to tag `v0.4.1` retroactively or
note in the CHANGELOG that it was never released.

### P1-8 **[DONE]** [VERIFIED] Publishing is not gated on tests

`.github/workflows/publish.yml:29` — the `publish` job has `needs: build` only. A tag push
ships to PyPI even if `unit_tests.yaml` is red on that commit. Nothing runs `twine check`,
inspects wheel contents, or installs the built artifact before publishing.

`pypa/gh-action-pypi-publish@release/v1` (`:42`) pins to a **branch** — mutable code
running with `id-token: write`. This is the one place SHA pinning genuinely matters.

**Fix.** Gate publish on the test matrix (`workflow_run`, or duplicate the job). Add
`twine check dist/*` and a wheel-contents assertion. Pin all actions to commit SHAs. Add
`permissions: {contents: read}` to the `build` job, `if-no-files-found: error` on the
artifact upload, and either restore or delete the commented-out `environment:` block
(`:31-33`) — as it stands it is a trap.

### P1-9 **[DONE]** [VERIFIED] The GUI ignores the selected runner when building the environment

`gui.py:61-68` — `_child_env()` hardcodes `resolve_runtime_env("default")`, and
`RunTab._run` (`gui.py:3111-3115`) injects it regardless of the user's runner choice.
This contradicts `env.py`'s documented contract (`env.py:58-59`), which deliberately
returns the bare parent environment for `native`/`docker`/`podman`.

**Fix.** Pass the selected runner: `_child_env(self.runner_combo.currentText())`.

### P1-10 **[DONE]** [VERIFIED] `--prefer-masters` is unreachable from the GUI

A systematic diff of CLI flags against GUI-emitted flags shows exactly one genuine gap
(`--output` is emitted as `-o`, so it is fine):

- `--prefer-masters` — implemented at `run_metis.py:1096`, documented at `README.md:317`,
  never emitted by `_build_cmd_args` (`gui.py:3048-3090`).

**Resolution — not a parity gap after all.** A checkbox was added and then removed.
Verified empirically: on a standard install the flag rewrites the value the Install tab
already pinned, leaving the file byte-identical during *and* after the run; and for
`--runner docker`/`podman` it patches the host's config while EDPS reads the container's.
It does something only when EDPS was configured outside MTR (typically `--runner
native`). The flag now warns in the two dead cases instead of appearing to work, and is
deliberately kept off the Run tab, which is overwhelmingly used with the `default` runner.

The open design question, deliberately not changed: the Install tab pins
`association_preference` at install time (`gui.py:1282-1284`) with no recorded rationale,
which is what makes the per-run flag redundant. Making it a genuine per-run control means
dropping that hardcoding — which would change reduction results for anyone who re-runs the
Install tab.

### P1-11 **[DONE]** [REPORTED] EDPS server and global config leak on interrupt

`run_metis.py:1486-1529`:

- `_set_association_preference` (`:1490`) mutates `~/.edps/application.properties`, but the
  `try/finally` only starts at `:1512`. A `KeyboardInterrupt` between them — notably during
  the `edps -lw` warm-up at `:1496`, which starts the daemon and can block indefinitely —
  leaves the user's global config permanently patched **and** the daemon running.
- Inside the `finally` (`:1523-1527`), `subprocess.run(..., timeout=15)` runs *before*
  `_restore_association_preference`. If the stop hangs, `TimeoutExpired` propagates out of
  the `finally`, the restore never happens, and the original failure is masked.
- There is no `KeyboardInterrupt`/`signal`/`atexit` handling anywhere in the package.
- The properties file is process-global state in `$HOME`: two concurrent `mtr-cli` runs
  clobber each other, and the second run's `edps -s` kills the first run's server.

**Fix.** Wrap the whole block in a context manager entered *before* the config is touched;
restore first, stop second, and guard the stop in its own `try/except`. Add a
`KeyboardInterrupt` handler that exits 130.

### P1-12 **[DONE]** [REPORTED] `_init_edps`'s `finally` masks the real error

`gui.py:1046-1060` — if `edps` is missing, the `try` raises `FileNotFoundError` and the
`finally`'s stop command raises a **second** one, which replaces the original. The user
sees the stop command's error, not the cause.

**Fix.** Wrap the stop in `try/except Exception: pass`.

### P1-13 **[DONE]** [REPORTED] Archive tab can destroy a running worker

`gui.py:2335, 2364, 2395, 2431, 2591` — `ArchiveTab` stores every worker in a single
`self._worker` slot. `_install_btn` is disabled during a MetisWISE install (`:2332`) but
**"Save & Test" is not**: clicking it mid-install rebinds `self._worker`, dropping the last
reference to a *running* `QThread` → `QThread: Destroyed while thread is still running`
abort. Same shape between Search (`:2395`) and Download (`:2431`).

**Fix.** Keep a set of live workers, or gate every page action behind one busy state.

### P1-14 **[DONE]** [REPORTED] `re.subn` with an interpolated path corrupts EDPS config

`gui.py:1094-1095` with the replacement built at `:1071`. `re.subn` interprets backslashes
and `\g<...>` in the *replacement* string, so a data dir containing `\` yields a corrupted
config or `re.error` — and this runs immediately after MTR renamed the only backup (P0-3).
`props.write_text(text)` at `:1101` is also non-atomic.

**Fix.** Use a lambda replacement (`re.subn(pat, lambda m: replacement, text)`), and write
atomically.

---

## P2 — Structure and maintainability

### P2-1 Split `gui.py` (3367 lines)

Proposed boundaries, with line ranges to move:

```
gitops.py            <- gui.py:94-230      (~140 l, pure, no Qt, already well tested)
ui/theme.py          <- gui.py:237-548     (extract the 136-line QSS f-string to a template)
ui/widgets.py        <- gui.py:555-612, 1293-1334, 3259-3280
workers/base.py      <- NEW CommandWorker: _step/_run/_ensure_pip
                        (gui.py:736-771 and 1203-1229 are VERBATIM duplicates)
workers/install.py   <- gui.py:619-1102
workers/uninstall.py <- gui.py:1109-1281
workers/refs.py      <- gui.py:1341-1366
workers/archive.py   <- gui.py:1700-1956   (5 classes)
tabs/install_tab.py  <- gui.py:1373-1693
tabs/archive/        <- gui.py:1963-2628 split by page:
      setup_page.py    (2027-2136, 2294-2382, 2600-2628)
      query_page.py    (2138-2213, 2386-2434)
      upload_page.py   (2215-2290, 2438-2596)
      archive_tab.py   (container only, ~60 l)
tabs/run_tab.py      <- gui.py:2635-3252   (_build_ui is 285 lines; split into
                        _build_inputs_group / _build_options_group / _build_actions)
main_window.py       <- gui.py:3287-3340
app.py               <- gui.py:3347-3367   (with argparse — see P2-5)
gui.py               <- thin facade re-exporting public names
```

**Migration hazard — read before cutting.** The test suite monkeypatches *module
attributes on `gui`*: `gui._git` (`test_gui.py:667, 1166, 1175, 1230, 1284, …`),
`gui.REPO_ROOT` / `gui.TARGET_B` (`:1036-1037, 1049-1050, 1058-1059`), `gui.InstallWorker`
(`:1594`), `gui._dirty_files` (`:1679, 1689, 1702, 1718, 1734`), `gui._describe_head`
(`:1759`), `gui.SMOKE_TEST` (`:1787`). A facade doing `from .gitops import _git` will
**silently break all of them** — the workers would keep calling `gitops._git`. Either have
the new modules do `from . import gitops` and call `gitops._git(...)` (then update the
tests to patch `gitops._git`), or keep the path aliases in one module everyone dereferences
at call time. `gui.py:36-40` already documents this indirection as deliberate — preserve it.

### P2-2 Split `run_metis.py` (1535 lines)

```
workflows.py  <- run_metis.py:59-231, 253-262, 503-583   (domain tables + inference)
fitsmeta.py   <- run_metis.py:307-346, 424-467
edps.py       <- run_metis.py:234-250, 841-846, 853-878, plus the server lifecycle
                 from 1486-1529 as a context manager
csvslice.py   <- run_metis.py:891-975   (self-contained, already pure)
cli.py        <- run_metis.py:982-1100  (+ main() decomposed per P1-1)
```

This also removes a real dependency cycle: `run_metis.py:1356` imports `archive`, while
`archive.py:837` imports `run_metis` — broken today only because both are function-local.
Five sites do this lazy-import dance (`direct.py:114`, `archive.py:837`, `gui.py:2164`,
`:2450`, `:2509`), which is the clearest structural signal that the tables want a neutral
leaf module.

Better still, replace `_build_sim_script` (`run_metis.py:608-769`, a **161-line Python
source-code generator**) with a real `_sim_driver.py` module in the package, parameterised
via JSON on stdin. That makes it lintable, testable and debuggable with real tracebacks,
and eliminates two escaping bugs at once (see P2-3).

### P2-3 [VERIFIED] Unescaped path interpolation in generated source

`run_metis.py:673` and `:740` interpolate a path **raw** into a single-quoted literal,
while every other value in the generator uses `!r`:

```python
f"    print('Instrument packages not found at {inst_pkgs_path}. Downloading …')",
```

A path containing `'`, `\` or a newline produces a `SyntaxError` in the generated script —
or, with a crafted path, injected statements running in the user's environment. The
existing `test_script_is_valid_python` tests (`tests/test_run_metis.py:1008-1028`) use only
clean fixed paths.

**Fix.** Use `!r` at both sites. **Test.** Property test that `ast.parse` succeeds for
paths containing `'`, `"`, `\`, `\n` and a space.

### P2-4 [VERIFIED] Remove dead code

Four functions in `run_metis.py` have **no production callers** (verified by grep across
`src/`), yet ~250 lines of tests guard them while `main()` has none:

| Function | Lines | Tests guarding it |
|---|---|---|
| `infer_workflow` | 349-413 | `test_run_metis.py:112-333` |
| `known_workflows` | 416-421 | `:334-351` (its docstring claims the GUI uses it; it does not) |
| `collect_tags_from_fits` | 454-467 | `:684-794` |
| `infer_workflow_from_fits` | 470-500 | — |

**Fix.** Delete them and their tests, or — if they are intended as public API — document
that and add `main()` coverage first. Note `infer_workflow_from_fits:492` compares a raw
header value while `scan_fits_inputs:342` normalises via `_normalize_tech`, so the dead
code is also inconsistent with the live code.

### P2-5 Shared option spec between CLI and GUI

The GUI hand-builds argv as string literals (`gui.py:3048-3090`) against
`parse_args` (`run_metis.py:982-1100`). Nothing type-checks this: renaming a flag would
pass every test and break at runtime. Duplicated knowledge:

| Knowledge | CLI | GUI |
|---|---|---|
| Option names | `parse_args` 982-1100 | `_build_cmd_args` 3048-3090 |
| Defaults (`--cores 4`, calib/static on) | 1007, 1013, 1021 | 2792, 2752, 2756, 3193-3197 |
| Output layout (`sim/`, `pipeline/`) | 1226-1230 | `_update_output_info` 2937-2959 |
| Input extensions | `INPUT_EXTS` 56 | 2993-2994 **and** 3017-3020 |
| `--csv-lines` grammar | `_parse_line_range` 894-929 | 3059-3063 |
| EDPS properties patching | 853-872 | `_patch_edps_config` 1062-1107 |
| Runner list | `choices` 1051 | `addItems` 2806 |

**Cheapest immediate win** (do this even without the refactor): a round-trip test.

```python
def test_gui_args_parse_cleanly(qapp):
    tab = _make_run_tab(qapp)
    tab.input_list.addItem("a.yaml")
    parse_args(tab._build_cmd_args())      # must not raise SystemExit
```

Verified that this passes today, so it locks in a currently-correct contract.

### P2-6 Other duplication worth factoring

| What | Sites | Fix |
|---|---|---|
| `_step`/`_run`/`_ensure_pip` — verbatim duplicates | `gui.py:736-771` vs `1203-1229` (3rd variant `1746-1754`) | `workers/base.py` |
| ANSI stripper | `gui.py:766, 1224, 3130` | one `strip_ansi()` |
| Worker start boilerplate | `gui.py:1599, 1678, 2335, 2364, 2395, 2431, 2591` | `_run_worker(...)` — also fixes P1-13 |
| Button construction ×22 | `gui.py:1436-1448, 2064-2068, …, 2899-2914` | `button(text, *, on_click, …)` |
| `[lst.item(i).text() for i in range(lst.count())]` ×8 | `gui.py:2440, 2943, 2996, 3015, 3030, 3068, 3087, 3243` | `list_items(w)` |
| Add/Remove/Clear list column ×2 | `gui.py:2661-2680+2990-3010` vs `2833-2870+3027-3044` | `PathListWidget` |
| `QSettings("METIS","TestRunner")` ×3 | `gui.py:1384, 1987, 2640` | one accessor; also unify key namespaces (`InstallTab` prefixes `install/`, `RunTab` writes bare keys) |
| Log widget construction | `gui.py:1453-1455` vs `2922-2925` | shared factory |
| `_child_env` / `_default_subprocess_env` | `gui.py:61` / `run_metis.py:792` | two names for one 1-line wrapper |

### P2-7 Split `archive.py` (945 lines)

Four unrelated concerns with four different failure modes (pip/network, OS keyring,
ORM/DB, pure graph logic):

```
metiswise_install.py <- archive.py:42-184   (~140 l, pure functions)
archive_config.py    <- archive.py:191-493  (~300 l, security-critical — isolate it)
archive_client.py    <- archive.py:501-769  (~270 l, the only network-touching part)
calib_plan.py        <- archive.py:786-902  (~120 l, PURE — currently held hostage by a
                        module that imports metiswise on use)
```

Keep `archive.py` as a re-export shim for the ~10 `from .archive import …` sites in
`gui.py`. Also delete `REPO_ROOT = paths.data_dir()` (`archive.py:35`) — it is unused, and
because `paths.data_dir()` does `mkdir()`, merely importing `archive` creates a directory
at import time and snapshots `METIS_DATA_DIR` forever.

---

## P3 — Tooling, CI and packaging

### P3-1 **[DONE]** No linter, formatter, type checker or pre-commit

Nothing exists: no `ruff.toml`, `.flake8`, `mypy.ini`, `.pre-commit-config.yaml`,
`.editorconfig`. `pyproject.toml` has only `[tool.hatch.*]` and a two-line
`[tool.pytest.ini_options]`. The codebase already writes `# noqa: F401`
(`tests/test_run_metis.py:701`) — flake8-style suppressions with no linter to honour them.

Verified baseline: **ruff reports 87 issues** (32 auto-fixable), including:

| Rule | Count | Note |
|---|---:|---|
| `I001` unsorted-imports | 24 | auto-fixable |
| `BLE001` blind-except | 15 | see P3-2 |
| `PLW1510` subprocess-run-without-check | 12 | |
| `RUF012` mutable-class-default | 10 | |
| `F821` undefined-name | 1 | **this is P0-1** |
| `F401` unused-import | 2 | incl. `QProgressBar` (P3-6) |

**Recommended config** (ruff replaces black + isort + flake8 in one dependency, which
matters for a single-maintainer project):

```toml
[tool.ruff]
target-version = "py312"
line-length = 110          # current style already exceeds 100; gui.py:18 is 118 chars

[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "B", "SIM", "RUF"]
```

Do **not** run `--fix` across `gui.py` unreviewed. For mypy, start scoped —
`credentials.py`, `paths.py`, `env.py`, `direct.py`, `indexes.py` are small and
well-annotated; `run_metis.py` has 6 annotated defs in 1535 lines, so a whole-package
strict run stalls immediately.

Pre-commit matters unusually much here: every commit goes straight to `main` with no PR
gate, so it is the *only* available pre-merge enforcement point.

### P3-2 **[DONE]** [VERIFIED] Blind excepts hide real failures

15 sites catch bare `Exception` and log only `str(exc)` — no traceback anywhere, so a
`KeyError` in a helper surfaces as `✗ Failed: 'foo'`:

`gui.py:730, 1157, 1165, 1173, 1777, 1839, 1887, 1921, 1954`;
`archive.py:617, 765`; `run_metis.py:331, 444, 488, 1389`.

The FITS-reading ones (`run_metis.py:331, 444, 488`) are worst: a corrupt or
permission-denied frame is indistinguishable from an unrecognised one, with no count or
warning.

**Fix.** Log `traceback.format_exc()` at minimum; narrow the exception types where the
expected failure is known; count and report skipped FITS files.

### P3-3 **[DONE]** CI gaps

`.github/workflows/unit_tests.yaml` — the matrix (`:11-12`) varies only OS; Python is
hardcoded to `"3.12"` at `:20`.

- **Python 3.13 is advertised and completely untested.** `pyproject.toml:11` declares
  `>=3.12,<3.14` and `:22` carries a `Python :: 3.13` classifier. This is the single most
  valuable CI change, because pinned `PyQt6==6.6.0` + 3.13 is exactly the combination most
  likely to break. `fail-fast: false` is already set.
- No coverage measurement, no lint job, no build check (`python -m build`, `twine check`,
  wheel-contents assertion).
- Never tests the installed artifact — always `pip install -e .`.
- Only `mtr --smoke-test` is exercised; `mtr-cli`, `mtr-exec`, `mtr-shell`
  (`pyproject.toml:42-44`) are never invoked. `mtr-cli --help` would have caught P0-1.
- No `concurrency` cancel, no pip caching (PyQt6 + Qt6 wheels are ~100 MB and dominate job
  time), no `permissions: {contents: read}`, no `timeout-minutes`, actions on floating
  major tags.
- No scheduled run — 0.4.5 deliberately *unpinned* `pycpl` and the Install tab resolves
  ESO-mirror packages at runtime, so a weekly cron is the only way to learn upstream broke
  you before a user does.

### P3-4 **[DONE]** Packaging

- **`PyQt6==6.6.0` / `PyQt6-Qt6==6.6.0`** (`pyproject.toml:26-28`). The comment documents
  the symptom ("newer wheels segfault on some Wayland setups") but gives no upstream issue,
  reproducer, affected-platform list or revisit date. `==` freezes every user on a 2023 Qt
  release. Prefer `>=6.6,<6.8` with known-bad versions excluded explicitly, plus a linked
  issue; failing that, add the reference and a revisit marker.
- **`"PyYAML"` has no bound at all** (`:29`) — the only fully unconstrained dependency.
  Add `>=6.0`.
- **`dev = ["pytest", "build", "twine"]`** (`:38`) — missing `pytest-cov`, `astropy`,
  `ruff`, `mypy`. `twine` is unused (publishing uses trusted publishing) unless you adopt
  `twine check`.
- **No `py.typed`** — the package ships meaningful annotations (130 in `gui.py`, fully
  typed `credentials.py`) that no downstream checker can see. One empty file.
- **License metadata is pre-PEP-639** (`:10`, `:14-24`): `license = { text = … }` +
  OSI classifier emits deprecation warnings on newer build front-ends. Modern form is
  `license = "BSD-3-Clause"` + `license-files = ["LICENSE"]`.
- **sdist omits `container/` and `.github/`** (`:57-64`).

**Package data — the examples ship, but by accident.** `[tool.hatch.build.targets.wheel]
packages = ["src/metis_test_runner"]` includes all files under the package dir, so the
`examples/*.yaml` and `*.csv` are in the wheel. But hatchling applies VCS ignore patterns,
and `.gitignore:34-35` (`uv.lock`, `*.log`) and `:38-39` (`*.fits`, `*.fits.gz`) are
**unanchored** — the day an example needs a companion `.fits` fixture it vanishes from the
wheel with no error. Add an explicit `include`, plus a CI wheel-contents assertion.

### P3-5 **[DONE]** [VERIFIED] The examples are unreachable after a pipx install

`README.md:37` tells users to run `mtr-cli examples/LMS_RAD_06.yaml` immediately after
`pipx install metis-test-runner`. After a pipx install there is no `examples/` in the cwd —
the files live in `site-packages/metis_test_runner/examples/`, and **no code anywhere in
`src/` references `examples`, `importlib.resources` or `pkgutil`**. The first command a new
user runs fails. Ten more README commands (`:346, 349, 352, 355, 361, 364, 367, 373, 377,
384`) and the relative links at `:228, 253, 255-257` have the same problem.

**Fix.** Add an accessor and a flag:

```python
def examples_dir() -> Path:
    from importlib.resources import files
    return Path(files("metis_test_runner") / "examples")
```

plus `mtr-cli --examples-dir` / `--copy-examples DIR`, and update the README. Alternatively
drop them from the wheel and keep them repo-only — but then fix the docs.

### P3-6 **[DONE]** [VERIFIED] Progress plumbing is 90 % built and fully dead

`QProgressBar` is imported at `gui.py:24` and **never instantiated** (0 occurrences).
`DownloadWorker.progress` and `UploadWorker.progress` are declared and emitted
(`gui.py:1896, 1910, 1930, 1943`) and **never connected** (0 occurrences). Downloads and
uploads show no progress at all.

**Fix.** Connect the existing signals to a `QProgressBar`. This is the cheapest visible UX
win in the repo.

### P3-7 **[DONE]** Repo hygiene

- `.gitignore` does not list `.pytest_cache/`, `.coverage`, `coverage.xml`, `htmlcov/`,
  `.mypy_cache/`, `.ruff_cache/`. `.pytest_cache/` currently stays untracked only because
  pytest writes its own self-ignoring `.gitignore` inside it — load-bearing accident.
- There is an **uncommitted `.gitignore` change** in the working tree adding `CLAUDE.md`.
  Commit or drop it.
- No `.gitattributes` (no `* text=auto eol=lf`, no `export-ignore`), no `.editorconfig`,
  no `dependabot.yml`.
- Tracked files are otherwise clean: 32 files, no stray artifacts.
- `LICENSE` says `Copyright (c) 2025` while the first commit is 2026-03-21 — worth a look.

### P3-8 **[DONE]** Docs

- **Badges point at the wrong repository.** `README.md:4, 6, 7` use `eiseleb47/MTR`; the
  remote and `pyproject.toml:50-52` are `AstarVienna/MTR`. The CI badge reports a personal
  fork's status.
- No PyPI badge despite being published; `:5` is a static "python 3" shield that doesn't
  reflect `>=3.12,<3.14`.
- **No screenshot** — this is a GUI app whose README spends 90 lines (`:91-182`) describing
  what the GUI looks like.
- **Nowhere records the release procedure or the test command.** Deliberately
  left that way — this is a solo repo, and both a `CONTRIBUTING.md` and a README
  **Development** section were written and then removed at the maintainer's
  request. The release steps instead live in the `verify` job's failure message
  in `publish.yml`, which is where they surface when they are actually needed.
  Run the tests with `QT_QPA_PLATFORM=offscreen pytest tests/`.
  Nothing recorded the release
  procedure (bump `pyproject.toml` → bump `__init__.__version__` → move CHANGELOG
  `Unreleased` → commit → tag `vX.Y.Z` → push), and nothing tells a contributor that
  `QT_QPA_PLATFORM=offscreen` is required for headless tests (that fact exists only in
  `unit_tests.yaml:35` and `tests/conftest.py:6`).
- No `ARCHITECTURE.md`. `gui.py` + `run_metis.py` are 79 % of the source; the README's
  "Repository Layout" (`:413-429`) is one line per file.
- No `SECURITY.md` — notable given the app handles OS-keyring secrets and past commits
  explicitly fixed credential leakage.
- No table of contents at 436 lines; `mtr-gui` (`pyproject.toml:47`) is undocumented.

**`CHANGELOG.md` is the best-maintained artifact in the repo** — every release present,
correctly categorised, explaining *why* not just *what*. Keep that discipline. Minor gaps:
no release dates, no "Keep a Changelog" header link, no compare links, and the
0.4.1/0.2.0/0.1.0 entries disagree with the tag history (P1-7).

---

## P4 — Features worth building

Ordered by value for a tool whose purpose is *pipeline testing*.

1. **Config validation / `--validate`.** The single biggest usability gap. `scan_yaml_inputs`
   (`run_metis.py:265-304`) is the only validation and it is defensive-by-skipping: a
   non-dict top level silently yields zero workflows; a non-string `tech`/`catg`/`mode`
   raises `AttributeError`; `do.catg` is trusted verbatim and never checked against the
   known tag universe, so a typo like `DARK_2RG_RAWW` produces a *successful-looking* run
   that reduced nothing. Today a 2-hour simulation starts before anyone notices. There is
   no jsonschema/pydantic/voluptuous anywhere (verified).
2. **Result diffing / regression comparison.** The most conspicuous absence for a *test
   runner*: no baseline capture, no FITS header/pixel comparison, no QC-parameter
   extraction. `mtr-cli diff <runA> <runB>` over `pipeline/` products is the natural shape.
3. **Structured output (`--json`).** Nothing machine-readable: no run manifest of inputs,
   resolved workflow, target flags, per-stage exit code and duration, product list,
   versions. Would make CI assertions possible. Note every `sys.exit` currently flattens to
   **1**, so CI cannot distinguish bad arguments from a simulation crash from EDPS failing.
4. **A real dry run.** `--csv-to-yaml` is a *translation* mode, not a dry run. There is no
   "print the resolved plan and exit" — and the exact `edps` argv, the hardest thing to
   reproduce by hand, is never printed at all.
5. **Run presets / profiles.** `RunTab` persists exactly one implicit configuration
   (`gui.py:3223-3252`). Users running several instrument modes retype everything.
6. **Cancellation everywhere.** Only the Run tab has Stop. Install, Uninstall, MetisWISE
   install, Save & Test, Search, Download and Upload have no cancel — a 20-minute pip
   install can only be interrupted by killing the app, which per P1-3 is itself unsafe.
7. **Log ergonomics.** No save-to-file, no copy-all, no search, no filtering, no
   follow-tail toggle — and `log_append` has **no cap** (`setMaximumBlockCount` is never
   set on any of the three log views at `gui.py:1453, 2010, 2922`), so a long run grows the
   document unboundedly while forcing auto-scroll. For a tool whose output *is* the
   deliverable, and whose own code tells users to paste logs into bug reports
   (`gui.py:1008-1015`), save-to-file is close to mandatory.
8. **Download cache + parallelism.** `download_file` has no skip-if-present, so
   `--auto-fetch-calibrations` re-pulls every master on every run, once *per sub-workflow*
   — `fetch_missing_calibrations` (`archive.py:932-944`) is strictly serial and dedupes
   nothing. A content-addressed cache under `paths.data_dir()` plus a bounded thread pool
   would be a large win. (Fix the connection leak first — see below.)
9. **Timeouts and resume.** Only one timeout exists in `run_metis.py` (`timeout=15` on the
   EDPS stop). `edps -lw`, the pipeline run and the simulation are unbounded — a wedged
   daemon hangs CI forever. There is also no state file, so a step-2 failure discards a
   completed simulation.
10. **Theme persistence** (`gui.py:3327-3333` never writes to QSettings, so every launch
    resets to dark), window geometry restore, and keyboard shortcuts (there are currently
    **none** — no Ctrl+R, no mnemonics, no accessible names).

---

## P5 — Test coverage gaps

The suite is genuinely good on pure helpers — git helpers, the `_clone_or_update` state
machine, `_patch_edps_config`, workflow inference, FITS classification, `_parse_line_range`.
The gaps cluster in orchestration and lifecycle:

**Highest value, cheapest first:**

1. **`main()` smoke test** (`run_metis.py:1107-1531`, 0 % covered) — would have caught P0-1.
2. **GUI↔CLI argv round-trip** (P2-5) — 4 lines, locks a currently-correct contract, would
   have caught the `--prefer-masters` gap.
3. **`fetch_missing_calibrations`** (`archive.py:905-945`) — **zero tests**, and it is what
   the CLI actually calls. Nothing pins the `items[-1]` selection, the "no items" path, or
   the "download returned None" path.
4. **`TASK_PRODUCTS` ⊆ `WORKFLOW_TASK_CHAIN` invariant** — 3 lines. Manual check found
   `metis_ifu_std_reduce`, `metis_lm_lss_adc_slitloss` and `metis_n_adc_slitloss` have **no**
   `TASK_PRODUCTS` entry and are therefore silently never fetchable (`archive.py:896-897`
   `continue`s past them).
5. **`RunTab` settings round-trip** (`gui.py:3192-3252`) — no test at all.
6. **File-mode assertion on `write_env_cfg`** — would have caught P1-5.
7. **Hostile-password parametrisation of `install_metiswise_command`** — would have caught
   P0-6.
8. **`log_append`'s `\r` terminal semantics** (`gui.py:579-581`) — the subtlest code in the
   helper, zero coverage, despite an explicit comment about the colour-survival invariant.

**Structural problems in the suite:**

- **No test starts a real `QThread`** — workers are exercised by calling `.run()`
  synchronously. Every lifetime bug in P1-3 and P1-13 is therefore structurally
  uncatchable. `pytest-qt`/`qtbot` is not used; `conftest.py` hand-rolls the fixtures.
- **`RunTab`'s entire process layer is untested** (`gui.py:3092-3146`) — `_run`, `_stop`,
  `_on_stdout/stderr/finished`, the `QProcessEnvironment` construction.
- **Dead code is tested; live code is not** (P2-4): ~250 test lines guard four functions
  with zero production callers.
- **Mocks that hide real bugs** (`tests/test_archive.py`): `DataItem.filename == x` returns
  a plain `list`, but the production code's own comments (`archive.py:546-548`) state the
  real return is a lazy `Select` whose `__bool__` **raises**. A regression writing
  `if results:` would pass the suite and explode in production. A ~15-line fake `Select`
  that raises on `__bool__` would pin the documented contract.
- **`importlib.reload(archive)` ~30 times** inside `patch.dict("sys.modules", …)` — heavy,
  order-sensitive, and it resets module globals outside the mock context. A test that
  forgets its trailing reload silently poisons later tests.
- **Non-hermetic archive tests**: `TestArchiveTab` (`test_gui.py:737-749`) constructs a real
  `ArchiveTab`, which calls `metiswise_available()` (a real import) and `read_env_cfg()`
  (reads the developer's real `~/.awe/Environment.cfg`). Results differ by machine.
- **Widgets are created and never destroyed** in most tests; only `MainWindow` tests call
  `.close()`. Leaked top-levels can flake under `offscreen`.
- **Shared QSettings mutation** (`test_gui.py:1637-1649`, `:1740-1753`) with hand-rolled
  cleanup — order-dependent.

---

## Appendix A — Items investigated that did NOT reproduce

Recorded so they are not re-filed.

- **QSettings single-element `QStringList` round-trip.** An audit flagged
  `gui.py:3212, 3216-3220` as iterating a string instead of a list when exactly one input
  file is saved. **Tested empirically on PyQt6 / Qt 6.11.2 with the Linux NativeFormat
  (INI) backend: single-element lists round-trip correctly as `list`.** Not a live bug
  here. Caveat: the project *pins* `PyQt6==6.6.0`, which was not available in this
  environment, so if you support that pin it is worth one 5-line check there. Using
  `s.value(key, [], type=list)` is harmless defensive practice regardless.
- **Cross-thread GUI calls from worker lambdas.** The ubiquitous
  `worker.log.connect(lambda t, c: log_append(...))` pattern is **safe** — PyQt6 queues
  signal→lambda connections to the thread that called `connect()`, verified with a
  headless `QThread` probe. Not a bug.

## Appendix B — Things that are done well (do not "fix" these)

- **`env.py` and `paths.py` are the right abstraction** — single seams for environment and
  path resolution, with docstrings explaining the resolution order. Several findings above
  are about code *bypassing* these seams; the seams themselves should be kept and used more.
- **Credentials never reach `argv`** (`archive.py:85-140`) — deliberately routed through
  `PIP_EXTRA_INDEX_URL`, enforced by a test. Preserve this while fixing P0-6.
- **QSettings no longer stores secrets** — `gui.py:2600-2617` actively scrubs eight legacy
  plaintext keys, and `_save_settings` persists nothing.
- **`CHANGELOG.md`** — consistently maintained, explains rationale, updated in the same
  commit as the feature.
- **Test isolation in `conftest.py`** — redirects `XDG_CONFIG_HOME`/`XDG_DATA_HOME` before
  PyQt6 import, with a comment explaining why `QSettings.setPath()` is insufficient.
- **The `--csv-lines` / `_slice_csv` code** — small, pure, well tested.

---

## Appendix C — Suggested execution order

A dependency-aware sequence, each step independently shippable:

1. **P0-1, P0-2** — CLI argument handling. Add the `main()` smoke test (P1-1 step 1) in the
   same PR; it is what makes the fix verifiable.
2. **P1-6, P3-7** — declare `astropy`, fix `.gitignore`. Unblocks a clean local test run for
   everyone who follows.
3. **P0-3, P0-4** — the two destructive paths. Highest user impact per line changed.
4. **P0-5, P0-6, P1-5** — the credential cluster. Land together; they share test scaffolding.
5. **P0-7** — auto-fetch correctness (needs the P0-5 work to be settled first, same module).
6. **P1-7, P1-8, P3-3** — release integrity: version derivation, tag gate, publish gating,
   3.13 in the matrix. Do before the next release, not after.
7. **P1-2, P1-3, P1-11, P1-12, P1-13** — process and thread lifetime. Related; one campaign.
8. **P3-1** — adopt ruff + pre-commit *after* the above, so the 87 existing findings don't
   bury the real fixes in review noise.
9. **P2-1 … P2-7** — the refactors. Read the P2-1 migration hazard before touching `gui.py`.
10. **P4** — features, once the foundations hold.

**P0-8** (container) is independent of everything else — do it whenever, but decide
repair-vs-delete rather than leaving it broken and documented.
