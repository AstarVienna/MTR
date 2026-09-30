"""Qt-free install / uninstall logic behind the GUI's Install tab.

The Install tab runs :class:`Installer` / :class:`Uninstaller` on a QThread.
Nothing here may import PyQt6: QtGui links libGL/libEGL/libX11, which headless
servers often lack.
"""

from __future__ import annotations

import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import traceback
from collections.abc import Callable
from pathlib import Path

from . import paths
from .env import ensurepip_command_if_needed, resolve_runtime_env
from .indexes import ESO_INDEX, PYCPL_INDEX

#: ``log(text, colour)`` — the GUI passes a signal's ``emit``.
LogFn = Callable[[str, str], None]

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
#
# Module-level aliases computed at import time so tests can monkeypatch them
# and have the call-time ``REPO_ROOT / "..."`` expressions pick that up.

# fmt: off
REPO_ROOT   = paths.data_dir()
TARGET_A    = paths.pipeline_dir()
TARGET_B    = paths.simulations_dir()
REPO_A_URL  = "https://github.com/AstarVienna/METIS_Pipeline.git"
REPO_B_URL  = "https://github.com/AstarVienna/METIS_Simulations.git"
# fmt: on

# (key, label, repo URL, clone target) for the two clones MTR manages.
REPOS = (
    ("pipeline", "METIS_Pipeline", REPO_A_URL, TARGET_A),
    ("simulations", "METIS_Simulations", REPO_B_URL, TARGET_B),
)


# ---------------------------------------------------------------------------
# Safety / logging helpers
# ---------------------------------------------------------------------------


def _assert_safe_to_remove(target: Path) -> None:
    """Refuse to recursively delete anything that isn't plausibly a data dir.

    ``REPO_ROOT`` is ``METIS_DATA_DIR`` verbatim (see paths.py), so a typo or a
    stray ``METIS_DATA_DIR=$HOME`` would otherwise point Uninstall's rmtree at
    the user's home directory.
    """
    resolved = target.resolve()
    if resolved.is_symlink() or not resolved.is_dir():
        raise RuntimeError(f"Refusing to remove {resolved}: not a real directory")
    forbidden = {Path("/"), Path.home().resolve(), Path.cwd().resolve()}
    forbidden |= set(Path.home().resolve().parents)
    if resolved in forbidden or len(resolved.parts) < 3:
        raise RuntimeError(
            f"Refusing to remove {resolved}: this does not look like an MTR "
            "data directory. Check METIS_DATA_DIR."
        )


def _log_exception(log: LogFn, label: str, exc: BaseException) -> None:
    """Report *exc* to *log*, with a traceback for unexpected errors.

    Workers used to log only ``str(exc)``, so a KeyError deep in a helper
    surfaced as ``✗ Failed: 'foo'`` with nothing to debug from. RuntimeError is
    how this code reports *expected* failures with a written-for-humans
    message, so those stay short.
    """
    log(f"\n✗ {label}: {exc}\n", "red")
    if not isinstance(exc, RuntimeError):
        log(traceback.format_exc(), "red")


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------
#
# Two ways to run git in this module:
#   * ``_git`` — captures, never raises, usable from the GUI thread. For probes
#     (is it dirty? what is HEAD? which refs does the remote have?) where a
#     failure is information rather than an error.
#   * ``Installer._run`` — streams to the log and raises on non-zero. For
#     the install steps themselves, where a failure must abort.

# Deliberately stricter than git's own check-ref-format: a pragmatic allowlist
# is easier to reason about than replicating git's rules, and it produces a far
# better message than git's "fatal: couldn't find remote ref".
_REF_OK = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._/+@-]{0,254}\Z")

# Branches hoisted to the top of the ref dropdown, in this order.
_DEFAULT_FIRST = ("main", "master", "develop", "dev")


def _git(args: list[str], cwd: Path | None = None, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run git, capture output, never raise.

    A missing binary, a timeout, or any OSError comes back as returncode 127
    with the reason in ``.stderr``, so callers only ever branch on returncode.
    ``GIT_TERMINAL_PROMPT=0`` guarantees a private/renamed repo can never block
    the caller on an interactive credential prompt.
    """
    env = resolve_runtime_env()
    env["GIT_TERMINAL_PROMPT"] = "0"
    try:
        return subprocess.run(
            ["git", *args],
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return subprocess.CompletedProcess(args, 127, "", str(exc))


def _validate_ref(ref: str | None) -> str:
    """Return the cleaned ref; ``""`` means the remote's default branch.

    Raises ``ValueError`` with a user-facing message on anything else. Argument
    injection is already closed by ``--end-of-options`` at every call site and
    by argv lists (never ``shell=True``); this exists to catch typos early and
    to keep a leading ``-`` out of argv even if a future call site forgets the
    flag.
    """
    ref = (ref or "").strip()
    if not ref:
        return ""
    if not _REF_OK.match(ref) or ".." in ref or ref.endswith((".lock", "/", ".")):
        raise ValueError(
            f"{ref!r} is not a valid branch, tag or commit.\n\n"
            "Use letters, digits and . _ / + @ - with no spaces — for example "
            "'main', 'feature/my-branch', 'v0.4.2', or a full 40-character "
            "commit SHA."
        )
    return ref


def _looks_like_abbrev_sha(ref: str) -> bool:
    """True for a short hex string.

    GitHub serves ``fetch origin <sha>`` only for *full* 40-character SHAs, so
    an abbreviated one is a guaranteed round-trip failure — better to skip
    straight to the full-history fallback than pay for it.
    """
    return bool(re.fullmatch(r"[0-9a-fA-F]{4,39}", ref))


def _same_remote(a: str, b: str) -> bool:
    """Compare remote URLs ignoring a trailing slash and the .git suffix."""

    def norm(u: str) -> str:
        return u.strip().rstrip("/").removesuffix(".git")

    return norm(a) == norm(b)


def _parse_ls_remote(text: str) -> list[str]:
    """Ref names from ``git ls-remote --heads --tags`` output.

    Strips the ``refs/heads/`` / ``refs/tags/`` prefixes, drops the peeled
    ``^{}`` duplicates annotated tags emit, hoists the usual default branches,
    and dedupes globally (a repo can have a branch and a tag of the same name).
    """
    heads: list[str] = []
    tags: list[str] = []
    for line in text.splitlines():
        _sha, _tab, ref = line.partition("\t")
        ref = ref.strip()
        if not ref or ref.endswith("^{}"):
            continue
        if ref.startswith("refs/heads/"):
            heads.append(ref[len("refs/heads/") :])
        elif ref.startswith("refs/tags/"):
            tags.append(ref[len("refs/tags/") :])
    hoisted = [b for b in _DEFAULT_FIRST if b in heads]
    rest = sorted((b for b in heads if b not in hoisted), key=str.lower)
    # Reverse-lexicographic is a good-enough "newest first" for vN.N.N tags.
    ordered = hoisted + rest + sorted(set(tags), key=str.lower, reverse=True)
    return list(dict.fromkeys(ordered))


def _dirty_files(target: Path) -> list[str]:
    """``git status --porcelain`` lines for *target*; [] when clean/not a repo.

    Raises ``RuntimeError`` when git cannot answer (corrupt index, NFS stall)
    so the caller can ask the user rather than assuming a clean tree.
    """
    if not (target / ".git").exists():
        return []
    cp = _git(["-C", str(target), "status", "--porcelain"], timeout=15)
    if cp.returncode != 0:
        raise RuntimeError(cp.stderr.strip() or "git status failed")
    return cp.stdout.splitlines()


def _describe_head(target: Path) -> str:
    """Short human description of what a clone is checked out at.

    e.g. ``main @ d2d257c5 · shallow``, ``v0.4.2 (tag) @ 8a50c604 · modified``,
    ``detached @ 8a50c604``, ``not cloned``.
    """
    if not (target / ".git").exists():
        return "not cloned"
    sha = _git(["-C", str(target), "rev-parse", "--short=8", "HEAD"])
    if sha.returncode != 0:
        return "not a git repository"

    name = _git(["-C", str(target), "symbolic-ref", "--quiet", "--short", "HEAD"])
    if name.returncode == 0:
        label = name.stdout.strip()
    else:
        tag = _git(["-C", str(target), "describe", "--tags", "--exact-match", "HEAD"])
        label = f"{tag.stdout.strip()} (tag)" if tag.returncode == 0 else "detached"

    try:
        dirty = " · modified" if _dirty_files(target) else ""
    except RuntimeError:
        dirty = " · status unknown"
    # Worth surfacing: a shallow clone is why an abbreviated SHA needs a slow
    # full-history fetch.
    shallow = ""
    if _git(["-C", str(target), "rev-parse", "--is-shallow-repository"]).stdout.strip() == "true":
        shallow = " · shallow"
    return f"{label} @ {sha.stdout.strip()}{dirty}{shallow}"


# ---------------------------------------------------------------------------
# Subprocess streaming
# ---------------------------------------------------------------------------


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def strip_ansi(text: str) -> str:
    """Remove ANSI colour/cursor escapes from subprocess output."""
    return _ANSI_RE.sub("", text)


def stream_subprocess(
    cmd: list,
    *,
    on_line,
    cwd: Path | None = None,
    stdin_text: str | None = None,
    timeout: int = 300,
    env: dict[str, str] | None = None,
) -> None:
    """Run *cmd*, stream its output to *on_line*, and enforce *timeout*.

    Raises ``RuntimeError`` on a non-zero exit and ``TimeoutError`` if the
    deadline expires.

    The timeout is enforced by a watchdog rather than ``proc.wait(timeout=…)``:
    draining ``proc.stdout`` blocks until EOF, so by the time ``wait`` is
    reached the process has always already exited and the timeout could never
    fire. A hung pip download or a ``git fetch`` against a dead mirror used to
    hang the worker thread forever, with no cancel path.

    The child gets its own session so the watchdog can kill the whole process
    group — pip and git spawn their own children, which a bare ``proc.kill()``
    would orphan.
    """
    on_line(f"$ {' '.join(str(c) for c in cmd)}\n", "")
    timed_out = threading.Event()

    with subprocess.Popen(
        [str(c) for c in cmd],
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE if stdin_text is not None else subprocess.DEVNULL,
        text=True,
        env=env if env is not None else resolve_runtime_env(),
        start_new_session=True,
    ) as proc:

        def _fire() -> None:
            timed_out.set()
            _kill_process_group(proc)

        watchdog = threading.Timer(timeout, _fire)
        watchdog.start()
        try:
            if stdin_text is not None:
                try:
                    proc.stdin.write(stdin_text)
                    proc.stdin.flush()
                except BrokenPipeError:
                    pass
                finally:
                    try:
                        proc.stdin.close()
                    except BrokenPipeError:
                        pass
            for line in proc.stdout:
                on_line(strip_ansi(line), "")
            proc.wait()
        finally:
            watchdog.cancel()
            if proc.poll() is None:  # loop exited early (e.g. an error)
                _kill_process_group(proc)

    if timed_out.is_set():
        raise TimeoutError(f"Command timed out after {timeout}s: {' '.join(str(c) for c in cmd)}")
    if proc.returncode not in (0, None):
        raise RuntimeError(f"Command exited {proc.returncode}: {' '.join(str(c) for c in cmd)}")


def _kill_process_group(proc: subprocess.Popen) -> None:
    """Kill *proc* and any children it spawned; never raises."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------


class Installer:
    """Executes all install steps sequentially, reporting through *log*."""

    # Pipeline dependencies installed from the ESO/ivh mirrors, as pip
    # requirement strings (cf. Uninstaller.PIPELINE_PACKAGES, which lists
    # bare distribution names).
    # TODO: ``pycpl`` is deliberately UNPINNED only while ivh's index churns —
    # re-pin it once that settles (0.4.4's 1.0.4.post6 pin was withdrawn there).
    PIP_REQUIREMENTS = [
        "pycpl",
        "edps",
        "pyesorex",
        "adari_core",
        "scopesim==0.11.3",
        "scopesim_templates==0.8.1",
    ]

    @classmethod
    def _pip_deps_command(cls) -> list[str]:
        """``pip install`` argv for the ESO-mirror pipeline dependencies."""
        # --upgrade is what makes the unpinned requirements actually track the
        # newest release: a bare ``pip install pycpl`` leaves an already-installed
        # older pycpl in place ("Requirement already satisfied").
        return [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--extra-index-url",
            PYCPL_INDEX,
            "--extra-index-url",
            ESO_INDEX,
            *cls.PIP_REQUIREMENTS,
        ]

    # ── public ──────────────────────────────────────────────────────────────

    def __init__(
        self,
        refs: dict[Path, str] | None = None,
        force: set[Path] | None = None,
        log: LogFn | None = None,
    ) -> None:
        """*refs* maps a clone target to a branch/tag/commit ("" = default).

        *force* is the set of targets whose local modifications the user has
        explicitly agreed to discard.  A **set**, not a single flag: confirming
        a reset of one clone must never authorise destroying the other.
        """
        self._refs = refs or {}
        self._force = force or set()
        self._log: LogFn = log or (lambda _text, _colour: None)

    def run(self) -> bool:
        """Run every step; return True on success. Failures are logged, not raised."""
        try:
            self._step(f"Cloning / updating METIS_Pipeline  →  {TARGET_A}")
            self._clone_or_update(REPO_A_URL, TARGET_A, self._refs.get(TARGET_A, ""), TARGET_A in self._force)

            self._step(f"Cloning / updating METIS_Simulations  →  {TARGET_B}")
            self._clone_or_update(REPO_B_URL, TARGET_B, self._refs.get(TARGET_B, ""), TARGET_B in self._force)

            self._check_layout()

            self._ensure_pip()

            self._step("Installing pipeline Python dependencies via pip…")
            recipe_dir = str(TARGET_A / "metisp" / "pyrecipes") + "/"
            os.environ["PYCPL_RECIPE_DIR"] = recipe_dir
            os.environ["PYESOREX_PLUGIN_DIR"] = recipe_dir
            # ESO-mirror dependencies are installed into the same interpreter
            # that hosts MTR (sys.executable points at pipx's isolated venv or
            # whatever venv the user installed MTR into).
            self._run(self._pip_deps_command(), cwd=REPO_ROOT)

            self._step("Installing pymetis (editable)…")
            # metiswise (Archive tab) depends on ``pymetis`` (``eso-pymetis`` in
            # 0.0.4), which is just the pip build of the clone above.
            # Install the clone editable with --no-deps so that:
            #   (a) ``pymetis`` is importable in-process (metiswise imports it), and
            #   (b) the Archive-tab metiswise install finds ``pymetis`` already
            #       satisfied — avoiding a 2nd pymetis copy and the clash between
            #       pymetis's pycpl==1.0.3.post11 pin and the newer pycpl the
            #       step above just resolved.
            # --no-deps keeps the pycpl/edps/pyesorex versions installed above
            # authoritative.
            # TODO: remove/revisit if metiswise drops the pymetis dependency
            # or pymetis stops pinning pycpl.
            self._run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--editable",
                    str(TARGET_A / "metisp" / "pymetis"),
                    "--no-deps",
                ],
                cwd=REPO_ROOT,
            )

            self._step("Installing METIS_Simulations (editable)…")
            # METIS_Simulations is cloned at install time (above) and pip-
            # installed editable so changes to the working tree are picked up
            # without a re-install.
            self._run(
                [sys.executable, "-m", "pip", "install", "--editable", str(TARGET_B)],
                cwd=REPO_ROOT,
            )

            self._step("Checking for existing EDPS configuration…")
            self._backup_edps_config()

            self._step("Initialising EDPS…")
            self._init_edps()

            self._step("Patching ~/.edps/application.properties…")
            self._patch_edps_config()

            self._log("\n✓ Installation complete.\n", "green")
            return True
        except Exception as exc:
            _log_exception(self._log, "Failed", exc)
            return False

    # ── private helpers ──────────────────────────────────────────────────────

    def _step(self, msg: str) -> None:
        self._log(f"\n── {msg}\n", "cyan")

    def _ensure_pip(self) -> None:
        """Bootstrap pip into MTR's interpreter if it is missing (pipx venvs)."""
        boot = ensurepip_command_if_needed()
        if boot:
            self._step("Bootstrapping pip (pipx app venvs ship without it)…")
            self._run(boot)

    def _run(
        self, cmd: list, cwd: Path | None = None, stdin_text: str | None = None, timeout: int = 300
    ) -> None:
        stream_subprocess(
            cmd,
            on_line=self._log,
            cwd=cwd,
            stdin_text=stdin_text,
            timeout=timeout,
        )

    def _clone_or_update(self, url: str, target: Path, ref: str = "", force: bool = False) -> None:
        """Put *target* on *ref* ("" = the remote's default branch).

        *force* permits discarding uncommitted local changes; the GUI collects
        that confirmation per-repo before the worker starts, and this method
        re-checks rather than trusting it, so it stays safe to call directly.
        """
        try:
            ref = _validate_ref(ref)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc

        # .git can be a directory (normal clone) OR a file pointing at
        # .git/modules/<name>/ (submodule or worktree checkout); both are
        # valid git repos.
        is_repo = (target / ".git").exists()

        # Hoisted above the ref dispatch: the pinned path runs `git init`,
        # which would otherwise happily initialise over a non-empty foreign
        # directory. If the dir is empty (a common leftover from an aborted
        # install) we can safely use it. Otherwise refuse — silently skipping
        # would leave the rest of the install referencing a bad checkout.
        if not is_repo and target.is_dir() and any(target.iterdir()):
            raise RuntimeError(
                f"{target} exists but is not a git repo and is not empty. "
                f"Remove or rename it and re-run the install."
            )

        if ref:
            self._checkout_ref(url, target, ref, force)
        elif is_repo:
            self._update_default_branch(url, target, force)
        else:
            self._run(["git", "clone", "--depth", "1", url, str(target)])

        self._log_head(target)

    # ── git plumbing ─────────────────────────────────────────────────────────

    def _checkout_ref(self, url: str, target: Path, ref: str, force: bool) -> None:
        """Move *target* onto *ref*, cloning from scratch if need be."""
        if not (target / ".git").exists():
            # `init` + `remote add` + `fetch <ref>` reaches an arbitrary commit,
            # which `git clone --branch` cannot; it also collapses the "no
            # clone yet" and "clone exists" cases into one path.
            self._run(["git", "init", str(target)])
            self._run(["git", "-C", str(target), "remote", "add", "origin", url])
        else:
            self._check_origin(target, url)

        # An abbreviated SHA cannot be fetched by name, so don't pay for a
        # round trip that is guaranteed to fail.
        ok = not _looks_like_abbrev_sha(ref) and self._try_fetch(target, ref, depth=1)

        if ok:
            # FETCH_HEAD is only meaningful after a *successful* fetch (a failed
            # one truncates it), so the checkout happens here and nowhere else.
            want = _git(["-C", str(target), "rev-parse", "FETCH_HEAD"]).stdout.strip()
            if self._already_at(target, ref, want):
                return
            self._make_room(target, force)
            if self._has_remote_branch(target, ref):
                # A branch fetch also creates refs/remotes/origin/<ref>, so this
                # tells branch from tag/commit with no extra network call.
                self._run(["git", "-C", str(target), "checkout", "-f", "-B", ref, "FETCH_HEAD"])
                _git(["-C", str(target), "branch", "--set-upstream-to", f"origin/{ref}", ref])
            else:
                self._run(["git", "-C", str(target), "checkout", "-f", "--detach", "FETCH_HEAD"])
            return

        # Fallback: an abbreviated SHA, or a commit that is not at any ref tip.
        # Only a full-history fetch can resolve those locally — and only a hex
        # commit id ever needs one, so a mistyped branch name fails immediately
        # instead of downloading the whole history first.
        if not re.fullmatch(r"[0-9a-fA-F]{4,40}", ref):
            raise RuntimeError(
                f"'{ref}' is not a branch or tag in {url}. Check the spelling, "
                f"or use the ↻ button to reload the ref list."
            )
        self._log(
            f"'{ref}' cannot be fetched directly — falling back to a full "
            f"history fetch (slow; paste the full 40-character SHA to avoid "
            f"this).\n",
            "yellow",
        )
        self._deepen(target)
        self._run(["git", "-C", str(target), "fetch", "--tags", "--force", "origin"], timeout=900)
        self._checkout_local(target, url, ref, force)

    def _update_default_branch(self, url: str, target: Path, force: bool) -> None:
        """Blank ref: fast-forward the checked-out branch, as MTR always has."""
        self._run(["git", "-C", str(target), "fetch", "--all", "--prune"])

        branch = self._current_branch(target)
        if branch is None:
            # Detached HEAD left behind by an earlier pinned install. There is
            # no upstream to fast-forward, and blank means "back to normal".
            default = self._remote_default_branch(url) or "HEAD"
            self._log(
                f"Clone is on a detached HEAD; returning to the remote's default branch ({default}).\n",
                "yellow",
            )
            self._checkout_ref(url, target, default, force)
            return

        cp = _git(["-C", str(target), "pull", "--ff-only"])
        self._log(cp.stdout + cp.stderr, "")
        if cp.returncode == 0:
            return
        if not force:
            raise RuntimeError(
                f"'git pull --ff-only' failed in {target} (see above). If you "
                f"have local commits, push or drop them; if you have local "
                f"edits, re-run the install and confirm the reset when asked."
            )
        self._make_room(target, force)
        self._run(["git", "-C", str(target), "pull", "--ff-only"])

    def _make_room(self, target: Path, force: bool) -> None:
        """Discard local modifications so a checkout can move HEAD."""
        if not (target / ".git").exists() or not _dirty_files(target):
            return
        if not force:
            raise RuntimeError(
                f"{target} has uncommitted changes and the reset was not "
                f"confirmed. Re-run the install and confirm when asked."
            )
        self._run(["git", "-C", str(target), "reset", "--hard"])
        # No -x: gitignored build artefacts, simulation *.fits output and
        # inst_pkgs/ are user data and must survive. Single -f also makes git
        # refuse to recurse into a nested repository.
        self._run(["git", "-C", str(target), "clean", "-fd"])

    def _try_fetch(self, target: Path, ref: str, depth: int = 1) -> bool:
        """Fetch exactly *ref*; return False instead of raising on failure."""
        args = ["-C", str(target), "fetch"]
        if depth:
            args += ["--depth", str(depth)]
        args += ["--end-of-options", "origin", ref]
        self._log(f"$ git {' '.join(args)}\n", "")
        cp = _git(args, timeout=600)
        self._log(cp.stdout + cp.stderr, "")
        return cp.returncode == 0

    def _deepen(self, target: Path) -> None:
        """Un-shallow *target*, but only if it actually is shallow."""
        # `fetch --unshallow` errors out on an already-complete repo, and a
        # freshly `git init`ed one is not shallow either.
        if _git(["-C", str(target), "rev-parse", "--is-shallow-repository"]).stdout.strip() == "true":
            self._run(["git", "-C", str(target), "fetch", "--unshallow", "origin"], timeout=900)

    def _has_remote_branch(self, target: Path, ref: str) -> bool:
        return (
            _git(
                ["-C", str(target), "rev-parse", "--verify", "--quiet", f"refs/remotes/origin/{ref}"]
            ).returncode
            == 0
        )

    def _already_at(self, target: Path, ref: str, want: str) -> bool:
        """True when the checkout already matches, so nothing need be touched."""
        if not want:
            return False
        head = _git(["-C", str(target), "rev-parse", "HEAD"]).stdout.strip()
        if head != want:
            return False
        branch = self._current_branch(target)
        if self._has_remote_branch(target, ref) and branch != ref:
            return False
        self._log(f"Already at {want[:8]}; working tree left untouched.\n", "")
        return True

    def _checkout_local(self, target: Path, url: str, ref: str, force: bool) -> None:
        """Check out a ref that is already present in the local object store."""
        # ^{commit} peels annotated tags and rejects a ref naming a tree/blob.
        cp = _git(
            ["-C", str(target), "rev-parse", "--verify", "--quiet", "--end-of-options", f"{ref}^{{commit}}"]
        )
        if cp.returncode != 0:
            raise RuntimeError(
                f"'{ref}' is not a branch, tag or commit in {url}. Check the "
                f"spelling, or use the ↻ button to reload the ref list."
            )
        want = cp.stdout.strip()
        if self._already_at(target, ref, want):
            return
        self._make_room(target, force)
        if self._has_remote_branch(target, ref):
            self._run(["git", "-C", str(target), "checkout", "-f", "-B", ref, f"origin/{ref}"])
        else:
            self._run(["git", "-C", str(target), "checkout", "-f", "--detach", want])

    def _check_origin(self, target: Path, url: str) -> None:
        """Warn — never fail — when the clone points somewhere unexpected.

        Developers on these repos legitimately point the clone at their own
        fork, which is the whole reason this feature exists, so a mismatch is
        reported and honoured rather than "corrected".
        """
        cp = _git(["-C", str(target), "remote", "get-url", "origin"])
        if cp.returncode != 0:
            self._run(["git", "-C", str(target), "remote", "add", "origin", url])
            return
        have = cp.stdout.strip()
        if not _same_remote(have, url):
            self._log(
                f"⚠ {target.name}: origin is {have}, not {url}. Fetching the "
                f"requested ref from that remote instead.\n",
                "yellow",
            )

    def _current_branch(self, target: Path) -> str | None:
        """Checked-out branch name, or None when HEAD is detached."""
        cp = _git(["-C", str(target), "symbolic-ref", "--quiet", "--short", "HEAD"])
        return cp.stdout.strip() if cp.returncode == 0 else None

    def _remote_default_branch(self, url: str) -> str | None:
        cp = _git(["ls-remote", "--symref", "--end-of-options", url, "HEAD"], timeout=20)
        for line in cp.stdout.splitlines():
            if line.startswith("ref: refs/heads/"):
                return line[len("ref: refs/heads/") :].split("\t")[0].strip()
        return None

    def _log_head(self, target: Path) -> None:
        """Log exactly which commit landed, so it can be quoted in a bug report."""
        if not (target / ".git").exists():
            return
        self._log(f"→ {target.name}: {_describe_head(target)}\n", "green")
        cp = _git(["-C", str(target), "log", "-1", "--format=%H  %cs  %s"])
        if cp.returncode == 0:
            self._log(f"   {cp.stdout.strip()}\n", "")

    def _check_layout(self) -> None:
        """Fail early when the selected refs predate the current repo layout.

        Without this, an old ref sails through checkout and then either dies
        inside `pip install --editable` with an opaque backend traceback, or —
        worse — silently yields a pipeline with zero recipes because
        PYCPL_RECIPE_DIR points at a directory that does not exist.
        """
        for needed in (
            TARGET_A / "metisp" / "pymetis" / "pyproject.toml",
            TARGET_A / "metisp" / "pyrecipes",
            TARGET_B / "pyproject.toml",
        ):
            if not needed.exists():
                raise RuntimeError(
                    f"{needed} does not exist at the selected ref. That ref "
                    f"probably predates the current repository layout — pick "
                    f"a newer one."
                )

    def _backup_edps_config(self) -> None:
        """Back up an existing application.properties, once.

        Only the *first* backup is the user's own file: on a re-install the
        file in place is MTR's own, so overwriting the backup with it would
        destroy the original for good (and Uninstall would then "restore"
        MTR's config as if it were theirs).
        """
        props = Path.home() / ".edps" / "application.properties"
        if not props.exists():
            return
        backup = props.with_name("application.properties_backup")
        if backup.exists():
            props.unlink()
            self._log(
                f"{backup} already exists — keeping the original backup and discarding the current config\n",
                "yellow",
            )
        else:
            props.rename(backup)
            self._log(
                f"Existing {props} found — backed up to {backup}\n",
                "yellow",
            )

    def _init_edps(self) -> None:
        """Run edps once to generate ~/.edps/application.properties, then stop it.

        EDPS prompts for a bookkeeping directory on first run; we send a newline
        to accept the default.  After edps daemonises the process exits, then we
        issue -s to stop the background server.
        """
        edps_bin = Path(sys.executable).parent / "edps"
        base = [str(edps_bin), "-P", "4444"]
        try:
            self._run(base, cwd=REPO_ROOT, stdin_text="\n", timeout=60)
        finally:
            # Guarded: if edps is missing, the try raises FileNotFoundError and
            # an unguarded stop here would raise a *second* one that replaces
            # it — the user would see the stop command's error, not the cause.
            try:
                subprocess.run(
                    base + ["-s"],
                    cwd=str(REPO_ROOT),
                    capture_output=True,
                    timeout=15,
                    env=resolve_runtime_env(),
                )
            except Exception as exc:
                self._log(f"(could not stop the EDPS server: {exc})\n", "yellow")

    def _patch_edps_config(self) -> None:
        props = Path.home() / ".edps" / "application.properties"
        if not props.exists():
            raise RuntimeError(f"{props} not found — did EDPS initialise correctly?")
        text = props.read_text()
        # fmt: off
        patches = {
            "port":         (r"^port=.*",         "port=4444"),
            "workflow_dir": (r"^workflow_dir=.*", f"workflow_dir={TARGET_A}/metisp/workflows"),
            "esorex_path":  (r"^esorex_path=.*",  "esorex_path=pyesorex"),
            "association_preference": (
                r"^association_preference=.*",
                "association_preference=master_per_quality_level",
            ),
            "categories": (r"^categories=.*", "categories=.*"),
            "pattern": (
                r"^pattern=.*",
                "pattern=$TASK/$TIMESTAMP/$object$_$pro.catg$.$EXT",
            ),
            # Hardlink products into the per-run output dir instead of copying
            # them, so they don't consume disk twice (once in the EDPS working
            # store ~/EDPS_data, once under the run's output folder). Requires
            # both to be on the same filesystem (true by default — both under
            # $HOME); use "symlink" instead if the output dir is on another mount.
            "mode": (r"^mode=.*", "mode=link"),
            # Wipe EDPS bookkeeping on every server startup. Without this, a
            # stale db.json entry whose on-disk outputs have been removed will
            # collide with a fresh submission's deterministic job UUID and make
            # EDPS short-circuit the run with a FileNotFoundError.
            "truncate": (r"^truncate=.*", "truncate=True"),
        }
        # fmt: on
        for key, (pattern, replacement) in patches.items():
            # A *function* replacement, because re.subn interprets backslashes
            # and \g<...> in a string replacement: a data dir containing a
            # backslash would corrupt the config or raise re.error. The paths
            # here come from METIS_DATA_DIR, which is arbitrary user input.
            text, count = re.subn(
                pattern,
                lambda _m, r=replacement: r,
                text,
                flags=re.MULTILINE,
            )
            if count == 0:
                raise RuntimeError(
                    f"{props} has no '{key}=' line to patch — EDPS config "
                    f"format may have changed; re-run EDPS initialisation."
                )
        paths.write_text_atomic(props, text)
        self._log(f"Patched {props}\n", "")


# ---------------------------------------------------------------------------
# Uninstall
# ---------------------------------------------------------------------------


class Uninstaller:
    """Reverses every change made by Installer (and the Archive-tab
    MetisWISE install).

    Steps: pip-uninstall the installed packages, delete the whole user data
    directory, restore or remove the EDPS configuration, and clear the stored
    archive credentials from the OS keyring.

    Each step is isolated in its own ``try/except`` so a single failure (e.g. a
    keyring backend that is unavailable, or a directory that is busy) is logged
    but does not abort the remaining steps.  ``run()`` returns False if any
    step failed.
    """

    # Top-level packages the Install tab installs: the pipeline deps plus the
    # two editable installs, by their distribution names (the pymetis clone
    # registers as ``pymetis``, or ``eso-pymetis`` for refs before 2026-07-13;
    # METIS_Simulations as ``metis_simulations``).
    PIPELINE_PACKAGES = [
        "pycpl",
        "edps",
        "pyesorex",
        "adari_core",
        "scopesim",
        "scopesim_templates",
        "pymetis",
        "eso-pymetis",
        "metis_simulations",
    ]

    def __init__(self, log: LogFn | None = None) -> None:
        self._log: LogFn = log or (lambda _text, _colour: None)

    def run(self) -> bool:
        """Run every step; return False if any of them failed."""
        from . import credentials as credstore

        # _METISWISE_RUNTIME_DEPS is the single source of truth for what the
        # Archive tab installs; reuse it so the two lists never drift apart.
        from .archive import _METISWISE_RUNTIME_DEPS

        ok = True

        # ── pip uninstall ─────────────────────────────────────────────────
        self._step("Uninstalling Python packages via pip…")
        self._log(
            "Only the explicitly-installed packages are removed; their "
            "transitive sub-dependencies are left in place.\n",
            "yellow",
        )
        packages = [*self.PIPELINE_PACKAGES, "metiswise", *_METISWISE_RUNTIME_DEPS]
        try:
            # pipx venvs have no pip; bootstrap it so the uninstall can run
            # (the packages live in the venv even when pip itself is absent).
            self._ensure_pip()
            # pip exits 0 for not-installed names ("WARNING: Skipping …"), so a
            # single call is safe whether or not MetisWISE was ever installed.
            self._run([sys.executable, "-m", "pip", "uninstall", "-y", *packages])
        except Exception as exc:
            ok = False
            self._log(f"✗ pip uninstall failed: {exc}\n", "red")

        # ── delete the user data directory ────────────────────────────────
        self._step(f"Removing the METIS data directory  →  {REPO_ROOT}")
        try:
            self._remove_data_dir()
        except Exception as exc:
            ok = False
            self._log(f"✗ Failed to remove data directory: {exc}\n", "red")

        # ── restore / remove EDPS configuration ───────────────────────────
        self._step("Cleaning up EDPS configuration…")
        try:
            self._cleanup_edps()
        except Exception as exc:
            ok = False
            self._log(f"✗ EDPS cleanup failed: {exc}\n", "red")

        # ── clear keyring credentials ─────────────────────────────────────
        self._step("Clearing stored archive credentials…")
        for label, deleter in (
            ("OmegaCEN pip", credstore.delete_pip_credentials),
            ("archive DB",   credstore.delete_db_credentials),
        ):  # fmt: skip
            try:
                deleter()
                self._log(f"Removed {label} credentials from the keyring.\n", "")
            except credstore.CredentialsUnavailable as exc:
                # A missing keyring backend is not fatal — there is simply
                # nothing persisted to clear.
                self._log(
                    f"Could not clear {label} credentials: {exc}\n",
                    "yellow",
                )

        if ok:
            self._log("\n✓ Uninstall complete.\n", "green")
        else:
            self._log(
                "\n⚠ Uninstall finished with errors (see above).\n",
                "yellow",
            )
        return ok

    # ── private helpers ───────────────────────────────────────────────────

    def _step(self, msg: str) -> None:
        self._log(f"\n── {msg}\n", "cyan")

    def _ensure_pip(self) -> None:
        """Bootstrap pip into MTR's interpreter if it is missing (pipx venvs)."""
        boot = ensurepip_command_if_needed()
        if boot:
            self._step("Bootstrapping pip (pipx app venvs ship without it)…")
            self._run(boot)

    def _run(self, cmd: list, timeout: int = 300) -> None:
        stream_subprocess(cmd, on_line=self._log, timeout=timeout)

    def _remove_data_dir(self) -> None:
        """Delete the whole user data dir, plus an externally-relocated
        simulations clone if one exists outside the data dir."""
        targets = [REPO_ROOT]
        # If METIS_SIMULATIONS_DIR points outside the data dir, removing
        # REPO_ROOT won't catch the clone — remove it explicitly.
        if TARGET_B != REPO_ROOT and REPO_ROOT not in TARGET_B.parents:
            targets.append(TARGET_B)
        for target in targets:
            if not target.exists():
                self._log(f"{target} does not exist — nothing to remove.\n", "")
                continue
            try:
                _assert_safe_to_remove(target)
            except RuntimeError as exc:
                self._log(f"✗ {exc}\n", "red")
                continue
            failures: list[str] = []
            shutil.rmtree(
                target,
                onexc=lambda _f, path, exc, _acc=failures: _acc.append(f"{path}: {exc}"),
            )
            if failures:
                self._log(
                    f"✗ Could not fully remove {target} ({len(failures)} item(s) left):\n",
                    "red",
                )
                for line in failures[:10]:
                    self._log(f"    {line}\n", "red")
            else:
                self._log(f"Removed {target}\n", "")

    def _cleanup_edps(self) -> None:
        """If the install backed up a pre-existing config, restore it; otherwise
        the install created the whole EDPS state, so remove it entirely
        (config dir + the base_dir bookkeeping/data directory)."""
        edps_dir = Path.home() / ".edps"
        props = edps_dir / "application.properties"
        backup = props.with_name("application.properties_backup")
        if backup.exists():
            if props.exists():
                props.unlink()
            backup.rename(props)
            self._log(f"Restored original {props} from backup.\n", "green")
            return
        # No backup → install owns everything here. Resolve the bookkeeping dir
        # from the config *before* deleting it.
        base_dir = self._edps_base_dir(props)
        for target in (edps_dir, base_dir):
            if target.exists():
                shutil.rmtree(target)
                self._log(f"Removed {target}\n", "")
            else:
                self._log(f"{target} does not exist — nothing to remove.\n", "")

    @staticmethod
    def _edps_base_dir(props: Path) -> Path:
        """Read ``base_dir=`` from the EDPS config; fall back to ~/EDPS_data."""
        default = Path.home() / "EDPS_data"
        if not props.exists():
            return default
        for line in props.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith("base_dir="):
                value = stripped.split("=", 1)[1].strip()
                if value:
                    return Path(value).expanduser()
        return default


def remote_refs(url: str) -> list[str]:
    """Branches and tags of *url* (see ``_parse_ls_remote``).

    Raises ``RuntimeError`` carrying git's last stderr line when the remote is
    unreachable, so callers can report "offline?" instead of an empty list.
    """
    cp = _git(["ls-remote", "--heads", "--tags", "--end-of-options", url], timeout=20)
    if cp.returncode != 0:
        raise RuntimeError((cp.stderr.strip().splitlines() or ["git ls-remote failed"])[-1])
    return _parse_ls_remote(cp.stdout)


# ---------------------------------------------------------------------------
# Confirmation texts for the Install tab's dialogs
# ---------------------------------------------------------------------------


def discard_summary(label: str, target: Path, entries: list) -> str:
    modified = [e for e in entries if not e.startswith("?")]
    untracked = [e for e in entries if e.startswith("?")]
    parts = [f"{label} ({target}) has uncommitted changes.\n"]
    for title, group in (("Will be reverted:", modified), ("Will be deleted:", untracked)):
        if not group:
            continue
        parts.append(title)
        parts.extend(f"  {e}" for e in group[:20])
        if len(group) > 20:
            parts.append(f"  … and {len(group) - 20} more")
        parts.append("")
    parts.append(
        "Ignored build artefacts (__pycache__, build/, *.fits, inst_pkgs/) "
        "are kept.\n"
        "These changes are discarded only if the checkout has to move.\n\n"
        "Continue?"
    )
    return "\n".join(parts)


def uninstall_summary() -> str:
    return (
        "This will permanently:\n"
        "• pip-uninstall all pipeline and MetisWISE packages\n"
        f"• delete the entire data directory ({REPO_ROOT})\n"
        "• restore or remove the EDPS configuration\n"
        "• delete stored archive credentials from the OS keyring\n\n"
        "Transitive sub-dependencies are not removed. Continue?"
    )
