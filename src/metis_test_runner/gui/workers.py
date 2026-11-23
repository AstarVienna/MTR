"""WorkerHost and the background QThread workers."""

# PyQt6 stubs type most widget getters as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

import os
from collections.abc import Callable
from pathlib import Path

from PyQt6.QtCore import (
    QThread,
    pyqtSignal,
)
from PyQt6.QtWidgets import (
    QMessageBox,
)

from .. import update
from ..env import ensurepip_command_if_needed
from ..installer import (
    Cancelled,
    Installer,
    Uninstaller,
    _describe_head,
    _dirty_files,
    _log_exception,
    remote_refs,
    stream_subprocess,
)


class WorkerHost:
    """Mixin for tabs that start QThread workers.

    Tracks every live worker so that (a) a second action cannot drop the last
    reference to a running QThread — which aborts with "QThread: Destroyed
    while thread is still running" — and (b) the window can wait for them on
    close instead of tearing the tab out from under them.
    """

    #: How long to wait for a worker to notice an interruption request.
    WORKER_STOP_MS = 5_000

    #: Installed by MainWindow; see jobs_blocking().
    _jobs_blocking_hook: "Callable[[WorkerHost, str], list[str]] | None" = None

    @property
    def _workers(self) -> set:
        # Created lazily so subclasses need no __init__ cooperation.
        if not hasattr(self, "_worker_set"):
            self._worker_set: set = set()
        return self._worker_set

    def track_worker[W: QThread](self, worker: W) -> W:
        """Register *worker* and drop it again once it finishes."""
        self._workers.add(worker)
        worker.finished.connect(lambda w=worker: self._workers.discard(w))
        worker.finished.connect(worker.deleteLater)
        return worker

    def live_workers(self) -> list:
        return [w for w in self._workers if w.isRunning()]

    def busy(self) -> bool:
        return bool(self.live_workers())

    def jobs_blocking(self, action: str) -> list[str]:
        """Jobs in *other* tabs that must finish before *action* may start.

        MainWindow installs the hook; a tab used on its own has none.
        """
        hook = self._jobs_blocking_hook
        return hook(self, action) if hook else []

    def reject_if_blocked(self, action: str) -> bool:
        """True (and explains why) if another tab's job blocks *action*."""
        jobs = self.jobs_blocking(action)
        if not jobs:
            return False
        QMessageBox.information(
            self,  # pyright: ignore[reportArgumentType]  # mixed into QWidget subclasses
            "Another job is running",
            f"Wait for {' and '.join(jobs)} to finish before starting {action}.",
        )
        return True

    def stop_workers(self) -> None:
        """Ask every live worker to stop, then wait briefly for each."""
        for worker in list(self._workers):
            if not worker.isRunning():
                continue
            worker.requestInterruption()
            worker.quit()
            worker.wait(self.WORKER_STOP_MS)


# ---------------------------------------------------------------------------
# Install worker (background thread)
# ---------------------------------------------------------------------------


class InstallWorker(QThread):
    """Runs :class:`installer.Installer` in a background thread."""

    # fmt: off
    log    = pyqtSignal(str, str)   # (text, colour)
    done   = pyqtSignal(bool)       # success
    # fmt: on

    def __init__(self, refs: dict[Path, str] | None = None, force: set[Path] | None = None) -> None:
        super().__init__()
        self._refs = refs
        self._force = force

    def run(self) -> None:
        installer = Installer(
            refs=self._refs, force=self._force, log=self.log.emit, should_stop=self.isInterruptionRequested
        )
        self.done.emit(installer.run())


# ---------------------------------------------------------------------------
# Uninstall worker (background thread)
# ---------------------------------------------------------------------------


class UninstallWorker(QThread):
    """Runs :class:`installer.Uninstaller` in a background thread."""

    # fmt: off
    log  = pyqtSignal(str, str)   # (text, colour)
    done = pyqtSignal(bool)       # success (False if any step failed)
    # fmt: on

    def run(self) -> None:
        # Only started once the user agreed to uninstall_summary().
        self.done.emit(
            Uninstaller(log=self.log.emit, should_stop=self.isInterruptionRequested, confirmed=True).run()
        )


# ---------------------------------------------------------------------------
# Ref discovery (background thread)
# ---------------------------------------------------------------------------


class RefWorker(QThread):
    """Resolve one repo's local HEAD and its remote branch/tag list.

    The local part is instant and always succeeds; the ``ls-remote`` is a
    network call that is allowed to fail — the ref combo stays usable as a
    plain text field when offline.
    """

    # fmt: off
    status = pyqtSignal(str, str)    # (key, "main @ d2d257c5")  — local, instant
    refs   = pyqtSignal(str, list)   # (key, ["main", "v0.4.2", …])
    failed = pyqtSignal(str, str)    # (key, reason) — status line only, never modal
    # fmt: on

    def __init__(self, key: str, url: str, target: Path) -> None:
        super().__init__()
        self._key, self._url, self._target = key, url, target

    def run(self) -> None:
        self.status.emit(self._key, _describe_head(self._target))
        try:
            self.refs.emit(self._key, remote_refs(self._url))
        except RuntimeError as exc:
            self.failed.emit(self._key, str(exc))


class HeadWorker(QThread):
    """Describe one repo's local HEAD (git calls, so never on the GUI thread)."""

    status = pyqtSignal(str, str)  # (key, "main @ d2d257c5")

    def __init__(self, key: str, target: Path) -> None:
        super().__init__()
        self._key, self._target = key, target

    def run(self) -> None:
        self.status.emit(self._key, _describe_head(self._target))


class DirtyWorker(QThread):
    """Collect local changes per clone: a list of entries, or the error text (str)."""

    result = pyqtSignal(dict)  # {target: entries | error text}

    def __init__(self, targets: list[Path]) -> None:
        super().__init__()
        self._targets = targets

    def run(self) -> None:
        out: dict[Path, list | str] = {}
        for target in self._targets:
            if self.isInterruptionRequested():
                return
            try:
                out[target] = _dirty_files(target)
            except RuntimeError as exc:
                out[target] = str(exc)
        self.result.emit(out)


class StageWorker(QThread):
    """Find FITS files under a folder and classify them (astropy opens each one)."""

    # fmt: off
    log      = pyqtSignal(str, str)
    batch    = pyqtSignal(list)       # [(path, tag | None), …]
    progress = pyqtSignal(int, int)
    done     = pyqtSignal(int)        # files found
    # fmt: on

    BATCH = 25

    def __init__(self, root: Path, skip: set[str], classify: bool) -> None:
        super().__init__()
        self._root, self._skip, self._classify = root, skip, classify

    def run(self) -> None:
        from ..run_metis import classify_fits_file

        found: list[Path] = []
        for pattern in ("*.fits", "*.fits.gz"):
            found.extend(self._root.rglob(pattern))
        found.sort()
        todo = [p for p in found if str(p) not in self._skip]
        pending: list[tuple[Path, str | None]] = []
        for i, path in enumerate(todo, 1):
            if self.isInterruptionRequested():
                break
            pending.append((path, classify_fits_file(path) if self._classify else None))
            if len(pending) >= self.BATCH:
                self.batch.emit(pending)
                pending = []
                self.progress.emit(i, len(todo))
        if pending:
            self.batch.emit(pending)
        self.progress.emit(len(todo), len(todo))
        self.done.emit(len(found))


# ---------------------------------------------------------------------------
# Archive workers (background threads)
# ---------------------------------------------------------------------------


class MetisWISEInstallWorker(QThread):
    """Install MetisWISE into MTR's venv via `sys.executable -m pip install`."""

    # fmt: off
    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)
    # fmt: on

    #: Deadline for each pip command.
    TIMEOUT_S = 1800

    def run(self) -> None:
        from ..archive import install_metiswise_command

        try:
            # install_metiswise_command returns a sequence of pip commands
            # plus env overrides carrying the extra index URLs.
            cmds, env_overrides = install_metiswise_command()
            # pipx venvs ship without pip; bootstrap it first.
            boot = ensurepip_command_if_needed()
            if boot:
                cmds = [boot, *cmds]
            for cmd in cmds:
                # Same runner as the Install tab: a timeout, a stop request
                # from the window closing, and pip's children killed with it.
                try:
                    stream_subprocess(
                        cmd,
                        on_line=self.log.emit,
                        env=os.environ | env_overrides,
                        timeout=self.TIMEOUT_S,
                        should_stop=self.isInterruptionRequested,
                    )
                except Cancelled:
                    self.log.emit("\n■ MetisWISE install stopped.\n", "yellow")
                    self.done.emit(False)
                    return
                except RuntimeError as exc:
                    self.log.emit(f"\n✗ Install failed: {exc}\n", "red")
                    self.done.emit(False)
                    return
            self.log.emit("\n✓ MetisWISE installed successfully.\n", "green")
            self.done.emit(True)
        except Exception as exc:
            _log_exception(self.log.emit, "Failed", exc)
            self.done.emit(False)


class TestConnectionWorker(QThread):
    """Probe the remote archive; on success store the fields in the keyring.

    Typed field values win; blanks are filled from the keyring entry (read
    here, on the worker thread, so an unlock prompt never freezes the UI).
    Nothing is persisted unless the connection probe succeeds.
    """

    # fmt: off
    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)
    # fmt: on
    # Emitted when fields are missing from both the form and the keyring;
    # the slot runs on the main thread (queued connection) and shows a dialog.
    needs_input = pyqtSignal(str)

    def __init__(self, fields: dict[str, str]) -> None:
        super().__init__()
        self._fields = fields

    def run(self) -> None:
        from ..archive import db_session

        with db_session():  # closes this thread's DB connection on exit
            self._run()

    def _run(self) -> None:
        from .. import credentials as credstore
        from ..archive import (
            apply_db_credentials,
            query_archive,
            reset_db_connection,
            scrub_env_cfg,
        )

        fields = dict(self._fields)
        blanks = [k for k, v in fields.items() if not v]
        if blanks:
            try:
                stored = credstore.get_db_credentials() or {}
            except credstore.CredentialsUnavailable:
                stored = {}
            filled = [k for k in blanks if stored.get(k)]
            for k in filled:
                fields[k] = stored[k]
            if filled:
                self.log.emit(
                    f"Loaded {len(filled)} field(s) from keyring.\n",
                    "",
                )
        missing = [k for k, v in fields.items() if not v]
        if missing:
            self.needs_input.emit(
                "These fields are neither filled in nor stored in the OS keyring:\n  " + "\n  ".join(missing),
            )
            self.done.emit(False)
            return

        try:
            apply_db_credentials(fields)
            reset_db_connection()
            self.log.emit("Probing archive connection…\n", "cyan")
            items = query_archive(
                on_log=lambda msg: self.log.emit(msg + "\n", ""),
            )
        except Exception as exc:
            # Nothing persisted on failure — keyring and legacy file untouched.
            _log_exception(self.log.emit, "Connection failed", exc)
            self.done.emit(False)
            return

        try:
            credstore.set_db_credentials(fields)
            self.log.emit("Saved credentials to the OS keyring.\n", "green")
            if scrub_env_cfg():
                self.log.emit(
                    "Migrated: removed credentials from ~/.awe/Environment.cfg\n",
                    "",
                )
        except credstore.CredentialsUnavailable:
            # Keep the legacy file intact — without a keyring backend it is
            # the only persistent store the user has.
            self.log.emit(
                "Keyring unavailable — credentials active for this session only (not saved).\n",
                "yellow",
            )
        self.log.emit(
            f"\n✓ Connected — {len(items)} item(s) visible.\n",
            "green",
        )
        self.done.emit(True)


class QueryWorker(QThread):
    """Query the archive for available files."""

    # fmt: off
    log     = pyqtSignal(str, str)
    results = pyqtSignal(list)
    done    = pyqtSignal(bool)
    # fmt: on

    def __init__(self, category: str | None = None) -> None:
        super().__init__()
        self._category = category

    def run(self) -> None:
        from ..archive import db_session

        with db_session():  # closes this thread's DB connection on exit
            self._run()

    def _run(self) -> None:
        from ..archive import query_archive

        try:
            items = query_archive(
                category=self._category,
                on_log=lambda msg: self.log.emit(msg + "\n", ""),
            )
            self.results.emit(items)
            self.log.emit(f"Found {len(items)} item(s).\n", "green")
            self.done.emit(True)
        except Exception as exc:
            _log_exception(self.log.emit, "Query failed", exc)
            self.done.emit(False)


class DownloadWorker(QThread):
    """Download files from the archive."""

    # fmt: off
    log      = pyqtSignal(str, str)
    progress = pyqtSignal(int, int)
    done     = pyqtSignal(bool)
    # fmt: on

    def __init__(self, filenames: list[str], dest_dir: Path) -> None:
        super().__init__()
        self._filenames = filenames
        self._dest_dir = dest_dir

    def run(self) -> None:
        from ..archive import db_session

        with db_session():  # closes this thread's DB connection on exit
            self._run()

    def _run(self) -> None:
        from ..archive import download_file

        try:
            total = len(self._filenames)
            downloaded = 0
            for i, fn in enumerate(self._filenames, 1):
                if self.isInterruptionRequested():
                    break
                self.progress.emit(i, total)
                path = download_file(
                    fn,
                    self._dest_dir,
                    on_log=lambda msg: self.log.emit(msg + "\n", ""),
                )
                if path:
                    downloaded += 1
            ok = downloaded == total
            self.log.emit(
                f"\n{'✓' if ok else '✗'} Downloaded {downloaded}/{total} file(s).\n",
                "green" if ok else "red",
            )
            self.done.emit(ok)
        except Exception as exc:
            _log_exception(self.log.emit, "Download failed", exc)
            self.done.emit(False)


class UploadWorker(QThread):
    """Upload local FITS files into the remote archive."""

    # fmt: off
    log      = pyqtSignal(str, str)
    progress = pyqtSignal(int, int)
    done     = pyqtSignal(bool)
    # fmt: on

    def __init__(self, entries: list[tuple[Path, str | None]]) -> None:
        super().__init__()
        self._entries = entries

    def run(self) -> None:
        from ..archive import db_session

        with db_session():  # closes this thread's DB connection on exit
            self._run()

    def _run(self) -> None:
        from ..archive import upload_file

        try:
            total = len(self._entries)
            uploaded = 0
            for i, (path, class_name) in enumerate(self._entries, 1):
                if self.isInterruptionRequested():
                    break
                self.progress.emit(i, total)
                ok = upload_file(
                    path,
                    class_name,
                    on_log=lambda msg: self.log.emit(msg + "\n", ""),
                )
                if ok:
                    uploaded += 1
            ok = uploaded == total
            self.log.emit(
                f"\n{'✓' if ok else '✗'} Uploaded {uploaded}/{total} file(s).\n",
                "green" if ok else "red",
            )
            self.done.emit(ok)
        except Exception as exc:
            _log_exception(self.log.emit, "Upload failed", exc)
            self.done.emit(False)


class UpgradeWorker(QThread):
    """Upgrades MTR. It cannot be cancelled, because killing pip can break the venv."""

    log = pyqtSignal(str, str)
    done = pyqtSignal(str, str)  # (new version, error); exactly one is empty

    def __init__(self, commands: list[list[str]]) -> None:
        super().__init__()
        self._commands = commands

    def run(self) -> None:
        try:
            self.done.emit(update.perform_upgrade(self._commands, self.log.emit), "")
        except (RuntimeError, OSError) as exc:
            self.done.emit("", str(exc))
