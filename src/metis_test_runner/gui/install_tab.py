"""The Install tab."""

# PyQt6 stubs type most widget getters as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

import os
import shutil
from pathlib import Path

from PyQt6.QtCore import (
    QSettings,
    Qt,
    QTimer,
    pyqtSignal,
)
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from .. import docs, installer, paths, tables
from ..installer import (
    _validate_ref,
    discard_summary,
    uninstall_summary,
)
from . import widgets
from .widgets import RefComboBox, _labeled, _linkify, log_append
from .workers import DirtyWorker, HeadWorker, InstallWorker, RefWorker, UninstallWorker, WorkerHost

# ---------------------------------------------------------------------------
# Install tab
# ---------------------------------------------------------------------------


class InstallTab(WorkerHost, QWidget):
    # Emitted after an own checkout was set or cleared (paths already reloaded).
    checkouts_changed = pyqtSignal()
    # Emitted once the Pipeline on disk changed (install, uninstall, checkout); tables are reset first.
    pipeline_changed = pyqtSignal()

    @property
    def REPOS(self) -> tuple:
        """(settings key, label, repo URL, clone target) under the QSettings keys
        the tab has always used, resolved on each use."""
        return tuple((f"{key}_ref", label, url, target) for key, label, url, target in installer.repos())

    def __init__(self) -> None:
        super().__init__()
        self._worker: InstallWorker | UninstallWorker | None = None
        self._settings = QSettings("METIS", "TestRunner")
        # Held so Python doesn't garbage-collect a running QThread mid-flight.
        self._ref_workers: dict[str, RefWorker] = {}
        self._refs_loaded: set[str] = set()
        self._head_seq: dict[str, int] = {}
        self._last_action = ""
        self._build_ui()
        self._load_settings()

    def busy(self) -> bool:
        # A read-only HEAD probe never blocks an action; it is only tracked so close waits for it.
        return any(not isinstance(w, HeadWorker) for w in self.live_workers())

    def showEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        super().showEvent(event)
        # Unlike ArchiveTab.showEvent this is a NETWORK probe, so it runs once
        # per session rather than on every tab switch. Deferred via a timer
        # because MainWindow.__init__ selects this tab during construction when
        # nothing is installed — we must not delay the first paint.
        QTimer.singleShot(0, lambda: self._refresh_refs(only_missing=True))

    # ── UI construction ─────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(14)
        layout.setContentsMargins(20, 20, 20, 20)

        desc = QLabel(
            "<b>METIS Pipeline Installation</b><br><br>"
            "Skip this tab if you have already installed the pipeline via one of "
            "these methods and go straight to <b>Run</b>:<br>"
            "<ul>"
            "<li><b>Bare-metal / ESO docs</b> — select runner <i>native</i></li>"
            "<li><b>Pipeline container</b> — select runner <i>docker</i> or "
            "<i>podman</i> and enter the container name</li>"
            "</ul>"
            "Otherwise, clicking <i>Install / Update</i> will perform the following "
            "steps:<br>"
            "<ol>"
            f"<li>Clone or update <b>METIS_Pipeline</b> and <b>METIS_Simulations</b> "
            f"into <code>{paths.data_dir()}</code></li>"
            "<li>Install all Python dependencies (pycpl, edps, pyesorex, adari_core, "
            "scopesim, scopesim_templates) into MTR's own pipx/venv</li>"
            "<li>Initialise and configure EDPS on port 4444</li>"
            "</ol>"
            "Leave the version fields blank and re-running is safe — existing "
            "repositories are fast-forwarded, not re-cloned. To work on a repository "
            "yourself, select your own checkout below (or set "
            "<code>METIS_PIPELINE_DIR</code> / <code>METIS_SIMULATIONS_DIR</code>): "
            "it is used as-is and never updated or deleted. "
            f'<a href="{docs.url("own-checkout")}">More in the README</a>.'
        )
        desc.setWordWrap(True)
        desc.setTextFormat(Qt.TextFormat.RichText)
        desc.setOpenExternalLinks(True)
        desc.setProperty("intro", "true")
        layout.addWidget(desc)

        layout.addWidget(self._build_version_group())

        btn_row = QHBoxLayout()

        self.install_btn = QPushButton("Install / Update")
        self.install_btn.setProperty("role", "success")
        self.install_btn.setMinimumHeight(36)
        self.install_btn.setMaximumWidth(200)
        self.install_btn.clicked.connect(self._start)
        btn_row.addWidget(self.install_btn)

        self.uninstall_btn = QPushButton("Uninstall")
        self.uninstall_btn.setProperty("role", "danger")
        self.uninstall_btn.setMinimumHeight(36)
        self.uninstall_btn.setMaximumWidth(200)
        self.uninstall_btn.clicked.connect(self._uninstall)
        btn_row.addWidget(self.uninstall_btn)

        btn_row.addStretch()
        layout.addLayout(btn_row)

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Monospace", 9))
        layout.addWidget(self.log_view, stretch=1)

    def _build_version_group(self) -> QGroupBox:
        grp = QGroupBox("Repositories (advanced)")
        lay = QVBoxLayout(grp)

        hint = QLabel(
            "Leave blank to track each repository's default branch. Developers "
            "can pick a branch or tag from the list, or paste a full "
            "40-character commit SHA. An existing clone is <b>overwritten</b> "
            "with the selection — to keep local edits, select your own "
            "checkout instead, then click <i>Install / Update</i>."
        )
        hint.setWordWrap(True)
        hint.setTextFormat(Qt.TextFormat.RichText)
        hint.setProperty("hint", "note")
        lay.addWidget(hint)

        self.ref_combos: dict[str, RefComboBox] = {}
        self.ref_status: dict[str, QLabel] = {}
        self.ref_buttons: dict[str, QPushButton] = {}
        self.checkout_edits: dict[str, QLineEdit] = {}
        self.checkout_buttons: dict[str, tuple[QPushButton, QPushButton]] = {}

        for key, label, _url, target in self.REPOS:
            combo = RefComboBox()
            combo.lineEdit().setPlaceholderText("default branch")
            self.ref_combos[key] = combo

            btn = QPushButton("↻")
            btn.setProperty("role", "info")
            btn.setProperty("symbol", "true")
            btn.setToolTip("Reload branches and tags")
            btn.setMaximumWidth(40)
            btn.clicked.connect(lambda _checked=False, k=key: self._reload(k))
            self.ref_buttons[key] = btn

            # Kept short: _labeled pins the label column at LABEL_W, and
            # "(branch / tag / commit)" overflows it. The hint above says it.
            lay.addWidget(_labeled(f"{label} version:", combo, btn))

            status = QLabel("currently: …")
            status.setProperty("hint", "true")
            self.ref_status[key] = status
            # Same row layout as the combo above, so the text starts at its edge.
            lay.addWidget(_labeled("", status))

            edit = QLineEdit()
            edit.setPlaceholderText(f"none — MTR manages {paths.data_dir() / label}")
            edit.editingFinished.connect(lambda k=key: self._set_checkout(k, self.checkout_edits[k].text()))
            self.checkout_edits[key] = edit
            browse = QPushButton("Browse…")
            browse.setProperty("role", "browse")
            browse.clicked.connect(lambda _checked=False, k=key: self._browse_checkout(k))
            clear = QPushButton("Clear")
            clear.setProperty("role", "browse")
            clear.setToolTip("Let MTR manage this repository again")
            clear.clicked.connect(lambda _checked=False, k=key: self._set_checkout(k, ""))
            self.checkout_buttons[key] = (browse, clear)
            lay.addWidget(_labeled(f"Own {label}:", edit, browse, clear))

            self._apply_repo_state(key, target)

        self.checkout_note = QLabel("Click <i>Install / Update</i> to install from the changed checkout.")
        self.checkout_note.setTextFormat(Qt.TextFormat.RichText)
        self.checkout_note.setProperty("hint", "note")
        self.checkout_note.hide()
        lay.addWidget(self.checkout_note)

        return grp

    # ── own checkouts ────────────────────────────────────────────────────────

    def _apply_repo_state(self, key: str, target: Path) -> None:
        """Sync one repo's widgets with whether *target* is the user's own checkout."""
        combo, btn, edit = self.ref_combos[key], self.ref_buttons[key], self.checkout_edits[key]
        env = paths.REPO_ENV[key.removesuffix("_ref")]
        owned = paths.user_owned_repos()
        own = target in owned
        # The user's own checkout: MTR never runs git there.
        combo.setEnabled(not own)
        btn.setEnabled(not own)
        combo.setToolTip(f"Set via {owned[target]} — MTR does not change this checkout." if own else "")
        edit.setText(str(target) if own else "")
        by_env = bool(os.environ.get(env))
        edit.setReadOnly(by_env)
        for b in self.checkout_buttons[key]:
            b.setEnabled(not by_env)
        edit.setToolTip(
            f"Set via {env}, which overrides this field."
            if by_env
            else "A checkout you manage yourself: MTR installs from it but never runs git in it "
            "or deletes it. Empty: MTR clones and updates the repository itself."
        )
        if own:
            self._probe_head(key, target)

    def _browse_checkout(self, key: str) -> None:
        label = next(lbl for k, lbl, _u, _t in self.REPOS if k == key)
        start = self.checkout_edits[key].text().strip() or str(Path.home())
        chosen = QFileDialog.getExistingDirectory(self, f"Select your own {label} checkout", start)
        if chosen:
            self._set_checkout(key, chosen)

    def _set_checkout(self, key: str, text: str) -> None:
        """Make *text* repo *key*'s own checkout ("" = let MTR manage it), if valid."""
        repo = key.removesuffix("_ref")
        label = next(lbl for k, lbl, _u, _t in self.REPOS if k == key)
        saved = paths.load_repo_dirs().get(repo)
        new = Path(text.strip()).expanduser() if text.strip() else None
        # Show what is in effect until the change is accepted, so a refused
        # change never lingers in the field (and editingFinished re-fires harmlessly).
        self._apply_repo_state(key, self._target(key))
        if os.environ.get(paths.REPO_ENV[repo]) or new == saved:
            return
        if self.busy():
            QMessageBox.information(self, "Install running", "Wait for the install or uninstall to finish.")
            return
        if self.reject_if_blocked("changing a checkout"):
            return
        if new is not None:
            problem = (
                f"{new} is not an existing directory (use an absolute path)."
                if not new.is_absolute() or not new.is_dir()
                else installer.checkout_problem(repo, new)
            )
            if problem:
                QMessageBox.warning(
                    self,
                    "Not a checkout",
                    f"{new} cannot be used as your own {label} checkout: {problem}.",
                )
                return
        try:
            paths.save_repo_dir(repo, new)
        except OSError as exc:
            QMessageBox.warning(self, "Could not save", f"Could not save the checkout setting: {exc}")
            return
        for k, _label, _url, target in self.REPOS:
            self._apply_repo_state(k, target)
        if new is None:
            # Back to the managed clone: its head and refs were never shown.
            self._refs_loaded.discard(key)
            self._refresh_status_labels()
            self._refresh_refs(only_missing=True)
        self.checkout_note.show()
        tables.reset()
        self.checkouts_changed.emit()
        self.pipeline_changed.emit()

    def _target(self, key: str) -> Path:
        return next(t for k, _l, _u, t in self.REPOS if k == key)

    # ── ref discovery ────────────────────────────────────────────────────────

    def _refresh_refs(self, only_missing: bool = False) -> None:
        if widgets.SMOKE_TEST or not shutil.which("git"):
            return
        owned = paths.user_owned_repos()
        for key, _label, url, target in self.REPOS:
            if target in owned:
                continue
            if only_missing and key in self._refs_loaded:
                continue
            if key in self._ref_workers:
                continue
            self._refs_loaded.add(key)
            self.ref_buttons[key].setEnabled(False)
            w = RefWorker(key, url, target)
            w.status.connect(self._on_ref_status)
            w.refs.connect(self._on_refs)
            w.failed.connect(self._on_ref_failed)
            w.finished.connect(lambda k=key: self._ref_worker_done(k))
            self._ref_workers[key] = w
            w.start()

    def _reload(self, key: str) -> None:
        self._refs_loaded.discard(key)
        self._refresh_refs(only_missing=True)

    def _ref_worker_done(self, key: str) -> None:
        self._ref_workers.pop(key, None)
        # Stays off if the repo became an own checkout while the probe ran.
        self.ref_buttons[key].setEnabled(self._target(key) not in paths.user_owned_repos())

    def _on_ref_status(self, key: str, text: str) -> None:
        if self._target(key) in paths.user_owned_repos():
            return  # switched to an own checkout while the probe ran
        self.ref_status[key].setText(f"currently: {text}")

    def _on_refs(self, key: str, items: list) -> None:
        self._populate(self.ref_combos[key], items)

    def _on_ref_failed(self, key: str, reason: str) -> None:
        # Never modal, and never in log_view — that widget is the install
        # transcript, and pre-install noise there is confusing.
        lbl = self.ref_status[key]
        lbl.setText(f"{lbl.text()}   (ref list unavailable — offline?)")
        self.ref_combos[key].setToolTip(reason)

    @staticmethod
    def _populate(combo: QComboBox, items: list) -> None:
        """Refill the dropdown without disturbing what the user has typed."""
        edit = combo.lineEdit()
        typed, focused = combo.currentText(), edit.hasFocus()
        pos = edit.cursorPosition()
        # addItems() on an editable combo with currentIndex == -1 silently
        # snaps the line edit to items[0], so the text must be restored.
        combo.blockSignals(True)
        combo.clear()
        combo.addItems([str(i) for i in items])
        combo.setCurrentText(typed)
        combo.blockSignals(False)
        if focused:
            edit.setCursorPosition(min(pos, len(typed)))

    @staticmethod
    def _status_text(target: Path, head: str) -> str:
        owned = paths.user_owned_repos()
        if target in owned:
            return f"your own checkout (set via {owned[target]}), not managed by MTR: {head}"
        return f"currently: {head}"

    def _probe_head(self, key: str, target: Path) -> None:
        """Show *target*'s HEAD on the label once a worker has read it (git is slow)."""
        self.ref_status[key].setText(self._status_text(target, "…"))
        if widgets.SMOKE_TEST:
            return
        seq = self._head_seq[key] = self._head_seq.get(key, 0) + 1
        w = self.track_worker(HeadWorker(key, target))
        w.status.connect(lambda k, text, t=target, n=seq: self._on_head(k, text, t, n))
        w.start()

    def _on_head(self, key: str, head: str, target: Path, seq: int) -> None:
        # A newer probe, or a switch to/from an own checkout, makes this one stale.
        if seq != self._head_seq.get(key) or self._target(key) != target:
            return
        self.ref_status[key].setText(self._status_text(target, head))

    def _refresh_status_labels(self) -> None:
        for key, _label, _url, target in self.REPOS:
            self._probe_head(key, target)

    # ── settings ─────────────────────────────────────────────────────────────

    def _load_settings(self) -> None:
        for key, _label, _url, _target in self.REPOS:
            self.ref_combos[key].setCurrentText(self._settings.value(f"install/{key}", "", type=str))

    def _save_settings(self) -> None:
        for key, _label, _url, _target in self.REPOS:
            self._settings.setValue(f"install/{key}", self.ref_combos[key].currentText().strip())

    def stop_ref_workers(self) -> None:
        """Let a pending ls-remote finish before the window goes away."""
        for w in list(self._ref_workers.values()):
            w.quit()
            w.wait(2000)

    # ── actions ──────────────────────────────────────────────────────────────

    def _start(self) -> None:
        if self.reject_if_blocked("the install"):
            return
        try:
            refs = {
                target: _validate_ref(self.ref_combos[key].currentText())
                for key, _label, _url, target in self.REPOS
                if target not in paths.user_owned_repos()
            }
        except ValueError as exc:
            QMessageBox.warning(self, "Invalid ref", str(exc))
            return

        targets = [t for _k, _l, _u, t in self.REPOS if t not in paths.user_owned_repos()]
        self.install_btn.setEnabled(False)
        self.uninstall_btn.setEnabled(False)
        w = self.track_worker(DirtyWorker(targets))
        w.result.connect(lambda results, r=refs: self._on_dirty_checked(r, results))
        w.start()

    def _on_dirty_checked(self, refs: dict[Path, str], results: dict) -> None:
        force = self._confirm_discard(results)
        if force is None:
            self.install_btn.setEnabled(True)
            self.uninstall_btn.setEnabled(True)
            return
        self._launch_install(refs, force)

    def _launch_install(self, refs: dict[Path, str], force: set[Path]) -> None:
        self._save_settings()
        self._last_action = "install"
        self.log_view.clear()
        self.install_btn.setEnabled(False)
        self.uninstall_btn.setEnabled(False)
        self._worker = self.track_worker(InstallWorker(refs=refs, force=force))
        self._worker.log.connect(lambda text, color: log_append(self.log_view, text, color))
        self._worker.done.connect(self._on_done)
        self._worker.start()

    def _confirm_discard(self, results: dict) -> set | None:
        """Targets whose local changes may be discarded, or None to abort.

        *results* maps each checked target to its dirty entries, or the error text.

        Checked for every dirty clone unconditionally — `checkout -f` discards
        tracked edits whether or not the selected ref actually changed, so
        gating this on a changed ref would destroy work silently.
        """
        force: set[Path] = set()
        owned = paths.user_owned_repos()
        for key, label, _url, target in self.REPOS:
            if target in owned:
                continue
            if target not in results:
                continue
            entries = results[target]
            if isinstance(entries, str):
                exc = entries
                # Assuming "clean" here risks silent data loss, so ask.
                if (
                    QMessageBox.question(
                        self,
                        "Could not check for local changes",
                        f"Could not determine whether {label} ({target}) has local "
                        f"changes:\n\n{exc}\n\nContinue anyway?",
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                        QMessageBox.StandardButton.No,
                    )
                    != QMessageBox.StandardButton.Yes
                ):
                    return None
                continue
            if not entries:
                continue
            if (
                QMessageBox.question(
                    self,
                    "Discard local changes?",
                    _linkify(discard_summary(label, target, entries)),
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No,
                )
                != QMessageBox.StandardButton.Yes
            ):
                return None
            force.add(target)
        return force

    def _uninstall(self) -> None:
        if self.reject_if_blocked("the uninstall"):
            return
        reply = QMessageBox.question(
            self,
            "Confirm uninstall",
            uninstall_summary(),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._last_action = "uninstall"
        self.log_view.clear()
        self.install_btn.setEnabled(False)
        self.uninstall_btn.setEnabled(False)
        self._worker = self.track_worker(UninstallWorker())
        self._worker.log.connect(lambda text, color: log_append(self.log_view, text, color))
        self._worker.done.connect(self._on_done)
        self._worker.start()

    def _on_done(self, success: bool) -> None:
        self.install_btn.setEnabled(True)
        self.uninstall_btn.setEnabled(True)
        if self._last_action == "uninstall" and success:
            # The clones are gone; leaving the pins behind would silently
            # re-pin the next install to a long-forgotten ref.
            for key, _label, _url, _target in self.REPOS:
                self.ref_combos[key].setCurrentText("")
                self._settings.remove(f"install/{key}")
            # The own-checkout settings lived in the deleted data directory.
            for key, _label, _url, target in self.REPOS:
                self._apply_repo_state(key, target)
            self.checkouts_changed.emit()
        if success:
            self.checkout_note.hide()
            tables.reset()
            self.pipeline_changed.emit()
        # HEAD just moved (or the clones vanished), so the labels are stale.
        self._refresh_status_labels()
