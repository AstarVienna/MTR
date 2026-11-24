"""Main window, update dialogs and the GUI entry point."""

# PyQt6 stubs type most widget getters as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

import os
import sys
import threading
from pathlib import Path

from PyQt6.QtCore import (
    QProcess,
    QSettings,
    Qt,
    QTimer,
    pyqtSignal,
)
from PyQt6.QtWidgets import (
    QApplication,
    QDialog,
    QDialogButtonBox,
    QGroupBox,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QTabWidget,
    QTextBrowser,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from .. import __version__, paths, update
from ..flow_view import FlowWindow
from ..reports_view import ReportsDialog, ReportsTab
from . import tour, widgets
from .archive_tab import ArchiveTab
from .install_tab import InstallTab
from .run_tab import RunTab
from .theme import THEMES, apply_theme
from .widgets import _ExpandingTabBar, _installation_complete, log_append
from .workers import UpgradeWorker

# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------


class UpdateDialog(QDialog):
    """Live log of the upgrade; it cannot be closed while the upgrade runs."""

    def __init__(self, target: str, commands: list[list[str]], parent: QWidget) -> None:
        super().__init__(parent)
        self.setWindowTitle(f"Updating to {target}")
        self.resize(700, 400)
        self.updated = False
        self._running = True

        self._status = QLabel("Upgrading MTR. The pipeline packages stay in place.")
        self._status.setWordWrap(True)
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._button = QPushButton("Close")
        self._button.setEnabled(False)
        self._button.clicked.connect(self.accept)
        layout = QVBoxLayout(self)
        layout.addWidget(self._status)
        layout.addWidget(self._log, stretch=1)
        layout.addWidget(self._button)

        self._worker = UpgradeWorker(commands)
        self._worker.log.connect(lambda text, color: log_append(self._log, text, color))
        self._worker.done.connect(self._on_done)
        self._worker.start()

    def _on_done(self, version: str, error: str) -> None:
        self._worker.wait()
        self._running = False
        self.updated = not error
        if error:
            self._status.setText(f"The update failed, so MTR stays on {__version__}.\n{error}")
        else:
            # Each run starts run_metis from disk, so the old GUI cannot carry on.
            self._status.setText(f"Updated to {version}. MTR must restart to use it.")
            self._button.setText("Restart now")
        self._button.setEnabled(True)

    def reject(self) -> None:
        if not self._running:
            super().reject()


class WhatsNewDialog(QDialog):
    """Release notes since the last used version, with required actions first."""

    def __init__(self, news: update.WhatsNew, warning_color: str, parent: QWidget) -> None:
        super().__init__(parent)
        self.setWindowTitle(news.title)
        self.resize(760, 640)
        layout = QVBoxLayout(self)

        if news.actions:
            box = QGroupBox("Action required before you continue")
            box.setStyleSheet(f"QGroupBox {{ border: 2px solid {warning_color}; color: {warning_color}; }}")
            QVBoxLayout(box).addWidget(self._browser(news.actions))
            layout.addWidget(box, stretch=1)
        layout.addWidget(self._browser(news.notes), stretch=2)

        link = QLabel(f'<a href="{update.CHANGELOG_PAGE}">Full changelog</a>')
        link.setOpenExternalLinks(True)
        layout.addWidget(link)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        buttons.accepted.connect(self.accept)
        layout.addWidget(buttons)

    @staticmethod
    def _browser(markdown: str) -> QTextBrowser:
        browser = QTextBrowser()
        browser.setOpenExternalLinks(True)
        browser.setMarkdown(markdown)
        return browser


class MainWindow(QMainWindow):
    update_found = pyqtSignal(str)

    def __init__(self, initial_theme: str = "dark") -> None:
        super().__init__()
        self.setWindowTitle("METIS Test Runner")
        self.setMinimumSize(600, 400)
        self.resize(1000, 900)

        self._current_theme = initial_theme
        self._home_dark = initial_theme  # "dark" or "pink" — the base non-light theme
        self._home_light = "pink_light" if initial_theme == "pink" else "light"

        toolbar = self.addToolBar("Theme")
        toolbar.setMovable(False)
        toolbar.setFloatable(False)
        self._theme_btn = QPushButton()
        self._theme_btn.setProperty("role", "accent")
        toolbar.addWidget(self._theme_btn)
        self._theme_btn.clicked.connect(self._toggle_theme)
        self._update_theme_btn_label()
        self._flow_btn = QPushButton("Data flow")
        self._flow_btn.setProperty("role", "accent")
        self._flow_btn.setToolTip("Show how data flows from the simulations through the pipeline")
        toolbar.addWidget(self._flow_btn)
        self._flow_btn.clicked.connect(self._show_flow)
        self._flow_window: FlowWindow | None = None
        self._tour_btn = QPushButton("Tour")
        self._tour_btn.setProperty("role", "accent")
        self._tour_btn.setToolTip("Replay the introduction tour of the tabs")
        toolbar.addWidget(self._tour_btn)
        self._tour_btn.clicked.connect(self.start_tour)
        self._tour: tour.TourOverlay | None = None
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar.addWidget(spacer)
        self._update_btn = QPushButton()
        self._update_btn.setProperty("role", "accent")
        self._update_btn.clicked.connect(self._start_update)
        # A toolbar widget is shown through its action; widget.hide() has no effect.
        self._update_action = toolbar.addWidget(self._update_btn)
        self._update_action.setVisible(False)
        self._update_target = ""
        self.restart_requested = False
        self.update_found.connect(self._offer_update)

        tabs = QTabWidget()
        tabs.setTabBar(_ExpandingTabBar())
        tabs.tabBar().setUsesScrollButtons(False)
        self._run_tab = RunTab()
        self._archive_tab = ArchiveTab()
        self._install_tab = InstallTab()
        self._reports_tab = ReportsTab(paths.output_dir)
        tabs.addTab(self._run_tab, "Run")
        tabs.addTab(self._install_tab, "Install")
        tabs.addTab(self._archive_tab, "Archive")
        tabs.addTab(self._reports_tab, "Reports")
        self._run_tab.run_finished.connect(lambda root: self._reports_tab.show_run(Path(root)))
        self._tabs = tabs
        for tab in (self._run_tab, self._install_tab, self._archive_tab):
            tab._jobs_blocking_hook = self._jobs_blocking
        self._install_tab.checkouts_changed.connect(self._run_tab._update_runner_fields)
        self._install_tab.pipeline_changed.connect(self._on_pipeline_changed)
        if not _installation_complete():
            tabs.setCurrentIndex(1)  # Install tab
        self.setCentralWidget(tabs)

    def _on_pipeline_changed(self) -> None:
        """Tables were reset: refresh what was built from them."""
        self._archive_tab.refresh_categories()
        if self._flow_window is not None and self._flow_window.isVisible():
            self._flow_window.rebuild(refresh=True)

    def _update_theme_btn_label(self) -> None:
        if self._current_theme in ("light", "pink_light"):
            self._theme_btn.setText("Dark theme")
        else:
            self._theme_btn.setText("Light theme")

    def _toggle_theme(self) -> None:
        if self._current_theme in ("light", "pink_light"):
            self._current_theme = self._home_dark
        else:
            self._current_theme = self._home_light
        apply_theme(QApplication.instance(), self._current_theme)  # pyright: ignore[reportArgumentType]
        self._update_theme_btn_label()
        if self._flow_window is not None:
            self._flow_window.apply_theme(THEMES[self._current_theme])

    def _show_flow(self) -> None:
        # Parentless, so it can sit beside or behind the main window; closeEvent closes it.
        if self._flow_window is None:
            self._flow_window = FlowWindow(
                THEMES[self._current_theme],
                self._run_tab.flow_input,
                self._run_flow_task,
                self._show_task_reports,
            )
        self._flow_window.show()
        self._flow_window.raise_()
        self._flow_window.activateWindow()

    def start_tour(self) -> None:
        """Walk through the tabs, then open the Install tab, where a new user starts."""
        if self._tour is not None:
            return

        def done() -> None:
            self._tour = None
            self._tabs.setCurrentWidget(self._install_tab)

        self._tour = tour.TourOverlay(self, tour.build_steps(self), THEMES[self._current_theme], done)
        self._tour.start()

    def maybe_start_tour(self) -> None:
        """Start the tour on the first launch only."""
        if not tour.completed():
            self.start_tour()

    def _show_task_reports(self, task: str) -> None:
        """Open *task*'s reports from the Reports tab's selected run (the newest by default)."""
        if self._reports_tab.current_run() is None:
            self._reports_tab.show_run()
        run = self._reports_tab.current_run()
        # A parent keeps the dialog alive (WA_DeleteOnClose frees it).
        dialog = ReportsDialog(task, run, self._reports_tab.reports_for(task), self._flow_window or self)
        dialog.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        dialog.show()

    def _run_flow_task(self, task: str) -> None:
        self._tabs.setCurrentWidget(self._run_tab)
        self.raise_()
        self.activateWindow()
        self._run_tab.run_task(task)

    def start_update_check(self) -> None:
        """Offer the cached result now, and refresh it in the background if due."""
        if not update.checks_enabled():
            return
        self._offer_update(update.cached_latest())
        if update.check_due():
            # Daemon, not QThread: urlopen's timeout doesn't cover DNS, and a
            # QThread still running when the window closes aborts the process.
            threading.Thread(target=self._check_for_update, daemon=True).start()

    def _check_for_update(self) -> None:
        latest = update.fetch_latest()
        if latest is None:
            return
        update.remember_latest(latest)
        try:
            self.update_found.emit(latest)
        except RuntimeError:  # the window has already gone
            pass

    def _offer_update(self, latest: str | None) -> None:
        if latest is None or not update.is_newer(latest, __version__):
            return
        self._update_target = latest
        self._update_btn.setText(f"Update to {latest}")
        self._update_btn.setToolTip(f"Installed: {__version__}\nLatest: {latest}")
        self._update_action.setVisible(True)

    def _start_update(self) -> None:
        busy = self._busy_jobs()
        if busy:
            QMessageBox.information(
                self,
                "Another job is running",
                f"Wait for {' and '.join(busy)} to finish before updating.",
            )
            return
        commands = update.upgrade_commands(self._update_target)
        if commands is None:
            self._show_manual_update()
            return
        reply = QMessageBox.question(
            self,
            "Update MTR",
            f"Update MTR {__version__} to {self._update_target}?\n\n"
            "The pipeline stays installed. MTR restarts when the update finishes.",
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        dialog = UpdateDialog(self._update_target, commands, self)
        dialog.exec()
        if dialog.updated:
            self._restart()

    def _show_manual_update(self) -> None:
        command = update.manual_command()
        box = QMessageBox(self)
        box.setWindowTitle("Update MTR")
        box.setText(f"MTR cannot upgrade this install itself. To get {self._update_target}, run:")
        box.setInformativeText(command)
        box.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        copy = box.addButton("Copy command", QMessageBox.ButtonRole.ActionRole)
        box.addButton(QMessageBox.StandardButton.Close)
        box.exec()
        if box.clickedButton() == copy:
            QApplication.clipboard().setText(command)

    def _restart(self) -> None:
        """Quit through closeEvent; main() starts the new version once the event loop ends."""
        if self.close():
            # execve skips the destructors that would otherwise write these out.
            for tab in (self._run_tab, self._install_tab, self._archive_tab):
                tab._settings.sync()
            self.restart_requested = True
            QApplication.quit()

    def show_whats_new(self) -> None:
        """Show what changed since the version last used here, however the update happened."""
        if update.install_kind() == "editable":
            return  # QSettings is per user, so a dev copy would skew a pipx install's history
        settings = QSettings("METIS", "TestRunner")
        settings.setFallbacksEnabled(False)  # macOS's allKeys() includes system-wide keys
        news = update.whats_new(
            settings.value("last_seen_version"),
            has_history=bool(settings.allKeys()),
            changelog=update.load_changelog(),
            current=__version__,
        )
        if news is not None:
            WhatsNewDialog(news, THEMES[self._current_theme]["btn_danger_bg"], self).exec()
        settings.setValue("last_seen_version", __version__)

    def _busy_jobs(self, tabs=None) -> list[str]:
        """Names of jobs still running in *tabs* (default: all), e.g. for the close confirmation."""
        tabs = (self._run_tab, self._install_tab, self._archive_tab) if tabs is None else tabs
        jobs = []
        proc = getattr(self._run_tab, "_process", None)
        if self._run_tab in tabs and proc is not None and proc.state() != QProcess.ProcessState.NotRunning:
            jobs.append("a pipeline run")
        for tab, label in (
            (self._install_tab, "an install/uninstall"),
            (self._archive_tab, "an archive operation"),
        ):
            if tab in tabs and tab.live_workers():
                jobs.append(label)
        return jobs

    def _jobs_blocking(self, tab, action: str) -> list[str]:
        """Jobs in other tabs that conflict with *action* started from *tab*.

        Install/Uninstall replace the packages and checkouts a run or the
        archive client is using; a run and the MetisWISE install need them
        to stay put while they work.
        """
        if tab is self._install_tab:
            blocking = (self._run_tab, self._archive_tab)
        elif tab is self._archive_tab:
            blocking = (self._run_tab, self._install_tab)
        else:
            blocking = (self._install_tab,)
        return self._busy_jobs(blocking)

    def closeEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        # Destroying a tab while one of its QThreads is still running aborts
        # with "QThread: Destroyed while thread is still running", and the
        # worker's queued log signal targets a QTextEdit that is going away.
        busy = self._busy_jobs()
        if busy and not widgets.SMOKE_TEST:
            reply = QMessageBox.question(
                self,
                "Job still running",
                "There is still " + " and ".join(busy) + " in progress.\n\n"
                "Quit anyway? The job will be stopped.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                event.ignore()
                return

        self._run_tab._save_settings()
        self._install_tab._save_settings()
        self._archive_tab._save_settings()

        self._run_tab.stop_process()
        self._install_tab.stop_ref_workers()
        for tab in (self._install_tab, self._archive_tab):
            tab.stop_workers()
        if self._flow_window is not None:
            self._flow_window.close()
        super().closeEvent(event)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    pink = "--pink" in sys.argv
    smoke_test = "--smoke-test" in sys.argv or os.environ.get("SMOKE_TEST")
    widgets.SMOKE_TEST = bool(smoke_test)
    argv = [a for a in sys.argv if a not in ("--pink", "--smoke-test")]
    app = QApplication(argv)
    app.setApplicationName("METIS Test Runner")
    initial = "pink" if pink else "dark"
    apply_theme(app, initial)
    win = MainWindow(initial_theme=initial)
    win.show()
    if smoke_test:
        # CI smoke test: exit cleanly once the event loop has started, so we
        # exercise imports + Qt init + window construction without blocking.
        QTimer.singleShot(0, app.quit)
    else:
        win.start_update_check()

        def greet() -> None:
            win.show_whats_new()
            win.maybe_start_tour()

        QTimer.singleShot(0, greet)
    code = app.exec()
    if win.restart_requested:
        try:
            update.restart()
        except OSError as exc:
            sys.exit(f"mtr: updated, but could not restart ({exc}). Start mtr again.")
    sys.exit(code)
