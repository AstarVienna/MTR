"""METIS Test Runner GUI

Three-tab graphical front-end:
  Install  — pip-installs every METIS pipeline dependency into MTR's own venv
             and clones the simulation/pipeline repos into the user data dir
             (logic in installer.py; headless: mtr-install / mtr-uninstall)
  Run      — wraps run_metis.py with a file-picker and options UI
  Archive  — install MetisWISE and upload/download FITS files

Layout: theme (styling), widgets (shared helpers), workers (QThreads),
install_tab / archive_tab / run_tab, tour (first-run guided tour),
main_window (dialogs, MainWindow, main()).
"""

from . import archive_tab, install_tab, main_window, run_tab, theme, tour, widgets, workers
from .archive_tab import ArchiveTab
from .install_tab import InstallTab
from .main_window import MainWindow, UpdateDialog, WhatsNewDialog, main
from .run_tab import RunTab
from .theme import LOG_COLORS, THEMES, apply_theme
from .widgets import RefComboBox, _installation_complete, log_append
from .workers import (
    DownloadWorker,
    InstallWorker,
    MetisWISEInstallWorker,
    QueryWorker,
    RefWorker,
    TestConnectionWorker,
    UninstallWorker,
    UpgradeWorker,
    UploadWorker,
    WorkerHost,
)

__all__ = [
    "LOG_COLORS",
    "THEMES",
    "ArchiveTab",
    "DownloadWorker",
    "InstallTab",
    "InstallWorker",
    "MainWindow",
    "MetisWISEInstallWorker",
    "QueryWorker",
    "RefComboBox",
    "RefWorker",
    "RunTab",
    "TestConnectionWorker",
    "UninstallWorker",
    "UpdateDialog",
    "UpgradeWorker",
    "UploadWorker",
    "WhatsNewDialog",
    "WorkerHost",
    "_installation_complete",
    "apply_theme",
    "archive_tab",
    "install_tab",
    "log_append",
    "main",
    "main_window",
    "run_tab",
    "theme",
    "tour",
    "widgets",
    "workers",
]
