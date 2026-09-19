import os
import sys
import tempfile

# Must be set before PyQt6 is imported anywhere
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

# Redirect QSettings at a per-process tmp dir by overriding XDG_CONFIG_HOME
# (and the macOS equivalent) BEFORE PyQt6 is imported. Without this, tests
# that assert default UI state (e.g. "auto_fetch checkbox unchecked") read
# from the user's real ~/.config/METIS/TestRunner.conf and flake on any
# machine where the GUI has been run with those options toggled on.
# QSettings.setPath() is not sufficient here: the default QSettings format
# on Linux is NativeFormat, whose path cannot be overridden by setPath —
# only the XDG_CONFIG_HOME env var reroutes it.
_qsettings_tmp = tempfile.mkdtemp(prefix="metis-qsettings-")
os.environ["XDG_CONFIG_HOME"] = _qsettings_tmp
os.environ["XDG_DATA_HOME"] = _qsettings_tmp

import pytest  # noqa: E402 (must follow the env setup above)


@pytest.fixture(scope="session")
def qapp():
    from PyQt6.QtWidgets import QApplication
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv[:1])
    yield app
