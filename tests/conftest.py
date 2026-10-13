import atexit
import os
import shutil
import sys
import tempfile
from pathlib import Path

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
atexit.register(shutil.rmtree, _qsettings_tmp, ignore_errors=True)
os.environ["XDG_CONFIG_HOME"] = _qsettings_tmp
os.environ["XDG_DATA_HOME"] = _qsettings_tmp
# A developer's own METIS_PIPELINE_DIR / METIS_SIMULATIONS_DIR would mark the
# repos user-owned at import time (installer.EXTERNAL) and skew the tests.
# Only the upstream contract tests see them, via the fixtures below.
_UPSTREAM_PIPELINE = os.environ.pop("METIS_PIPELINE_DIR", None)
_UPSTREAM_SIMULATIONS = os.environ.pop("METIS_SIMULATIONS_DIR", None)

import pytest  # noqa: E402 (must follow the env setup above)


@pytest.fixture(scope="session")
def qapp():
    from PyQt6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv[:1])
    yield app


def _upstream_dir(value, env):
    if not value:
        pytest.skip(f"{env} not set (upstream contract test)")
    return Path(value).expanduser().resolve()


@pytest.fixture(scope="session")
def upstream_pipeline_dir():
    return _upstream_dir(_UPSTREAM_PIPELINE, "METIS_PIPELINE_DIR")


@pytest.fixture(scope="session")
def upstream_simulations_dir():
    return _upstream_dir(_UPSTREAM_SIMULATIONS, "METIS_SIMULATIONS_DIR")
