"""Unit tests for launcher.py — the `mtr` entry point that guards the GUI import."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from metis_test_runner import launcher


def _failing_import(exc):
    def fake():
        raise exc

    return fake


def test_missing_pyqt6_explains_instead_of_a_traceback(monkeypatch):
    exc = ModuleNotFoundError("No module named 'PyQt6'", name="PyQt6")
    monkeypatch.setattr(launcher, "_import_gui", _failing_import(exc))
    with pytest.raises(SystemExit) as err:
        launcher.main()
    assert "needs PyQt6" in str(err.value) and "mtr-install" in str(err.value)


def test_missing_qt_system_library_is_explained(monkeypatch):
    exc = ImportError("libEGL.so.1: cannot open shared object file: No such file or directory")
    monkeypatch.setattr(launcher, "_import_gui", _failing_import(exc))
    with pytest.raises(SystemExit) as err:
        launcher.main()
    assert "libEGL.so.1" in str(err.value) and "System dependencies" in str(err.value)


@pytest.mark.parametrize(
    "exc",
    [ModuleNotFoundError("No module named 'yaml'", name="yaml"), ImportError("cannot import name 'x'")],
)
def test_unrelated_import_errors_still_raise(monkeypatch, exc):
    # A bug in MTR itself must keep its traceback.
    monkeypatch.setattr(launcher, "_import_gui", _failing_import(exc))
    with pytest.raises(type(exc)):
        launcher.main()


def test_starts_the_gui(monkeypatch):
    started = []
    monkeypatch.setattr(launcher, "_import_gui", lambda: lambda: started.append(True))
    launcher.main()
    assert started == [True]


def test_a_real_missing_pyqt6_is_recognised():
    # A fresh interpreter: this process has PyQt6.QtCore loaded already, which
    # would let the GUI import succeed (and start the event loop).
    code = "import sys; sys.modules['PyQt6'] = None; from metis_test_runner import launcher; launcher.main()"
    cp = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "PYTHONPATH": str(Path(launcher.__file__).parents[1])},
    )
    assert cp.returncode == 1
    assert "needs PyQt6" in cp.stderr
