"""End-to-end: click the Install tab's button and check the pipeline really installed.

Skipped unless MTR_INSTALL_TAB_E2E is set (the install-tab CI job sets it): this
needs the network and pip-installs the pipeline into the current interpreter.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from PyQt6.QtCore import QEventLoop, Qt
from PyQt6.QtTest import QTest

from metis_test_runner import gui, installer

pytestmark = pytest.mark.skipif(
    not os.environ.get("MTR_INSTALL_TAB_E2E"), reason="MTR_INSTALL_TAB_E2E not set (installs for real)"
)


def _run(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


@pytest.fixture(scope="module")
def installed(qapp, tmp_path_factory):
    """Run the install once from the tab; conftest already redirects the data dir."""
    # HOME too, for ~/.edps: monkeypatch is function-scoped, hence MonkeyPatch.context().
    with pytest.MonkeyPatch.context() as mp:
        home = tmp_path_factory.mktemp("home")
        mp.setenv("HOME", str(home))

        tab = gui.InstallTab()
        QTest.mouseClick(tab.install_btn, Qt.MouseButton.LeftButton)
        assert tab._worker is not None, "the click did not start an install"
        assert not tab.install_btn.isEnabled()

        # Every installer step has a timeout, so `done` always arrives.
        results = []
        loop = QEventLoop()
        tab._worker.done.connect(results.append)
        tab._worker.done.connect(lambda _ok: loop.quit())
        loop.exec()
        tab._worker.wait()

        yield tab, results[0], home


def test_install_succeeds(installed):
    tab, success, _home = installed
    log = tab.log_view.toPlainText()
    assert success, log[-4000:]
    assert "Installation complete" in log
    assert tab.install_btn.isEnabled()
    assert gui._installation_complete()


@pytest.mark.parametrize("target", [installer.TARGET_A, installer.TARGET_B], ids=lambda t: t.name)
def test_repos_cloned(installed, target):
    cp = _run("git", "-C", str(target), "rev-parse", "HEAD")
    assert cp.returncode == 0, cp.stderr


def test_edps_configured(installed):
    _tab, _success, home = installed
    props = (home / ".edps" / "application.properties").read_text()
    workflows = installer.TARGET_A / "metisp" / "workflows"
    assert f"workflow_dir={workflows}\n" in props
    assert workflows.is_dir()


def test_dependencies_consistent(installed):
    cp = _run(sys.executable, "-m", "pip", "check")
    assert cp.returncode == 0, cp.stdout


def test_packages_import(installed):
    # A fresh process: editable installs are not importable in this one.
    # Not scopesim_templates: importing it downloads from scopesim.univie.ac.at.
    cp = _run(sys.executable, "-c", "import adari_core, cpl, edps, pyesorex, pymetis, scopesim")
    assert cp.returncode == 0, cp.stderr


def test_pyesorex_finds_the_recipes(installed):
    # Fails if the derived environment no longer points at the cloned recipes.
    cp = _run(str(Path(sys.executable).parent / "mtr-exec"), "pyesorex", "--recipes")
    assert cp.returncode == 0, cp.stderr
    assert "metis_" in cp.stdout, cp.stdout
