"""
Unit tests for gui.py.

Covers:
  - append_log helper
  - MainWindow / tab construction (3 tabs: Run, Install, Archive)
  - Runner-dependent field visibility
  - _build_cmd_args argument construction (including auto-fetch flag)
  - InstallWorker._patch_edps_config regex patching (including association_preference)
  - InstallWorker._write_env file content
  - ArchiveTab construction

All tests run with QT_QPA_PLATFORM=offscreen (set in conftest.py) so no
display is required.
"""

import sys
import pytest
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_run_tab(qapp):
    from gui import RunTab
    return RunTab()


# ---------------------------------------------------------------------------
# append_log
# ---------------------------------------------------------------------------

class TestAppendLog:
    def test_plain_text_appended(self, qapp):
        from PyQt6.QtWidgets import QTextEdit
        from gui import log_append
        w = QTextEdit()
        log_append(w, "hello world")
        assert "hello world" in w.toPlainText()

    def test_multiple_appends_accumulate(self, qapp):
        from PyQt6.QtWidgets import QTextEdit
        from gui import log_append
        w = QTextEdit()
        log_append(w, "line one\n")
        log_append(w, "line two\n")
        text = w.toPlainText()
        assert "line one" in text
        assert "line two" in text

    def test_coloured_text_appended(self, qapp):
        from PyQt6.QtWidgets import QTextEdit
        from gui import log_append
        w = QTextEdit()
        log_append(w, "error message", "red")
        assert "error message" in w.toPlainText()

    def test_empty_colour_treated_as_no_colour(self, qapp):
        from PyQt6.QtWidgets import QTextEdit
        from gui import log_append
        w = QTextEdit()
        log_append(w, "neutral", "")   # empty string — no crash, text still added
        assert "neutral" in w.toPlainText()


# ---------------------------------------------------------------------------
# MainWindow / tab construction
# ---------------------------------------------------------------------------

class TestWindowConstruction:
    def test_main_window_creates(self, qapp):
        from gui import MainWindow
        win = MainWindow()
        assert win is not None
        win.close()

    def test_window_has_three_tabs(self, qapp):
        from PyQt6.QtWidgets import QTabWidget
        from gui import MainWindow
        win = MainWindow()
        tabs = win.findChild(QTabWidget)
        assert tabs is not None
        assert tabs.count() == 3
        win.close()

    def test_tab_labels(self, qapp):
        from PyQt6.QtWidgets import QTabWidget
        from gui import MainWindow
        win = MainWindow()
        tabs = win.findChild(QTabWidget)
        labels = [tabs.tabText(i) for i in range(tabs.count())]
        assert "Install" in labels
        assert "Run" in labels
        assert "Archive" in labels
        win.close()


# ---------------------------------------------------------------------------
# Runner field visibility
# ---------------------------------------------------------------------------

class TestRunnerFieldVisibility:
    def test_metapkg_shows_meta_pkg_row_hides_container(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("metapkg")
        assert not tab.meta_pkg_row.isHidden()
        assert tab.container_row.isHidden()

    def test_native_hides_both_conditional_rows(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("native")
        assert tab.meta_pkg_row.isHidden()
        assert tab.container_row.isHidden()

    def test_docker_shows_container_hides_meta_pkg(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("docker")
        assert not tab.container_row.isHidden()
        assert tab.meta_pkg_row.isHidden()

    def test_podman_shows_container_hides_meta_pkg(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("podman")
        assert not tab.container_row.isHidden()
        assert tab.meta_pkg_row.isHidden()

    def test_switching_runner_updates_visibility(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("metapkg")
        assert not tab.meta_pkg_row.isHidden()
        tab.runner_combo.setCurrentText("docker")
        assert tab.meta_pkg_row.isHidden()
        assert not tab.container_row.isHidden()


# ---------------------------------------------------------------------------
# Instrument packages placeholder text
# ---------------------------------------------------------------------------

class TestInstPkgsPlaceholder:
    def test_metapkg_runner_placeholder_shows_resolved_path(self, qapp):
        import gui
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("metapkg")
        assert tab.inst_edit.placeholderText() == str(gui.REPO_ROOT / "inst_pkgs")

    def test_native_runner_placeholder_shows_resolved_path(self, qapp):
        import gui
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("native")
        assert tab.inst_edit.placeholderText() == str(gui.REPO_ROOT / "inst_pkgs")

    def test_docker_runner_placeholder_indicates_container(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("docker")
        assert "container" in tab.inst_edit.placeholderText()

    def test_podman_runner_placeholder_indicates_container(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("podman")
        assert "container" in tab.inst_edit.placeholderText()

    def test_switching_runner_updates_placeholder(self, qapp):
        import gui
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("metapkg")
        assert tab.inst_edit.placeholderText() == str(gui.REPO_ROOT / "inst_pkgs")
        tab.runner_combo.setCurrentText("docker")
        assert "container" in tab.inst_edit.placeholderText()


# ---------------------------------------------------------------------------
# _build_cmd_args
# ---------------------------------------------------------------------------

class TestBuildCmdArgs:
    def _tab_with_yaml(self, qapp, *paths):
        tab = _make_run_tab(qapp)
        for p in paths:
            tab.yaml_list.addItem(p)
        return tab

    def test_yaml_files_appear_in_args(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs1.yaml", "obs2.yaml")
        args = tab._build_cmd_args()
        assert "obs1.yaml" in args
        assert "obs2.yaml" in args

    def test_yaml_files_are_last(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        args = tab._build_cmd_args()
        assert args[-1] == "obs.yaml"

    def test_runner_always_included(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.runner_combo.setCurrentText("native")
        args = tab._build_cmd_args()
        assert "--runner" in args
        assert args[args.index("--runner") + 1] == "native"

    def test_cores_always_included(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.cores_spin.setValue(2)
        args = tab._build_cmd_args()
        assert "--cores" in args
        assert args[args.index("--cores") + 1] == "2"

    def test_calib_flag_when_checked(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.calib_cb.setChecked(True)
        assert "--calib" in tab._build_cmd_args()

    def test_calib_flag_absent_when_unchecked(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.calib_cb.setChecked(False)
        assert "--calib" not in tab._build_cmd_args()

    def test_no_pipeline_flag_for_sim_only_mode(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.rb_sim_only.setChecked(True)
        args = tab._build_cmd_args()
        assert "--no-pipeline" in args
        assert "--no-sim" not in args

    def test_no_sim_flag_for_pipeline_only_mode(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.rb_pipe_only.setChecked(True)
        args = tab._build_cmd_args()
        assert "--no-sim" in args
        assert "--no-pipeline" not in args

    def test_neither_flag_for_both_mode(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.rb_both.setChecked(True)
        args = tab._build_cmd_args()
        assert "--no-sim" not in args
        assert "--no-pipeline" not in args

    def test_output_dir_included_when_set(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.output_edit.setText("/tmp/myrun")
        args = tab._build_cmd_args()
        assert "-o" in args
        assert args[args.index("-o") + 1] == "/tmp/myrun"

    def test_output_dir_omitted_when_empty(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.output_edit.setText("")
        assert "-o" not in tab._build_cmd_args()

    def test_container_included_for_docker_runner(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.runner_combo.setCurrentText("docker")
        tab.container_edit.setText("my-container")
        args = tab._build_cmd_args()
        assert "--container" in args
        assert args[args.index("--container") + 1] == "my-container"

    def test_container_omitted_for_native_runner(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.runner_combo.setCurrentText("native")
        tab.container_edit.setText("ignored")
        assert "--container" not in tab._build_cmd_args()

    def test_meta_pkg_included_for_metapkg_runner(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.runner_combo.setCurrentText("metapkg")
        tab.meta_pkg_edit.setText("/opt/meta")
        args = tab._build_cmd_args()
        assert "--meta-pkg" in args
        assert args[args.index("--meta-pkg") + 1] == "/opt/meta"

    def test_meta_pkg_omitted_for_native_runner(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.runner_combo.setCurrentText("native")
        tab.meta_pkg_edit.setText("/opt/meta")
        assert "--meta-pkg" not in tab._build_cmd_args()

    def test_simulations_dir_included_when_set(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.sim_dir_edit.setText("/data/sims")
        args = tab._build_cmd_args()
        assert "--simulations-dir" in args
        assert args[args.index("--simulations-dir") + 1] == "/data/sims"

    def test_inst_pkgs_included_when_set(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.inst_edit.setText("/data/inst_pkgs")
        args = tab._build_cmd_args()
        assert "--inst-pkgs" in args
        assert args[args.index("--inst-pkgs") + 1] == "/data/inst_pkgs"

    def test_multiple_yaml_files_all_present(self, qapp):
        tab = self._tab_with_yaml(qapp, "a.yaml", "b.yaml", "c.yaml")
        args = tab._build_cmd_args()
        assert args[-3:] == ["a.yaml", "b.yaml", "c.yaml"]


# ---------------------------------------------------------------------------
# InstallWorker._patch_edps_config
# ---------------------------------------------------------------------------

class TestPatchEdpsConfig:
    # All four keys must be present for _patch_edps_config to succeed — it
    # raises if any pattern matches zero times.
    FULL_PROPS = (
        "port=5000\nworkflow_dir=/old\nesorex_path=esorex\n"
        "association_preference=raw_per_quality_level\n"
    )

    def _make_worker(self, qapp):
        from gui import InstallWorker
        return InstallWorker()

    def _seed(self, tmp_path, content):
        edps = tmp_path / ".edps"
        edps.mkdir()
        (edps / "application.properties").write_text(content)
        return edps / "application.properties"

    def test_patches_port(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker(qapp)._patch_edps_config()
        assert "port=4444" in props.read_text()

    def test_patches_workflow_dir(self, qapp, tmp_path, monkeypatch):
        from gui import TARGET_A
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker(qapp)._patch_edps_config()
        assert f"{TARGET_A}/metisp/workflows" in props.read_text()

    def test_patches_esorex_path(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker(qapp)._patch_edps_config()
        assert "esorex_path=pyesorex" in props.read_text()

    def test_preserves_unrelated_lines(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(
            tmp_path,
            "port=5000\nsome.other.key=value\n"
            "workflow_dir=/old\nesorex_path=esorex\n"
            "association_preference=raw_per_quality_level\n",
        )
        self._make_worker(qapp)._patch_edps_config()
        assert "some.other.key=value" in props.read_text()

    def test_raises_when_file_missing(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        # No .edps directory created
        with pytest.raises(RuntimeError, match="not found"):
            self._make_worker(qapp)._patch_edps_config()

    def test_patches_all_three_keys_at_once(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker(qapp)._patch_edps_config()
        content = props.read_text()
        assert "port=4444" in content
        assert "esorex_path=pyesorex" in content
        assert "workflow_dir=/old" not in content

    def test_patches_association_preference(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker(qapp)._patch_edps_config()
        assert "association_preference=master_per_quality_level" in props.read_text()

    def test_raises_when_port_key_absent(self, qapp, tmp_path, monkeypatch):
        # If EDPS drifts its config format, we want a loud error that names
        # the missing key, not a silent no-op that rewrites the file unchanged.
        monkeypatch.setenv("HOME", str(tmp_path))
        self._seed(
            tmp_path,
            "workflow_dir=/old\nesorex_path=esorex\nassociation_preference=raw\n",
        )
        with pytest.raises(RuntimeError, match="port"):
            self._make_worker(qapp)._patch_edps_config()

    def test_raises_when_workflow_dir_key_absent(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        self._seed(
            tmp_path,
            "port=5000\nesorex_path=esorex\nassociation_preference=raw\n",
        )
        with pytest.raises(RuntimeError, match="workflow_dir"):
            self._make_worker(qapp)._patch_edps_config()


# ---------------------------------------------------------------------------
# InstallWorker._write_env
# ---------------------------------------------------------------------------

class TestWriteEnv:
    def _make_worker(self, qapp):
        from gui import InstallWorker
        return InstallWorker()

    def test_env_file_created(self, qapp, tmp_path, monkeypatch):
        import gui
        monkeypatch.setattr(gui, "META_PKG", tmp_path)
        self._make_worker(qapp)._write_env()
        assert (tmp_path / ".env").exists()

    def test_env_contains_pythonpath(self, qapp, tmp_path, monkeypatch):
        import gui
        monkeypatch.setattr(gui, "META_PKG", tmp_path)
        self._make_worker(qapp)._write_env()
        assert "PYTHONPATH" in (tmp_path / ".env").read_text()

    def test_env_contains_pycpl_recipe_dir(self, qapp, tmp_path, monkeypatch):
        import gui
        monkeypatch.setattr(gui, "META_PKG", tmp_path)
        self._make_worker(qapp)._write_env()
        assert "PYCPL_RECIPE_DIR" in (tmp_path / ".env").read_text()

    def test_env_contains_pyesorex_plugin_dir(self, qapp, tmp_path, monkeypatch):
        import gui
        monkeypatch.setattr(gui, "META_PKG", tmp_path)
        self._make_worker(qapp)._write_env()
        assert "PYESOREX_PLUGIN_DIR" in (tmp_path / ".env").read_text()

    def test_env_paths_reference_target_a(self, qapp, tmp_path, monkeypatch):
        import gui
        monkeypatch.setattr(gui, "META_PKG", tmp_path)
        self._make_worker(qapp)._write_env()
        content = (tmp_path / ".env").read_text()
        assert str(gui.TARGET_A) in content


# ---------------------------------------------------------------------------
# InstallWorker._clone_or_update — submodule (.git as file) handling
# ---------------------------------------------------------------------------

class TestCloneOrUpdateSubmodule:
    def _make_worker(self, qapp):
        from gui import InstallWorker
        return InstallWorker()

    def test_submodule_checkout_takes_update_branch(self, qapp, tmp_path, monkeypatch):
        # Submodules store .git as a FILE pointing at the parent's
        # .git/modules/<name>/, not a directory. The old is_dir() check would
        # misclassify this as "not a git repo" and refuse to update.
        target = tmp_path / "submodule_checkout"
        target.mkdir()
        (target / ".git").write_text("gitdir: ../.git/modules/submodule_checkout\n")
        (target / "README.md").write_text("content\n")  # non-empty

        worker = self._make_worker(qapp)
        invoked = []
        # Replace both _run (fetch) and subprocess.run (pull) so nothing hits
        # the real git binary.
        monkeypatch.setattr(worker, "_run", lambda cmd, **kw: invoked.append(cmd))
        from unittest.mock import patch as mock_patch, MagicMock
        fake_pull = MagicMock(return_value=MagicMock(stdout="", stderr=""))
        with mock_patch("gui.subprocess.run", fake_pull):
            worker._clone_or_update("http://example.invalid/x.git", target)

        # Took the fetch path (via _run), not the clone path.
        assert invoked, "_clone_or_update should have called _run for fetch"
        assert "fetch" in invoked[0]
        # Did not raise the "not a git repo and is not empty" error.


# ---------------------------------------------------------------------------
# Auto-fetch checkbox in RunTab
# ---------------------------------------------------------------------------

class TestAutoFetchCheckbox:
    def test_auto_fetch_flag_when_checked(self, qapp):
        tab = _make_run_tab(qapp)
        tab.yaml_list.addItem("obs.yaml")
        tab.auto_fetch_cb.setChecked(True)
        args = tab._build_cmd_args()
        assert "--auto-fetch-calibrations" in args

    def test_auto_fetch_flag_absent_when_unchecked(self, qapp):
        tab = _make_run_tab(qapp)
        tab.yaml_list.addItem("obs.yaml")
        tab.auto_fetch_cb.setChecked(False)
        args = tab._build_cmd_args()
        assert "--auto-fetch-calibrations" not in args

    def test_auto_fetch_unchecked_by_default(self, qapp):
        tab = _make_run_tab(qapp)
        assert not tab.auto_fetch_cb.isChecked()


# ---------------------------------------------------------------------------
# ArchiveTab construction
# ---------------------------------------------------------------------------

class TestArchiveTab:
    def test_archive_tab_creates(self, qapp):
        from gui import ArchiveTab
        tab = ArchiveTab()
        assert tab is not None

    def test_archive_tab_has_stacked_widget(self, qapp):
        from PyQt6.QtWidgets import QStackedWidget
        from gui import ArchiveTab
        tab = ArchiveTab()
        stack = tab.findChild(QStackedWidget)
        assert stack is not None
        assert stack.count() == 2
