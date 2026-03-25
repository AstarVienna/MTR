"""
Unit tests for gui.py.

Covers:
  - append_log helper
  - MainWindow / tab construction
  - Runner-dependent field visibility
  - _build_cmd_args argument construction
  - InstallWorker._patch_edps_config regex patching
  - InstallWorker._write_env file content

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

    def test_window_has_two_tabs(self, qapp):
        from PyQt6.QtWidgets import QTabWidget
        from gui import MainWindow
        win = MainWindow()
        tabs = win.findChild(QTabWidget)
        assert tabs is not None
        assert tabs.count() == 2
        win.close()

    def test_tab_labels(self, qapp):
        from PyQt6.QtWidgets import QTabWidget
        from gui import MainWindow
        win = MainWindow()
        tabs = win.findChild(QTabWidget)
        labels = [tabs.tabText(i) for i in range(tabs.count())]
        assert "Install" in labels
        assert "Run" in labels
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
        tab.cores_spin.setValue(8)
        args = tab._build_cmd_args()
        assert "--cores" in args
        assert args[args.index("--cores") + 1] == "8"

    def test_calib_flag_when_checked(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.calib_cb.setChecked(True)
        assert "--calib" in tab._build_cmd_args()

    def test_calib_flag_absent_when_unchecked(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.calib_cb.setChecked(False)
        assert "--calib" not in tab._build_cmd_args()

    def test_small_flag_when_checked(self, qapp):
        tab = self._tab_with_yaml(qapp, "obs.yaml")
        tab.small_cb.setChecked(True)
        assert "--small" in tab._build_cmd_args()

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
    def _make_worker(self, qapp):
        from gui import InstallWorker
        return InstallWorker()

    def test_patches_port(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        edps = tmp_path / ".edps"
        edps.mkdir()
        (edps / "application.properties").write_text("port=5000\n")
        self._make_worker(qapp)._patch_edps_config()
        assert "port=4444" in (edps / "application.properties").read_text()

    def test_patches_workflow_dir(self, qapp, tmp_path, monkeypatch):
        from gui import TARGET_A
        monkeypatch.setenv("HOME", str(tmp_path))
        edps = tmp_path / ".edps"
        edps.mkdir()
        (edps / "application.properties").write_text("workflow_dir=/old/path\n")
        self._make_worker(qapp)._patch_edps_config()
        content = (edps / "application.properties").read_text()
        assert f"{TARGET_A}/metisp/workflows" in content

    def test_patches_esorex_path(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        edps = tmp_path / ".edps"
        edps.mkdir()
        (edps / "application.properties").write_text("esorex_path=esorex\n")
        self._make_worker(qapp)._patch_edps_config()
        assert "esorex_path=pyesorex" in (edps / "application.properties").read_text()

    def test_preserves_unrelated_lines(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        edps = tmp_path / ".edps"
        edps.mkdir()
        (edps / "application.properties").write_text(
            "port=5000\nsome.other.key=value\nesorex_path=esorex\n"
        )
        self._make_worker(qapp)._patch_edps_config()
        assert "some.other.key=value" in (edps / "application.properties").read_text()

    def test_raises_when_file_missing(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        # No .edps directory created
        with pytest.raises(RuntimeError, match="not found"):
            self._make_worker(qapp)._patch_edps_config()

    def test_patches_all_three_keys_at_once(self, qapp, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        edps = tmp_path / ".edps"
        edps.mkdir()
        (edps / "application.properties").write_text(
            "port=5000\nworkflow_dir=/old\nesorex_path=esorex\n"
        )
        self._make_worker(qapp)._patch_edps_config()
        content = (edps / "application.properties").read_text()
        assert "port=4444" in content
        assert "esorex_path=pyesorex" in content
        assert "workflow_dir=/old" not in content


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
