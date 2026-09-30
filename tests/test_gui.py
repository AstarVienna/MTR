"""
Unit tests for gui.py.

Covers:
  - append_log helper
  - MainWindow / tab construction (3 tabs: Run, Install, Archive)
  - Runner-dependent field visibility
  - _build_cmd_args argument construction (including auto-fetch flag)
  - InstallTab ref combos, QSettings round-trip, and the dirty-tree dialog
    (the install logic itself is covered in test_installer.py)
  - ArchiveTab construction

All tests run with QT_QPA_PLATFORM=offscreen (set in conftest.py) so no
display is required.
"""

import subprocess
from pathlib import Path

from metis_test_runner import gui, installer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_run_tab(qapp):
    from metis_test_runner.gui import RunTab

    return RunTab()


def _argv_text(args):
    """argv as a string with absolute paths dropped.

    tmp_path names contain the very words the assertions look for (a test
    called test_unshallow_… produces a dir with "unshallow" in it), so paths
    must never take part in matching.
    """
    return " ".join(str(a) for a in args if not str(a).startswith("/"))


def _cp(stdout="", returncode=0, stderr=""):
    """A CompletedProcess standing in for one gui._git() call."""
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def _fake_git(responses, recorder=None):
    """Replacement for gui._git driven by a {substring: CompletedProcess} map.

    The first key that appears anywhere in the argv wins; anything unmatched
    comes back as a successful empty result. Pass *recorder* to capture argv.
    """

    def fake(args, cwd=None, timeout=30):
        if recorder is not None:
            recorder.append(list(args))
        joined = _argv_text(args)
        for needle, result in responses.items():
            if needle in joined:
                return result
        return _cp("")

    return fake


# ---------------------------------------------------------------------------
# append_log
# ---------------------------------------------------------------------------


class TestAppendLog:
    def test_plain_text_appended(self, qapp):
        from PyQt6.QtWidgets import QTextEdit

        from metis_test_runner.gui import log_append

        w = QTextEdit()
        log_append(w, "hello world")
        assert "hello world" in w.toPlainText()

    def test_multiple_appends_accumulate(self, qapp):
        from PyQt6.QtWidgets import QTextEdit

        from metis_test_runner.gui import log_append

        w = QTextEdit()
        log_append(w, "line one\n")
        log_append(w, "line two\n")
        text = w.toPlainText()
        assert "line one" in text
        assert "line two" in text

    def test_coloured_text_appended(self, qapp):
        from PyQt6.QtWidgets import QTextEdit

        from metis_test_runner.gui import log_append

        w = QTextEdit()
        log_append(w, "error message", "red")
        assert "error message" in w.toPlainText()

    def test_empty_colour_treated_as_no_colour(self, qapp):
        from PyQt6.QtWidgets import QTextEdit

        from metis_test_runner.gui import log_append

        w = QTextEdit()
        log_append(w, "neutral", "")  # empty string — no crash, text still added
        assert "neutral" in w.toPlainText()


# ---------------------------------------------------------------------------
# MainWindow / tab construction
# ---------------------------------------------------------------------------


class TestWindowConstruction:
    def test_main_window_creates(self, qapp):
        from metis_test_runner.gui import MainWindow

        win = MainWindow()
        assert win is not None
        win.close()

    def test_window_has_three_tabs(self, qapp):
        from PyQt6.QtWidgets import QTabWidget

        from metis_test_runner.gui import MainWindow

        win = MainWindow()
        tabs = win.findChild(QTabWidget)
        assert tabs is not None
        assert tabs.count() == 3
        win.close()

    def test_tab_labels(self, qapp):
        from PyQt6.QtWidgets import QTabWidget

        from metis_test_runner.gui import MainWindow

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
    def test_default_hides_container(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("default")
        assert tab.container_row.isHidden()

    def test_native_hides_container(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("native")
        assert tab.container_row.isHidden()

    def test_docker_shows_container(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("docker")
        assert not tab.container_row.isHidden()

    def test_podman_shows_container(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("podman")
        assert not tab.container_row.isHidden()

    def test_switching_runner_updates_visibility(self, qapp):
        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("default")
        assert tab.container_row.isHidden()
        tab.runner_combo.setCurrentText("docker")
        assert not tab.container_row.isHidden()


# ---------------------------------------------------------------------------
# Instrument packages placeholder text
# ---------------------------------------------------------------------------


class TestInstPkgsPlaceholder:
    def test_default_runner_placeholder_shows_resolved_path(self, qapp):
        from metis_test_runner import gui

        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("default")
        assert tab.inst_edit.placeholderText() == str(gui.REPO_ROOT / "inst_pkgs")

    def test_native_runner_placeholder_shows_resolved_path(self, qapp):
        from metis_test_runner import gui

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
        from metis_test_runner import gui

        tab = _make_run_tab(qapp)
        tab.runner_combo.setCurrentText("default")
        assert tab.inst_edit.placeholderText() == str(gui.REPO_ROOT / "inst_pkgs")
        tab.runner_combo.setCurrentText("docker")
        assert "container" in tab.inst_edit.placeholderText()


# ---------------------------------------------------------------------------
# _build_cmd_args
# ---------------------------------------------------------------------------


class TestBuildCmdArgs:
    def _tab_with_inputs(self, qapp, *paths):
        """Build a RunTab and populate input_list with the given paths.
        Caller can mix .yaml / .yml / .csv extensions freely."""
        tab = _make_run_tab(qapp)
        for p in paths:
            tab.input_list.addItem(p)
        return tab

    def test_yaml_files_appear_in_args(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs1.yaml", "obs2.yaml")
        args = tab._build_cmd_args()
        assert "obs1.yaml" in args
        assert "obs2.yaml" in args

    def test_csv_files_appear_in_args(self, qapp):
        tab = self._tab_with_inputs(qapp, "seq1.csv", "seq2.csv")
        args = tab._build_cmd_args()
        assert "seq1.csv" in args
        assert "seq2.csv" in args

    def test_mixed_yaml_and_csv_preserved_in_order(self, qapp):
        tab = self._tab_with_inputs(qapp, "a.yaml", "b.csv", "c.yaml")
        args = tab._build_cmd_args()
        # The three input paths should appear in the order they were added,
        # tail of the args list (positional args go last).
        assert args[-3:] == ["a.yaml", "b.csv", "c.yaml"]

    def test_inputs_are_last(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.csv")
        args = tab._build_cmd_args()
        assert args[-1] == "obs.csv"

    def test_runner_always_included(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.runner_combo.setCurrentText("native")
        args = tab._build_cmd_args()
        assert "--runner" in args
        assert args[args.index("--runner") + 1] == "native"

    def test_cores_always_included(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.cores_spin.setValue(2)
        args = tab._build_cmd_args()
        assert "--cores" in args
        assert args[args.index("--cores") + 1] == "2"

    def test_csv_lines_omitted_when_both_unset(self, qapp):
        tab = self._tab_with_inputs(qapp, "seq.csv")
        # Default spin values are 0 (= unset).
        assert "--csv-lines" not in tab._build_cmd_args()

    def test_csv_lines_both_bounds(self, qapp):
        tab = self._tab_with_inputs(qapp, "seq.csv")
        tab.csv_start_spin.setValue(6)
        tab.csv_end_spin.setValue(12)
        args = tab._build_cmd_args()
        assert args[args.index("--csv-lines") + 1] == "6:12"

    def test_csv_lines_open_end(self, qapp):
        tab = self._tab_with_inputs(qapp, "seq.csv")
        tab.csv_start_spin.setValue(6)
        args = tab._build_cmd_args()
        assert args[args.index("--csv-lines") + 1] == "6:"

    def test_csv_lines_open_start(self, qapp):
        tab = self._tab_with_inputs(qapp, "seq.csv")
        tab.csv_end_spin.setValue(12)
        args = tab._build_cmd_args()
        assert args[args.index("--csv-lines") + 1] == ":12"

    def test_calib_flag_when_checked(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.calib_cb.setChecked(True)
        args = tab._build_cmd_args()
        assert "--calib" in args
        assert args[args.index("--calib") + 1] == "1"

    def test_calib_flag_zero_when_unchecked(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.calib_cb.setChecked(False)
        args = tab._build_cmd_args()
        assert "--calib" in args
        assert args[args.index("--calib") + 1] == "0"

    def test_static_flag_when_checked(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.static_cb.setChecked(True)
        args = tab._build_cmd_args()
        assert "--static" in args
        assert args[args.index("--static") + 1] == "1"

    def test_static_flag_zero_when_unchecked(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.static_cb.setChecked(False)
        args = tab._build_cmd_args()
        assert "--static" in args
        assert args[args.index("--static") + 1] == "0"

    def test_static_checked_by_default(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        assert tab.static_cb.isChecked()

    def test_no_pipeline_flag_for_sim_only_mode(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.rb_sim_only.setChecked(True)
        args = tab._build_cmd_args()
        assert "--no-pipeline" in args
        assert "--no-sim" not in args

    def test_no_sim_flag_for_pipeline_only_mode(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.rb_pipe_only.setChecked(True)
        args = tab._build_cmd_args()
        assert "--no-sim" in args
        assert "--no-pipeline" not in args

    def test_neither_flag_for_both_mode(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.rb_both.setChecked(True)
        args = tab._build_cmd_args()
        assert "--no-sim" not in args
        assert "--no-pipeline" not in args

    def test_output_dir_included_when_set(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.output_edit.setText("/tmp/myrun")
        args = tab._build_cmd_args()
        assert "-o" in args
        assert args[args.index("-o") + 1] == "/tmp/myrun"

    def test_output_dir_omitted_when_empty(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.output_edit.setText("")
        assert "-o" not in tab._build_cmd_args()

    def test_container_included_for_docker_runner(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.runner_combo.setCurrentText("docker")
        tab.container_edit.setText("my-container")
        args = tab._build_cmd_args()
        assert "--container" in args
        assert args[args.index("--container") + 1] == "my-container"

    def test_container_omitted_for_native_runner(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.runner_combo.setCurrentText("native")
        tab.container_edit.setText("ignored")
        assert "--container" not in tab._build_cmd_args()

    def test_meta_pkg_flag_never_emitted(self, qapp):
        # The --meta-pkg flag was removed when the metapkg runner was retired.
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        for runner in ("default", "native", "docker", "podman"):
            tab.runner_combo.setCurrentText(runner)
            assert "--meta-pkg" not in tab._build_cmd_args()

    def test_simulations_dir_included_when_set(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.sim_dir_edit.setText("/data/sims")
        args = tab._build_cmd_args()
        assert "--simulations-dir" in args
        assert args[args.index("--simulations-dir") + 1] == "/data/sims"

    def test_inst_pkgs_included_when_set(self, qapp):
        tab = self._tab_with_inputs(qapp, "obs.yaml")
        tab.inst_edit.setText("/data/inst_pkgs")
        args = tab._build_cmd_args()
        assert "--inst-pkgs" in args
        assert args[args.index("--inst-pkgs") + 1] == "/data/inst_pkgs"

    def test_multiple_input_files_all_present(self, qapp):
        tab = self._tab_with_inputs(qapp, "a.yaml", "b.yaml", "c.yaml")
        args = tab._build_cmd_args()
        assert args[-3:] == ["a.yaml", "b.yaml", "c.yaml"]

    def test_workflow_arg_never_emitted(self, qapp):
        """The --workflow override was removed; the GUI never emits it."""
        tab = self._tab_with_inputs(qapp, "obs.csv")
        assert "--workflow" not in tab._build_cmd_args()

    # --- Input status line ---

    def test_input_status_counts_yaml_and_csv(self, qapp):
        tab = self._tab_with_inputs(qapp, "a.yaml", "b.csv", "c.yml")
        tab._refresh_input_status()
        assert tab.input_status.text() == "2 YAML  ·  1 CSV"

    def test_input_status_zero_when_empty(self, qapp):
        tab = _make_run_tab(qapp)
        assert tab.input_status.text() == "0 YAML  ·  0 CSV"


# ---------------------------------------------------------------------------
# Auto-fetch checkbox in RunTab
# ---------------------------------------------------------------------------


class TestAutoFetchCheckbox:
    def test_auto_fetch_flag_when_checked(self, qapp):
        tab = _make_run_tab(qapp)
        tab.input_list.addItem("obs.yaml")
        tab.auto_fetch_cb.setChecked(True)
        args = tab._build_cmd_args()
        assert "--auto-fetch-calibrations" in args

    def test_auto_fetch_flag_absent_when_unchecked(self, qapp):
        tab = _make_run_tab(qapp)
        tab.input_list.addItem("obs.yaml")
        tab.auto_fetch_cb.setChecked(False)
        args = tab._build_cmd_args()
        assert "--auto-fetch-calibrations" not in args

    def test_auto_fetch_unchecked_by_default(self, qapp):
        tab = _make_run_tab(qapp)
        assert not tab.auto_fetch_cb.isChecked()


class TestCsvToYamlCheckbox:
    def test_flag_when_checked(self, qapp):
        tab = _make_run_tab(qapp)
        tab.input_list.addItem("seq.csv")
        tab.csv_to_yaml_cb.setChecked(True)
        assert "--csv-to-yaml" in tab._build_cmd_args()

    def test_flag_absent_when_unchecked(self, qapp):
        tab = _make_run_tab(qapp)
        tab.input_list.addItem("seq.csv")
        tab.csv_to_yaml_cb.setChecked(False)
        assert "--csv-to-yaml" not in tab._build_cmd_args()

    def test_unchecked_by_default(self, qapp):
        tab = _make_run_tab(qapp)
        assert not tab.csv_to_yaml_cb.isChecked()

    def test_disables_mode_and_calib_options_when_checked(self, qapp):
        tab = _make_run_tab(qapp)
        tab.csv_to_yaml_cb.setChecked(True)
        for w in (
            tab.rb_both,
            tab.rb_sim_only,
            tab.rb_pipe_only,
            tab.calib_cb,
            tab.static_cb,
            tab.auto_fetch_cb,
            tab.cores_spin,
        ):
            assert not w.isEnabled()
        tab.csv_to_yaml_cb.setChecked(False)
        for w in (
            tab.rb_both,
            tab.rb_sim_only,
            tab.rb_pipe_only,
            tab.calib_cb,
            tab.static_cb,
            tab.auto_fetch_cb,
            tab.cores_spin,
        ):
            assert w.isEnabled()


# ---------------------------------------------------------------------------
# ArchiveTab construction
# ---------------------------------------------------------------------------


class TestArchiveTab:
    def test_archive_tab_creates(self, qapp):
        from metis_test_runner.gui import ArchiveTab

        tab = ArchiveTab()
        assert tab is not None

    def test_archive_tab_has_stacked_widget(self, qapp):
        from PyQt6.QtWidgets import QStackedWidget

        from metis_test_runner.gui import ArchiveTab

        tab = ArchiveTab()
        stack = tab.findChild(QStackedWidget)
        assert stack is not None
        assert stack.count() == 3


# ---------------------------------------------------------------------------
# ArchiveTab — upload page (page 2)
# ---------------------------------------------------------------------------


class TestArchiveTabUploadPage:
    """Exercise the Page 2 staging table, add/remove handlers, and upload
    gating on unresolved rows.  Classification is stubbed out so the tests
    don't depend on astropy."""

    def _make_tab(self, qapp, monkeypatch, classify=None):
        """Create an ArchiveTab with classify_fits_file patched."""
        from metis_test_runner import run_metis

        if classify is None:
            classify = lambda _p: "LM_FLAT_LAMP_RAW"
        monkeypatch.setattr(run_metis, "classify_fits_file", classify)
        from metis_test_runner.gui import ArchiveTab

        return ArchiveTab()

    def test_page_upload_table_has_three_columns(self, qapp, monkeypatch):
        tab = self._make_tab(qapp, monkeypatch)
        assert tab._stage_table.columnCount() == 3
        headers = [tab._stage_table.horizontalHeaderItem(c).text() for c in range(3)]
        assert headers == ["Filename", "DataItem class", "Full path"]

    def test_add_staged_file_appends_row_with_auto_class(
        self,
        qapp,
        tmp_path,
        monkeypatch,
    ):
        tab = self._make_tab(qapp, monkeypatch, classify=lambda _p: "LM_FLAT_LAMP_RAW")
        fits = tmp_path / "flat.fits"
        fits.write_bytes(b"")
        tab._add_staged_file(fits)
        assert tab._stage_table.rowCount() == 1
        assert tab._stage_table.item(0, 0).text() == "flat.fits"
        assert tab._stage_table.item(0, 1).text() == "LM_FLAT_LAMP_RAW"
        assert tab._stage_table.item(0, 2).text() == str(fits)

    def test_add_staged_file_unknown_uses_placeholder(
        self,
        qapp,
        tmp_path,
        monkeypatch,
    ):
        tab = self._make_tab(qapp, monkeypatch, classify=lambda _p: None)
        fits = tmp_path / "u.fits"
        fits.write_bytes(b"")
        tab._add_staged_file(fits)
        assert tab._stage_table.item(0, 1).text() == tab._UNKNOWN_CLASS_PLACEHOLDER

    def test_add_staged_file_dedupes_by_full_path(
        self,
        qapp,
        tmp_path,
        monkeypatch,
    ):
        tab = self._make_tab(qapp, monkeypatch)
        fits = tmp_path / "x.fits"
        fits.write_bytes(b"")
        tab._add_staged_file(fits)
        tab._add_staged_file(fits)
        assert tab._stage_table.rowCount() == 1

    def test_on_remove_staged_removes_selected_rows(
        self,
        qapp,
        tmp_path,
        monkeypatch,
    ):
        tab = self._make_tab(qapp, monkeypatch)
        for n in ("a.fits", "b.fits", "c.fits"):
            p = tmp_path / n
            p.write_bytes(b"")
            tab._add_staged_file(p)
        assert tab._stage_table.rowCount() == 3
        tab._stage_table.selectRow(1)
        tab._on_remove_staged()
        remaining = [tab._stage_table.item(r, 0).text() for r in range(tab._stage_table.rowCount())]
        assert remaining == ["a.fits", "c.fits"]

    def test_candidate_class_names_includes_raw_tags_and_masters(
        self,
        qapp,
        monkeypatch,
    ):
        tab = self._make_tab(qapp, monkeypatch)
        candidates = tab._candidate_class_names()
        # Covers raw tags (from DPR_TO_TAG) and master pro_catgs.
        assert "LM_FLAT_LAMP_RAW" in candidates
        assert "DARK_IFU_RAW" in candidates
        assert "MASTER_DARK_2RG" in candidates

    def test_on_upload_blocks_unresolved_rows(
        self,
        qapp,
        tmp_path,
        monkeypatch,
    ):
        from unittest.mock import patch as mock_patch

        tab = self._make_tab(qapp, monkeypatch, classify=lambda _p: None)
        fits = tmp_path / "u.fits"
        fits.write_bytes(b"")
        tab._add_staged_file(fits)
        # Select the one unresolved row, then click Upload.
        tab._stage_table.selectRow(0)
        with mock_patch("metis_test_runner.gui.QMessageBox.warning") as warn:
            tab._on_upload()
        warn.assert_called_once()
        # Upload button stays enabled when the guard fires (no worker dispatched).
        assert tab._upload_btn.isEnabled()

    def test_on_upload_dispatches_worker_when_all_resolved(
        self,
        qapp,
        tmp_path,
        monkeypatch,
    ):
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        tab = self._make_tab(qapp, monkeypatch, classify=lambda _p: "LM_FLAT_LAMP_RAW")
        fits = tmp_path / "ok.fits"
        fits.write_bytes(b"")
        tab._add_staged_file(fits)

        captured = {}

        class StubWorker:
            def __init__(self, entries):
                captured["entries"] = entries
                self.log = MagicMock()
                self.done = MagicMock()
                self.finished = MagicMock()
                self.progress = MagicMock()
                self.log.connect = MagicMock()
                self.done.connect = MagicMock()
                self.finished.connect = MagicMock()
                self.progress.connect = MagicMock()

            def start(self):
                captured["started"] = True

            def isRunning(self):
                return False

            def deleteLater(self):
                pass

        with mock_patch("metis_test_runner.gui.UploadWorker", StubWorker):
            tab._on_upload()
        assert captured.get("started") is True
        assert captured["entries"] == [(fits, "LM_FLAT_LAMP_RAW")]

    def test_page_1_has_continue_to_upload_button(self, qapp, monkeypatch):
        tab = self._make_tab(qapp, monkeypatch)
        assert hasattr(tab, "_to_upload_btn")
        # Clicking the button moves the stack to index 2 (upload page).
        tab._to_upload_btn.click()
        assert tab._stack.currentIndex() == 2


# ---------------------------------------------------------------------------
# UploadWorker
# ---------------------------------------------------------------------------


class TestUploadWorker:
    """Run the worker's body synchronously (call .run() directly) so we can
    observe signal emissions without a Qt event loop."""

    def _connect(self, worker):
        emitted = {"log": [], "progress": [], "done": []}
        worker.log.connect(lambda t, c: emitted["log"].append((t, c)))
        worker.progress.connect(lambda i, n: emitted["progress"].append((i, n)))
        worker.done.connect(lambda ok: emitted["done"].append(ok))
        return emitted

    def test_all_succeed(self, qapp, tmp_path):
        from unittest.mock import patch as mock_patch

        from metis_test_runner.gui import UploadWorker

        entries = [
            (tmp_path / "a.fits", "LM_FLAT_LAMP_RAW"),
            (tmp_path / "b.fits", "DARK_IFU_RAW"),
        ]
        worker = UploadWorker(entries)
        emitted = self._connect(worker)
        with mock_patch("metis_test_runner.archive.upload_file", return_value=True):
            worker.run()
        assert emitted["done"] == [True]
        # Summary log mentions 2/2.
        summary = "".join(t for t, _ in emitted["log"])
        assert "2/2" in summary

    def test_partial_failure_still_reports_success_signal(self, qapp, tmp_path):
        from unittest.mock import patch as mock_patch

        from metis_test_runner.gui import UploadWorker

        entries = [
            (tmp_path / "a.fits", "X"),
            (tmp_path / "b.fits", "Y"),
            (tmp_path / "c.fits", "Z"),
        ]
        worker = UploadWorker(entries)
        emitted = self._connect(worker)
        with mock_patch("metis_test_runner.archive.upload_file", side_effect=[True, False, True]):
            worker.run()
        assert emitted["done"] == [True]
        summary = "".join(t for t, _ in emitted["log"])
        assert "2/3" in summary

    def test_exception_emits_false_done(self, qapp, tmp_path):
        from unittest.mock import patch as mock_patch

        from metis_test_runner.gui import UploadWorker

        worker = UploadWorker([(tmp_path / "a.fits", "X")])
        emitted = self._connect(worker)
        with mock_patch("metis_test_runner.archive.upload_file", side_effect=RuntimeError("boom")):
            worker.run()
        assert emitted["done"] == [False]
        log_text = "".join(t for t, _ in emitted["log"])
        assert "Upload failed" in log_text

    def test_progress_emits_per_file(self, qapp, tmp_path):
        from unittest.mock import patch as mock_patch

        from metis_test_runner.gui import UploadWorker

        entries = [(tmp_path / f"{n}.fits", "X") for n in ("a", "b", "c")]
        worker = UploadWorker(entries)
        emitted = self._connect(worker)
        with mock_patch("metis_test_runner.archive.upload_file", return_value=True):
            worker.run()
        assert emitted["progress"] == [(1, 3), (2, 3), (3, 3)]


# ---------------------------------------------------------------------------
# InstallTab — ref selection UI
# ---------------------------------------------------------------------------


def _install_tab(qapp):
    from metis_test_runner.gui import InstallTab

    return InstallTab()


class _FakeWorker:
    """Stands in for InstallWorker so _start can be driven without a thread."""

    constructed = []

    def __init__(self, refs=None, force=None):
        self.refs, self.force = refs, force
        type(self).constructed.append(self)
        self.log = self.done = None
        # WorkerHost.track_worker uses these; real QThreads have them.
        self.finished = _Sig()

    def start(self):
        pass

    def isRunning(self):
        return False

    def deleteLater(self):
        pass


def _stub_worker(monkeypatch):
    _FakeWorker.constructed = []

    class W(_FakeWorker):
        def __init__(self, refs=None, force=None):
            super().__init__(refs, force)
            # _start connects to these before calling start().
            self.log = _Sig()
            self.done = _Sig()

    monkeypatch.setattr(gui, "InstallWorker", W)
    return _FakeWorker


class _Sig:
    def connect(self, *_a, **_k):
        pass


class TestInstallTabRefWidgets:
    def test_both_repos_get_an_editable_combo(self, qapp):
        tab = _install_tab(qapp)
        assert set(tab.ref_combos) == {"pipeline_ref", "simulations_ref"}
        for combo in tab.ref_combos.values():
            assert combo.isEditable()

    def test_enter_does_not_append_typed_text_to_the_dropdown(self, qapp):
        from PyQt6.QtWidgets import QComboBox

        tab = _install_tab(qapp)
        for combo in tab.ref_combos.values():
            assert combo.insertPolicy() == QComboBox.InsertPolicy.NoInsert

    def test_defaults_to_blank_meaning_default_branch(self, qapp):
        tab = _install_tab(qapp)
        for combo in tab.ref_combos.values():
            assert combo.currentText() == ""

    def test_populate_preserves_text_the_user_typed(self, qapp):
        # addItems() on an editable combo with currentIndex == -1 silently
        # snaps the line edit to items[0].
        tab = _install_tab(qapp)
        combo = tab.ref_combos["pipeline_ref"]
        combo.setCurrentText("my/topic-branch")
        tab._populate(combo, ["main", "develop"])
        assert combo.currentText() == "my/topic-branch"
        assert [combo.itemText(i) for i in range(combo.count())] == ["main", "develop"]

    def test_populate_leaves_an_untouched_combo_blank(self, qapp):
        tab = _install_tab(qapp)
        combo = tab.ref_combos["simulations_ref"]
        tab._populate(combo, ["main", "develop"])
        assert combo.currentText() == ""

    def test_refs_round_trip_through_qsettings(self, qapp):
        tab = _install_tab(qapp)
        tab.ref_combos["pipeline_ref"].setCurrentText("feature/x")
        tab.ref_combos["simulations_ref"].setCurrentText("v2025.05.15")
        tab._save_settings()

        fresh = _install_tab(qapp)
        assert fresh.ref_combos["pipeline_ref"].currentText() == "feature/x"
        assert fresh.ref_combos["simulations_ref"].currentText() == "v2025.05.15"
        # leave the shared store clean for other tests
        for c in fresh.ref_combos.values():
            c.setCurrentText("")
        fresh._save_settings()

    def test_ref_failure_is_reported_inline_not_in_the_install_log(self, qapp):
        tab = _install_tab(qapp)
        tab._on_ref_failed("pipeline_ref", "could not resolve host")
        assert "unavailable" in tab.ref_status["pipeline_ref"].text()
        assert tab.log_view.toPlainText() == ""
        assert "could not resolve host" in tab.ref_combos["pipeline_ref"].toolTip()


class TestInstallTabStart:
    def test_invalid_ref_blocks_the_install(self, qapp, monkeypatch):
        from PyQt6.QtWidgets import QMessageBox

        tab = _install_tab(qapp)
        fake = _stub_worker(monkeypatch)
        warned = []
        monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: warned.append(a))
        tab.ref_combos["pipeline_ref"].setCurrentText("--upload-pack=echo")
        tab._start()
        assert warned, "expected a warning dialog"
        assert fake.constructed == []

    def test_clean_trees_are_never_questioned(self, qapp, monkeypatch):
        from PyQt6.QtWidgets import QMessageBox

        tab = _install_tab(qapp)
        fake = _stub_worker(monkeypatch)
        asked = []
        monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: asked.append(a))
        monkeypatch.setattr(gui, "_dirty_files", lambda t: [])
        tab._start()
        assert asked == []
        assert len(fake.constructed) == 1
        assert fake.constructed[0].force == set()

    def test_declining_the_discard_dialog_aborts(self, qapp, monkeypatch):
        from PyQt6.QtWidgets import QMessageBox

        tab = _install_tab(qapp)
        fake = _stub_worker(monkeypatch)
        monkeypatch.setattr(gui, "_dirty_files", lambda t: [" M x.py"])
        monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: QMessageBox.StandardButton.No)
        tab._start()
        assert fake.constructed == []

    def test_accepting_the_dialog_forces_only_that_repo(self, qapp, monkeypatch):
        from PyQt6.QtWidgets import QMessageBox

        tab = _install_tab(qapp)
        fake = _stub_worker(monkeypatch)
        # Only the pipeline clone is dirty; confirming it must NOT authorise
        # discarding work in the simulations clone.
        monkeypatch.setattr(gui, "_dirty_files", lambda t: [" M x.py"] if t == gui.TARGET_A else [])
        monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: QMessageBox.StandardButton.Yes)
        tab._start()
        assert len(fake.constructed) == 1
        assert fake.constructed[0].force == {gui.TARGET_A}

    def test_dirty_tree_is_checked_even_when_the_ref_is_unchanged(self, qapp, monkeypatch):
        # checkout -f discards tracked edits whether or not the ref moved, so
        # the dialog must not be gated on a ref change.
        from PyQt6.QtWidgets import QMessageBox

        tab = _install_tab(qapp)
        _stub_worker(monkeypatch)
        asked = []
        monkeypatch.setattr(gui, "_dirty_files", lambda t: [" M x.py"])
        monkeypatch.setattr(
            QMessageBox, "question", lambda *a, **k: (asked.append(a), QMessageBox.StandardButton.Yes)[1]
        )
        tab._start()  # every combo left blank — no ref change at all
        assert asked, "a dirty tree must be questioned even with a blank ref"

    def test_unreadable_status_asks_rather_than_assuming_clean(self, qapp, monkeypatch):
        from PyQt6.QtWidgets import QMessageBox

        tab = _install_tab(qapp)
        fake = _stub_worker(monkeypatch)

        def boom(_t):
            raise RuntimeError("index corrupt")

        monkeypatch.setattr(gui, "_dirty_files", boom)
        monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: QMessageBox.StandardButton.No)
        tab._start()
        assert fake.constructed == []

    def test_selected_refs_reach_the_worker(self, qapp, monkeypatch):
        from PyQt6.QtWidgets import QMessageBox

        tab = _install_tab(qapp)
        fake = _stub_worker(monkeypatch)
        monkeypatch.setattr(gui, "_dirty_files", lambda t: [])
        monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: QMessageBox.StandardButton.Yes)
        tab.ref_combos["pipeline_ref"].setCurrentText("  feature/x  ")
        tab._start()
        assert fake.constructed[0].refs[gui.TARGET_A] == "feature/x"
        assert fake.constructed[0].refs[gui.TARGET_B] == ""
        for c in tab.ref_combos.values():
            c.setCurrentText("")
        tab._save_settings()


class TestRefWorker:
    def test_status_is_emitted_before_the_network_call(self, qapp, monkeypatch):
        seen = []
        monkeypatch.setattr(gui, "_describe_head", lambda t: "main @ abc")
        monkeypatch.setattr(installer, "_git", _fake_git({"ls-remote": _cp("aaa\trefs/heads/main\n")}))
        w = gui.RefWorker("k", "http://x.invalid/r.git", Path("/nope"))
        w.status.connect(lambda k, s: seen.append(("status", s)))
        w.refs.connect(lambda k, r: seen.append(("refs", r)))
        w.run()
        assert seen[0] == ("status", "main @ abc")
        assert seen[1] == ("refs", ["main"])

    def test_ls_remote_failure_emits_failed_not_refs(self, qapp, monkeypatch):
        seen = []
        monkeypatch.setattr(gui, "_describe_head", lambda t: "not cloned")
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "ls-remote": _cp("", 128, "fatal: could not read Username"),
                }
            ),
        )
        w = gui.RefWorker("k", "http://x.invalid/r.git", Path("/nope"))
        w.refs.connect(lambda k, r: seen.append("refs"))
        w.failed.connect(lambda k, r: seen.append(("failed", r)))
        w.run()
        assert seen == [("failed", "fatal: could not read Username")]


class TestWorkersDelegate:
    """The QThread workers are thin wrappers over installer.Installer/Uninstaller."""

    @staticmethod
    def _stub(result, built):
        class Stub:
            def __init__(self, **kw):
                self.kw = kw
                built.append(self)

            def run(self):
                self.kw["log"]("hello\n", "cyan")
                return result

        return Stub

    def test_install_worker_forwards_refs_force_and_log(self, qapp, monkeypatch):
        built, logs, done = [], [], []
        monkeypatch.setattr(gui, "Installer", self._stub(True, built))
        refs, force = {gui.TARGET_A: "develop"}, {gui.TARGET_A}
        w = gui.InstallWorker(refs=refs, force=force)
        w.log.connect(lambda text, colour: logs.append((text, colour)))
        w.done.connect(done.append)
        w.run()  # same thread: direct connections fire synchronously
        assert built[0].kw["refs"] == refs and built[0].kw["force"] == force
        assert logs == [("hello\n", "cyan")]
        assert done == [True]

    def test_uninstall_worker_reports_failure(self, qapp, monkeypatch):
        built, done = [], []
        monkeypatch.setattr(gui, "Uninstaller", self._stub(False, built))
        w = gui.UninstallWorker()
        w.done.connect(done.append)
        w.run()
        assert len(built) == 1
        assert done == [False]


class TestSmokeTestGuard:
    def test_smoke_test_suppresses_the_network_probe(self, qapp, monkeypatch):
        # main() sets this; CI runs `mtr --smoke-test`, which shows the window
        # (firing showEvent) and then quits immediately.
        tab = _install_tab(qapp)
        monkeypatch.setattr(gui, "SMOKE_TEST", True)
        tab._refresh_refs()
        assert tab._ref_workers == {}


class TestMainWindowKeepsInstallTab:
    def test_install_tab_handle_exists_for_close_event(self, qapp):
        # MainWindow used to construct InstallTab inline, so closeEvent could
        # not save its settings or stop its ref-list thread.
        from metis_test_runner.gui import InstallTab, MainWindow

        win = MainWindow()
        assert isinstance(win._install_tab, InstallTab)
        win.close()


# ---------------------------------------------------------------------------
# RefComboBox — mouse access to the dropdown
# ---------------------------------------------------------------------------


class TestRefComboBox:
    def _click(self, qapp, combo):
        """A REAL click: press *and* release.

        Sending only the press hid a bug where the popup opened and the
        unhandled release immediately closed it again, so the list survived
        only while the button was held down.
        """
        from PyQt6.QtCore import QEvent, QPointF, Qt
        from PyQt6.QtGui import QMouseEvent

        def send(widget, typ):
            qapp.sendEvent(
                widget,
                QMouseEvent(
                    typ,
                    QPointF(10, 10),
                    QPointF(10, 10),
                    Qt.MouseButton.LeftButton,
                    Qt.MouseButton.LeftButton,
                    Qt.KeyboardModifier.NoModifier,
                ),
            )

        send(combo.lineEdit(), QEvent.Type.MouseButtonPress)
        # A real release goes wherever the popup now is.
        target = combo.view() if combo.view().isVisible() else combo.lineEdit()
        send(target, QEvent.Type.MouseButtonRelease)
        qapp.processEvents()

    def _combo(self, qapp, text=""):
        c = gui.RefComboBox()
        c.addItems(["main", "AIT_Templates"])
        c.setCurrentText(text)
        c.show()
        return c

    def test_is_editable_so_a_sha_can_be_pasted(self, qapp):
        assert self._combo(qapp).isEditable()

    def test_enter_does_not_append_typed_text_to_the_list(self, qapp):
        from PyQt6.QtWidgets import QComboBox

        assert self._combo(qapp).insertPolicy() == QComboBox.InsertPolicy.NoInsert

    def test_click_on_blank_field_opens_the_list(self, qapp):
        # An editable combo normally only opens from the arrow, which left the
        # branch list reachable by keyboard alone.
        c = self._combo(qapp)
        assert not c.view().isVisible()
        self._click(qapp, c)
        assert c.view().isVisible()
        c.hidePopup()

    def test_list_stays_open_after_the_button_is_released(self, qapp):
        # Regression: the popup used to close on the release, so it survived
        # only while the left button was held down.
        c = self._combo(qapp)
        self._click(qapp, c)
        qapp.processEvents()
        assert c.view().isVisible()
        c.hidePopup()

    def test_click_on_a_value_from_the_list_reopens_it(self, qapp):
        c = self._combo(qapp, "AIT_Templates")
        self._click(qapp, c)
        assert c.view().isVisible()
        c.hidePopup()

    def test_click_on_hand_typed_text_keeps_it_editable(self, qapp):
        # A pasted SHA must stay cursor-editable rather than being hijacked.
        c = self._combo(qapp, "8a50c60d4a54")
        self._click(qapp, c)
        assert not c.view().isVisible()

    def test_a_second_click_lands_on_the_list_not_the_text_field(self, qapp):
        # Once the popup is up it grabs the mouse, so the next click dismisses
        # it rather than being swallowed by the line edit and re-opening it.
        # (Synthetic events can't faithfully drive item activation, so this
        # asserts dismissal only; the keyboard path below covers selection.)
        c = self._combo(qapp)
        self._click(qapp, c)
        assert c.view().isVisible()
        self._click(qapp, c)
        assert not c.view().isVisible()

    def test_picking_from_the_open_list_sets_the_value(self, qapp):
        from PyQt6.QtCore import QEvent, Qt
        from PyQt6.QtGui import QKeyEvent

        c = self._combo(qapp)
        self._click(qapp, c)
        assert c.view().isVisible()
        for key in (Qt.Key.Key_Down, Qt.Key.Key_Return):
            qapp.sendEvent(c.view(), QKeyEvent(QEvent.Type.KeyPress, key, Qt.KeyboardModifier.NoModifier))
        qapp.processEvents()
        assert not c.view().isVisible()
        assert c.currentText() in ("main", "AIT_Templates")

    def test_empty_list_does_not_pop_an_empty_box(self, qapp):
        c = gui.RefComboBox()
        c.show()
        self._click(qapp, c)
        assert not c.view().isVisible()


class TestComboArrowIsNotSuppressed:
    def test_theme_does_not_style_the_drop_down_subcontrol(self, qapp):
        # Styling QComboBox::drop-down at all suppresses Qt's native chevron,
        # which is an editable combo's only mouse affordance for its list.
        import re

        from PyQt6.QtWidgets import QApplication

        gui.apply_theme(QApplication.instance(), "dark")
        # Strip /* … */ comments: the stylesheet explains this rule's absence
        # by naming the subcontrol.
        qss = re.sub(r"/\*.*?\*/", "", QApplication.instance().styleSheet(), flags=re.S)
        assert "QComboBox::drop-down" not in qss
        assert "QComboBox::down-arrow" not in qss


# ---------------------------------------------------------------------------
# WorkerHost — real QThreads (nothing else in this suite starts one)
# ---------------------------------------------------------------------------


class _SleepyWorker(gui.QThread):
    """A worker that runs until interrupted, so lifetime can be observed."""

    def run(self):
        while not self.isInterruptionRequested():
            self.msleep(10)


class _Host(gui.WorkerHost):
    WORKER_STOP_MS = 3_000


class TestWorkerHost:
    def test_tracks_a_running_worker(self, qapp):
        host, worker = _Host(), _SleepyWorker()
        host.track_worker(worker)
        worker.start()
        try:
            assert worker in host.live_workers()
            assert host.busy()
        finally:
            host.stop_workers()

    def test_stop_workers_joins_them(self, qapp):
        host, worker = _Host(), _SleepyWorker()
        host.track_worker(worker)
        worker.start()
        host.stop_workers()
        assert not worker.isRunning()
        assert not host.busy()

    def test_finished_worker_is_untracked(self, qapp):
        host, worker = _Host(), _SleepyWorker()
        host.track_worker(worker)
        worker.start()
        host.stop_workers()
        worker.wait(3000)
        qapp.processEvents()  # let the queued finished handler run
        assert not host.busy()

    def test_tracking_survives_a_second_worker(self, qapp):
        """Two live workers must both be tracked — the old single-slot design
        dropped the first one's last reference and aborted the process."""
        host = _Host()
        a, b = _SleepyWorker(), _SleepyWorker()
        host.track_worker(a)
        host.track_worker(b)
        a.start()
        b.start()
        try:
            assert len(host.live_workers()) == 2
        finally:
            host.stop_workers()
        assert not a.isRunning() and not b.isRunning()

    def test_idle_host_is_not_busy(self, qapp):
        assert not _Host().busy()


class TestArchiveTabBusyGuard:
    """A second archive action must be refused, not allowed to clobber the
    running worker's slot."""

    def _tab(self, qapp, monkeypatch):
        monkeypatch.setattr(gui, "_installation_complete", lambda: True)
        monkeypatch.setattr(
            "metis_test_runner.archive.metiswise_available",
            lambda: True,
        )
        monkeypatch.setattr(
            "metis_test_runner.archive.read_env_cfg",
            lambda: {},
        )
        return gui.ArchiveTab()

    def test_second_action_is_refused_while_busy(self, qapp, monkeypatch):
        from PyQt6.QtWidgets import QMessageBox

        tab = self._tab(qapp, monkeypatch)
        worker = _SleepyWorker()
        tab.track_worker(worker)
        worker.start()
        infos = []
        monkeypatch.setattr(QMessageBox, "information", lambda *a, **k: infos.append(a))
        try:
            assert tab._reject_if_busy() is True
            assert infos, "expected a 'busy' dialog"
        finally:
            tab.stop_workers()

    def test_action_allowed_when_idle(self, qapp, monkeypatch):
        tab = self._tab(qapp, monkeypatch)
        assert tab._reject_if_busy() is False


# ---------------------------------------------------------------------------
# GUI argv <-> CLI parser contract
# ---------------------------------------------------------------------------


class TestGuiArgsParseAsCli:
    """The GUI hand-builds an argv list that run_metis's argparse must accept.

    Nothing type-checks that coupling, so a renamed flag would pass every
    string-membership test and only fail at runtime. This closes the loop —
    it is how --prefer-masters stayed GUI-unreachable unnoticed.
    """

    def _tab(self, qapp):
        tab = _make_run_tab(qapp)
        tab.input_list.addItem("a.yaml")
        return tab

    def test_default_args_round_trip(self, qapp):
        from metis_test_runner.run_metis import parse_args

        parsed = parse_args(self._tab(qapp)._build_cmd_args())
        assert parsed.input_files == ["a.yaml"]

    def test_every_toggle_combination_round_trips(self, qapp):
        from metis_test_runner.run_metis import parse_args

        tab = self._tab(qapp)
        for cb in (tab.calib_cb, tab.static_cb, tab.auto_fetch_cb):
            for state in (True, False):
                cb.setChecked(state)
                parse_args(tab._build_cmd_args())  # must not SystemExit

    def test_prefer_masters_is_not_exposed_in_the_gui(self, qapp):
        """Deliberately CLI-only: the Install tab already pins
        association_preference, so the flag is a no-op for the `default`
        runner the Run tab is overwhelmingly used with."""
        tab = self._tab(qapp)
        assert not hasattr(tab, "prefer_masters_cb")
        assert "--prefer-masters" not in tab._build_cmd_args()

    def test_csv_to_yaml_suppresses_pipeline_only_flags(self, qapp):
        tab = self._tab(qapp)
        tab.auto_fetch_cb.setChecked(True)
        tab.csv_to_yaml_cb.setChecked(True)
        args = tab._build_cmd_args()
        assert "--csv-to-yaml" in args
        assert "--auto-fetch-calibrations" not in args

    def test_runner_and_container_round_trip(self, qapp):
        from metis_test_runner.run_metis import parse_args

        tab = self._tab(qapp)
        tab.runner_combo.setCurrentText("docker")
        tab.container_edit.setText("metis-pipeline")
        parsed = parse_args(tab._build_cmd_args())
        assert parsed.runner == "docker"
        assert parsed.container == "metis-pipeline"

    def test_pipeline_only_mode_round_trips(self, qapp):
        from metis_test_runner.run_metis import parse_args

        tab = self._tab(qapp)
        tab.rb_pipe_only.setChecked(True)
        tab.pipeline_input_list.addItem("/data/fits")
        parsed = parse_args(tab._build_cmd_args())
        assert parsed.no_sim is True
        assert parsed.pipeline_input == ["/data/fits"]
