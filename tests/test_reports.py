"""ADARI reports: finding a run's EDPS jobs, reading their output.json, and the Reports views."""

import json
from argparse import Namespace

import pytest

from metis_test_runner import reports
from metis_test_runner import run_metis as rm


def _job(base, task, job_id, inputs):
    job = base / "METIS" / task / job_id
    job.mkdir(parents=True)
    (job / "input.sof").write_text("".join(f"{p} TAG\n" for p in inputs))
    return job


def _report(job, name, catgs=(), recorded_dir="/elsewhere"):
    d = job / name
    d.mkdir()
    (d / "genreport.stdout").write_text("")
    entries = []
    for catg in catgs:
        png = d / f"{name}_{catg.lower()}.png"
        _png(png)
        entries.append(
            {
                "filename": f"{recorded_dir}/{png.name}",
                "mime_type": "image/png",
                "description": f"desc {catg}",
                "prodcatg": catg,
            }
        )
    (d / "output.json").write_text(json.dumps(entries))
    return d


def _png(path):
    from PyQt6.QtGui import QImage

    img = QImage(8, 4, QImage.Format.Format_RGB32)
    img.fill(0)
    assert img.save(str(path))


@pytest.fixture
def layout(tmp_path):
    """A run with lingain -> dark, and an unrelated earlier run's job."""
    base, run, other = tmp_path / "edps", tmp_path / "output" / "run2", tmp_path / "output" / "run1"
    for d in (run / "sim", run / "pipeline", other / "sim", other / "pipeline"):
        d.mkdir(parents=True)
    lingain = _job(base, "metis_ifu_lingain", "a1", [run / "sim" / "detlin.fits"])
    dark = _job(base, "metis_ifu_dark", "b2", [run / "sim" / "dark.fits", lingain / "GAIN_MAP.fits"])
    calib = _job(base, "metis_ifu_rsrf", "c3", [dark / "MASTER_DARK.fits"])  # only via a product
    old = _job(base, "metis_ifu_lingain", "d4", [other / "sim" / "detlin.fits"])
    return Namespace(base=base, run=run, lingain=lingain, dark=dark, calib=calib, old=old)


class TestRunJobs:
    def test_follows_products_and_skips_other_runs(self, layout):
        assert reports.run_jobs(layout.base, [layout.run]) == [layout.lingain, layout.dark, layout.calib]

    def test_unrelated_root_has_no_jobs(self, layout, tmp_path):
        assert reports.run_jobs(layout.base, [tmp_path / "nowhere"]) == []


class TestLoadRun:
    def test_reads_output_json_next_to_the_images(self, layout):
        _report(layout.lingain, "metis_rawdisp", ["DETLIN_IFU_RAW"])
        _report(layout.lingain, "metis_det_lingain", ["GAIN_MAP_IFU", "LINEARITY_IFU"])
        _report(layout.old, "metis_det_lingain", ["GAIN_MAP_IFU"])
        reports.write_index(layout.run, layout.base, [layout.run])
        got = reports.load_run(layout.run)
        assert [(r.task, r.name, [p.prodcatg for p in r.panels]) for r in got] == [
            ("metis_ifu_lingain", "metis_det_lingain", ["GAIN_MAP_IFU", "LINEARITY_IFU"]),
            ("metis_ifu_lingain", "metis_rawdisp", ["DETLIN_IFU_RAW"]),
        ]
        assert all(p.path.is_file() and p.path.parent.parent == layout.lingain for r in got for p in r.panels)

    def test_failed_report_carries_stderr(self, layout):
        d = layout.dark / "metis_det_dark"
        d.mkdir()
        (d / "genreport.stdout").write_text("")
        (d / "genreport.stderr").write_text("Traceback ...\nKeyError: 'DATA'\n")
        reports.write_index(layout.run, layout.base, [layout.run])
        (failed,) = reports.load_run(layout.run)
        assert failed.panels == () and "KeyError" in failed.error

    def test_without_index_scans_base_dir(self, layout):
        _report(layout.dark, "metis_det_dark", ["MASTER_DARK_IFU"])
        assert not (layout.run / reports.INDEX_NAME).exists()
        assert [r.name for r in reports.load_run(layout.run, layout.base)] == ["metis_det_dark"]
        assert reports.load_run(layout.run) == []  # no index and no base_dir

    def test_list_runs_newest_first(self, layout, tmp_path):
        import os

        os.utime(layout.run, (1, 1))
        assert reports.list_runs(tmp_path / "output") == [tmp_path / "output" / "run1", layout.run]
        assert reports.list_runs(tmp_path / "missing") == []


class TestIndexReports:
    def _args(self, report_type="all", pipeline_input=None):
        return Namespace(report_type=report_type, pipeline_input=pipeline_input)

    def test_writes_index_from_edps_config(self, layout, monkeypatch, capsys):
        import configparser

        cfg = configparser.ConfigParser()
        cfg["executor"] = {"base_dir": str(layout.base)}
        monkeypatch.setattr(rm, "_read_edps_config", lambda: cfg)
        _report(layout.dark, "metis_det_dark", ["MASTER_DARK_IFU"])
        rm.index_reports(self._args(), "default", layout.run, layout.run / "pipeline")
        index = json.loads((layout.run / reports.INDEX_NAME).read_text())
        assert index["jobs"] == [str(layout.lingain), str(layout.dark), str(layout.calib)]
        assert "1 image(s) from 3 job(s)" in capsys.readouterr().out

    def test_pipeline_input_dirs_are_roots(self, layout, monkeypatch, tmp_path):
        import configparser

        cfg = configparser.ConfigParser()
        cfg["executor"] = {"base_dir": str(layout.base)}
        monkeypatch.setattr(rm, "_read_edps_config", lambda: cfg)
        out = tmp_path / "fresh"
        out.mkdir()
        rm.index_reports(
            self._args(pipeline_input=[str(layout.run / "sim")]), "native", out, out / "pipeline"
        )
        assert len(json.loads((out / reports.INDEX_NAME).read_text())["jobs"]) == 3

    @pytest.mark.parametrize("runner,report_type", [("docker", "all"), ("default", "none")])
    def test_skipped(self, layout, monkeypatch, runner, report_type):
        monkeypatch.setattr(rm, "_read_edps_config", lambda: pytest.fail("must not read the config"))
        rm.index_reports(self._args(report_type), runner, layout.run, layout.run / "pipeline")
        assert not (layout.run / reports.INDEX_NAME).exists()


class TestViews:
    def test_reports_tab_lists_run(self, qapp, layout):
        from metis_test_runner.reports_view import ReportsTab

        _report(layout.lingain, "metis_det_lingain", ["GAIN_MAP_IFU"])
        _report(layout.dark, "metis_det_dark", ["MASTER_DARK_IFU"])
        reports.write_index(layout.run, layout.base, [layout.run])
        tab = ReportsTab(lambda: layout.run.parent)
        tab.show_run(layout.run)
        assert tab.current_run() == layout.run
        assert tab.run_combo.count() == 2
        assert tab.viewer.tree.topLevelItemCount() == 2
        assert "2 image(s) from 2 report(s) in 2 EDPS job(s)" in tab.status.text()
        assert [r.name for r in tab.reports_for("metis_ifu_dark")] == ["metis_det_dark"]
        # The first panel is selected and its image loaded
        assert tab.viewer._pixmap is not None and not tab.viewer._pixmap.isNull()
        assert tab.viewer.open_btn.isEnabled()

    def test_reports_tab_without_reports(self, qapp, layout, monkeypatch):
        from metis_test_runner import reports_view
        from metis_test_runner.reports_view import ReportsTab

        monkeypatch.setattr(reports_view, "edps_base_dir", lambda run: layout.base)
        tab = ReportsTab(lambda: layout.run.parent)
        tab.show_run(layout.run)
        assert tab.viewer.tree.topLevelItemCount() == 0
        assert "No ADARI reports" in tab.status.text()

    def test_dialog_shows_task_reports(self, qapp, layout):
        from metis_test_runner.reports_view import ReportsDialog

        _report(layout.dark, "metis_det_dark", ["MASTER_DARK_IFU"])
        reports.write_index(layout.run, layout.base, [layout.run])
        items = [r for r in reports.load_run(layout.run) if r.task == "metis_ifu_dark"]
        dialog = ReportsDialog("metis_ifu_dark", layout.run, items)
        assert dialog.viewer.tree.topLevelItemCount() == 1
        assert "metis_ifu_dark" in dialog.windowTitle()

    def test_main_window_wires_run_finished_and_flow(self, qapp, layout, monkeypatch):
        from metis_test_runner import gui
        from metis_test_runner.reports_view import ReportsDialog

        _report(layout.dark, "metis_det_dark", ["MASTER_DARK_IFU"])
        reports.write_index(layout.run, layout.base, [layout.run])
        win = gui.MainWindow()
        win._run_tab.run_finished.emit(str(layout.run))
        assert win._reports_tab.current_run() == layout.run
        win._show_task_reports("metis_ifu_dark")
        (dialog,) = [w for w in qapp.topLevelWidgets() if isinstance(w, ReportsDialog) and w.isVisible()]
        assert dialog.viewer.tree.topLevelItemCount() == 1
        dialog.close()
        win.close()

    def test_flow_window_task_menu_calls_back(self, qapp, monkeypatch):
        from PyQt6.QtCore import QPoint
        from PyQt6.QtWidgets import QMenu

        from metis_test_runner.flow_view import FlowWindow

        shown = []
        win = FlowWindow({}, None, None, shown.append)
        monkeypatch.setattr(QMenu, "exec", lambda self, pos: self.actions()[0])
        win.task_menu("metis_ifu_dark", QPoint(0, 0))
        assert shown == ["metis_ifu_dark"]
        win.close()
