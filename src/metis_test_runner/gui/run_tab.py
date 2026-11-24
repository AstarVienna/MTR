"""The Run tab."""

# PyQt6 stubs type most widget getters as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

import os
import re
import shutil
import subprocess
from pathlib import Path

from PyQt6.QtCore import (
    QProcess,
    QProcessEnvironment,
    QSettings,
    Qt,
    QTimer,
    QUrl,
    pyqtSignal,
)
from PyQt6.QtGui import QDesktopServices, QFont
from PyQt6.QtWidgets import (
    QAbstractSpinBox,
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QSplitter,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from .. import paths
from ..flow import RunInput
from ..installer import (
    StreamDecoder,
    strip_ansi,
)
from .widgets import LABEL_W, _child_env, _dir_picker, _labeled, _resolve_run_metis_command, log_append
from .workers import WorkerHost

# ---------------------------------------------------------------------------
# Run tab
# ---------------------------------------------------------------------------


class RunTab(WorkerHost, QWidget):
    #: How long to let run_metis clean up after SIGTERM before resorting to
    #: SIGKILL: stopping its children takes up to 3 s and `edps -s` up to 15 s.
    STOP_GRACE_MS = 20_000

    #: Output root of a finished run (whatever its exit code), for the Reports tab
    run_finished = pyqtSignal(str)

    def __init__(self) -> None:
        super().__init__()
        self._process: QProcess | None = None
        self._stopping = False
        self._decoders = {"stdout": StreamDecoder(), "stderr": StreamDecoder()}
        #: Task the data-flow window asked for; replaces the inferred EDPS target for one run
        self._edps_target: str | None = None
        # One timer, stopped when the run finishes, so a late SIGKILL can
        # never hit the next run.
        self._kill_timer = QTimer(self)
        self._kill_timer.setSingleShot(True)
        self._kill_timer.timeout.connect(self._kill_after_grace)
        self._settings = QSettings("METIS", "TestRunner")
        self._build_ui()
        self._load_settings()
        self._update_runner_fields()
        self._update_mode_fields()

    # ── UI construction ──────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setContentsMargins(20, 20, 20, 20)

        top = QWidget()
        top_lay = QVBoxLayout(top)
        top_lay.setSpacing(12)
        top_lay.setContentsMargins(0, 0, 0, 0)

        # ── Input file list (YAML and/or CSV) ──
        self.input_grp = QGroupBox("Input Files  (YAML / CSV)")
        input_lay = QVBoxLayout(self.input_grp)
        file_row = QHBoxLayout()
        self.input_list = QListWidget()
        self.input_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        self.input_list.setMaximumHeight(120)
        file_row.addWidget(self.input_list)
        btn_col = QVBoxLayout()
        add_btn = QPushButton("Add…")
        add_btn.setProperty("role", "info")
        add_btn.clicked.connect(self._add_input)
        remove_btn = QPushButton("Remove")
        remove_btn.setProperty("role", "danger")
        remove_btn.clicked.connect(self._remove_input)
        clear_btn = QPushButton("Clear")
        clear_btn.setProperty("role", "success")
        clear_btn.clicked.connect(self._clear_inputs)
        btn_col.addWidget(add_btn)
        btn_col.addWidget(remove_btn)
        btn_col.addWidget(clear_btn)
        btn_col.addStretch()
        file_row.addLayout(btn_col)
        input_lay.addLayout(file_row)
        # Live tally of YAML vs CSV files in the list
        self.input_status = QLabel("0 YAML  ·  0 CSV")
        self.input_status.setProperty("hint", "true")
        input_lay.addWidget(self.input_status)

        # CSV line range (power-user): restrict .csv inputs to 1-based file
        # lines. 0 = unset (shown as "—"). The header block is always kept.
        self.csv_start_spin = QSpinBox()
        self.csv_start_spin.setRange(0, 1_000_000)
        self.csv_start_spin.setSpecialValueText("—")
        self.csv_start_spin.setMaximumWidth(90)
        self.csv_end_spin = QSpinBox()
        self.csv_end_spin.setRange(0, 1_000_000)
        self.csv_end_spin.setSpecialValueText("—")
        self.csv_end_spin.setMaximumWidth(90)
        # Build the row by hand so the "Start:" / "End:" words sit directly
        # next to their spin boxes and a trailing stretch keeps everything
        # left-aligned (the generic _labeled() helper lets the words drift).
        csv_range_row = QWidget()
        csv_h = QHBoxLayout(csv_range_row)
        csv_h.setContentsMargins(0, 0, 0, 0)
        csv_main_lbl = QLabel("CSV line range  (--csv-lines):")
        csv_main_lbl.setFixedWidth(LABEL_W)
        csv_main_lbl.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        csv_h.addWidget(csv_main_lbl)
        csv_h.addWidget(QLabel("Start:"))
        csv_h.addWidget(self.csv_start_spin)
        csv_h.addSpacing(16)
        csv_h.addWidget(QLabel("End:"))
        csv_h.addWidget(self.csv_end_spin)
        csv_h.addStretch()
        csv_range_row.setToolTip(
            "Restrict CSV inputs to these 1-based file lines (0 = unset).\n"
            "The header block (column row + component/description/type rows) "
            "is always kept. Applies to .csv inputs only; ignored with --no-sim."
        )
        input_lay.addWidget(csv_range_row)
        top_lay.addWidget(self.input_grp)

        # ── Options ──
        self.opts_grp = opts_grp = QGroupBox("Options")
        opts_lay = QVBoxLayout(opts_grp)
        opts_lay.setSpacing(10)
        opts_lay.setContentsMargins(9, 9, 9, 12)

        # Output directory
        self.output_edit = QLineEdit()
        out_browse = _dir_picker(self.output_edit, self)
        opts_lay.addWidget(_labeled("Output directory:", self.output_edit, out_browse))

        # Output path info hint
        self.output_info = QLabel()
        self.output_info.setProperty("hint", "true")
        info_row = QWidget()
        info_h = QHBoxLayout(info_row)
        info_h.setContentsMargins(0, 0, 0, 0)
        spacer_lbl = QLabel()
        spacer_lbl.setFixedWidth(LABEL_W)
        info_h.addWidget(spacer_lbl)
        info_h.addWidget(self.output_info)
        opts_lay.addWidget(info_row)
        self.output_edit.textChanged.connect(self._update_output_info)

        # Checkboxes
        cb_row = QWidget()
        cb_h = QHBoxLayout(cb_row)
        cb_h.setContentsMargins(0, 0, 0, 0)
        lbl = QLabel("")
        lbl.setFixedWidth(LABEL_W)
        cb_h.addWidget(lbl)
        self.calib_cb = QCheckBox("Auto-generate calibration frames  (--calib)")
        self.calib_cb.setChecked(True)
        cb_h.addWidget(self.calib_cb)
        self.static_cb = QCheckBox("Auto-generate static calibration prototypes  (--static)")
        self.static_cb.setChecked(True)
        cb_h.addWidget(self.static_cb)
        cb_h.addStretch()
        opts_lay.addWidget(cb_row)

        # Auto-fetch calibrations checkbox
        af_row = QWidget()
        af_h = QHBoxLayout(af_row)
        af_h.setContentsMargins(0, 0, 0, 0)
        af_lbl = QLabel("")
        af_lbl.setFixedWidth(LABEL_W)
        af_h.addWidget(af_lbl)
        self.auto_fetch_cb = QCheckBox(
            "Auto-fetch missing calibrations from archive  (--auto-fetch-calibrations)"
        )
        af_h.addWidget(self.auto_fetch_cb)
        af_h.addStretch()
        opts_lay.addWidget(af_row)

        # CSV → YAML translation (dry run): no simulation, no pipeline.
        cty_row = QWidget()
        cty_h = QHBoxLayout(cty_row)
        cty_h.setContentsMargins(0, 0, 0, 0)
        cty_lbl = QLabel("")
        cty_lbl.setFixedWidth(LABEL_W)
        cty_h.addWidget(cty_lbl)
        self.csv_to_yaml_cb = QCheckBox("Translate CSV → YAML, no simulation  (--csv-to-yaml)")
        self.csv_to_yaml_cb.toggled.connect(self._update_csv_to_yaml_state)
        cty_h.addWidget(self.csv_to_yaml_cb)
        cty_h.addStretch()
        opts_lay.addWidget(cty_row)

        # Cores
        self.cores_spin = QSpinBox()
        self.cores_spin.setRange(1, os.cpu_count() or 16)
        self.cores_spin.setValue(4)
        self.cores_spin.setButtonSymbols(QAbstractSpinBox.ButtonSymbols.PlusMinus)
        opts_lay.addWidget(_labeled("CPU cores  (--cores):", self.cores_spin))

        # The EDPS workflow and targets are auto-detected for every input type
        # (YAML pre-sim, CSV from the simulated FITS). There is no workflow
        # override here: for manual control over the EDPS/pyesorex invocation
        # use the mtr-exec / mtr-shell commands.
        self.input_list.model().rowsInserted.connect(lambda *_a: self._refresh_input_status())
        self.input_list.model().rowsRemoved.connect(lambda *_a: self._refresh_input_status())

        # Runner
        self.runner_combo = QComboBox()
        self.runner_combo.addItems(["default", "native", "docker", "podman"])
        opts_lay.addWidget(_labeled("Runner  (--runner):", self.runner_combo))

        # Pipeline mode
        mode_row = QWidget()
        mode_h = QHBoxLayout(mode_row)
        mode_h.setContentsMargins(0, 0, 0, 0)
        lbl2 = QLabel("Pipeline mode:")
        lbl2.setFixedWidth(LABEL_W)
        mode_h.addWidget(lbl2)
        self._mode_grp = QButtonGroup(self)
        # fmt: off
        self.rb_both      = QRadioButton("Simulate + run pipeline")
        self.rb_sim_only  = QRadioButton("Simulate only  (--no-pipeline)")
        self.rb_pipe_only = QRadioButton("Pipeline only  (--no-sim)")
        # fmt: on
        for rb in (self.rb_both, self.rb_sim_only, self.rb_pipe_only):
            self._mode_grp.addButton(rb)
            mode_h.addWidget(rb)
        mode_h.addStretch()
        self.rb_both.setChecked(True)
        opts_lay.addWidget(mode_row)

        # Pipeline input dirs  [pipeline-only mode only]
        self.pipeline_input_list = QListWidget()
        self.pipeline_input_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        pipe_in_content = QHBoxLayout()
        pipe_in_content.addWidget(self.pipeline_input_list)
        pipe_in_btns = QVBoxLayout()
        pipe_add_btn = QPushButton("Add…")
        pipe_add_btn.setProperty("role", "info")
        pipe_add_btn.clicked.connect(self._add_pipeline_input)
        pipe_rm_btn = QPushButton("Remove")
        pipe_rm_btn.setProperty("role", "danger")
        pipe_rm_btn.clicked.connect(self._remove_pipeline_input)
        pipe_clear_btn = QPushButton("Clear")
        pipe_clear_btn.setProperty("role", "success")
        pipe_clear_btn.clicked.connect(self._clear_pipeline_inputs)
        pipe_in_btns.addWidget(pipe_add_btn)
        pipe_in_btns.addWidget(pipe_rm_btn)
        pipe_in_btns.addWidget(pipe_clear_btn)
        pipe_in_btns.addStretch()
        pipe_in_content.addLayout(pipe_in_btns)
        # The list is capped at 100px; when the row gets more height the list
        # would be vertically centred while the button column (top-stretched)
        # stays at the top. Pin the list to the top so both edges line up.
        pipe_in_content.setAlignment(self.pipeline_input_list, Qt.AlignmentFlag.AlignTop)

        self.pipeline_input_row = QWidget()
        pi_outer = QVBoxLayout(self.pipeline_input_row)
        pi_outer.setContentsMargins(0, 0, 0, 0)
        pi_lbl = QLabel("Pipeline input dirs  (--pipeline-input):")
        pi_outer.addWidget(pi_lbl)
        pi_outer.addLayout(pipe_in_content)
        # Cap the list at the button column's height (computed after the
        # layouts are parented so style spacing resolves) so the list's
        # bottom edge lines up with the Clear button.
        self.pipeline_input_list.setMaximumHeight(pipe_in_btns.sizeHint().height())
        opts_lay.addWidget(self.pipeline_input_row)
        # Connect mode radio buttons now that pipeline_input_row exists
        for rb in (self.rb_both, self.rb_sim_only, self.rb_pipe_only):
            rb.toggled.connect(self._update_mode_fields)

        # Container name  [docker / podman only]
        self.container_edit = QLineEdit()
        self.container_edit.setPlaceholderText("e.g. metis-pipeline")
        self.container_row = _labeled("Container  (--container):", self.container_edit)
        opts_lay.addWidget(self.container_row)
        # Connect runner signal now that container_row exists
        self.runner_combo.currentTextChanged.connect(self._update_runner_fields)

        # Simulations dir  [native / docker / podman only]. The default runner
        # uses the Install tab's checkout, so it shows that instead: one place
        # to set it, rather than a per-run field competing with a permanent one.
        self.sim_dir_edit = QLineEdit()
        sim_browse = _dir_picker(self.sim_dir_edit, self)
        self.sim_dir_row = _labeled("Simulations dir  (--simulations-dir):", self.sim_dir_edit, sim_browse)
        opts_lay.addWidget(self.sim_dir_row)
        self.sim_dir_info = QLabel()
        self.sim_dir_info.setProperty("hint", "true")
        self.sim_dir_row_default = _labeled("Simulations dir  (Install tab):", self.sim_dir_info)
        opts_lay.addWidget(self.sim_dir_row_default)

        # Instrument packages  [always visible]
        self.inst_edit = QLineEdit()
        inst_browse = _dir_picker(self.inst_edit, self)
        self.inst_edit.setPlaceholderText(str(paths.inst_pkgs_dir()))
        opts_lay.addWidget(_labeled("Instrument packages  (--inst-pkgs):", self.inst_edit, inst_browse))

        top_lay.addWidget(opts_grp)

        # ── Run / Stop ──
        run_row = QHBoxLayout()
        self.run_btn = QPushButton("Run")
        self.run_btn.setProperty("role", "success")
        self.run_btn.setMinimumHeight(36)
        self.run_btn.setMaximumWidth(120)
        self.run_btn.clicked.connect(self._run)
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setProperty("role", "danger")
        self.stop_btn.setMinimumHeight(36)
        self.stop_btn.setMaximumWidth(120)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self._stop)
        self.open_folder_btn = QPushButton("Open Folder")
        self.open_folder_btn.setProperty("role", "info")
        self.open_folder_btn.setMinimumHeight(36)
        self.open_folder_btn.setMaximumWidth(120)
        self.open_folder_btn.clicked.connect(self._open_folder)
        run_row.addWidget(self.run_btn)
        run_row.addWidget(self.stop_btn)
        run_row.addWidget(self.open_folder_btn)
        run_row.addStretch()
        top_lay.addLayout(run_row)

        # ── Output log ──
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Monospace", 9))
        self.log_view.setMinimumHeight(150)

        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(top)
        splitter.addWidget(self.log_view)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setChildrenCollapsible(False)
        outer.addWidget(splitter)

    # ── Output path info ─────────────────────────────────────────────────────

    def _update_output_info(self) -> None:
        base = self.output_edit.text().strip()
        root = Path(base) if base else paths.output_dir() / "<timestamp>"
        pipe_out = root / "pipeline"

        if self.rb_pipe_only.isChecked():
            dirs = [self.pipeline_input_list.item(i).text() for i in range(self.pipeline_input_list.count())]
            pipe_in_str = ", ".join(dirs) if dirs else f"{root / 'sim'}/"
            lines = [f"Pipeline input \u2192 {pipe_in_str}", f"Pipeline products \u2192 {pipe_out}/"]
        elif self.rb_sim_only.isChecked():
            lines = [f"Simulations \u2192 {root / 'sim'}/"]
        else:
            lines = [f"Simulations \u2192 {root / 'sim'}/", f"Pipeline products \u2192 {pipe_out}/"]
        # One line keeps the Run tab within the default 900 px window; long
        # paths are cut off at the right edge, so the tooltip shows them in full.
        self.output_info.setText("   \u00b7   ".join(lines))
        self.output_info.setToolTip("\n".join(lines))

    # ── Mode-dependent field visibility ──────────────────────────────────────

    def _update_mode_fields(self) -> None:
        pipe_only = self.rb_pipe_only.isChecked()
        self.input_grp.setVisible(not pipe_only)
        self.pipeline_input_row.setVisible(pipe_only)
        self._update_output_info()

    def _update_csv_to_yaml_state(self) -> None:
        """CSV→YAML is a translate-only dry run, so the simulation/pipeline
        options don't apply — disable them while it's ticked."""
        on = self.csv_to_yaml_cb.isChecked()
        for w in (
            self.rb_both,
            self.rb_sim_only,
            self.rb_pipe_only,
            self.calib_cb,
            self.static_cb,
            self.auto_fetch_cb,
            self.cores_spin,
        ):
            w.setEnabled(not on)

    # ── Runner-dependent field visibility ────────────────────────────────────

    def _update_runner_fields(self) -> None:
        runner = self.runner_combo.currentText()
        container = runner in ("docker", "podman")
        self.container_row.setVisible(container)
        if container:
            ph = "(resolved inside container)"
        else:
            ph = str(paths.inst_pkgs_dir())
        self.inst_edit.setPlaceholderText(ph)
        self.sim_dir_row.setVisible(runner != "default")
        self.sim_dir_row_default.setVisible(runner == "default")
        sims = paths.simulations_dir()
        self.sim_dir_edit.setPlaceholderText("/home/metis/METIS_Simulations" if container else str(sims))
        self.sim_dir_info.setText(str(sims))
        self.sim_dir_info.setToolTip("The default runner uses this checkout. Change it on the Install tab.")

    # ── Input list (YAML / CSV) ──────────────────────────────────────────────

    def _add_input(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Select input files",
            str(Path.cwd()),
            "Observation inputs (*.yaml *.yml *.csv);;YAML (*.yaml *.yml);;CSV (*.csv);;All files (*)",
        )
        existing = {self.input_list.item(i).text() for i in range(self.input_list.count())}
        for f in files:
            if f not in existing:
                self.input_list.addItem(f)
        self._refresh_input_status()

    def _remove_input(self) -> None:
        for item in self.input_list.selectedItems():
            self.input_list.takeItem(self.input_list.row(item))
        self._refresh_input_status()

    def _clear_inputs(self) -> None:
        self.input_list.clear()
        self._refresh_input_status()

    def _input_format_counts(self) -> tuple[int, int]:
        """Return (yaml_count, csv_count) across self.input_list."""
        n_yaml = n_csv = 0
        for i in range(self.input_list.count()):
            ext = Path(self.input_list.item(i).text()).suffix.lower()
            if ext in (".yaml", ".yml"):
                n_yaml += 1
            elif ext == ".csv":
                n_csv += 1
        return n_yaml, n_csv

    def _refresh_input_status(self) -> None:
        n_yaml, n_csv = self._input_format_counts()
        self.input_status.setText(f"{n_yaml} YAML  ·  {n_csv} CSV")

    def _add_pipeline_input(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Select input directory", str(Path.cwd()))
        if d and not any(
            self.pipeline_input_list.item(i).text() == d for i in range(self.pipeline_input_list.count())
        ):
            self.pipeline_input_list.addItem(d)
            self._update_output_info()

    def _remove_pipeline_input(self) -> None:
        for it in self.pipeline_input_list.selectedItems():
            self.pipeline_input_list.takeItem(self.pipeline_input_list.row(it))
        self._update_output_info()

    def _clear_pipeline_inputs(self) -> None:
        self.pipeline_input_list.clear()
        self._update_output_info()

    def flow_input(self) -> RunInput:
        """The next run as the data-flow window highlights it (mirrors _build_cmd_args)."""
        translate_only = self.csv_to_yaml_cb.isChecked()
        return RunInput(
            files=[self.input_list.item(i).text() for i in range(self.input_list.count())],
            fits_dirs=[
                self.pipeline_input_list.item(i).text() for i in range(self.pipeline_input_list.count())
            ],
            simulate=not translate_only and not self.rb_pipe_only.isChecked(),
            pipeline=not translate_only and not self.rb_sim_only.isChecked(),
            auto_fetch=self.auto_fetch_cb.isChecked(),
        )

    # ── Run ──────────────────────────────────────────────────────────────────

    def _build_cmd_args(self) -> list[str]:
        args = []

        if self.output_edit.text().strip():
            args += ["-o", self.output_edit.text().strip()]
        # Checkbox checked → --calib 1 (default ON); unchecked → --calib 0.
        args += ["--calib", "1" if self.calib_cb.isChecked() else "0"]
        # Same pattern for static calibration prototypes.
        args += ["--static", "1" if self.static_cb.isChecked() else "0"]
        args += ["--cores", str(self.cores_spin.value())]
        # CSV line range: 0 means unset on either side. Emit START:END with the
        # unset side left blank (e.g. "6:", ":12"); skip entirely if both unset.
        csv_start = self.csv_start_spin.value()
        csv_end = self.csv_end_spin.value()
        if csv_start or csv_end:
            args += ["--csv-lines", f"{csv_start or ''}:{csv_end or ''}"]
        if self.rb_sim_only.isChecked():
            args.append("--no-pipeline")
        elif self.rb_pipe_only.isChecked():
            args.append("--no-sim")
            for i in range(self.pipeline_input_list.count()):
                args += ["--pipeline-input", self.pipeline_input_list.item(i).text()]

        runner = self.runner_combo.currentText()
        args += ["--runner", runner]
        if runner in ("docker", "podman") and self.container_edit.text().strip():
            args += ["--container", self.container_edit.text().strip()]
        # The default runner uses the Install tab's checkout (see _update_runner_fields).
        if runner != "default" and self.sim_dir_edit.text().strip():
            args += ["--simulations-dir", self.sim_dir_edit.text().strip()]
        if self.inst_edit.text().strip():
            args += ["--inst-pkgs", self.inst_edit.text().strip()]
        # Only applies when a pipeline stage actually runs. Emitting it from a
        # control that _update_csv_to_yaml_state has greyed out would assert a
        # value the user cannot see or change.
        if not self.csv_to_yaml_cb.isChecked():
            if self.auto_fetch_cb.isChecked():
                args.append("--auto-fetch-calibrations")
        else:
            # Dry-run translate; run_metis handles this early and ignores the
            # sim/pipeline flags above.
            args.append("--csv-to-yaml")

        if self._edps_target:
            args += ["--edps-target", self._edps_target]

        for i in range(self.input_list.count()):
            args.append(self.input_list.item(i).text())

        return args

    def run_task(self, task: str) -> None:
        """Run the current input with EDPS targeting *task* (from the data-flow window)."""
        if not self.run_btn.isEnabled():
            QMessageBox.warning(self, "Run in progress", "Wait for the current run to finish.")
            return
        self._edps_target = task
        try:
            self._run()
        finally:
            self._edps_target = None

    def _run(self) -> None:
        if self.reject_if_blocked("a pipeline run"):
            return
        if not self.rb_pipe_only.isChecked() and self.input_list.count() == 0:
            QMessageBox.warning(self, "No input files", "Add at least one input file (YAML or CSV).")
            return

        self._save_settings()
        self.log_view.clear()
        self._stopping = False
        self._decoders = {"stdout": StreamDecoder(), "stderr": StreamDecoder()}

        args = self._build_cmd_args()

        # A new QProcess per run, parented to the tab: without this the old
        # ones accumulate for the lifetime of the window.
        if self._process is not None:
            self._process.deleteLater()
        self._process = QProcess(self)
        self._process.setWorkingDirectory(str(paths.data_dir()))
        self._process.readyReadStandardOutput.connect(self._on_stdout)
        self._process.readyReadStandardError.connect(self._on_stderr)
        self._process.finished.connect(self._on_finished)
        self._process.errorOccurred.connect(self._on_process_error)

        # Resolve the environment for the runner the user actually picked:
        # env.py deliberately returns the bare parent environment for
        # native/docker/podman, where the tools live outside MTR's venv.
        qenv = QProcessEnvironment()
        for k, v in _child_env(self.runner_combo.currentText()).items():
            qenv.insert(k, v)
        self._process.setProcessEnvironment(qenv)

        cmd = _resolve_run_metis_command()
        python_exe, module_args = cmd[0], cmd[1:]
        log_append(self.log_view, f"$ {' '.join(cmd + args)}\n\n", "cyan")
        self._process.start(python_exe, module_args + args)
        self.run_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

    def stop_process(self) -> None:
        """Stop a running pipeline on window close, without prompting."""
        if self._process is None:
            return
        if self._process.state() == QProcess.ProcessState.NotRunning:
            return
        self._process.terminate()
        if not self._process.waitForFinished(self.STOP_GRACE_MS):
            self._process.kill()
            self._process.waitForFinished(2000)

    def _on_process_error(self, error) -> None:
        """Recover the buttons when the process never starts.

        Without this, a FailedToStart never reaches `finished`, so Run stayed
        disabled and Stop enabled forever — the tab was dead until restart.
        """
        if error != QProcess.ProcessError.FailedToStart:
            return
        log_append(
            self.log_view,
            f"\n✗ Could not start the run: {self._process.errorString()}\n",
            "red",
        )
        self.run_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)

    def _stop(self) -> None:
        """Ask the run to stop, then insist — without blocking the GUI.

        SIGKILL alone (the old behaviour) skipped run_metis's own cleanup, so
        the EDPS server stayed up on its port, ~/.edps/application.properties
        stayed patched by --prefer-masters, and the temp sim script leaked.
        run_metis turns SIGTERM into a clean unwind; the kill is the fallback.
        Waiting here used to freeze the window, so the cleanup's own output
        only appeared once it was over.
        """
        if not self._process:
            return
        if self._process.state() == QProcess.ProcessState.NotRunning:
            return
        self._stopping = True
        self.stop_btn.setEnabled(False)
        log_append(self.log_view, "\nStopping — waiting for cleanup…\n", "yellow")
        self._process.terminate()
        self._kill_timer.start(self.STOP_GRACE_MS)

    def _kill_after_grace(self) -> None:
        if self._process is None or self._process.state() == QProcess.ProcessState.NotRunning:
            return
        log_append(
            self.log_view,
            "Cleanup did not finish in time; killing the process.\n"
            "Check for a stray EDPS server if the next run misbehaves.\n",
            "red",
        )
        self._process.kill()

    @staticmethod
    def _strip_ansi(text: str) -> str:
        return strip_ansi(text)

    def _on_stdout(self) -> None:
        data = self._process.readAllStandardOutput().data()
        log_append(self.log_view, self._decoders["stdout"].feed(data))

    def _on_stderr(self) -> None:
        data = self._process.readAllStandardError().data()
        log_append(self.log_view, self._decoders["stderr"].feed(data), "orange")

    def _on_finished(self, exit_code: int, status) -> None:
        self._kill_timer.stop()
        for stream, colour in (("stdout", ""), ("stderr", "orange")):
            tail = self._decoders[stream].flush()
            if tail:
                log_append(self.log_view, tail, colour)
        self.run_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        if self._stopping:
            # A user-initiated Stop is not a failure; reporting it as an exit
            # code made it indistinguishable from a real one.
            log_append(self.log_view, "\n■ Stopped.\n", "yellow")
        elif status == QProcess.ExitStatus.CrashExit:
            log_append(self.log_view, "\n✗ The run crashed.\n", "red")
        elif exit_code == 0:
            log_append(self.log_view, "\n✓ Done.\n", "green")
        else:
            log_append(self.log_view, f"\n✗ Exited with code {exit_code}.\n", "red")
        self._stopping = False
        m = re.search(r"^Output root\s*: (.+)$", self.log_view.toPlainText(), re.MULTILINE)
        if m:
            self.run_finished.emit(m.group(1).strip())

    # ── Open output folder ───────────────────────────────────────────────────

    def _open_folder(self) -> None:
        """Open the output directory in the system file manager or a terminal."""
        text = self.output_edit.text().strip()
        target = Path(text) if text else paths.output_dir()

        if not target.exists():
            QMessageBox.information(
                self,
                "Directory not found",
                f"The output directory does not exist yet:\n\n{target}\n\n"
                "Run the pipeline first to create it.",
            )
            return

        url = QUrl.fromLocalFile(str(target))
        if QDesktopServices.openUrl(url):
            return

        # No file manager — fall back to opening a terminal at the directory.
        for term in ("x-terminal-emulator", "xterm", "konsole", "gnome-terminal", "xfce4-terminal"):
            exe = shutil.which(term)
            if not exe:
                continue
            try:
                if term == "gnome-terminal":
                    subprocess.Popen([exe, "--working-directory", str(target)])
                elif term == "konsole":
                    subprocess.Popen([exe, "--workdir", str(target)])
                else:
                    subprocess.Popen([exe], cwd=str(target))
                return
            except OSError:
                continue

        QMessageBox.information(
            self,
            "Cannot open folder",
            f"No file manager or terminal emulator found.\n\nOutput directory:\n{target}",
        )

    # ── Settings persistence ─────────────────────────────────────────────────

    def _load_settings(self) -> None:
        s = self._settings
        self.output_edit.setText(s.value("output", ""))
        self._update_output_info()
        self.calib_cb.setChecked(s.value("calib", True, type=bool))
        self.static_cb.setChecked(s.value("static", True, type=bool))
        self.cores_spin.setValue(s.value("cores", 4, type=int))
        self.csv_start_spin.setValue(s.value("csv_start", 0, type=int))
        self.csv_end_spin.setValue(s.value("csv_end", 0, type=int))
        mode = s.value("pipeline_mode", "both")
        {"sim_only": self.rb_sim_only, "pipe_only": self.rb_pipe_only}.get(mode, self.rb_both).setChecked(
            True
        )
        self.runner_combo.setCurrentText(s.value("runner", "default"))
        self.container_edit.setText(s.value("container", ""))
        self.sim_dir_edit.setText(s.value("sim_dir", ""))
        self.inst_edit.setText(s.value("inst_pkgs", ""))
        self.auto_fetch_cb.setChecked(s.value("auto_fetch", False, type=bool))
        self.csv_to_yaml_cb.setChecked(s.value("csv_to_yaml", False, type=bool))
        self._update_csv_to_yaml_state()
        for f in s.value("pipeline_input_dirs") or []:
            self.pipeline_input_list.addItem(f)
        # Backward-compat: prefer the new "input_files" key, fall back to the
        # legacy "yaml_files" key for users with existing settings.
        saved_inputs = s.value("input_files")
        if saved_inputs is None:
            saved_inputs = s.value("yaml_files") or []
        for f in saved_inputs:
            self.input_list.addItem(f)
        self._refresh_input_status()

    def _save_settings(self) -> None:
        s = self._settings
        s.setValue("output", self.output_edit.text())
        s.setValue("calib", self.calib_cb.isChecked())
        s.setValue("static", self.static_cb.isChecked())
        s.setValue("cores", self.cores_spin.value())
        s.setValue("csv_start", self.csv_start_spin.value())
        s.setValue("csv_end", self.csv_end_spin.value())
        mode = "both"
        if self.rb_sim_only.isChecked():
            mode = "sim_only"
        elif self.rb_pipe_only.isChecked():
            mode = "pipe_only"
        s.setValue("pipeline_mode", mode)
        s.setValue("runner", self.runner_combo.currentText())
        s.setValue("container", self.container_edit.text())
        s.setValue("sim_dir", self.sim_dir_edit.text())
        s.setValue("inst_pkgs", self.inst_edit.text())
        s.setValue("auto_fetch", self.auto_fetch_cb.isChecked())
        s.setValue("csv_to_yaml", self.csv_to_yaml_cb.isChecked())
        s.setValue(
            "pipeline_input_dirs",
            [self.pipeline_input_list.item(i).text() for i in range(self.pipeline_input_list.count())],
        )
        s.setValue("input_files", [self.input_list.item(i).text() for i in range(self.input_list.count())])
        # Remove legacy keys after successful migration to avoid confusion.
        s.remove("yaml_files")
        s.remove("workflow")
