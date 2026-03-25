#!/usr/bin/env python3
"""gui.py — METIS Test Runner GUI

Two-tab graphical front-end:
  Install  — runs the metis-meta-package bootstrap steps non-interactively
  Run      — wraps run_metis.py with a file-picker and options UI
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from PyQt6.QtCore import Qt, QProcess, QSettings, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QTextCharFormat, QTextCursor
from PyQt6.QtWidgets import (
    QApplication, QButtonGroup, QCheckBox, QComboBox, QFileDialog,
    QGroupBox, QHBoxLayout, QLabel, QLineEdit, QListWidget,
    QMainWindow, QMessageBox, QPushButton, QRadioButton,
    QSpinBox, QTabWidget, QTextEdit, QVBoxLayout, QWidget,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT   = Path(__file__).parent.resolve()
META_PKG    = REPO_ROOT / "metis-meta-package"
TARGET_A    = REPO_ROOT / "METIS_Pipeline"
TARGET_B    = REPO_ROOT / "METIS_Simulations"
REPO_A_URL  = "https://github.com/AstarVienna/METIS_Pipeline.git"
REPO_B_URL  = "https://github.com/AstarVienna/METIS_Simulations.git"

LABEL_W = 190   # fixed label column width in the Run options form


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def log_append(widget: QTextEdit, text: str, color: str | None = None) -> None:
    """Append text to a read-only QTextEdit, optionally in colour."""
    cursor = widget.textCursor()
    cursor.movePosition(QTextCursor.MoveOperation.End)
    fmt = QTextCharFormat()
    if color:
        fmt.setForeground(QColor(color))
    cursor.setCharFormat(fmt)
    cursor.insertText(text)
    widget.setTextCursor(cursor)
    widget.ensureCursorVisible()


def _labeled(label_text: str, *content_widgets) -> QWidget:
    """Return a QWidget containing a fixed-width label followed by content widgets."""
    row = QWidget()
    h = QHBoxLayout(row)
    h.setContentsMargins(0, 0, 0, 0)
    lbl = QLabel(label_text)
    lbl.setFixedWidth(LABEL_W)
    h.addWidget(lbl)
    for w in content_widgets:
        h.addWidget(w)
    return row


def _dir_picker(edit: QLineEdit, parent: QWidget) -> QPushButton:
    """Wire up a Browse button for a directory edit; return the button."""
    btn = QPushButton("Browse…")
    btn.clicked.connect(
        lambda: edit.setText(
            QFileDialog.getExistingDirectory(parent, "Select directory", str(REPO_ROOT))
            or edit.text()
        )
    )
    return btn


# ---------------------------------------------------------------------------
# Install worker (background thread)
# ---------------------------------------------------------------------------

class InstallWorker(QThread):
    """Executes all bootstrap steps sequentially in a background thread."""

    log    = pyqtSignal(str, str)   # (text, colour)
    done   = pyqtSignal(bool)       # success

    # ── public ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        try:
            self._step("Checking for uv…")
            self._ensure_uv()

            self._step(f"Cloning / updating METIS_Pipeline  →  {TARGET_A}")
            self._clone_or_update(REPO_A_URL, TARGET_A)

            self._step(f"Cloning / updating METIS_Simulations  →  {TARGET_B}")
            self._clone_or_update(REPO_B_URL, TARGET_B)

            self._step("Installing Python dependencies (uv sync)…")
            self._run(["uv", "sync"], cwd=META_PKG)

            self._step("Initialising EDPS…")
            self._init_edps()

            self._step("Patching ~/.edps/application.properties…")
            self._patch_edps_config()

            self._step("Writing .env…")
            self._write_env()

            self.log.emit("\n✓ Installation complete.\n", "green")
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Failed: {exc}\n", "red")
            self.done.emit(False)

    # ── private helpers ──────────────────────────────────────────────────────

    def _step(self, msg: str) -> None:
        self.log.emit(f"\n── {msg}\n", "cyan")

    def _run(self, cmd: list, cwd: Path | None = None,
             stdin_text: str | None = None, timeout: int = 300) -> None:
        self.log.emit(f"$ {' '.join(str(c) for c in cmd)}\n", "")
        proc = subprocess.Popen(
            [str(c) for c in cmd],
            cwd=str(cwd) if cwd else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE if stdin_text is not None else subprocess.DEVNULL,
            text=True,
        )
        if stdin_text is not None:
            try:
                proc.stdin.write(stdin_text)
                proc.stdin.flush()
                proc.stdin.close()
            except BrokenPipeError:
                pass
        for line in proc.stdout:
            self.log.emit(line, "")
        proc.wait(timeout=timeout)
        if proc.returncode not in (0, None):
            raise RuntimeError(
                f"Command exited {proc.returncode}: {' '.join(str(c) for c in cmd)}"
            )

    def _ensure_uv(self) -> None:
        if shutil.which("uv"):
            self.log.emit("uv is already on PATH.\n", "")
            return
        self.log.emit("uv not found — downloading installer…\n", "")
        curl = subprocess.run(
            ["curl", "-LsSf", "https://astral.sh/uv/install.sh"],
            capture_output=True,
        )
        if curl.returncode != 0:
            raise RuntimeError("Failed to download the uv installer via curl.")
        proc = subprocess.Popen(
            ["sh"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        out, _ = proc.communicate(input=curl.stdout.decode())
        self.log.emit(out, "")
        if proc.returncode != 0:
            raise RuntimeError("uv installer script failed.")
        # Make uv visible to subsequent calls in this process
        env_script = Path.home() / ".local" / "bin" / "env"
        if env_script.exists():
            result = subprocess.run(
                ["bash", "-c", f"source {env_script} && echo $PATH"],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                os.environ["PATH"] = result.stdout.strip()

    def _clone_or_update(self, url: str, target: Path) -> None:
        if (target / ".git").is_dir():
            self._run(["git", "-C", str(target), "fetch", "--all", "--prune"])
            result = subprocess.run(
                ["git", "-C", str(target), "pull", "--ff-only"],
                capture_output=True, text=True,
            )
            self.log.emit(result.stdout + result.stderr, "")
        elif target.is_dir():
            self.log.emit(f"⚠ {target} exists but is not a git repo — skipping.\n", "yellow")
        else:
            self._run(["git", "clone", "--depth", "1", url, str(target)])

    def _init_edps(self) -> None:
        """Run edps once to generate ~/.edps/application.properties."""
        env_file = META_PKG / ".env"
        cmd = ["uv", "run", "--env-file", str(env_file), "edps", "-P", "4444"]
        self.log.emit(f"$ {' '.join(cmd)}\n", "")
        proc = subprocess.Popen(
            cmd, cwd=str(META_PKG),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            out, _ = proc.communicate(input="\n", timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
        self.log.emit(out or "", "")
        # Stop any running instance
        subprocess.run(
            ["uv", "run", "--env-file", str(env_file), "edps", "-P", "4444", "-s"],
            cwd=str(META_PKG), capture_output=True, timeout=15,
        )

    def _patch_edps_config(self) -> None:
        props = Path.home() / ".edps" / "application.properties"
        if not props.exists():
            raise RuntimeError(
                f"{props} not found — did EDPS initialise correctly?"
            )
        text = props.read_text()
        patches = {
            r"^port=.*":          "port=4444",
            r"^workflow_dir=.*":  f"workflow_dir={TARGET_A}/metisp/workflows",
            r"^esorex_path=.*":   "esorex_path=pyesorex",
        }
        for pattern, replacement in patches.items():
            text = re.sub(pattern, replacement, text, flags=re.MULTILINE)
        props.write_text(text)
        self.log.emit(f"Patched {props}\n", "")

    def _write_env(self) -> None:
        env_path = META_PKG / ".env"
        env_path.write_text(
            f"PYTHONPATH={TARGET_A}/metisp/pymetis/src/\n"
            f"PYCPL_RECIPE_DIR={TARGET_A}/metisp/pyrecipes/\n"
            f"PYESOREX_PLUGIN_DIR={TARGET_A}/metisp/pyrecipes/\n"
        )
        self.log.emit(f"Written {env_path}\n", "")


# ---------------------------------------------------------------------------
# Install tab
# ---------------------------------------------------------------------------

class InstallTab(QWidget):

    def __init__(self) -> None:
        super().__init__()
        self._worker: InstallWorker | None = None
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(14)

        desc = QLabel(
            "<b>METIS Pipeline Installation</b><br><br>"
            "Clicking <i>Install / Update</i> will perform the following steps:<br>"
            "<ol>"
            "<li>Install <code>uv</code> if not already on PATH</li>"
            f"<li>Clone or update <b>METIS_Pipeline</b> and <b>METIS_Simulations</b> "
            f"into <code>{REPO_ROOT}</code></li>"
            "<li>Install all Python dependencies via <code>uv sync</code></li>"
            "<li>Initialise and configure EDPS on port 4444</li>"
            f"<li>Write <code>{META_PKG / '.env'}</code></li>"
            "</ol>"
            "Re-running is safe — existing repositories will be updated, not re-cloned."
        )
        desc.setWordWrap(True)
        desc.setTextFormat(Qt.TextFormat.RichText)
        layout.addWidget(desc)

        self.install_btn = QPushButton("Install / Update")
        self.install_btn.setMinimumHeight(36)
        self.install_btn.setMaximumWidth(200)
        self.install_btn.clicked.connect(self._start)
        layout.addWidget(self.install_btn)

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Monospace", 9))
        layout.addWidget(self.log_view)

    def _start(self) -> None:
        self.log_view.clear()
        self.install_btn.setEnabled(False)
        self._worker = InstallWorker()
        self._worker.log.connect(lambda text, color: log_append(self.log_view, text, color))
        self._worker.done.connect(self._on_done)
        self._worker.start()

    def _on_done(self, success: bool) -> None:
        self.install_btn.setEnabled(True)


# ---------------------------------------------------------------------------
# Run tab
# ---------------------------------------------------------------------------

class RunTab(QWidget):

    def __init__(self) -> None:
        super().__init__()
        self._process: QProcess | None = None
        self._settings = QSettings("METIS", "TestRunner")
        self._build_ui()
        self._load_settings()
        self._update_runner_fields()

    # ── UI construction ──────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setSpacing(12)

        # ── YAML file list ──
        yaml_grp = QGroupBox("YAML Input Files")
        yaml_lay = QVBoxLayout(yaml_grp)
        file_row = QHBoxLayout()
        self.yaml_list = QListWidget()
        self.yaml_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        self.yaml_list.setMaximumHeight(120)
        file_row.addWidget(self.yaml_list)
        btn_col = QVBoxLayout()
        add_btn = QPushButton("Add…")
        add_btn.clicked.connect(self._add_yaml)
        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(self._remove_yaml)
        btn_col.addWidget(add_btn)
        btn_col.addWidget(remove_btn)
        btn_col.addStretch()
        file_row.addLayout(btn_col)
        yaml_lay.addLayout(file_row)
        outer.addWidget(yaml_grp)

        # ── Options ──
        opts_grp = QGroupBox("Options")
        opts_lay = QVBoxLayout(opts_grp)
        opts_lay.setSpacing(6)

        # Output directory
        self.output_edit = QLineEdit()
        out_browse = _dir_picker(self.output_edit, self)
        opts_lay.addWidget(
            _labeled("Output directory:", self.output_edit, out_browse)
        )

        # Checkboxes
        cb_row = QWidget()
        cb_h = QHBoxLayout(cb_row)
        cb_h.setContentsMargins(0, 0, 0, 0)
        lbl = QLabel("")
        lbl.setFixedWidth(LABEL_W)
        cb_h.addWidget(lbl)
        self.calib_cb = QCheckBox("Auto-generate calibration frames  (--calib)")
        self.small_cb = QCheckBox("Small detectors 32×32  (--small)")
        cb_h.addWidget(self.calib_cb)
        cb_h.addWidget(self.small_cb)
        cb_h.addStretch()
        opts_lay.addWidget(cb_row)

        # Cores
        self.cores_spin = QSpinBox()
        self.cores_spin.setRange(1, os.cpu_count() or 16)
        self.cores_spin.setValue(4)
        self.cores_spin.setMaximumWidth(70)
        opts_lay.addWidget(_labeled("CPU cores  (--cores):", self.cores_spin))

        # Pipeline mode
        mode_row = QWidget()
        mode_h = QHBoxLayout(mode_row)
        mode_h.setContentsMargins(0, 0, 0, 0)
        lbl2 = QLabel("Pipeline mode:")
        lbl2.setFixedWidth(LABEL_W)
        mode_h.addWidget(lbl2)
        self._mode_grp = QButtonGroup(self)
        self.rb_both      = QRadioButton("Simulate + run pipeline")
        self.rb_sim_only  = QRadioButton("Simulate only  (--no-pipeline)")
        self.rb_pipe_only = QRadioButton("Pipeline only  (--no-sim)")
        for rb in (self.rb_both, self.rb_sim_only, self.rb_pipe_only):
            self._mode_grp.addButton(rb)
            mode_h.addWidget(rb)
        mode_h.addStretch()
        self.rb_both.setChecked(True)
        opts_lay.addWidget(mode_row)

        # Runner
        self.runner_combo = QComboBox()
        self.runner_combo.addItems(["metapkg", "native", "docker", "podman"])
        self.runner_combo.setMaximumWidth(130)
        self.runner_combo.currentTextChanged.connect(self._update_runner_fields)
        opts_lay.addWidget(_labeled("Runner  (--runner):", self.runner_combo))

        # Container name  [docker / podman only]
        self.container_edit = QLineEdit()
        self.container_edit.setPlaceholderText("e.g. metis-pipeline")
        self.container_row = _labeled("Container  (--container):", self.container_edit)
        opts_lay.addWidget(self.container_row)

        # Meta-package dir  [metapkg only]
        self.meta_pkg_edit = QLineEdit()
        meta_browse = _dir_picker(self.meta_pkg_edit, self)
        self.meta_pkg_edit.setPlaceholderText(str(META_PKG))
        self.meta_pkg_row = _labeled("Meta-package dir  (--meta-pkg):", self.meta_pkg_edit, meta_browse)
        opts_lay.addWidget(self.meta_pkg_row)

        # Simulations dir  [always visible]
        self.sim_dir_edit = QLineEdit()
        sim_browse = _dir_picker(self.sim_dir_edit, self)
        self.sim_dir_edit.setPlaceholderText(str(TARGET_B))
        opts_lay.addWidget(_labeled("Simulations dir  (--simulations-dir):", self.sim_dir_edit, sim_browse))

        # Instrument packages  [always visible]
        self.inst_edit = QLineEdit()
        inst_browse = _dir_picker(self.inst_edit, self)
        self.inst_edit.setPlaceholderText("(auto-resolved per runner)")
        opts_lay.addWidget(_labeled("Instrument packages  (--inst-pkgs):", self.inst_edit, inst_browse))

        outer.addWidget(opts_grp)

        # ── Run / Stop ──
        run_row = QHBoxLayout()
        self.run_btn = QPushButton("Run")
        self.run_btn.setMinimumHeight(36)
        self.run_btn.setMaximumWidth(120)
        self.run_btn.clicked.connect(self._run)
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setMinimumHeight(36)
        self.stop_btn.setMaximumWidth(120)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self._stop)
        run_row.addWidget(self.run_btn)
        run_row.addWidget(self.stop_btn)
        run_row.addStretch()
        outer.addLayout(run_row)

        # ── Output log ──
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Monospace", 9))
        outer.addWidget(self.log_view)

    # ── Runner-dependent field visibility ────────────────────────────────────

    def _update_runner_fields(self) -> None:
        runner = self.runner_combo.currentText()
        self.container_row.setVisible(runner in ("docker", "podman"))
        self.meta_pkg_row.setVisible(runner == "metapkg")

    # ── YAML list ────────────────────────────────────────────────────────────

    def _add_yaml(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self, "Select YAML files", str(REPO_ROOT), "YAML files (*.yaml *.yml)"
        )
        existing = {self.yaml_list.item(i).text() for i in range(self.yaml_list.count())}
        for f in files:
            if f not in existing:
                self.yaml_list.addItem(f)

    def _remove_yaml(self) -> None:
        for item in self.yaml_list.selectedItems():
            self.yaml_list.takeItem(self.yaml_list.row(item))

    # ── Run ──────────────────────────────────────────────────────────────────

    def _build_cmd_args(self) -> list[str]:
        args = []

        if self.output_edit.text().strip():
            args += ["-o", self.output_edit.text().strip()]
        if self.calib_cb.isChecked():
            args.append("--calib")
        if self.small_cb.isChecked():
            args.append("--small")
        args += ["--cores", str(self.cores_spin.value())]
        if self.rb_sim_only.isChecked():
            args.append("--no-pipeline")
        elif self.rb_pipe_only.isChecked():
            args.append("--no-sim")

        runner = self.runner_combo.currentText()
        args += ["--runner", runner]
        if runner in ("docker", "podman") and self.container_edit.text().strip():
            args += ["--container", self.container_edit.text().strip()]
        if runner == "metapkg" and self.meta_pkg_edit.text().strip():
            args += ["--meta-pkg", self.meta_pkg_edit.text().strip()]
        if self.sim_dir_edit.text().strip():
            args += ["--simulations-dir", self.sim_dir_edit.text().strip()]
        if self.inst_edit.text().strip():
            args += ["--inst-pkgs", self.inst_edit.text().strip()]

        for i in range(self.yaml_list.count()):
            args.append(self.yaml_list.item(i).text())

        return args

    def _run(self) -> None:
        if self.yaml_list.count() == 0:
            QMessageBox.warning(self, "No input files", "Add at least one YAML file.")
            return

        self._save_settings()
        self.log_view.clear()

        args = self._build_cmd_args()
        script = str(REPO_ROOT / "run_metis.py")

        self._process = QProcess(self)
        self._process.setWorkingDirectory(str(REPO_ROOT))
        self._process.readyReadStandardOutput.connect(self._on_stdout)
        self._process.readyReadStandardError.connect(self._on_stderr)
        self._process.finished.connect(self._on_finished)

        log_append(self.log_view, f"$ {sys.executable} {script} {' '.join(args)}\n\n", "cyan")
        self._process.start(sys.executable, [script] + args)
        self.run_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

    def _stop(self) -> None:
        if self._process and self._process.state() != QProcess.ProcessState.NotRunning:
            self._process.kill()

    def _on_stdout(self) -> None:
        data = self._process.readAllStandardOutput().data().decode(errors="replace")
        log_append(self.log_view, data)

    def _on_stderr(self) -> None:
        data = self._process.readAllStandardError().data().decode(errors="replace")
        log_append(self.log_view, data, "orange")

    def _on_finished(self, exit_code: int, _status) -> None:
        self.run_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        if exit_code == 0:
            log_append(self.log_view, "\n✓ Done.\n", "green")
        else:
            log_append(self.log_view, f"\n✗ Exited with code {exit_code}.\n", "red")

    # ── Settings persistence ─────────────────────────────────────────────────

    def _load_settings(self) -> None:
        s = self._settings
        self.output_edit.setText(s.value("output", ""))
        self.calib_cb.setChecked(s.value("calib", False, type=bool))
        self.small_cb.setChecked(s.value("small", False, type=bool))
        self.cores_spin.setValue(s.value("cores", 4, type=int))
        mode = s.value("pipeline_mode", "both")
        {"sim_only": self.rb_sim_only, "pipe_only": self.rb_pipe_only}.get(
            mode, self.rb_both
        ).setChecked(True)
        self.runner_combo.setCurrentText(s.value("runner", "metapkg"))
        self.container_edit.setText(s.value("container", ""))
        self.meta_pkg_edit.setText(s.value("meta_pkg", ""))
        self.sim_dir_edit.setText(s.value("sim_dir", ""))
        self.inst_edit.setText(s.value("inst_pkgs", ""))
        for f in s.value("yaml_files", []):
            self.yaml_list.addItem(f)

    def _save_settings(self) -> None:
        s = self._settings
        s.setValue("output", self.output_edit.text())
        s.setValue("calib", self.calib_cb.isChecked())
        s.setValue("small", self.small_cb.isChecked())
        s.setValue("cores", self.cores_spin.value())
        mode = "both"
        if self.rb_sim_only.isChecked():
            mode = "sim_only"
        elif self.rb_pipe_only.isChecked():
            mode = "pipe_only"
        s.setValue("pipeline_mode", mode)
        s.setValue("runner", self.runner_combo.currentText())
        s.setValue("container", self.container_edit.text())
        s.setValue("meta_pkg", self.meta_pkg_edit.text())
        s.setValue("sim_dir", self.sim_dir_edit.text())
        s.setValue("inst_pkgs", self.inst_edit.text())
        s.setValue("yaml_files", [
            self.yaml_list.item(i).text() for i in range(self.yaml_list.count())
        ])


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("METIS Test Runner")
        self.resize(900, 720)

        tabs = QTabWidget()
        self._run_tab = RunTab()
        tabs.addTab(InstallTab(), "Install")
        tabs.addTab(self._run_tab, "Run")
        self.setCentralWidget(tabs)

    def closeEvent(self, event) -> None:
        self._run_tab._save_settings()
        super().closeEvent(event)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    app = QApplication(sys.argv)
    app.setApplicationName("METIS Test Runner")
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
