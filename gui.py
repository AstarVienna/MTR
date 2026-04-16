#!/usr/bin/env python3
"""gui.py — METIS Test Runner GUI

Three-tab graphical front-end:
  Install  — runs the metis-meta-package bootstrap steps non-interactively
  Run      — wraps run_metis.py with a file-picker and options UI
  Archive  — manage Podman-based archive stack and upload/download FITS files
"""

import os
import re
import subprocess
import sys
from pathlib import Path

from PyQt6.QtCore import Qt, QProcess, QProcessEnvironment, QSettings, QThread, QTimer, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QPalette, QTextCharFormat, QTextCursor
from PyQt6.QtWidgets import (
    QAbstractSpinBox, QApplication, QButtonGroup, QCheckBox, QComboBox,
    QFileDialog, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QListWidget,
    QMainWindow, QMessageBox, QProgressBar, QPushButton, QRadioButton,
    QSpinBox, QStackedWidget, QTabBar, QTabWidget, QTextEdit, QVBoxLayout,
    QWidget,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT   = Path(__file__).parent.resolve()
META_PKG    = Path(os.environ.get("METIS_META_PKG", str(REPO_ROOT / "metis-meta-package")))
TARGET_A    = REPO_ROOT / "METIS_Pipeline"
TARGET_B    = REPO_ROOT / "METIS_Simulations"
REPO_A_URL  = "https://github.com/AstarVienna/METIS_Pipeline.git"
REPO_B_URL  = "https://github.com/AstarVienna/METIS_Simulations.git"

LABEL_W = 280   # fixed label column width in the Run options form


# ---------------------------------------------------------------------------
# Subprocess environment
# ---------------------------------------------------------------------------

def _child_env() -> dict[str, str]:
    """Return a copy of os.environ with uv's venv-activation variables removed.

    The GUI is launched via ``uv run gui.py`` (from launch.sh), which sets
    ``VIRTUAL_ENV`` to MTR's own .venv. If we inherit that into subprocesses
    that invoke ``uv run --project <meta-pkg>``, uv prints a warning on every
    call about the mismatched active venv. Stripping these variables silences
    that warning without affecting uv's project resolution.
    """
    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)
    env.pop("UV_PROJECT_ENVIRONMENT", None)
    return env


# ---------------------------------------------------------------------------
# Themes
# ---------------------------------------------------------------------------

THEMES: dict[str, dict[str, str]] = {
    "dark": {
        # SWP-S50 colour palette
        "window":         "#16203A",
        "window_text":    "#E8EAF6",
        "base":           "#0E1830",
        "alt_base":       "#243046",
        "button":         "#243046",
        "button_text":    "#E8EAF6",
        "highlight":      "#4A9EFF",
        "highlight_text": "#16203A",
        "placeholder":    "#4A5568",
        "tooltip_base":   "#243046",
        "tooltip_text":   "#E8EAF6",
        "border":         "#3A4E6E",
        "accent":         "#4A9EFF",
        "accent_dim":     "#1C3A6E",
        "btn_success_bg": "#a6e3a1",   # keep Catppuccin green (no local equivalent)
        "btn_success_fg": "#16203A",
        "btn_danger_bg":  "#FF6B6B",   # SWP-S50 red
        "btn_danger_fg":  "#16203A",
        "btn_info_bg":    "#4A9EFF",   # SWP-S50 blue
        "btn_info_fg":    "#16203A",
        "log_green":      "#a6e3a1",
        "log_red":        "#FF6B6B",
        "log_cyan":       "#94e2d5",
        "log_yellow":     "#f9e2af",
        "log_orange":     "#fab387",
        "log_gray":       "#4A5568",
        "log_default":    "#E8EAF6",
    },
    "light": {
        # Catppuccin Latte-inspired
        "window":         "#eff1f5",
        "window_text":    "#4c4f69",
        "base":           "#e6e9ef",
        "alt_base":       "#dce0e8",
        "button":         "#ddd9f5",   # lavender-tinted instead of cold gray
        "button_text":    "#4c4f69",
        "highlight":      "#7287fd",   # Latte Lavender (pastel, replaces bold blue)
        "highlight_text": "#eff1f5",
        "placeholder":    "#9ca0b0",
        "tooltip_base":   "#ddd9f5",
        "tooltip_text":   "#4c4f69",
        "border":         "#bcc0cc",
        "accent":         "#7287fd",   # Latte Lavender
        "accent_dim":     "#b4b8f5",   # pale lilac for scroll handles
        "btn_success_bg": "#b8e8b8",   # pastel green
        "btn_success_fg": "#1e3a1e",
        "btn_danger_bg":  "#f8b8c4",   # pastel red/pink
        "btn_danger_fg":  "#3a1e24",
        "btn_info_bg":    "#b8d0f8",   # pastel blue
        "btn_info_fg":    "#1e2a4a",
        "log_green":      "#40a02b",
        "log_red":        "#d20f39",
        "log_cyan":       "#179299",
        "log_yellow":     "#df8e1d",
        "log_orange":     "#fe640b",
        "log_gray":       "#9ca0b0",
        "log_default":    "#4c4f69",
    },
    "pink": {
        # Catppuccin-flavoured deep magenta
        "window":         "#1e1228",
        "window_text":    "#f5d0fe",
        "base":           "#160e1e",
        "alt_base":       "#2d1a42",
        "button":         "#3e2060",
        "button_text":    "#f5d0fe",
        "highlight":      "#e879f9",
        "highlight_text": "#1e1228",
        "placeholder":    "#9f6fb0",
        "tooltip_base":   "#3e2060",
        "tooltip_text":   "#f5d0fe",
        "border":         "#5b3070",
        "accent":         "#f5c2e7",   # Mocha Pink
        "accent_dim":     "#9a6080",   # muted Pink for scroll handles
        "btn_success_bg": "#a6e3a1",   # green (contrasts well against pink)
        "btn_success_fg": "#1e1228",
        "btn_danger_bg":  "#f38ba8",   # red-pink
        "btn_danger_fg":  "#1e1228",
        "btn_info_bg":    "#cba6f7",   # mauve/purple
        "btn_info_fg":    "#1e1228",
        "log_green":      "#a6e3a1",
        "log_red":        "#f38ba8",
        "log_cyan":       "#f5c2e7",
        "log_yellow":     "#f9e2af",
        "log_orange":     "#fab387",
        "log_gray":       "#9f6fb0",
        "log_default":    "#f5d0fe",
    },
}

# Mutable mapping updated by apply_theme(); used by log_append()
LOG_COLORS: dict[str, str] = {}


def apply_theme(app: QApplication, name: str) -> None:
    """Apply a named theme to the application (palette + stylesheet)."""
    t = THEMES[name]
    LOG_COLORS.update({k: t[k] for k in t if k.startswith("log_")})

    app.setStyle("Fusion")

    def c(key: str) -> QColor:
        return QColor(t[key])

    pal = QPalette()
    pal.setColor(QPalette.ColorRole.Window,          c("window"))
    pal.setColor(QPalette.ColorRole.WindowText,      c("window_text"))
    pal.setColor(QPalette.ColorRole.Base,            c("base"))
    pal.setColor(QPalette.ColorRole.AlternateBase,   c("alt_base"))
    pal.setColor(QPalette.ColorRole.Button,          c("button"))
    pal.setColor(QPalette.ColorRole.ButtonText,      c("button_text"))
    pal.setColor(QPalette.ColorRole.Highlight,       c("highlight"))
    pal.setColor(QPalette.ColorRole.HighlightedText, c("highlight_text"))
    pal.setColor(QPalette.ColorRole.PlaceholderText, c("placeholder"))
    pal.setColor(QPalette.ColorRole.ToolTipBase,     c("tooltip_base"))
    pal.setColor(QPalette.ColorRole.ToolTipText,     c("tooltip_text"))
    pal.setColor(QPalette.ColorRole.Text,            c("window_text"))
    pal.setColor(QPalette.ColorRole.BrightText,      c("highlight"))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.WindowText, c("placeholder"))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, c("placeholder"))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text,       c("placeholder"))
    app.setPalette(pal)

    brd        = t["border"]
    hl         = t["highlight"]
    win        = t["window"]
    win_text   = t["window_text"]
    alt        = t["alt_base"]
    log_gray   = t["log_gray"]
    accent     = t["accent"]
    accent_dim = t["accent_dim"]
    s_bg  = t["btn_success_bg"];  s_fg  = t["btn_success_fg"]
    d_bg  = t["btn_danger_bg"];   d_fg  = t["btn_danger_fg"]
    i_bg  = t["btn_info_bg"];     i_fg  = t["btn_info_fg"]
    # hover colours: slightly darker for light theme, lighter for dark/pink
    _shift = (lambda col: QColor(col).darker(110).name()) if name == "light" \
             else (lambda col: QColor(col).lighter(118).name())
    s_hov = _shift(s_bg); d_hov = _shift(d_bg)
    i_hov = _shift(i_bg); a_hov = _shift(accent)
    a_fg           = t["highlight_text"]   # accent-role button uses highlight_text
    highlight_text = a_fg

    app.setStyleSheet(f"""
        QGroupBox {{
            border: 1px solid {brd};
            border-radius: 6px;
            margin-top: 10px;
            padding-top: 4px;
            font-weight: bold;
        }}
        QGroupBox::title {{
            color: {accent};
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 4px;
        }}
        QTabWidget::pane {{
            border: 1px solid {brd};
        }}
        QTabBar::tab {{
            padding: 8px 0;
            min-width: 0;
            border: 1px solid {accent_dim};
            border-bottom: none;
            border-top-left-radius: 6px;
            border-top-right-radius: 6px;
        }}
        QTabBar::tab:selected {{
            background: {win};
            color: {accent};
            border-color: {accent};
            border-top: 3px solid {accent};
        }}
        QTabBar::tab:hover:!selected {{
            background: {alt};
            border-color: {accent};
        }}
        QLineEdit:focus, QTextEdit:focus {{
            border: 1px solid {hl};
        }}
        QScrollBar::handle:vertical, QScrollBar::handle:horizontal {{
            background: {accent_dim};
            border-radius: 5px;
            min-height: 20px;
            min-width: 20px;
        }}
        QScrollBar::handle:vertical:hover, QScrollBar::handle:horizontal:hover {{
            background: {accent};
        }}
        QLabel[hint="true"] {{
            color: {log_gray};
            font-size: 10px;
        }}
        QComboBox {{
            border: 1px solid {accent_dim};
            border-radius: 6px;
            padding: 3px 8px;
            background-color: {alt};
            color: {win_text};
            min-height: 22px;
        }}
        QComboBox:focus {{
            border-color: {hl};
        }}
        QComboBox::drop-down {{
            border: none;
            width: 20px;
        }}
        QComboBox QAbstractItemView {{
            border: 1px solid {accent_dim};
            background-color: {alt};
            color: {win_text};
            selection-background-color: {hl};
            selection-color: {highlight_text};
            outline: none;
            padding: 2px;
        }}
        QComboBox QAbstractItemView::item {{
            color: {win_text};
            background-color: {alt};
            padding: 4px 8px;
        }}
        QComboBox QAbstractItemView::item:selected {{
            background-color: {hl};
            color: {highlight_text};
        }}
        QToolBar {{
            border: none;
            border-bottom: 1px solid {brd};
            spacing: 4px;
            padding: 2px 6px;
        }}
        QPushButton {{
            border: none;
            border-radius: 8px;
            padding: 5px 16px;
            min-height: 24px;
            font-weight: 500;
        }}
        QPushButton:disabled {{
            opacity: 0.45;
        }}
        QPushButton[role="success"] {{
            background-color: {s_bg}; color: {s_fg};
        }}
        QPushButton[role="success"]:hover {{
            background-color: {s_hov};
        }}
        QPushButton[role="danger"] {{
            background-color: {d_bg}; color: {d_fg};
        }}
        QPushButton[role="danger"]:hover {{
            background-color: {d_hov};
        }}
        QPushButton[role="info"] {{
            background-color: {i_bg}; color: {i_fg};
        }}
        QPushButton[role="info"]:hover {{
            background-color: {i_hov};
        }}
        QPushButton[role="accent"] {{
            background-color: {accent}; color: {a_fg};
        }}
        QPushButton[role="accent"]:hover {{
            background-color: {a_hov};
        }}
        QPushButton[role="browse"] {{
            background-color: transparent;
            border: 1px solid {accent_dim};
            color: {accent};
        }}
        QPushButton[role="browse"]:hover {{
            border-color: {accent};
            background-color: {alt};
        }}
    """)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def log_append(widget: QTextEdit, text: str, color: str | None = None) -> None:
    """Append text to a read-only QTextEdit, optionally in colour."""
    cursor = widget.textCursor()
    cursor.movePosition(QTextCursor.MoveOperation.End)
    fmt = QTextCharFormat()
    if color:
        resolved = LOG_COLORS.get(f"log_{color}", color)
        fmt.setForeground(QColor(resolved))
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
    lbl.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    h.addWidget(lbl)
    for w in content_widgets:
        h.addWidget(w)
    return row


def _dir_picker(edit: QLineEdit, parent: QWidget) -> QPushButton:
    """Wire up a Browse button for a directory edit; return the button."""
    btn = QPushButton("Browse…")
    btn.setProperty("role", "browse")
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
            self._step(f"Cloning / updating METIS_Pipeline  →  {TARGET_A}")
            self._clone_or_update(REPO_A_URL, TARGET_A)

            self._step(f"Cloning / updating METIS_Simulations  →  {TARGET_B}")
            self._clone_or_update(REPO_B_URL, TARGET_B)

            self._step("Installing Python dependencies (uv sync --group pipeline)…")
            recipe_dir = str(TARGET_A / "metisp" / "pyrecipes") + "/"
            os.environ["PYCPL_RECIPE_DIR"] = recipe_dir
            os.environ["PYESOREX_PLUGIN_DIR"] = recipe_dir
            self._run(["uv", "sync", "--group", "pipeline"], cwd=REPO_ROOT)

            self._step("Writing .env…")
            self._write_env()

            self._step("Initialising EDPS…")
            self._init_edps()

            self._step("Patching ~/.edps/application.properties…")
            self._patch_edps_config()

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
            env=_child_env(),
        )
        if stdin_text is not None:
            try:
                proc.stdin.write(stdin_text)
                proc.stdin.flush()
                proc.stdin.close()
            except BrokenPipeError:
                pass
        for line in proc.stdout:
            self.log.emit(re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", line), "")
        proc.wait(timeout=timeout)
        if proc.returncode not in (0, None):
            raise RuntimeError(
                f"Command exited {proc.returncode}: {' '.join(str(c) for c in cmd)}"
            )

    def _clone_or_update(self, url: str, target: Path) -> None:
        # .git can be a directory (normal clone) OR a file pointing at
        # .git/modules/<name>/ (submodule checkout); both are valid git repos.
        if (target / ".git").exists():
            self._run(["git", "-C", str(target), "fetch", "--all", "--prune"])
            result = subprocess.run(
                ["git", "-C", str(target), "pull", "--ff-only"],
                capture_output=True, text=True,
            )
            self.log.emit(result.stdout + result.stderr, "")
        elif target.is_dir():
            # Directory exists but is not a git repo. If it's empty (a common
            # leftover from an aborted previous install) we can safely clone
            # into it. Otherwise refuse — silently skipping would leave the
            # rest of the install referencing a non-existent checkout.
            if any(target.iterdir()):
                raise RuntimeError(
                    f"{target} exists but is not a git repo and is not empty. "
                    f"Remove or rename it and re-run the install."
                )
            self._run(["git", "clone", "--depth", "1", url, str(target)])
        else:
            self._run(["git", "clone", "--depth", "1", url, str(target)])

    def _init_edps(self) -> None:
        """Run edps once to generate ~/.edps/application.properties, then stop it.

        EDPS prompts for a bookkeeping directory on first run; we send a newline
        to accept the default.  After edps daemonises the process exits, then we
        issue -s to stop the background server.
        """
        env_file = REPO_ROOT / ".env"
        base = ["uv", "run", "--no-sync", "--env-file", str(env_file),
                "edps", "-P", "4444"]
        try:
            self._run(base, cwd=REPO_ROOT, stdin_text="\n", timeout=60)
        finally:
            subprocess.run(base + ["-s"], cwd=str(REPO_ROOT),
                           capture_output=True, timeout=15,
                           env=_child_env())

    def _patch_edps_config(self) -> None:
        props = Path.home() / ".edps" / "application.properties"
        if not props.exists():
            raise RuntimeError(
                f"{props} not found — did EDPS initialise correctly?"
            )
        text = props.read_text()
        patches = {
            "port":         (r"^port=.*",         "port=4444"),
            "workflow_dir": (r"^workflow_dir=.*", f"workflow_dir={TARGET_A}/metisp/workflows"),
            "esorex_path":  (r"^esorex_path=.*",  "esorex_path=pyesorex"),
            "association_preference": (
                r"^association_preference=.*",
                "association_preference=master_per_quality_level",
            ),
        }
        for key, (pattern, replacement) in patches.items():
            text, count = re.subn(pattern, replacement, text, flags=re.MULTILINE)
            if count == 0:
                raise RuntimeError(
                    f"{props} has no '{key}=' line to patch — EDPS config "
                    f"format may have changed; re-run EDPS initialisation."
                )
        props.write_text(text)
        self.log.emit(f"Patched {props}\n", "")

    def _write_env(self) -> None:
        env_path = REPO_ROOT / ".env"
        env_path.write_text(
            f"PYTHONPATH={TARGET_B}:{TARGET_A}/metisp/pymetis/src/\n"
            f"PYCPL_RECIPE_DIR={TARGET_A}/metisp/pyrecipes/\n"
            f"PYESOREX_PLUGIN_DIR={TARGET_A}/metisp/pyrecipes/\n"
            "PYESOREX_MSG_LEVEL=debug\n"
            "PYESOREX_LOG_LEVEL=debug\n"
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
        layout.setContentsMargins(20, 20, 20, 20)

        desc = QLabel(
            "<b>METIS Pipeline Installation</b><br><br>"
            "Skip this tab if you have already installed the pipeline via one of "
            "these methods and go straight to <b>Run</b>:<br>"
            "<ul>"
            "<li><b>metis-meta-package</b> — select runner <i>metapkg</i> and set "
            "<i>Meta-package dir</i> to your <code>metis-meta-package</code> folder</li>"
            "<li><b>Bare-metal / ESO docs</b> — select runner <i>native</i></li>"
            "<li><b>Pipeline container</b> — select runner <i>docker</i> or "
            "<i>podman</i> and enter the container name</li>"
            "</ul>"
            "Otherwise, clicking <i>Install / Update</i> will perform the following "
            "steps:<br>"
            "<ol>"
            f"<li>Clone or update <b>METIS_Pipeline</b> and <b>METIS_Simulations</b> "
            f"into <code>{REPO_ROOT}</code></li>"
            "<li>Install all Python dependencies via <code>uv sync --group pipeline</code></li>"
            f"<li>Write <code>{REPO_ROOT / '.env'}</code></li>"
            "<li>Initialise and configure EDPS on port 4444</li>"
            "</ol>"
            "Re-running is safe — existing repositories will be updated, not re-cloned."
        )
        desc.setWordWrap(True)
        desc.setTextFormat(Qt.TextFormat.RichText)
        layout.addWidget(desc)

        self.install_btn = QPushButton("Install / Update")
        self.install_btn.setProperty("role", "success")
        self.install_btn.setMinimumHeight(36)
        self.install_btn.setMaximumWidth(200)
        self.install_btn.clicked.connect(self._start)
        layout.addWidget(self.install_btn)

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Monospace", 9))
        layout.addWidget(self.log_view, stretch=1)

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
# Archive workers (background threads)
# ---------------------------------------------------------------------------

class PodmanInstallWorker(QThread):
    """Install Podman via the system package manager."""

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)

    def __init__(self, distro_id: str) -> None:
        super().__init__()
        self._distro_id = distro_id

    def run(self) -> None:
        from archive import install_podman_commands
        try:
            cmds = install_podman_commands(self._distro_id)
            for cmd in cmds:
                self.log.emit(f"$ {' '.join(cmd)}\n", "cyan")
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True,
                )
                for line in iter(proc.stdout.readline, ""):
                    self.log.emit(line, "")
                proc.wait()
                if proc.returncode != 0:
                    self.log.emit(
                        f"\n✗ Install failed (exit code {proc.returncode}).\n",
                        "red",
                    )
                    self.done.emit(False)
                    return
            self.log.emit("\n✓ Podman installed successfully.\n", "green")
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Failed: {exc}\n", "red")
            self.done.emit(False)


class ArchiveImageBuildWorker(QThread):
    """Build the minimal archive container image."""

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)

    def __init__(self, credentials: str) -> None:
        super().__init__()
        self._credentials = credentials

    def run(self) -> None:
        from archive import build_archive_image, ARCHIVE_COMPOSE_DIR
        try:
            self.log.emit("Building archive image…\n", "")
            rc = build_archive_image(
                compose_dir=ARCHIVE_COMPOSE_DIR,
                credentials=self._credentials,
                on_output=lambda line: self.log.emit(line, ""),
            )
            if rc == 0:
                self.log.emit("\n✓ Image built successfully.\n", "green")
                self.done.emit(True)
            else:
                self.log.emit(f"\n✗ Build failed (exit code {rc}).\n", "red")
                self.done.emit(False)
        except Exception as exc:
            self.log.emit(f"\n✗ Failed: {exc}\n", "red")
            self.done.emit(False)


class ArchiveStackWorker(QThread):
    """Start or stop the archive container stack."""

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)

    def __init__(self, action: str) -> None:
        super().__init__()
        self._action = action  # "up" or "down"

    def run(self) -> None:
        from archive import archive_stack_up, archive_stack_down, ARCHIVE_COMPOSE_DIR
        try:
            if self._action == "up":
                self.log.emit("Starting archive stack…\n", "")
                result = archive_stack_up(ARCHIVE_COMPOSE_DIR)
            else:
                self.log.emit("Stopping archive stack…\n", "")
                result = archive_stack_down(ARCHIVE_COMPOSE_DIR)
            if result.stdout:
                self.log.emit(result.stdout, "")
            if result.stderr:
                self.log.emit(result.stderr, "orange")
            if result.returncode == 0:
                verb = "started" if self._action == "up" else "stopped"
                self.log.emit(f"\n✓ Archive stack {verb}.\n", "green")
                self.done.emit(True)
            else:
                self.log.emit(f"\n✗ Failed (exit code {result.returncode}).\n", "red")
                self.done.emit(False)
        except Exception as exc:
            self.log.emit(f"\n✗ Failed: {exc}\n", "red")
            self.done.emit(False)


class UploadWorker(QThread):
    """Upload FITS files to the archive."""

    log      = pyqtSignal(str, str)
    progress = pyqtSignal(int, int)
    done     = pyqtSignal(bool)

    def __init__(self, files: list[Path]) -> None:
        super().__init__()
        self._files = files

    def run(self) -> None:
        from archive import upload_files, ARCHIVE_COMPOSE_DIR
        try:
            total = len(self._files)
            count = [0]

            def on_log(msg: str) -> None:
                self.log.emit(msg + "\n", "")
                if msg.startswith("["):
                    try:
                        idx = int(msg.split("/")[0].strip("["))
                        self.progress.emit(idx, total)
                    except (ValueError, IndexError):
                        pass
                    count[0] += 1

            ingested = upload_files(self._files, ARCHIVE_COMPOSE_DIR, on_log=on_log)
            self.log.emit(
                f"\n✓ Uploaded {len(ingested)}/{total} file(s).\n", "green",
            )
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Upload failed: {exc}\n", "red")
            self.done.emit(False)


class DownloadWorker(QThread):
    """Download files from the archive."""

    log      = pyqtSignal(str, str)
    progress = pyqtSignal(int, int)
    done     = pyqtSignal(bool)

    def __init__(self, filenames: list[str], dest_dir: Path) -> None:
        super().__init__()
        self._filenames = filenames
        self._dest_dir = dest_dir

    def run(self) -> None:
        from archive import download_file, ARCHIVE_COMPOSE_DIR
        try:
            total = len(self._filenames)
            downloaded = 0
            for i, fn in enumerate(self._filenames, 1):
                self.progress.emit(i, total)
                path = download_file(
                    fn, self._dest_dir, ARCHIVE_COMPOSE_DIR,
                    on_log=lambda msg: self.log.emit(msg + "\n", ""),
                )
                if path:
                    downloaded += 1
            self.log.emit(
                f"\n✓ Downloaded {downloaded}/{total} file(s).\n", "green",
            )
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Download failed: {exc}\n", "red")
            self.done.emit(False)


# ---------------------------------------------------------------------------
# Archive tab
# ---------------------------------------------------------------------------

class ArchiveTab(QWidget):
    """Third tab: manage the Podman archive stack and upload/download FITS."""

    def __init__(self) -> None:
        super().__init__()
        self._settings = QSettings("METIS", "TestRunner")
        self._worker: QThread | None = None
        self._build_ui()
        self._load_settings()
        self._check_state()

    # ── UI construction ─────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setSpacing(14)
        outer.setContentsMargins(20, 20, 20, 20)

        self._stack = QStackedWidget()
        outer.addWidget(self._stack)

        # ── Page 0: Podman not installed ────────────────────────────────────
        page0 = QWidget()
        lay0 = QVBoxLayout(page0)
        lay0.setSpacing(12)
        desc0 = QLabel(
            "<b>Archive Integration</b><br><br>"
            "The archive requires <b>Podman</b> and <b>podman-compose</b> to "
            "run a local data server in containers.<br><br>"
            "Podman does not appear to be installed on this system."
        )
        desc0.setWordWrap(True)
        desc0.setTextFormat(Qt.TextFormat.RichText)
        lay0.addWidget(desc0)

        self._os_label = QLabel()
        self._os_label.setProperty("hint", "true")
        lay0.addWidget(self._os_label)

        self._install_podman_btn = QPushButton("Install Podman")
        self._install_podman_btn.setProperty("role", "success")
        self._install_podman_btn.setMinimumHeight(36)
        self._install_podman_btn.setMaximumWidth(200)
        self._install_podman_btn.clicked.connect(self._on_install_podman)
        lay0.addWidget(self._install_podman_btn)

        self._log0 = QTextEdit()
        self._log0.setReadOnly(True)
        self._log0.setFont(QFont("Monospace", 9))
        lay0.addWidget(self._log0, stretch=1)
        self._stack.addWidget(page0)

        # ── Page 1: Stack management + archive operations ───────────────────
        page1 = QWidget()
        lay1 = QVBoxLayout(page1)
        lay1.setSpacing(10)

        desc1 = QLabel(
            "<b>Archive Integration</b><br><br>"
            "Manage the local archive stack and upload/download FITS files."
        )
        desc1.setWordWrap(True)
        desc1.setTextFormat(Qt.TextFormat.RichText)
        lay1.addWidget(desc1)

        # -- Image build section --
        img_grp = QGroupBox("Container Image")
        img_lay = QVBoxLayout(img_grp)
        img_lay.setSpacing(8)
        img_info = QLabel(
            "A container image is required before starting the stack. "
            "Enter your OmegaCEN credentials to build it."
        )
        img_info.setWordWrap(True)
        img_lay.addWidget(img_info)
        cred_row = QWidget()
        cred_h = QHBoxLayout(cred_row)
        cred_h.setContentsMargins(0, 0, 0, 0)
        cred_h.addWidget(QLabel("Credentials (user:pass):"))
        self._cred_edit = QLineEdit()
        self._cred_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self._cred_edit.setPlaceholderText("username:password")
        cred_h.addWidget(self._cred_edit)
        img_lay.addWidget(cred_row)
        self._build_img_btn = QPushButton("Build Image")
        self._build_img_btn.setProperty("role", "info")
        self._build_img_btn.setMinimumHeight(32)
        self._build_img_btn.setMaximumWidth(160)
        self._build_img_btn.clicked.connect(self._on_build_image)
        img_lay.addWidget(self._build_img_btn)
        self._img_status = QLabel()
        self._img_status.setProperty("hint", "true")
        img_lay.addWidget(self._img_status)
        lay1.addWidget(img_grp)

        # -- Stack control --
        stack_grp = QGroupBox("Archive Stack")
        stack_lay = QHBoxLayout(stack_grp)
        self._start_btn = QPushButton("Start Stack")
        self._start_btn.setProperty("role", "success")
        self._start_btn.setMinimumHeight(32)
        self._start_btn.clicked.connect(lambda: self._on_stack_action("up"))
        self._stop_btn = QPushButton("Stop Stack")
        self._stop_btn.setProperty("role", "danger")
        self._stop_btn.setMinimumHeight(32)
        self._stop_btn.clicked.connect(lambda: self._on_stack_action("down"))
        self._stack_status = QLabel("Status: unknown")
        stack_lay.addWidget(self._start_btn)
        stack_lay.addWidget(self._stop_btn)
        stack_lay.addWidget(self._stack_status)
        stack_lay.addStretch()
        lay1.addWidget(stack_grp)

        # -- Upload section --
        up_grp = QGroupBox("Upload FITS to Archive")
        up_lay = QVBoxLayout(up_grp)
        up_row = QHBoxLayout()
        self._upload_list = QListWidget()
        self._upload_list.setMaximumHeight(100)
        up_row.addWidget(self._upload_list)
        up_btns = QVBoxLayout()
        add_btn = QPushButton("Add Files…")
        add_btn.setProperty("role", "info")
        add_btn.clicked.connect(self._on_add_upload_files)
        clear_btn = QPushButton("Clear")
        clear_btn.setProperty("role", "danger")
        clear_btn.clicked.connect(self._upload_list.clear)
        up_btns.addWidget(add_btn)
        up_btns.addWidget(clear_btn)
        up_btns.addStretch()
        up_row.addLayout(up_btns)
        up_lay.addLayout(up_row)
        self._upload_btn = QPushButton("Upload to Archive")
        self._upload_btn.setProperty("role", "success")
        self._upload_btn.setMinimumHeight(32)
        self._upload_btn.setMaximumWidth(200)
        self._upload_btn.clicked.connect(self._on_upload)
        up_lay.addWidget(self._upload_btn)
        lay1.addWidget(up_grp)

        # -- Download section --
        dl_grp = QGroupBox("Download from Archive")
        dl_lay = QVBoxLayout(dl_grp)
        dl_top = QHBoxLayout()
        self._archive_list = QListWidget()
        self._archive_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        self._archive_list.setMaximumHeight(100)
        dl_top.addWidget(self._archive_list)
        dl_btns = QVBoxLayout()
        refresh_btn = QPushButton("Refresh")
        refresh_btn.setProperty("role", "info")
        refresh_btn.clicked.connect(self._on_refresh_archive)
        dl_btns.addWidget(refresh_btn)
        dl_btns.addStretch()
        dl_top.addLayout(dl_btns)
        dl_lay.addLayout(dl_top)
        dl_row = QHBoxLayout()
        self._download_btn = QPushButton("Download Selected")
        self._download_btn.setProperty("role", "success")
        self._download_btn.setMinimumHeight(32)
        self._download_btn.setMaximumWidth(200)
        self._download_btn.clicked.connect(self._on_download)
        dl_row.addWidget(self._download_btn)
        dl_row.addStretch()
        dl_lay.addLayout(dl_row)
        lay1.addWidget(dl_grp)

        # -- Log view --
        self._log1 = QTextEdit()
        self._log1.setReadOnly(True)
        self._log1.setFont(QFont("Monospace", 9))
        lay1.addWidget(self._log1, stretch=1)
        self._stack.addWidget(page1)

    # ── State management ────────────────────────────────────────────────────

    def _check_state(self) -> None:
        """Determine which page to show based on Podman availability."""
        from archive import podman_available, podman_compose_available, detect_os

        os_family, distro_id = detect_os()
        self._distro_id = distro_id
        self._os_label.setText(f"Detected OS: {os_family} / {distro_id}")

        if podman_available() and podman_compose_available():
            self._stack.setCurrentIndex(1)
            self._update_image_status()
            self._update_stack_status()
        else:
            self._stack.setCurrentIndex(0)

    def _update_image_status(self) -> None:
        from archive import archive_image_exists
        if archive_image_exists():
            self._img_status.setText("✓ Image found")
            self._build_img_btn.setText("Rebuild Image")
        else:
            self._img_status.setText("Image not built yet")

    def _update_stack_status(self) -> None:
        from archive import archive_stack_status, ARCHIVE_COMPOSE_DIR
        status = archive_stack_status(ARCHIVE_COMPOSE_DIR)
        if status:
            parts = [f"{svc}: {st}" for svc, st in status.items()]
            self._stack_status.setText("Status: " + ", ".join(parts))
        else:
            self._stack_status.setText("Status: not running")

    # ── Podman install ──────────────────────────────────────────────────────

    def _on_install_podman(self) -> None:
        self._log0.clear()
        self._install_podman_btn.setEnabled(False)
        self._worker = PodmanInstallWorker(self._distro_id)
        self._worker.log.connect(lambda t, c: log_append(self._log0, t, c))
        self._worker.done.connect(self._on_podman_installed)
        self._worker.start()

    def _on_podman_installed(self, success: bool) -> None:
        self._install_podman_btn.setEnabled(True)
        if success:
            self._check_state()

    # ── Image build ─────────────────────────────────────────────────────────

    def _on_build_image(self) -> None:
        creds = self._cred_edit.text().strip()
        if not creds:
            QMessageBox.warning(
                self, "Missing credentials",
                "Enter OmegaCEN credentials (username:password) to build the image.",
            )
            return
        self._log1.clear()
        self._build_img_btn.setEnabled(False)
        self._worker = ArchiveImageBuildWorker(creds)
        self._worker.log.connect(lambda t, c: log_append(self._log1, t, c))
        self._worker.done.connect(self._on_image_built)
        self._worker.start()

    def _on_image_built(self, success: bool) -> None:
        self._build_img_btn.setEnabled(True)
        self._update_image_status()

    # ── Stack control ───────────────────────────────────────────────────────

    def _on_stack_action(self, action: str) -> None:
        self._log1.clear()
        self._start_btn.setEnabled(False)
        self._stop_btn.setEnabled(False)
        self._worker = ArchiveStackWorker(action)
        self._worker.log.connect(lambda t, c: log_append(self._log1, t, c))
        self._worker.done.connect(self._on_stack_done)
        self._worker.start()

    def _on_stack_done(self, success: bool) -> None:
        self._start_btn.setEnabled(True)
        self._stop_btn.setEnabled(True)
        self._update_stack_status()

    # ── Upload ──────────────────────────────────────────────────────────────

    def _on_add_upload_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self, "Select FITS files", str(REPO_ROOT), "FITS files (*.fits)",
        )
        existing = {
            self._upload_list.item(i).text()
            for i in range(self._upload_list.count())
        }
        for f in files:
            if f not in existing:
                self._upload_list.addItem(f)

    def _on_upload(self) -> None:
        if self._upload_list.count() == 0:
            QMessageBox.warning(self, "No files", "Add FITS files to upload.")
            return
        files = [
            Path(self._upload_list.item(i).text())
            for i in range(self._upload_list.count())
        ]
        self._upload_btn.setEnabled(False)
        self._worker = UploadWorker(files)
        self._worker.log.connect(lambda t, c: log_append(self._log1, t, c))
        self._worker.done.connect(lambda _ok: self._upload_btn.setEnabled(True))
        self._worker.start()

    # ── Download ────────────────────────────────────────────────────────────

    def _on_refresh_archive(self) -> None:
        from archive import query_archive, ARCHIVE_COMPOSE_DIR
        self._archive_list.clear()
        log_append(self._log1, "Querying archive…\n", "cyan")
        items = query_archive(compose_dir=ARCHIVE_COMPOSE_DIR,
                              on_log=lambda msg: log_append(self._log1, msg + "\n", ""))
        for it in items:
            label = it.get("filename", "?")
            catg = it.get("pro_catg", "")
            if catg:
                label += f"  [{catg}]"
            self._archive_list.addItem(label)
        log_append(self._log1, f"Found {len(items)} item(s).\n", "green")

    def _on_download(self) -> None:
        selected = self._archive_list.selectedItems()
        if not selected:
            QMessageBox.warning(self, "No selection", "Select files to download.")
            return
        dest = QFileDialog.getExistingDirectory(self, "Download destination", str(REPO_ROOT))
        if not dest:
            return
        filenames = [item.text().split("  [")[0] for item in selected]
        self._download_btn.setEnabled(False)
        self._worker = DownloadWorker(filenames, Path(dest))
        self._worker.log.connect(lambda t, c: log_append(self._log1, t, c))
        self._worker.done.connect(lambda _ok: self._download_btn.setEnabled(True))
        self._worker.start()

    # ── Settings ────────────────────────────────────────────────────────────

    def _load_settings(self) -> None:
        self._cred_edit.setText(self._settings.value("archive_cred", ""))

    def _save_settings(self) -> None:
        self._settings.setValue("archive_cred", self._cred_edit.text())


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
        self._update_mode_fields()

    # ── UI construction ──────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setSpacing(12)
        outer.setContentsMargins(20, 20, 20, 20)

        # ── YAML file list ──
        self.yaml_grp = QGroupBox("YAML Input Files")
        yaml_lay = QVBoxLayout(self.yaml_grp)
        file_row = QHBoxLayout()
        self.yaml_list = QListWidget()
        self.yaml_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        self.yaml_list.setMaximumHeight(120)
        file_row.addWidget(self.yaml_list)
        btn_col = QVBoxLayout()
        add_btn = QPushButton("Add…")
        add_btn.setProperty("role", "info")
        add_btn.clicked.connect(self._add_yaml)
        remove_btn = QPushButton("Remove")
        remove_btn.setProperty("role", "danger")
        remove_btn.clicked.connect(self._remove_yaml)
        btn_col.addWidget(add_btn)
        btn_col.addWidget(remove_btn)
        btn_col.addStretch()
        file_row.addLayout(btn_col)
        yaml_lay.addLayout(file_row)
        outer.addWidget(self.yaml_grp)

        # ── Options ──
        opts_grp = QGroupBox("Options")
        opts_lay = QVBoxLayout(opts_grp)
        opts_lay.setSpacing(10)
        opts_lay.setContentsMargins(9, 9, 9, 12)

        # Output directory
        self.output_edit = QLineEdit()
        out_browse = _dir_picker(self.output_edit, self)
        opts_lay.addWidget(
            _labeled("Output directory:", self.output_edit, out_browse)
        )

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

        # Cores
        self.cores_spin = QSpinBox()
        self.cores_spin.setRange(1, os.cpu_count() or 16)
        self.cores_spin.setValue(4)
        self.cores_spin.setButtonSymbols(QAbstractSpinBox.ButtonSymbols.PlusMinus)
        opts_lay.addWidget(_labeled("CPU cores  (--cores):", self.cores_spin))

        # Runner
        self.runner_combo = QComboBox()
        self.runner_combo.addItems(["metapkg", "native", "docker", "podman"])
        opts_lay.addWidget(_labeled("Runner  (--runner):", self.runner_combo))

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

        # Pipeline input dir  [pipeline-only mode only]
        self.pipeline_input_edit = QLineEdit()
        pipe_in_browse = _dir_picker(self.pipeline_input_edit, self)
        self.pipeline_input_edit.setPlaceholderText("(defaults to output/<YYYYMMDDTHHMMSS>/sim/)")
        self.pipeline_input_edit.textChanged.connect(self._update_output_info)
        self.pipeline_input_row = _labeled(
            "Pipeline input  (--pipeline-input):",
            self.pipeline_input_edit, pipe_in_browse,
        )
        opts_lay.addWidget(self.pipeline_input_row)
        # Connect mode radio buttons now that pipeline_input_row exists
        for rb in (self.rb_both, self.rb_sim_only, self.rb_pipe_only):
            rb.toggled.connect(self._update_mode_fields)

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
        # Connect runner signal now that container_row and meta_pkg_row exist
        self.runner_combo.currentTextChanged.connect(self._update_runner_fields)

        # Simulations dir  [always visible]
        self.sim_dir_edit = QLineEdit()
        sim_browse = _dir_picker(self.sim_dir_edit, self)
        self.sim_dir_edit.setPlaceholderText(str(TARGET_B))
        opts_lay.addWidget(_labeled("Simulations dir  (--simulations-dir):", self.sim_dir_edit, sim_browse))

        # Instrument packages  [always visible]
        self.inst_edit = QLineEdit()
        inst_browse = _dir_picker(self.inst_edit, self)
        opts_lay.addWidget(_labeled("Instrument packages  (--inst-pkgs):", self.inst_edit, inst_browse))

        outer.addWidget(opts_grp)

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
        run_row.addWidget(self.run_btn)
        run_row.addWidget(self.stop_btn)
        run_row.addStretch()
        outer.addLayout(run_row)

        # ── Output log ──
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Monospace", 9))
        outer.addWidget(self.log_view, stretch=1)

    # ── Output path info ─────────────────────────────────────────────────────

    def _update_output_info(self) -> None:
        base = self.output_edit.text().strip()
        root = Path(base) if base else REPO_ROOT / "output" / "<timestamp>"
        pipe_out = root / "pipeline"

        if self.rb_pipe_only.isChecked():
            pipe_in = self.pipeline_input_edit.text().strip()
            pipe_in_str = pipe_in if pipe_in else f"{root / 'sim'}/"
            self.output_info.setText(
                f"Pipeline input \u2192 {pipe_in_str}   \u00b7   "
                f"Pipeline products \u2192 {pipe_out}/"
            )
        elif self.rb_sim_only.isChecked():
            self.output_info.setText(
                f"Simulations \u2192 {root / 'sim'}/"
            )
        else:
            self.output_info.setText(
                f"Simulations \u2192 {root / 'sim'}/   \u00b7   "
                f"Pipeline products \u2192 {pipe_out}/"
            )

    # ── Mode-dependent field visibility ──────────────────────────────────────

    def _update_mode_fields(self) -> None:
        pipe_only = self.rb_pipe_only.isChecked()
        self.yaml_grp.setVisible(not pipe_only)
        self.pipeline_input_row.setVisible(pipe_only)
        self._update_output_info()

    # ── Runner-dependent field visibility ────────────────────────────────────

    def _update_runner_fields(self) -> None:
        runner = self.runner_combo.currentText()
        self.container_row.setVisible(runner in ("docker", "podman"))
        self.meta_pkg_row.setVisible(runner == "metapkg")
        if runner in ("docker", "podman"):
            ph = "(resolved inside container)"
        else:
            ph = str(REPO_ROOT / "inst_pkgs")
        self.inst_edit.setPlaceholderText(ph)

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
        # Checkbox checked → --calib 1 (default ON); unchecked → --calib 0.
        args += ["--calib", "1" if self.calib_cb.isChecked() else "0"]
        # Same pattern for static calibration prototypes.
        args += ["--static", "1" if self.static_cb.isChecked() else "0"]
        args += ["--cores", str(self.cores_spin.value())]
        if self.rb_sim_only.isChecked():
            args.append("--no-pipeline")
        elif self.rb_pipe_only.isChecked():
            args.append("--no-sim")
            if self.pipeline_input_edit.text().strip():
                args += ["--pipeline-input", self.pipeline_input_edit.text().strip()]

        runner = self.runner_combo.currentText()
        args += ["--runner", runner]
        if runner in ("docker", "podman") and self.container_edit.text().strip():
            args += ["--container", self.container_edit.text().strip()]
        if runner == "metapkg":
            if (REPO_ROOT / ".env").exists():
                meta_pkg_dir = str(REPO_ROOT)
            elif self.meta_pkg_edit.text().strip():
                meta_pkg_dir = self.meta_pkg_edit.text().strip()
            else:
                meta_pkg_dir = str(META_PKG)
            args += ["--meta-pkg", meta_pkg_dir]
        if self.sim_dir_edit.text().strip():
            args += ["--simulations-dir", self.sim_dir_edit.text().strip()]
        if self.inst_edit.text().strip():
            args += ["--inst-pkgs", self.inst_edit.text().strip()]
        if self.auto_fetch_cb.isChecked():
            args.append("--auto-fetch-calibrations")

        for i in range(self.yaml_list.count()):
            args.append(self.yaml_list.item(i).text())

        return args

    def _run(self) -> None:
        if not self.rb_pipe_only.isChecked() and self.yaml_list.count() == 0:
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

        # Strip uv's venv-activation variables so it doesn't warn about a
        # mismatched active venv on every internal `uv run --project <meta-pkg>`
        # call inside run_metis.py. See _child_env() for details.
        qenv = QProcessEnvironment()
        for k, v in _child_env().items():
            qenv.insert(k, v)
        self._process.setProcessEnvironment(qenv)

        venv_python = REPO_ROOT / ".venv" / "bin" / "python3"
        if not venv_python.exists():
            venv_python = META_PKG / ".venv" / "bin" / "python3"
        python_exe = str(venv_python) if venv_python.exists() else sys.executable
        log_append(self.log_view, f"$ {python_exe} {script} {' '.join(args)}\n\n", "cyan")
        self._process.start(python_exe, ["-u", script] + args)
        self.run_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

    def _stop(self) -> None:
        if self._process and self._process.state() != QProcess.ProcessState.NotRunning:
            self._process.kill()

    @staticmethod
    def _strip_ansi(text: str) -> str:
        return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)

    def _on_stdout(self) -> None:
        data = self._process.readAllStandardOutput().data().decode(errors="replace")
        log_append(self.log_view, self._strip_ansi(data))

    def _on_stderr(self) -> None:
        data = self._process.readAllStandardError().data().decode(errors="replace")
        log_append(self.log_view, self._strip_ansi(data), "orange")

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
        self._update_output_info()
        self.calib_cb.setChecked(s.value("calib", True, type=bool))
        self.static_cb.setChecked(s.value("static", True, type=bool))
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
        self.auto_fetch_cb.setChecked(s.value("auto_fetch", False, type=bool))
        self.pipeline_input_edit.setText(s.value("pipeline_input", ""))
        for f in (s.value("yaml_files") or []):
            self.yaml_list.addItem(f)

    def _save_settings(self) -> None:
        s = self._settings
        s.setValue("output", self.output_edit.text())
        s.setValue("calib", self.calib_cb.isChecked())
        s.setValue("static", self.static_cb.isChecked())
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
        s.setValue("auto_fetch", self.auto_fetch_cb.isChecked())
        s.setValue("pipeline_input", self.pipeline_input_edit.text())
        s.setValue("yaml_files", [
            self.yaml_list.item(i).text() for i in range(self.yaml_list.count())
        ])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _ExpandingTabBar(QTabBar):
    """Tab bar that divides its width equally among all tabs, overriding QSS."""

    def resizeEvent(self, event):
        super().resizeEvent(event)
        count = self.count()
        if count > 0:
            new_ss = f"QTabBar::tab {{ width: {event.size().width() // count}px; }}"
            if self.styleSheet() != new_ss:
                self.setStyleSheet(new_ss)

    def sizeHint(self):
        sh = super().sizeHint()
        parent = self.parent()
        if parent and parent.width() > 0:
            sh.setWidth(parent.width())
        return sh

    def minimumSizeHint(self):
        sh = super().minimumSizeHint()
        sh.setWidth(0)
        return sh


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):

    def __init__(self, initial_theme: str = "dark") -> None:
        super().__init__()
        self.setWindowTitle("METIS Test Runner")
        self.resize(1000, 900)

        self._current_theme = initial_theme
        self._home_dark = initial_theme  # "dark" or "pink" — the base non-light theme

        toolbar = self.addToolBar("Theme")
        toolbar.setMovable(False)
        toolbar.setFloatable(False)
        self._theme_btn = QPushButton()
        self._theme_btn.setProperty("role", "accent")
        toolbar.addWidget(self._theme_btn)
        self._theme_btn.clicked.connect(self._toggle_theme)
        self._update_theme_btn_label()

        tabs = QTabWidget()
        tabs.setTabBar(_ExpandingTabBar())
        tabs.tabBar().setUsesScrollButtons(False)
        self._run_tab = RunTab()
        self._archive_tab = ArchiveTab()
        tabs.addTab(self._run_tab, "Run")
        tabs.addTab(InstallTab(), "Install")
        tabs.addTab(self._archive_tab, "Archive")
        self.setCentralWidget(tabs)

    def _update_theme_btn_label(self) -> None:
        if self._current_theme == "light":
            self._theme_btn.setText("Dark theme")
        else:
            self._theme_btn.setText("Light theme")

    def _toggle_theme(self) -> None:
        if self._current_theme == "light":
            self._current_theme = self._home_dark
        else:
            self._current_theme = "light"
        apply_theme(QApplication.instance(), self._current_theme)
        self._update_theme_btn_label()

    def closeEvent(self, event) -> None:
        self._run_tab._save_settings()
        self._archive_tab._save_settings()
        super().closeEvent(event)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    pink = "--pink" in sys.argv
    smoke_test = "--smoke-test" in sys.argv or os.environ.get("SMOKE_TEST")
    argv = [a for a in sys.argv if a not in ("--pink", "--smoke-test")]
    app = QApplication(argv)
    app.setApplicationName("METIS Test Runner")
    initial = "pink" if pink else "dark"
    apply_theme(app, initial)
    win = MainWindow(initial_theme=initial)
    win.show()
    if smoke_test:
        # CI smoke test: exit cleanly once the event loop has started, so we
        # exercise imports + Qt init + window construction without blocking.
        QTimer.singleShot(0, app.quit)
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
