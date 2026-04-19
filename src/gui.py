#!/usr/bin/env python3
"""gui.py — METIS Test Runner GUI

Three-tab graphical front-end:
  Install  — runs the metis-meta-package bootstrap steps non-interactively
  Run      — wraps run_metis.py with a file-picker and options UI
  Archive  — install MetisWISE and upload/download FITS files
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from PyQt6.QtCore import Qt, QProcess, QProcessEnvironment, QSettings, QThread, QTimer, QUrl, pyqtSignal
from PyQt6.QtGui import QColor, QDesktopServices, QFont, QPalette, QTextCharFormat, QTextCursor
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

REPO_ROOT   = Path(__file__).resolve().parent.parent
META_PKG    = Path(os.environ.get("METIS_META_PKG", str(REPO_ROOT / "metis-meta-package")))
TARGET_A    = REPO_ROOT / "METIS_Pipeline"
TARGET_B    = Path(os.environ.get("METIS_SIMULATIONS_DIR", str(REPO_ROOT / "METIS_Simulations")))
INST_PKGS   = Path(os.environ.get("METIS_INST_PKGS", str(REPO_ROOT / "inst_pkgs")))
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
    env["PYTHONUNBUFFERED"] = "1"
    return env


def _installation_complete() -> bool:
    """Return True if the essential install artifacts exist."""
    return (TARGET_A / ".git").exists() and (REPO_ROOT / ".env").exists()


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
    "pink_light": {
        # Soft pastel pink — light counterpart to "pink", centred on #FFD7EE
        "window":         "#fff4f9",
        "window_text":    "#5c3a4e",
        "base":           "#ffe8f2",
        "alt_base":       "#ffd7ee",
        "button":         "#ffd7ee",
        "button_text":    "#5c3a4e",
        "highlight":      "#f06292",
        "highlight_text": "#fff4f9",
        "placeholder":    "#c9a0b4",
        "tooltip_base":   "#ffd7ee",
        "tooltip_text":   "#5c3a4e",
        "border":         "#e8b8d0",
        "accent":         "#f06292",
        "accent_dim":     "#f8bbd0",
        "btn_success_bg": "#c8e6c9",   # pastel green
        "btn_success_fg": "#2e4830",
        "btn_danger_bg":  "#f8b8c4",   # pastel red-pink
        "btn_danger_fg":  "#4a1e28",
        "btn_info_bg":    "#f8bbd0",   # pastel pink
        "btn_info_fg":    "#4a1e30",
        "log_green":      "#2e7d32",
        "log_red":        "#c62828",
        "log_cyan":       "#00838f",
        "log_yellow":     "#f57f17",
        "log_orange":     "#e65100",
        "log_gray":       "#c9a0b4",
        "log_default":    "#5c3a4e",
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
    # hover colours: slightly darker for light themes, lighter for dark/pink
    _shift = (lambda col: QColor(col).darker(110).name()) if name in ("light", "pink_light") \
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

            self._step("Checking for existing EDPS configuration…")
            self._backup_edps_config()

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

    def _backup_edps_config(self) -> None:
        """If an existing application.properties exists, back it up."""
        props = Path.home() / ".edps" / "application.properties"
        if props.exists():
            backup = props.with_name("application.properties_backup")
            props.rename(backup)
            self.log.emit(
                f"Existing {props} found — backed up to {backup}\n",
                "yellow",
            )

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
            "categories": (r"^categories=.*", "categories=.*"),
            "pattern": (
                r"^pattern=.*",
                "pattern=$TASK/$TIMESTAMP/$object$_$pro.catg$.$EXT",
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

class MetisWISEInstallWorker(QThread):
    """Install MetisWISE into the project .venv via uv pip install."""

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)

    def __init__(self, credentials: str) -> None:
        super().__init__()
        self._credentials = credentials

    def run(self) -> None:
        from archive import install_metiswise_command
        try:
            cmd = install_metiswise_command(self._credentials)
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
            self.log.emit("\n✓ MetisWISE installed successfully.\n", "green")
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Failed: {exc}\n", "red")
            self.done.emit(False)


class PodmanInstallWorker(QThread):
    """Install Podman via the system package manager."""

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)

    def run(self) -> None:
        from archive import detect_podman_install_cmd
        try:
            cmd = detect_podman_install_cmd()
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
                    f"\n✗ Podman install failed (exit code {proc.returncode}).\n",
                    "red",
                )
                self.done.emit(False)
                return
            self.log.emit("\n✓ Podman installed successfully.\n", "green")
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Failed: {exc}\n", "red")
            self.done.emit(False)


class BuildStartDBWorker(QThread):
    """Build the archive container image (if needed) and start the pod."""

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)

    def __init__(self, credentials: str) -> None:
        super().__init__()
        self._credentials = credentials

    def run(self) -> None:
        from archive import (
            archive_image_exists, build_archive_image,
            archive_pod_running, start_archive_pod,
        )
        try:
            if not archive_image_exists():
                self.log.emit("── Building archive container image…\n", "cyan")
                build_archive_image(
                    self._credentials,
                    on_log=lambda msg: self.log.emit(msg + "\n", ""),
                )
                self.log.emit("\n✓ Image built.\n", "green")
            else:
                self.log.emit("Archive image already exists — skipping build.\n", "green")

            if archive_pod_running():
                self.log.emit("Pod is already running.\n", "green")
                self.done.emit(True)
                return

            self.log.emit("\n── Starting local archive pod…\n", "cyan")
            start_archive_pod(
                on_log=lambda msg: self.log.emit(msg + "\n", ""),
            )
            self.log.emit("\n✓ Local archive is running.\n", "green")
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Failed: {exc}\n", "red")
            self.done.emit(False)


class StopDBWorker(QThread):
    """Stop and remove the local archive pod."""

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)

    def run(self) -> None:
        from archive import stop_archive_pod
        try:
            stop_archive_pod(
                on_log=lambda msg: self.log.emit(msg + "\n", ""),
            )
            self.log.emit("\n✓ Pod stopped.\n", "green")
            self.done.emit(True)
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
        from archive import upload_files
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

            ingested = upload_files(self._files, on_log=on_log)
            self.log.emit(
                f"\n✓ Uploaded {len(ingested)}/{total} file(s).\n", "green",
            )
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Upload failed: {exc}\n", "red")
            self.done.emit(False)


class QueryWorker(QThread):
    """Query the archive for available files."""

    log     = pyqtSignal(str, str)
    results = pyqtSignal(list)
    done    = pyqtSignal(bool)

    def __init__(self, category: str | None = None) -> None:
        super().__init__()
        self._category = category

    def run(self) -> None:
        from archive import query_archive
        try:
            items = query_archive(
                category=self._category,
                on_log=lambda msg: self.log.emit(msg + "\n", ""),
            )
            self.results.emit(items)
            self.log.emit(f"Found {len(items)} item(s).\n", "green")
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Query failed: {exc}\n", "red")
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
        from archive import download_file
        try:
            total = len(self._filenames)
            downloaded = 0
            for i, fn in enumerate(self._filenames, 1):
                self.progress.emit(i, total)
                path = download_file(
                    fn, self._dest_dir,
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
    """Third tab: local archive setup and FITS upload/download.

    Page 0 — Setup: install MetisWISE, install Podman, build & start
    the local database pod.
    Page 1 — Operations: connect to the local database, upload/download.
    """

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

        # ── Page 0: Setup prerequisites ────────────────────────────────────
        page0 = QWidget()
        lay0 = QVBoxLayout(page0)
        lay0.setSpacing(12)

        desc0 = QLabel(
            "<b>Archive Setup</b><br><br>"
            "The local archive uses a <b>PostgreSQL</b> database and "
            "<b>dataserver</b> running in a Podman pod.  Complete the "
            "three steps below to get started.<br><br>"
            "<b>Note:</b> Please run the <b>Install</b> tab first to set "
            "up the project environment."
        )
        desc0.setWordWrap(True)
        desc0.setTextFormat(Qt.TextFormat.RichText)
        lay0.addWidget(desc0)

        # -- 1. MetisWISE --
        mw_grp = QGroupBox("1. MetisWISE")
        mw_lay = QVBoxLayout(mw_grp)
        mw_desc = QLabel(
            "Install the <b>MetisWISE</b> Python package (OmegaCEN "
            "credentials required).  These credentials are also used "
            "to build the archive container image."
        )
        mw_desc.setWordWrap(True)
        mw_lay.addWidget(mw_desc)
        cred_row = QWidget()
        cred_h = QHBoxLayout(cred_row)
        cred_h.setContentsMargins(0, 0, 0, 0)
        cred_h.addWidget(QLabel("OmegaCEN credentials (user:pass):"))
        self._cred_edit = QLineEdit()
        self._cred_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self._cred_edit.setPlaceholderText("username:password")
        cred_h.addWidget(self._cred_edit)
        mw_lay.addWidget(cred_row)
        mw_btn_row = QHBoxLayout()
        self._install_btn = QPushButton("Install MetisWISE")
        self._install_btn.setProperty("role", "success")
        self._install_btn.setMinimumHeight(32)
        self._install_btn.setMaximumWidth(200)
        self._install_btn.clicked.connect(self._on_install_metiswise)
        mw_btn_row.addWidget(self._install_btn)
        self._mw_status = QLabel()
        mw_btn_row.addWidget(self._mw_status)
        mw_btn_row.addStretch()
        mw_lay.addLayout(mw_btn_row)
        lay0.addWidget(mw_grp)

        # -- 2. Podman --
        pm_grp = QGroupBox("2. Podman")
        pm_lay = QVBoxLayout(pm_grp)
        pm_btn_row = QHBoxLayout()
        self._podman_install_btn = QPushButton("Install Podman")
        self._podman_install_btn.setProperty("role", "success")
        self._podman_install_btn.setMinimumHeight(32)
        self._podman_install_btn.setMaximumWidth(200)
        self._podman_install_btn.clicked.connect(self._on_install_podman)
        pm_btn_row.addWidget(self._podman_install_btn)
        self._pm_status = QLabel()
        pm_btn_row.addWidget(self._pm_status)
        pm_btn_row.addStretch()
        pm_lay.addLayout(pm_btn_row)
        lay0.addWidget(pm_grp)

        # -- 3. Local database --
        db_grp = QGroupBox("3. Local Database")
        db_lay = QVBoxLayout(db_grp)
        db_desc = QLabel(
            "Build the archive container image and start the local "
            "PostgreSQL + dataserver pod.  Uses the OmegaCEN credentials "
            "above for the container build."
        )
        db_desc.setWordWrap(True)
        db_lay.addWidget(db_desc)
        db_btn_row = QHBoxLayout()
        self._build_start_btn = QPushButton("Build Image && Start Database")
        self._build_start_btn.setProperty("role", "success")
        self._build_start_btn.setMinimumHeight(32)
        self._build_start_btn.setMaximumWidth(280)
        self._build_start_btn.clicked.connect(self._on_build_start_db)
        db_btn_row.addWidget(self._build_start_btn)
        self._stop_db_btn0 = QPushButton("Stop Database")
        self._stop_db_btn0.setProperty("role", "danger")
        self._stop_db_btn0.setMinimumHeight(32)
        self._stop_db_btn0.setMaximumWidth(160)
        self._stop_db_btn0.clicked.connect(self._on_stop_db)
        db_btn_row.addWidget(self._stop_db_btn0)
        self._db_status = QLabel()
        db_btn_row.addWidget(self._db_status)
        db_btn_row.addStretch()
        db_lay.addLayout(db_btn_row)
        lay0.addWidget(db_grp)

        # -- Log view --
        self._log0 = QTextEdit()
        self._log0.setReadOnly(True)
        self._log0.setFont(QFont("Monospace", 9))
        lay0.addWidget(self._log0, stretch=1)
        self._stack.addWidget(page0)

        # ── Page 1: Archive operations ──────────────────────────────────────
        page1 = QWidget()
        lay1 = QVBoxLayout(page1)
        lay1.setSpacing(10)

        desc1 = QLabel(
            "<b>Local Archive</b><br><br>"
            "Upload and download FITS files from the local archive "
            "database.  The default credentials (<code>AWTEST</code> / "
            "<code>lmno</code>) are created during database setup."
        )
        desc1.setWordWrap(True)
        desc1.setTextFormat(Qt.TextFormat.RichText)
        lay1.addWidget(desc1)

        # -- Database credentials --
        db_cred_grp = QGroupBox("Database Credentials")
        db_cred_lay = QHBoxLayout(db_cred_grp)
        db_cred_lay.addWidget(QLabel("Username:"))
        self._db_user_edit = QLineEdit()
        self._db_user_edit.setPlaceholderText("e.g. AWTEST")
        self._db_user_edit.setMaximumWidth(160)
        db_cred_lay.addWidget(self._db_user_edit)
        db_cred_lay.addSpacing(12)
        db_cred_lay.addWidget(QLabel("Password:"))
        self._db_pass_edit = QLineEdit()
        self._db_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self._db_pass_edit.setMaximumWidth(160)
        db_cred_lay.addWidget(self._db_pass_edit)
        db_cred_lay.addSpacing(12)
        self._connect_btn = QPushButton("Connect")
        self._connect_btn.setProperty("role", "info")
        self._connect_btn.setMinimumHeight(32)
        self._connect_btn.clicked.connect(self._on_connect_db)
        db_cred_lay.addWidget(self._connect_btn)
        db_cred_lay.addStretch()
        lay1.addWidget(db_cred_grp)

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

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Category:"))
        self._catg_combo = QComboBox()
        self._catg_combo.setEditable(True)
        self._catg_combo.setMinimumWidth(200)
        self._catg_combo.addItem("(all)")
        from archive import TASK_TO_MASTER_PROCATG
        for catg in sorted(set(TASK_TO_MASTER_PROCATG.values())):
            self._catg_combo.addItem(catg)
        self._catg_combo.insertSeparator(self._catg_combo.count())
        from run_metis import DPR_TO_TAG
        for tag in sorted(set(DPR_TO_TAG.values())):
            self._catg_combo.addItem(tag)
        filter_row.addWidget(self._catg_combo)
        filter_row.addSpacing(12)
        filter_row.addWidget(QLabel("Filename:"))
        self._filename_filter = QLineEdit()
        self._filename_filter.setPlaceholderText("filter results locally…")
        self._filename_filter.textChanged.connect(self._apply_filename_filter)
        filter_row.addWidget(self._filename_filter)
        filter_row.addSpacing(12)
        self._refresh_btn = QPushButton("Search")
        self._refresh_btn.setProperty("role", "info")
        self._refresh_btn.setMinimumHeight(32)
        self._refresh_btn.clicked.connect(self._on_refresh_archive)
        filter_row.addWidget(self._refresh_btn)
        dl_lay.addLayout(filter_row)

        self._archive_list = QListWidget()
        self._archive_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        self._archive_list.setMaximumHeight(120)
        dl_lay.addWidget(self._archive_list)

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

        # -- Stop database + log view --
        bottom_row = QHBoxLayout()
        self._stop_db_btn1 = QPushButton("Stop Database")
        self._stop_db_btn1.setProperty("role", "danger")
        self._stop_db_btn1.setMinimumHeight(32)
        self._stop_db_btn1.setMaximumWidth(160)
        self._stop_db_btn1.clicked.connect(self._on_stop_db)
        bottom_row.addWidget(self._stop_db_btn1)
        self._back_to_setup_btn = QPushButton("Back to Setup")
        self._back_to_setup_btn.setMinimumHeight(32)
        self._back_to_setup_btn.setMaximumWidth(140)
        self._back_to_setup_btn.clicked.connect(lambda: self._stack.setCurrentIndex(0))
        bottom_row.addWidget(self._back_to_setup_btn)
        bottom_row.addStretch()
        lay1.addLayout(bottom_row)

        self._log1 = QTextEdit()
        self._log1.setReadOnly(True)
        self._log1.setFont(QFont("Monospace", 9))
        lay1.addWidget(self._log1, stretch=1)
        self._stack.addWidget(page1)

    # ── State management ────────────────────────────────────────────────────

    def _check_state(self) -> None:
        """Update status labels and determine which page to show."""
        from archive import (
            metiswise_available, podman_available,
            archive_pod_running, check_stale_environment_cfg,
        )

        mw_ok = metiswise_available()
        pm_ok = podman_available()
        pod_ok = archive_pod_running()

        # Status labels on page 0.
        if mw_ok:
            self._mw_status.setText("Installed")
            self._mw_status.setStyleSheet("color: green; font-weight: bold;")
            self._install_btn.setEnabled(False)
        else:
            self._mw_status.setText("Not installed")
            self._mw_status.setStyleSheet("color: red;")
            self._install_btn.setEnabled(True)

        if pm_ok:
            self._pm_status.setText("Installed")
            self._pm_status.setStyleSheet("color: green; font-weight: bold;")
            self._podman_install_btn.setEnabled(False)
        else:
            self._pm_status.setText("Not found")
            self._pm_status.setStyleSheet("color: red;")
            self._podman_install_btn.setEnabled(True)

        if pod_ok:
            self._db_status.setText("Running")
            self._db_status.setStyleSheet("color: green; font-weight: bold;")
            self._stop_db_btn0.setEnabled(True)
        else:
            self._db_status.setText("Stopped")
            self._db_status.setStyleSheet("color: gray;")
            self._stop_db_btn0.setEnabled(False)

        # Build/start button requires MetisWISE + Podman.
        self._build_start_btn.setEnabled(mw_ok and pm_ok)

        # Show page 1 only when all prerequisites are met.
        if mw_ok and pm_ok and pod_ok:
            self._stack.setCurrentIndex(1)
            # Write saved DB credentials pointing to localhost.
            user = self._db_user_edit.text().strip()
            pw = self._db_pass_edit.text().strip()
            if user and pw:
                from archive import write_db_credentials
                write_db_credentials(user, pw, host="localhost")
            warning = check_stale_environment_cfg()
            if warning:
                log_append(self._log1, warning + "\n", "orange")
        else:
            self._stack.setCurrentIndex(0)

    # ── MetisWISE install ──────────────────────────────────────────────────

    def _on_install_metiswise(self) -> None:
        creds = self._cred_edit.text().strip()
        if not creds:
            QMessageBox.warning(
                self, "Missing credentials",
                "Enter OmegaCEN credentials (username:password) to install MetisWISE.",
            )
            return
        self._log0.clear()
        self._install_btn.setEnabled(False)
        self._worker = MetisWISEInstallWorker(creds)
        self._worker.log.connect(lambda t, c: log_append(self._log0, t, c))
        self._worker.done.connect(self._on_metiswise_installed)
        self._worker.start()

    def _on_metiswise_installed(self, success: bool) -> None:
        self._install_btn.setEnabled(True)
        if success:
            self._check_state()

    # ── Podman install ─────────────────────────────────────────────────────

    def _on_install_podman(self) -> None:
        self._log0.clear()
        self._podman_install_btn.setEnabled(False)
        self._worker = PodmanInstallWorker()
        self._worker.log.connect(lambda t, c: log_append(self._log0, t, c))
        self._worker.done.connect(self._on_podman_installed)
        self._worker.start()

    def _on_podman_installed(self, success: bool) -> None:
        self._podman_install_btn.setEnabled(True)
        if success:
            self._check_state()

    # ── Build & start database ─────────────────────────────────────────────

    def _on_build_start_db(self) -> None:
        creds = self._cred_edit.text().strip()
        if not creds:
            QMessageBox.warning(
                self, "Missing credentials",
                "Enter OmegaCEN credentials — they are needed to build "
                "the archive container image.",
            )
            return
        self._log0.clear()
        self._build_start_btn.setEnabled(False)
        self._worker = BuildStartDBWorker(creds)
        self._worker.log.connect(lambda t, c: log_append(self._log0, t, c))
        self._worker.done.connect(self._on_db_started)
        self._worker.start()

    def _on_db_started(self, success: bool) -> None:
        self._build_start_btn.setEnabled(True)
        if success:
            self._save_settings()
            self._check_state()

    # ── Stop database ──────────────────────────────────────────────────────

    def _on_stop_db(self) -> None:
        log_view = self._log1 if self._stack.currentIndex() == 1 else self._log0
        self._stop_db_btn0.setEnabled(False)
        self._stop_db_btn1.setEnabled(False)
        self._worker = StopDBWorker()
        self._worker.log.connect(lambda t, c: log_append(log_view, t, c))
        self._worker.done.connect(self._on_db_stopped)
        self._worker.start()

    def _on_db_stopped(self, _success: bool) -> None:
        self._check_state()

    # ── Database credentials ───────────────────────────────────────────────

    def _on_connect_db(self) -> None:
        user = self._db_user_edit.text().strip()
        pw = self._db_pass_edit.text().strip()
        if not user or not pw:
            QMessageBox.warning(
                self, "Missing credentials",
                "Enter database username and password.",
            )
            return
        from archive import write_db_credentials, reset_db_connection
        cfg = write_db_credentials(user, pw, host="localhost")
        reset_db_connection()
        log_append(self._log1, f"Credentials written to {cfg}\n", "green")
        log_append(self._log1, "Testing connection…\n", "cyan")
        self._save_settings()
        self._connect_btn.setEnabled(False)
        self._worker = QueryWorker()  # quick "(all)" query to test connectivity
        self._worker.log.connect(lambda t, c: log_append(self._log1, t, c))
        self._worker.results.connect(
            lambda items: log_append(
                self._log1,
                f"Connected — {len(items)} item(s) in archive.\n", "green",
            )
        )
        self._worker.done.connect(lambda _ok: self._connect_btn.setEnabled(True))
        self._worker.start()

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
        self._archive_list.clear()
        self._refresh_btn.setEnabled(False)
        catg_text = self._catg_combo.currentText().strip()
        category = None if catg_text in ("", "(all)") else catg_text
        if category:
            log_append(self._log1, f"Querying archive for {category}…\n", "cyan")
        else:
            log_append(self._log1, "Querying archive (all items)…\n", "cyan")
        self._worker = QueryWorker(category=category)
        self._worker.log.connect(lambda t, c: log_append(self._log1, t, c))
        self._worker.results.connect(self._on_query_results)
        self._worker.done.connect(lambda _ok: self._refresh_btn.setEnabled(True))
        self._worker.start()

    def _on_query_results(self, items: list) -> None:
        self._query_items = items
        self._apply_filename_filter()

    def _apply_filename_filter(self) -> None:
        """Re-populate the archive list, filtering by the filename text."""
        items = getattr(self, "_query_items", [])
        needle = self._filename_filter.text().strip().lower()
        self._archive_list.clear()
        for it in items:
            label = it.get("filename", "?")
            if needle and needle not in label.lower():
                continue
            catg = it.get("pro_catg", "") or it.get("class_name", "")
            if catg:
                label += f"  [{catg}]"
            self._archive_list.addItem(label)

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
        self._db_user_edit.setText(
            self._settings.value("archive_db_user", "AWTEST")
        )
        self._db_pass_edit.setText(
            self._settings.value("archive_db_pass", "lmno")
        )

    def _save_settings(self) -> None:
        self._settings.setValue("archive_cred", self._cred_edit.text())
        self._settings.setValue("archive_db_user", self._db_user_edit.text())
        self._settings.setValue("archive_db_pass", self._db_pass_edit.text())


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

        # Pipeline input dirs  [pipeline-only mode only]
        self.pipeline_input_list = QListWidget()
        self.pipeline_input_list.setSelectionMode(
            QListWidget.SelectionMode.ExtendedSelection)
        self.pipeline_input_list.setMaximumHeight(100)
        pipe_in_content = QHBoxLayout()
        pipe_in_content.addWidget(self.pipeline_input_list)
        pipe_in_btns = QVBoxLayout()
        pipe_add_btn = QPushButton("Add…")
        pipe_add_btn.setProperty("role", "info")
        pipe_add_btn.clicked.connect(self._add_pipeline_input)
        pipe_rm_btn = QPushButton("Remove")
        pipe_rm_btn.setProperty("role", "danger")
        pipe_rm_btn.clicked.connect(self._remove_pipeline_input)
        pipe_in_btns.addWidget(pipe_add_btn)
        pipe_in_btns.addWidget(pipe_rm_btn)
        pipe_in_btns.addStretch()
        pipe_in_content.addLayout(pipe_in_btns)

        self.pipeline_input_row = QWidget()
        pi_outer = QVBoxLayout(self.pipeline_input_row)
        pi_outer.setContentsMargins(0, 0, 0, 0)
        pi_lbl = QLabel("Pipeline input dirs  (--pipeline-input):")
        pi_outer.addWidget(pi_lbl)
        pi_outer.addLayout(pipe_in_content)
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
        self.inst_edit.setPlaceholderText(str(INST_PKGS))
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
        self.open_folder_btn = QPushButton("Open Folder")
        self.open_folder_btn.setProperty("role", "info")
        self.open_folder_btn.setMinimumHeight(36)
        self.open_folder_btn.setMaximumWidth(120)
        self.open_folder_btn.clicked.connect(self._open_folder)
        run_row.addWidget(self.run_btn)
        run_row.addWidget(self.stop_btn)
        run_row.addWidget(self.open_folder_btn)
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
            dirs = [self.pipeline_input_list.item(i).text()
                    for i in range(self.pipeline_input_list.count())]
            pipe_in_str = ", ".join(dirs) if dirs else f"{root / 'sim'}/"
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

    def _add_pipeline_input(self) -> None:
        d = QFileDialog.getExistingDirectory(
            self, "Select input directory", str(REPO_ROOT))
        if d and not any(
            self.pipeline_input_list.item(i).text() == d
            for i in range(self.pipeline_input_list.count())
        ):
            self.pipeline_input_list.addItem(d)
            self._update_output_info()

    def _remove_pipeline_input(self) -> None:
        for it in self.pipeline_input_list.selectedItems():
            self.pipeline_input_list.takeItem(self.pipeline_input_list.row(it))
        self._update_output_info()

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
            for i in range(self.pipeline_input_list.count()):
                args += ["--pipeline-input",
                         self.pipeline_input_list.item(i).text()]

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
        script = str(REPO_ROOT / "src" / "run_metis.py")

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

    # ── Open output folder ───────────────────────────────────────────────────

    def _open_folder(self) -> None:
        """Open the output directory in the system file manager or a terminal."""
        text = self.output_edit.text().strip()
        target = Path(text) if text else REPO_ROOT / "output"

        if not target.exists():
            QMessageBox.information(
                self, "Directory not found",
                f"The output directory does not exist yet:\n\n{target}\n\n"
                "Run the pipeline first to create it.",
            )
            return

        url = QUrl.fromLocalFile(str(target))
        if QDesktopServices.openUrl(url):
            return

        # No file manager — fall back to opening a terminal at the directory.
        for term in ("x-terminal-emulator", "xterm", "konsole",
                     "gnome-terminal", "xfce4-terminal"):
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
            self, "Cannot open folder",
            f"No file manager or terminal emulator found.\n\n"
            f"Output directory:\n{target}",
        )

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
        for f in (s.value("pipeline_input_dirs") or []):
            self.pipeline_input_list.addItem(f)
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
        s.setValue("pipeline_input_dirs", [
            self.pipeline_input_list.item(i).text()
            for i in range(self.pipeline_input_list.count())
        ])
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
        self._home_light = "pink_light" if initial_theme == "pink" else "light"

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
        if not _installation_complete():
            tabs.setCurrentIndex(1)  # Install tab
        self.setCentralWidget(tabs)

    def _update_theme_btn_label(self) -> None:
        if self._current_theme in ("light", "pink_light"):
            self._theme_btn.setText("Dark theme")
        else:
            self._theme_btn.setText("Light theme")

    def _toggle_theme(self) -> None:
        if self._current_theme in ("light", "pink_light"):
            self._current_theme = self._home_dark
        else:
            self._current_theme = self._home_light
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
