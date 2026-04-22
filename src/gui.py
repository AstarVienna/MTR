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
    QDialog, QDialogButtonBox, QFileDialog, QGroupBox, QHBoxLayout,
    QHeaderView, QLabel, QLineEdit, QListWidget, QMainWindow, QMessageBox,
    QProgressBar, QPushButton, QRadioButton, QSpinBox, QSplitter,
    QStackedWidget, QTabBar, QTableWidget, QTableWidgetItem, QTabWidget,
    QTextEdit, QVBoxLayout, QWidget,
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

            self._step("Installing Python dependencies (uv sync --group pipeline --inexact)…")
            recipe_dir = str(TARGET_A / "metisp" / "pyrecipes") + "/"
            os.environ["PYCPL_RECIPE_DIR"] = recipe_dir
            os.environ["PYESOREX_PLUGIN_DIR"] = recipe_dir
            # --inexact preserves packages installed via `uv pip install` that
            # aren't tracked in pyproject.toml (e.g. MetisWISE from the
            # Archive tab).  Without it, uv sync wipes them on every run.
            self._run(["uv", "sync", "--group", "pipeline", "--inexact"],
                      cwd=REPO_ROOT)

            self._step("Installing METIS_Simulations (editable) into .venv…")
            # METIS_Simulations is not tracked in pyproject.toml because the
            # checkout only exists after the clone above — a fresh `./launch.sh`
            # would otherwise fail to resolve it.  Install it out-of-band;
            # --inexact on subsequent `uv sync` runs preserves it.
            self._run(
                ["uv", "pip", "install", "--editable", str(TARGET_B),
                 "--python", str(REPO_ROOT / ".venv" / "bin" / "python")],
                cwd=REPO_ROOT,
            )

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
            # Wipe EDPS bookkeeping on every server startup. Without this, a
            # stale db.json entry whose on-disk outputs have been removed will
            # collide with a fresh submission's deterministic job UUID and make
            # EDPS short-circuit the run with a FileNotFoundError.
            "truncate": (r"^truncate=.*", "truncate=True"),
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


class TestConnectionWorker(QThread):
    """Write the five Environment.cfg fields and probe the remote archive."""

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)

    def __init__(self, fields: dict[str, str]) -> None:
        super().__init__()
        self._fields = fields

    def run(self) -> None:
        from archive import write_env_cfg, reset_db_connection, query_archive
        try:
            cfg = write_env_cfg(**self._fields)
            self.log.emit(f"Wrote credentials to {cfg}\n", "green")
            reset_db_connection()
            self.log.emit("Probing archive connection…\n", "cyan")
            items = query_archive(
                on_log=lambda msg: self.log.emit(msg + "\n", ""),
            )
            self.log.emit(
                f"\n✓ Connected — {len(items)} item(s) visible.\n", "green",
            )
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Connection failed: {exc}\n", "red")
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


class UploadWorker(QThread):
    """Upload local FITS files into the remote archive."""

    log      = pyqtSignal(str, str)
    progress = pyqtSignal(int, int)
    done     = pyqtSignal(bool)

    def __init__(self, entries: list[tuple[Path, str | None]]) -> None:
        super().__init__()
        self._entries = entries

    def run(self) -> None:
        from archive import upload_file
        try:
            total = len(self._entries)
            uploaded = 0
            for i, (path, class_name) in enumerate(self._entries, 1):
                self.progress.emit(i, total)
                ok = upload_file(
                    path, class_name,
                    on_log=lambda msg: self.log.emit(msg + "\n", ""),
                )
                if ok:
                    uploaded += 1
            self.log.emit(
                f"\n✓ Uploaded {uploaded}/{total} file(s).\n", "green",
            )
            self.done.emit(True)
        except Exception as exc:
            self.log.emit(f"\n✗ Upload failed: {exc}\n", "red")
            self.done.emit(False)


# ---------------------------------------------------------------------------
# Archive tab
# ---------------------------------------------------------------------------

class ArchiveTab(QWidget):
    """Archive tab: install MetisWISE + configure remote archive, then
    query / download / upload files.

    Page 0 — Install & Configure: pip-install MetisWISE into the project
    venv, fill the five ``[global]`` fields written to
    ``~/.awe/Environment.cfg``, and run a test connection.
    Page 1 — Query & Download: search the remote archive by category /
    filename and retrieve files.
    Page 2 — Upload: stage local FITS files (individually or by folder),
    auto-classify via DPR headers, and ingest into the archive.
    """

    _CFG_FIELD_ORDER = (
        "database_user",
        "database_password",
        "project",
        "database_tablespacename",
        "database_name",
    )

    def __init__(self) -> None:
        super().__init__()
        self._settings = QSettings("METIS", "TestRunner")
        self._worker: QThread | None = None
        self._connection_ok = False
        self._build_ui()
        self._load_settings()
        self._refresh_install_status()

    def showEvent(self, event) -> None:  # noqa: N802 (Qt override)
        # Re-check whether MetisWISE is still installed every time the tab
        # becomes visible — the Install tab's `uv sync` can remove it, and
        # the user needs the Install button to re-enable in that case.
        super().showEvent(event)
        self._refresh_install_status()

    # ── UI construction ─────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setContentsMargins(20, 20, 20, 20)

        self._stack = QStackedWidget()

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setFont(QFont("Monospace", 9))
        self._log.setMinimumHeight(100)

        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(self._stack)
        splitter.addWidget(self._log)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 1)
        splitter.setChildrenCollapsible(False)
        outer.addWidget(splitter)

        self._stack.addWidget(self._build_page_install())
        self._stack.addWidget(self._build_page_query())
        self._stack.addWidget(self._build_page_upload())

    def _build_page_install(self) -> QWidget:
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setSpacing(12)

        desc = QLabel(
            "<b>Install MetisWISE & connect to the remote archive</b><br><br>"
            "The Archive tab talks to the METIS AIT archive via the "
            "<a href='https://github.com/AstarVienna/MetisWISE'>MetisWISE</a> "
            "Python client. Paste the OmegaCEN credentials from the "
            "<a href='https://metis.strw.leidenuniv.nl/wiki/doku.php?id=ait:archive'>"
            "METIS wiki</a> to pip-install the package, then fill in the five "
            "database fields (also from the wiki) and click Save &amp; Test. "
            "The values are written to <code>~/.awe/Environment.cfg</code>; "
            "<code>data_server</code>, port and protocol are inherited from the "
            "MetisWISE-packaged default (<code>metis-ds.hpc.rug.nl:8013</code>, "
            "https)."
        )
        desc.setWordWrap(True)
        desc.setTextFormat(Qt.TextFormat.RichText)
        desc.setOpenExternalLinks(True)
        lay.addWidget(desc)

        # -- 1. MetisWISE install --
        mw_grp = QGroupBox("1. Install MetisWISE")
        mw_lay = QVBoxLayout(mw_grp)
        cred_row = QHBoxLayout()
        cred_row.addWidget(QLabel("OmegaCEN credentials (user:pass):"))
        self._cred_edit = QLineEdit()
        self._cred_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self._cred_edit.setPlaceholderText("username:password")
        cred_row.addWidget(self._cred_edit)
        mw_lay.addLayout(cred_row)
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
        lay.addWidget(mw_grp)

        # -- 2. Remote archive credentials --
        cfg_grp = QGroupBox("2. Remote archive credentials")
        cfg_lay = QVBoxLayout(cfg_grp)
        cfg_desc = QLabel(
            "These values are written under <code>[global]</code> in "
            "<code>~/.awe/Environment.cfg</code>."
        )
        cfg_desc.setWordWrap(True)
        cfg_desc.setTextFormat(Qt.TextFormat.RichText)
        cfg_lay.addWidget(cfg_desc)

        self._cfg_edits: dict[str, QLineEdit] = {}
        labels = {
            "database_user":            "database_user:",
            "database_password":        "database_password:",
            "project":                  "project:",
            "database_tablespacename":  "database_tablespacename:",
            "database_name":            "database_name:",
        }
        for key in self._CFG_FIELD_ORDER:
            row = QHBoxLayout()
            label = QLabel(labels[key])
            label.setMinimumWidth(200)
            row.addWidget(label)
            edit = QLineEdit()
            if key == "database_password":
                edit.setEchoMode(QLineEdit.EchoMode.Password)
            edit.textChanged.connect(self._invalidate_connection)
            row.addWidget(edit)
            cfg_lay.addLayout(row)
            self._cfg_edits[key] = edit

        btn_row = QHBoxLayout()
        self._save_test_btn = QPushButton("Save && Test Connection")
        self._save_test_btn.setProperty("role", "success")
        self._save_test_btn.setMinimumHeight(32)
        self._save_test_btn.setMaximumWidth(240)
        self._save_test_btn.clicked.connect(self._on_save_and_test)
        btn_row.addWidget(self._save_test_btn)
        self._cfg_status = QLabel()
        btn_row.addWidget(self._cfg_status)
        btn_row.addStretch()
        cfg_lay.addLayout(btn_row)
        lay.addWidget(cfg_grp)

        # -- 3. Continue --
        cont_row = QHBoxLayout()
        cont_row.addStretch()
        self._continue_btn = QPushButton("Continue to Query / Download  →")
        self._continue_btn.setMinimumHeight(32)
        self._continue_btn.setMaximumWidth(280)
        self._continue_btn.clicked.connect(
            lambda: self._stack.setCurrentIndex(1),
        )
        self._continue_btn.setEnabled(False)
        cont_row.addWidget(self._continue_btn)
        lay.addLayout(cont_row)

        lay.addStretch()
        return page

    def _build_page_query(self) -> QWidget:
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setSpacing(10)

        desc = QLabel(
            "<b>Query &amp; download from the remote archive</b><br><br>"
            "Pick a category (raw classification tag or master PRO.CATG), "
            "hit <i>Search</i>, then select one or more files to retrieve."
        )
        desc.setWordWrap(True)
        desc.setTextFormat(Qt.TextFormat.RichText)
        lay.addWidget(desc)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Category:"))
        self._catg_combo = QComboBox()
        self._catg_combo.setEditable(True)
        self._catg_combo.setMinimumWidth(200)
        self._catg_combo.addItem("(all)")
        from itertools import chain as _chain
        from archive import TASK_PRODUCTS
        all_produces = set(_chain.from_iterable(p.produces for p in TASK_PRODUCTS.values()))
        for catg in sorted(all_produces):
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
        lay.addLayout(filter_row)

        self._archive_list = QListWidget()
        self._archive_list.setSelectionMode(
            QListWidget.SelectionMode.ExtendedSelection,
        )
        self._archive_list.setMinimumHeight(120)
        lay.addWidget(self._archive_list, stretch=1)

        dl_row = QHBoxLayout()
        self._download_btn = QPushButton("Download Selected")
        self._download_btn.setProperty("role", "success")
        self._download_btn.setMinimumHeight(32)
        self._download_btn.setMaximumWidth(200)
        self._download_btn.clicked.connect(self._on_download)
        dl_row.addWidget(self._download_btn)
        dl_row.addStretch()
        self._to_upload_btn = QPushButton("Continue to Upload  →")
        self._to_upload_btn.setMinimumHeight(32)
        self._to_upload_btn.setMaximumWidth(200)
        self._to_upload_btn.clicked.connect(
            lambda: self._stack.setCurrentIndex(2),
        )
        dl_row.addWidget(self._to_upload_btn)
        self._back_btn = QPushButton("←  Back to setup")
        self._back_btn.setMinimumHeight(32)
        self._back_btn.setMaximumWidth(160)
        self._back_btn.clicked.connect(
            lambda: self._stack.setCurrentIndex(0),
        )
        dl_row.addWidget(self._back_btn)
        lay.addLayout(dl_row)

        return page

    def _build_page_upload(self) -> QWidget:
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setSpacing(10)

        desc = QLabel(
            "<b>Upload local FITS files to the remote archive</b><br><br>"
            "Stage individual files or whole folders, confirm the "
            "auto-detected DataItem class for each row (from DPR headers), "
            "then click <i>Upload Selected</i>. Ingestion mirrors MetisWISE's "
            "<code>tools/ingest_file.py</code>: "
            "<code>Raw(path).store() + .commit()</code>."
        )
        desc.setWordWrap(True)
        desc.setTextFormat(Qt.TextFormat.RichText)
        lay.addWidget(desc)

        add_row = QHBoxLayout()
        self._add_files_btn = QPushButton("Add files…")
        self._add_files_btn.setMinimumHeight(32)
        self._add_files_btn.clicked.connect(self._on_add_upload_files)
        add_row.addWidget(self._add_files_btn)
        self._add_folder_btn = QPushButton("Add folder…")
        self._add_folder_btn.setMinimumHeight(32)
        self._add_folder_btn.clicked.connect(self._on_add_upload_folder)
        add_row.addWidget(self._add_folder_btn)
        self._remove_staged_btn = QPushButton("Remove selected")
        self._remove_staged_btn.setMinimumHeight(32)
        self._remove_staged_btn.clicked.connect(self._on_remove_staged)
        add_row.addWidget(self._remove_staged_btn)
        add_row.addStretch()
        lay.addLayout(add_row)

        self._stage_table = QTableWidget(0, 3)
        self._stage_table.setHorizontalHeaderLabels(
            ["Filename", "DataItem class", "Full path"],
        )
        self._stage_table.setSelectionBehavior(
            QTableWidget.SelectionBehavior.SelectRows,
        )
        self._stage_table.setSelectionMode(
            QTableWidget.SelectionMode.ExtendedSelection,
        )
        self._stage_table.setEditTriggers(
            QTableWidget.EditTrigger.NoEditTriggers,
        )
        self._stage_table.verticalHeader().setVisible(False)
        hdr = self._stage_table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self._stage_table.setMinimumHeight(120)
        lay.addWidget(self._stage_table, stretch=1)

        action_row = QHBoxLayout()
        self._set_class_btn = QPushButton("Set class for selected…")
        self._set_class_btn.setMinimumHeight(32)
        self._set_class_btn.clicked.connect(self._on_set_class_selected)
        action_row.addWidget(self._set_class_btn)
        action_row.addStretch()
        self._upload_btn = QPushButton("Upload Selected")
        self._upload_btn.setProperty("role", "success")
        self._upload_btn.setMinimumHeight(32)
        self._upload_btn.setMaximumWidth(200)
        self._upload_btn.clicked.connect(self._on_upload)
        action_row.addWidget(self._upload_btn)
        self._back_to_query_btn = QPushButton("←  Back to Query")
        self._back_to_query_btn.setMinimumHeight(32)
        self._back_to_query_btn.setMaximumWidth(160)
        self._back_to_query_btn.clicked.connect(
            lambda: self._stack.setCurrentIndex(1),
        )
        action_row.addWidget(self._back_to_query_btn)
        lay.addLayout(action_row)

        return page

    # ── State helpers ───────────────────────────────────────────────────────

    def _refresh_install_status(self) -> None:
        """Update the MetisWISE status label + Continue button gate."""
        from archive import metiswise_available
        if metiswise_available():
            self._mw_status.setText("Installed")
            self._mw_status.setStyleSheet("color: green; font-weight: bold;")
            self._install_btn.setEnabled(False)
        else:
            self._mw_status.setText("Not installed")
            self._mw_status.setStyleSheet("color: red;")
            self._install_btn.setEnabled(True)
        self._update_continue_button()

    def _update_continue_button(self) -> None:
        from archive import metiswise_available
        self._continue_btn.setEnabled(
            metiswise_available() and self._connection_ok,
        )

    def _invalidate_connection(self) -> None:
        """Any edit to the 5 fields invalidates the last test-connection result."""
        if self._connection_ok:
            self._connection_ok = False
            self._cfg_status.setText("")
            self._update_continue_button()

    # ── MetisWISE install ──────────────────────────────────────────────────

    def _on_install_metiswise(self) -> None:
        creds = self._cred_edit.text().strip()
        if not creds:
            QMessageBox.warning(
                self, "Missing credentials",
                "Enter OmegaCEN credentials (username:password) "
                "to install MetisWISE.",
            )
            return
        self._log.clear()
        self._install_btn.setEnabled(False)
        self._worker = MetisWISEInstallWorker(creds)
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
        self._worker.done.connect(self._on_metiswise_installed)
        self._worker.start()

    def _on_metiswise_installed(self, _success: bool) -> None:
        self._save_settings()
        self._refresh_install_status()

    # ── Save & Test Connection ─────────────────────────────────────────────

    def _on_save_and_test(self) -> None:
        from archive import metiswise_available
        if not metiswise_available():
            QMessageBox.warning(
                self, "MetisWISE not installed",
                "Install MetisWISE first — the test connection needs it.",
            )
            return
        fields = {k: e.text().strip() for k, e in self._cfg_edits.items()}
        missing = [k for k, v in fields.items() if not v]
        if missing:
            QMessageBox.warning(
                self, "Missing fields",
                "Fill in all five credential fields:\n  "
                + "\n  ".join(missing),
            )
            return
        self._save_test_btn.setEnabled(False)
        self._cfg_status.setText("Testing…")
        self._cfg_status.setStyleSheet("color: gray;")
        self._worker = TestConnectionWorker(fields)
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
        self._worker.done.connect(self._on_test_connection_done)
        self._worker.start()

    def _on_test_connection_done(self, success: bool) -> None:
        self._save_test_btn.setEnabled(True)
        self._connection_ok = success
        if success:
            self._cfg_status.setText("Connected")
            self._cfg_status.setStyleSheet("color: green; font-weight: bold;")
            self._save_settings()
        else:
            self._cfg_status.setText("Failed — see log")
            self._cfg_status.setStyleSheet("color: red;")
        self._update_continue_button()

    # ── Download ────────────────────────────────────────────────────────────

    def _on_refresh_archive(self) -> None:
        self._archive_list.clear()
        self._refresh_btn.setEnabled(False)
        catg_text = self._catg_combo.currentText().strip()
        category = None if catg_text in ("", "(all)") else catg_text
        if category:
            log_append(self._log, f"Querying archive for {category}…\n", "cyan")
        else:
            log_append(self._log, "Querying archive (all items)…\n", "cyan")
        self._worker = QueryWorker(category=category)
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
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
        dest = QFileDialog.getExistingDirectory(
            self, "Download destination", str(REPO_ROOT),
        )
        if not dest:
            return
        filenames = [item.text().split("  [")[0] for item in selected]
        self._download_btn.setEnabled(False)
        self._worker = DownloadWorker(filenames, Path(dest))
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
        self._worker.done.connect(lambda _ok: self._download_btn.setEnabled(True))
        self._worker.start()

    # ── Upload ──────────────────────────────────────────────────────────────

    _UNKNOWN_CLASS_PLACEHOLDER = "⚠ pick class"

    def _staged_paths(self) -> set[str]:
        return {
            self._stage_table.item(r, 2).text()
            for r in range(self._stage_table.rowCount())
        }

    def _add_staged_file(self, path: Path) -> None:
        """Append a single row to the staging table with auto-classification."""
        if str(path) in self._staged_paths():
            return
        from run_metis import classify_fits_file
        tag = classify_fits_file(path)
        row = self._stage_table.rowCount()
        self._stage_table.insertRow(row)

        name_item = QTableWidgetItem(path.name)
        name_item.setFlags(name_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self._stage_table.setItem(row, 0, name_item)

        class_text = tag if tag else self._UNKNOWN_CLASS_PLACEHOLDER
        class_item = QTableWidgetItem(class_text)
        class_item.setFlags(class_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        if not tag:
            class_item.setForeground(QColor("#FF6B6B"))
        self._stage_table.setItem(row, 1, class_item)

        path_item = QTableWidgetItem(str(path))
        path_item.setFlags(path_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self._stage_table.setItem(row, 2, path_item)

    def _on_add_upload_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self, "Stage FITS files for upload",
            str(REPO_ROOT),
            "FITS files (*.fits *.fits.gz)",
        )
        for f in files:
            self._add_staged_file(Path(f))

    def _on_add_upload_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(
            self, "Stage folder of FITS files", str(REPO_ROOT),
        )
        if not folder:
            return
        root = Path(folder)
        discovered: list[Path] = []
        for pattern in ("*.fits", "*.fits.gz"):
            discovered.extend(root.rglob(pattern))
        discovered.sort()
        for p in discovered:
            self._add_staged_file(p)
        log_append(
            self._log,
            f"Staged {len(discovered)} file(s) from {folder}\n",
            "cyan",
        )

    def _on_remove_staged(self) -> None:
        rows = sorted(
            {idx.row() for idx in self._stage_table.selectedIndexes()},
            reverse=True,
        )
        for r in rows:
            self._stage_table.removeRow(r)

    def _candidate_class_names(self) -> list[str]:
        from itertools import chain as _chain
        from archive import TASK_PRODUCTS
        from run_metis import DPR_TO_TAG
        produces = _chain.from_iterable(p.produces for p in TASK_PRODUCTS.values())
        candidates = set(DPR_TO_TAG.values()) | set(produces)
        return sorted(candidates)

    def _on_set_class_selected(self) -> None:
        rows = sorted({idx.row() for idx in self._stage_table.selectedIndexes()})
        if not rows:
            QMessageBox.warning(
                self, "No selection",
                "Select one or more rows, then choose a class to apply.",
            )
            return

        dlg = QDialog(self)
        dlg.setWindowTitle("Set DataItem class")
        v = QVBoxLayout(dlg)
        v.addWidget(QLabel(
            f"Apply to {len(rows)} selected row(s):",
        ))
        combo = QComboBox()
        combo.setEditable(True)
        combo.addItems(self._candidate_class_names())
        v.addWidget(combo)
        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        v.addWidget(btns)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        value = combo.currentText().strip()
        if not value:
            return
        for r in rows:
            item = self._stage_table.item(r, 1)
            item.setText(value)
            item.setForeground(QPalette().color(QPalette.ColorRole.Text))

    def _on_upload(self) -> None:
        total_rows = self._stage_table.rowCount()
        if total_rows == 0:
            QMessageBox.warning(
                self, "Nothing to upload",
                "Add files or a folder first.",
            )
            return

        selected_rows = sorted(
            {idx.row() for idx in self._stage_table.selectedIndexes()},
        )
        if not selected_rows:
            selected_rows = list(range(total_rows))

        entries: list[tuple[Path, str | None]] = []
        unresolved: list[str] = []
        for r in selected_rows:
            path = Path(self._stage_table.item(r, 2).text())
            class_text = self._stage_table.item(r, 1).text()
            if class_text == self._UNKNOWN_CLASS_PLACEHOLDER:
                unresolved.append(path.name)
                continue
            entries.append((path, class_text))

        if unresolved:
            QMessageBox.warning(
                self, "Unresolved rows",
                "These files have no DataItem class set. Use "
                "“Set class for selected…” first:\n  "
                + "\n  ".join(unresolved),
            )
            return

        self._upload_btn.setEnabled(False)
        log_append(
            self._log,
            f"Uploading {len(entries)} file(s) to archive…\n",
            "cyan",
        )
        self._worker = UploadWorker(entries)
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
        self._worker.done.connect(
            lambda _ok: self._upload_btn.setEnabled(True),
        )
        self._worker.start()

    # ── Settings ────────────────────────────────────────────────────────────

    def _load_settings(self) -> None:
        # Scrub any archive fields previously persisted to QSettings
        # (~/.config/METIS/TestRunner.conf) by older builds.
        for stale in ("archive_cred",
                      "archive_cfg_database_user",
                      "archive_cfg_database_password",
                      "archive_cfg_project",
                      "archive_cfg_database_tablespacename",
                      "archive_cfg_database_name",
                      "archive_db_user",
                      "archive_db_pass"):
            self._settings.remove(stale)

        # Pre-populate from ~/.awe/Environment.cfg if present.
        from archive import read_env_cfg
        existing = read_env_cfg()
        for key, edit in self._cfg_edits.items():
            edit.setText(existing.get(key, ""))

    def _save_settings(self) -> None:
        # No archive fields are persisted to QSettings. The DB fields
        # live in ~/.awe/Environment.cfg after Save & Test; the OmegaCEN
        # creds are kept in-memory only for this session.
        return


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
        outer.setContentsMargins(20, 20, 20, 20)

        top = QWidget()
        top_lay = QVBoxLayout(top)
        top_lay.setSpacing(12)
        top_lay.setContentsMargins(0, 0, 0, 0)

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
        top_lay.addWidget(self.yaml_grp)

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
        self.setMinimumSize(600, 400)
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
