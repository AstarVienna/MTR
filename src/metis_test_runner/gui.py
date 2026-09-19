#!/usr/bin/env python3
"""gui.py — METIS Test Runner GUI

Three-tab graphical front-end:
  Install  — pip-installs every METIS pipeline dependency into MTR's own venv
             and clones the simulation/pipeline repos into the user data dir
  Run      — wraps run_metis.py with a file-picker and options UI
  Archive  — install MetisWISE and upload/download FITS files
"""

import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import traceback
from pathlib import Path

from PyQt6.QtCore import (
    QEvent,
    QProcess,
    QProcessEnvironment,
    QSettings,
    Qt,
    QThread,
    QTimer,
    QUrl,
    pyqtSignal,
)
from PyQt6.QtGui import QColor, QDesktopServices, QFont, QPalette, QTextCharFormat, QTextCursor
from PyQt6.QtWidgets import (
    QAbstractSpinBox,
    QApplication,
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QSplitter,
    QStackedWidget,
    QTabBar,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from . import paths
from .env import ensurepip_command_if_needed, resolve_runtime_env
from .indexes import ESO_INDEX, PYCPL_INDEX

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
#
# Module-level aliases for the user data directory and the artifacts under it.
# Computed at import time from `paths.<X>()` so that tests can monkeypatch
# `gui.REPO_ROOT` (etc.) directly and have the call-time `REPO_ROOT / "..."`
# expressions inside methods pick up the patched value.

REPO_ROOT   = paths.data_dir()
TARGET_A    = paths.pipeline_dir()
TARGET_B    = paths.simulations_dir()
INST_PKGS   = paths.inst_pkgs_dir()
REPO_A_URL  = "https://github.com/AstarVienna/METIS_Pipeline.git"
REPO_B_URL  = "https://github.com/AstarVienna/METIS_Simulations.git"

LABEL_W = 280   # fixed label column width in the Run options form

# Set by main() under --smoke-test. The CI smoke test calls win.show(), which
# fires InstallTab.showEvent and would otherwise hit the network (and leave a
# QThread running into app.quit()).
SMOKE_TEST = False


# ---------------------------------------------------------------------------
# Subprocess environment
# ---------------------------------------------------------------------------

def _child_env(runner: str = "default") -> dict[str, str]:
    """Build the environment for subprocesses spawned by the GUI.

    Thin wrapper over the shared env-resolution seam so the GUI launcher, the
    orchestrated run, and the direct mtr-exec / mtr-shell commands all resolve
    the same environment (derived from paths.py, with an optional .env override).

    *runner* must be the runner the user selected: env.py deliberately returns
    the bare parent environment for native/docker/podman, where the tools are
    on PATH or inside a container rather than in MTR's venv. Hardcoding
    "default" here injected MTR's venv paths into every runner.
    """
    return resolve_runtime_env(runner)


def _installation_complete() -> bool:
    """Return True if the essential install artifacts exist.

    The runtime environment is derived from paths.py (see env.py), so this no
    longer depends on a generated .env — only the pipeline clone is required.
    """
    return (TARGET_A / ".git").exists()


def _assert_safe_to_remove(target: Path) -> None:
    """Refuse to recursively delete anything that isn't plausibly a data dir.

    ``REPO_ROOT`` is ``METIS_DATA_DIR`` verbatim (see paths.py), so a typo or a
    stray ``METIS_DATA_DIR=$HOME`` would otherwise point Uninstall's rmtree at
    the user's home directory.
    """
    resolved = target.resolve()
    if resolved.is_symlink() or not resolved.is_dir():
        raise RuntimeError(f"Refusing to remove {resolved}: not a real directory")
    forbidden = {Path("/"), Path.home().resolve(), Path.cwd().resolve()}
    forbidden |= set(Path.home().resolve().parents)
    if resolved in forbidden or len(resolved.parts) < 3:
        raise RuntimeError(
            f"Refusing to remove {resolved}: this does not look like an MTR "
            "data directory. Check METIS_DATA_DIR."
        )


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------
#
# Two ways to run git in this module:
#   * ``_git`` — captures, never raises, usable from the GUI thread. For probes
#     (is it dirty? what is HEAD? which refs does the remote have?) where a
#     failure is information rather than an error.
#   * ``InstallWorker._run`` — streams to the log and raises on non-zero. For
#     the install steps themselves, where a failure must abort.

# Deliberately stricter than git's own check-ref-format: a pragmatic allowlist
# is easier to reason about than replicating git's rules, and it produces a far
# better message than git's "fatal: couldn't find remote ref".
_REF_OK = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._/+@-]{0,254}\Z")

# Branches hoisted to the top of the ref dropdown, in this order.
_DEFAULT_FIRST = ("main", "master", "develop", "dev")


def _git(args: list[str], cwd: Path | None = None,
         timeout: int = 30) -> subprocess.CompletedProcess:
    """Run git, capture output, never raise.

    A missing binary, a timeout, or any OSError comes back as returncode 127
    with the reason in ``.stderr``, so callers only ever branch on returncode.
    ``GIT_TERMINAL_PROMPT=0`` guarantees a private/renamed repo can never block
    the caller on an interactive credential prompt.
    """
    env = _child_env()
    env["GIT_TERMINAL_PROMPT"] = "0"
    try:
        return subprocess.run(
            ["git", *args],
            cwd=str(cwd) if cwd else None,
            capture_output=True, text=True, timeout=timeout, env=env,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return subprocess.CompletedProcess(args, 127, "", str(exc))


def _validate_ref(ref: str | None) -> str:
    """Return the cleaned ref; ``""`` means the remote's default branch.

    Raises ``ValueError`` with a user-facing message on anything else. Argument
    injection is already closed by ``--end-of-options`` at every call site and
    by argv lists (never ``shell=True``); this exists to catch typos early and
    to keep a leading ``-`` out of argv even if a future call site forgets the
    flag.
    """
    ref = (ref or "").strip()
    if not ref:
        return ""
    if (not _REF_OK.match(ref) or ".." in ref
            or ref.endswith((".lock", "/", "."))):
        raise ValueError(
            f"{ref!r} is not a valid branch, tag or commit.\n\n"
            "Use letters, digits and . _ / + @ - with no spaces — for example "
            "'main', 'feature/my-branch', 'v0.4.2', or a full 40-character "
            "commit SHA."
        )
    return ref


def _looks_like_abbrev_sha(ref: str) -> bool:
    """True for a short hex string.

    GitHub serves ``fetch origin <sha>`` only for *full* 40-character SHAs, so
    an abbreviated one is a guaranteed round-trip failure — better to skip
    straight to the full-history fallback than pay for it.
    """
    return bool(re.fullmatch(r"[0-9a-fA-F]{4,39}", ref))


def _same_remote(a: str, b: str) -> bool:
    """Compare remote URLs ignoring a trailing slash and the .git suffix."""
    def norm(u: str) -> str:
        return u.strip().rstrip("/").removesuffix(".git")
    return norm(a) == norm(b)


def _parse_ls_remote(text: str) -> list[str]:
    """Ref names from ``git ls-remote --heads --tags`` output.

    Strips the ``refs/heads/`` / ``refs/tags/`` prefixes, drops the peeled
    ``^{}`` duplicates annotated tags emit, hoists the usual default branches,
    and dedupes globally (a repo can have a branch and a tag of the same name).
    """
    heads: list[str] = []
    tags: list[str] = []
    for line in text.splitlines():
        _sha, _tab, ref = line.partition("\t")
        ref = ref.strip()
        if not ref or ref.endswith("^{}"):
            continue
        if ref.startswith("refs/heads/"):
            heads.append(ref[len("refs/heads/"):])
        elif ref.startswith("refs/tags/"):
            tags.append(ref[len("refs/tags/"):])
    hoisted = [b for b in _DEFAULT_FIRST if b in heads]
    rest = sorted((b for b in heads if b not in hoisted), key=str.lower)
    # Reverse-lexicographic is a good-enough "newest first" for vN.N.N tags.
    ordered = hoisted + rest + sorted(set(tags), key=str.lower, reverse=True)
    return list(dict.fromkeys(ordered))


def _dirty_files(target: Path) -> list[str]:
    """``git status --porcelain`` lines for *target*; [] when clean/not a repo.

    Raises ``RuntimeError`` when git cannot answer (corrupt index, NFS stall)
    so the caller can ask the user rather than assuming a clean tree.
    """
    if not (target / ".git").exists():
        return []
    cp = _git(["-C", str(target), "status", "--porcelain"], timeout=15)
    if cp.returncode != 0:
        raise RuntimeError(cp.stderr.strip() or "git status failed")
    return cp.stdout.splitlines()


def _describe_head(target: Path) -> str:
    """Short human description of what a clone is checked out at.

    e.g. ``main @ d2d257c5 · shallow``, ``v0.4.2 (tag) @ 8a50c604 · modified``,
    ``detached @ 8a50c604``, ``not cloned``.
    """
    if not (target / ".git").exists():
        return "not cloned"
    sha = _git(["-C", str(target), "rev-parse", "--short=8", "HEAD"])
    if sha.returncode != 0:
        return "not a git repository"

    name = _git(["-C", str(target), "symbolic-ref", "--quiet", "--short", "HEAD"])
    if name.returncode == 0:
        label = name.stdout.strip()
    else:
        tag = _git(["-C", str(target), "describe", "--tags", "--exact-match",
                    "HEAD"])
        label = f"{tag.stdout.strip()} (tag)" if tag.returncode == 0 else "detached"

    try:
        dirty = " · modified" if _dirty_files(target) else ""
    except RuntimeError:
        dirty = " · status unknown"
    # Worth surfacing: a shallow clone is why an abbreviated SHA needs a slow
    # full-history fetch.
    shallow = ""
    if _git(["-C", str(target), "rev-parse",
             "--is-shallow-repository"]).stdout.strip() == "true":
        shallow = " · shallow"
    return f"{label} @ {sha.stdout.strip()}{dirty}{shallow}"


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
        QLabel[hint="note"] {{
            color: {log_gray};
            font-size: 14px;
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
        /* Deliberately NOT styling QComboBox::drop-down: touching that
           subcontrol suppresses Qt's native chevron, which is the only mouse
           affordance an *editable* combo has for opening its list. */
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
    """Append text to a read-only QTextEdit, optionally in colour.

    Carriage returns are interpreted like a terminal: a lone ``\\r`` returns to
    the start of the current line so the following text overwrites it. This lets
    tqdm-style progress bars (package downloads, ScopeSim FOV/trace steps) update
    one line in place instead of scrolling the log. Text without ``\\r`` behaves
    exactly as a plain append.
    """
    cursor = widget.textCursor()
    cursor.movePosition(QTextCursor.MoveOperation.End)
    fmt = QTextCharFormat()
    if color:
        resolved = LOG_COLORS.get(f"log_{color}", color)
        fmt.setForeground(QColor(resolved))
    cursor.setCharFormat(fmt)
    # Split on line separators (kept) so a bare CR can overwrite the current
    # line rather than being inserted literally. Pass `fmt` to every insert:
    # removeSelectedText() resets the cursor's char format, so relying on
    # setCharFormat above would drop the colour on overwritten (CR) lines.
    for token in re.split(r"(\r\n|\r|\n)", text):
        if token in ("\n", "\r\n"):
            cursor.insertText("\n", fmt)
        elif token == "\r":
            cursor.movePosition(QTextCursor.MoveOperation.StartOfLine,
                                QTextCursor.MoveMode.KeepAnchor)
            cursor.removeSelectedText()
        elif token:
            cursor.insertText(token, fmt)
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


def _log_exception(log_signal, label: str, exc: BaseException) -> None:
    """Report *exc* to the GUI log, with a traceback for unexpected errors.

    Workers used to log only ``str(exc)``, so a KeyError deep in a helper
    surfaced as ``✗ Failed: 'foo'`` with nothing to debug from. RuntimeError is
    how this code reports *expected* failures with a written-for-humans
    message, so those stay short.
    """
    log_signal.emit(f"\n✗ {label}: {exc}\n", "red")
    if not isinstance(exc, RuntimeError):
        log_signal.emit(traceback.format_exc(), "red")


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def strip_ansi(text: str) -> str:
    """Remove ANSI colour/cursor escapes from subprocess output."""
    return _ANSI_RE.sub("", text)


def stream_subprocess(
    cmd: list,
    *,
    on_line,
    cwd: Path | None = None,
    stdin_text: str | None = None,
    timeout: int = 300,
    env: dict[str, str] | None = None,
) -> None:
    """Run *cmd*, stream its output to *on_line*, and enforce *timeout*.

    Raises ``RuntimeError`` on a non-zero exit and ``TimeoutError`` if the
    deadline expires.

    The timeout is enforced by a watchdog rather than ``proc.wait(timeout=…)``:
    draining ``proc.stdout`` blocks until EOF, so by the time ``wait`` is
    reached the process has always already exited and the timeout could never
    fire. A hung pip download or a ``git fetch`` against a dead mirror used to
    hang the worker thread forever, with no cancel path.

    The child gets its own session so the watchdog can kill the whole process
    group — pip and git spawn their own children, which a bare ``proc.kill()``
    would orphan.
    """
    on_line(f"$ {' '.join(str(c) for c in cmd)}\n", "")
    timed_out = threading.Event()

    with subprocess.Popen(
        [str(c) for c in cmd],
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE if stdin_text is not None else subprocess.DEVNULL,
        text=True,
        env=env if env is not None else _child_env(),
        start_new_session=True,
    ) as proc:
        def _fire() -> None:
            timed_out.set()
            _kill_process_group(proc)

        watchdog = threading.Timer(timeout, _fire)
        watchdog.start()
        try:
            if stdin_text is not None:
                try:
                    proc.stdin.write(stdin_text)
                    proc.stdin.flush()
                except BrokenPipeError:
                    pass
                finally:
                    try:
                        proc.stdin.close()
                    except BrokenPipeError:
                        pass
            for line in proc.stdout:
                on_line(strip_ansi(line), "")
            proc.wait()
        finally:
            watchdog.cancel()
            if proc.poll() is None:          # loop exited early (e.g. an error)
                _kill_process_group(proc)

    if timed_out.is_set():
        raise TimeoutError(
            f"Command timed out after {timeout}s: {' '.join(str(c) for c in cmd)}"
        )
    if proc.returncode not in (0, None):
        raise RuntimeError(
            f"Command exited {proc.returncode}: {' '.join(str(c) for c in cmd)}"
        )


def _kill_process_group(proc: subprocess.Popen) -> None:
    """Kill *proc* and any children it spawned; never raises."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass


class WorkerHost:
    """Mixin for tabs that start QThread workers.

    Tracks every live worker so that (a) a second action cannot drop the last
    reference to a running QThread — which aborts with "QThread: Destroyed
    while thread is still running" — and (b) the window can wait for them on
    close instead of tearing the tab out from under them.
    """

    #: How long to wait for a worker to notice an interruption request.
    WORKER_STOP_MS = 5_000

    @property
    def _workers(self) -> set:
        # Created lazily so subclasses need no __init__ cooperation.
        if not hasattr(self, "_worker_set"):
            self._worker_set: set = set()
        return self._worker_set

    def track_worker(self, worker: QThread) -> QThread:
        """Register *worker* and drop it again once it finishes."""
        self._workers.add(worker)
        worker.finished.connect(lambda w=worker: self._workers.discard(w))
        worker.finished.connect(worker.deleteLater)
        return worker

    def live_workers(self) -> list:
        return [w for w in self._workers if w.isRunning()]

    def busy(self) -> bool:
        return bool(self.live_workers())

    def stop_workers(self) -> None:
        """Ask every live worker to stop, then wait briefly for each."""
        for worker in list(self._workers):
            if not worker.isRunning():
                continue
            worker.requestInterruption()
            worker.quit()
            worker.wait(self.WORKER_STOP_MS)


# ---------------------------------------------------------------------------
# Install worker (background thread)
# ---------------------------------------------------------------------------

class InstallWorker(QThread):
    """Executes all install steps sequentially in a background thread."""

    log    = pyqtSignal(str, str)   # (text, colour)
    done   = pyqtSignal(bool)       # success

    # Pipeline dependencies installed from the ESO/ivh mirrors, as pip
    # requirement strings (cf. UninstallWorker.PIPELINE_PACKAGES, which lists
    # bare distribution names).
    # TODO: ``pycpl`` is deliberately UNPINNED only while ivh's index churns —
    # re-pin it once that settles (0.4.4's 1.0.4.post6 pin was withdrawn there).
    PIP_REQUIREMENTS = [
        "pycpl",
        "edps",
        "pyesorex",
        "adari_core",
        "scopesim==0.11.3",
        "scopesim_templates==0.8.1",
    ]

    @classmethod
    def _pip_deps_command(cls) -> list[str]:
        """``pip install`` argv for the ESO-mirror pipeline dependencies."""
        # --upgrade is what makes the unpinned requirements actually track the
        # newest release: a bare ``pip install pycpl`` leaves an already-installed
        # older pycpl in place ("Requirement already satisfied").
        return [
            sys.executable, "-m", "pip", "install", "--upgrade",
            "--extra-index-url", PYCPL_INDEX,
            "--extra-index-url", ESO_INDEX,
            *cls.PIP_REQUIREMENTS,
        ]

    # ── public ──────────────────────────────────────────────────────────────

    def __init__(self, refs: dict[Path, str] | None = None,
                 force: set[Path] | None = None) -> None:
        """*refs* maps a clone target to a branch/tag/commit ("" = default).

        *force* is the set of targets whose local modifications the user has
        explicitly agreed to discard.  A **set**, not a single flag: confirming
        a reset of one clone must never authorise destroying the other.
        """
        super().__init__()
        self._refs = refs or {}
        self._force = force or set()

    def run(self) -> None:
        try:
            self._step(f"Cloning / updating METIS_Pipeline  →  {TARGET_A}")
            self._clone_or_update(REPO_A_URL, TARGET_A,
                                  self._refs.get(TARGET_A, ""),
                                  TARGET_A in self._force)

            self._step(f"Cloning / updating METIS_Simulations  →  {TARGET_B}")
            self._clone_or_update(REPO_B_URL, TARGET_B,
                                  self._refs.get(TARGET_B, ""),
                                  TARGET_B in self._force)

            self._check_layout()

            self._ensure_pip()

            self._step("Installing pipeline Python dependencies via pip…")
            recipe_dir = str(TARGET_A / "metisp" / "pyrecipes") + "/"
            os.environ["PYCPL_RECIPE_DIR"] = recipe_dir
            os.environ["PYESOREX_PLUGIN_DIR"] = recipe_dir
            # ESO-mirror dependencies are installed into the same interpreter
            # that hosts MTR (sys.executable points at pipx's isolated venv or
            # whatever venv the user installed MTR into).
            self._run(self._pip_deps_command(), cwd=REPO_ROOT)

            self._step("Installing pymetis (eso-pymetis, editable)…")
            # metiswise 0.0.4 (Archive tab) depends on ``eso-pymetis``, which is
            # just the pip build of the ``pymetis`` we already clone above.
            # Install the clone editable with --no-deps so that:
            #   (a) ``pymetis`` is importable in-process (metiswise imports it), and
            #   (b) the Archive-tab metiswise install finds ``eso-pymetis`` already
            #       satisfied — avoiding a 2nd pymetis copy and the clash between
            #       eso-pymetis's pycpl==1.0.3.post4 pin and the newer pycpl the
            #       step above just resolved.
            # --no-deps keeps the pycpl/edps/pyesorex versions installed above
            # authoritative.
            # TODO: remove/revisit if metiswise drops the eso-pymetis dependency
            # or eso-pymetis stops pinning pycpl.
            self._run(
                [sys.executable, "-m", "pip", "install", "--editable",
                 str(TARGET_A / "metisp" / "pymetis"), "--no-deps"],
                cwd=REPO_ROOT,
            )

            self._step("Installing METIS_Simulations (editable)…")
            # METIS_Simulations is cloned at install time (above) and pip-
            # installed editable so changes to the working tree are picked up
            # without a re-install.
            self._run(
                [sys.executable, "-m", "pip", "install", "--editable", str(TARGET_B)],
                cwd=REPO_ROOT,
            )

            self._step("Checking for existing EDPS configuration…")
            self._backup_edps_config()

            self._step("Initialising EDPS…")
            self._init_edps()

            self._step("Patching ~/.edps/application.properties…")
            self._patch_edps_config()

            self.log.emit("\n✓ Installation complete.\n", "green")
            self.done.emit(True)
        except Exception as exc:
            _log_exception(self.log, "Failed", exc)
            self.done.emit(False)

    # ── private helpers ──────────────────────────────────────────────────────

    def _step(self, msg: str) -> None:
        self.log.emit(f"\n── {msg}\n", "cyan")

    def _ensure_pip(self) -> None:
        """Bootstrap pip into MTR's interpreter if it is missing (pipx venvs)."""
        boot = ensurepip_command_if_needed()
        if boot:
            self._step("Bootstrapping pip (pipx app venvs ship without it)…")
            self._run(boot)

    def _run(self, cmd: list, cwd: Path | None = None,
             stdin_text: str | None = None, timeout: int = 300) -> None:
        stream_subprocess(
            cmd, on_line=self.log.emit, cwd=cwd,
            stdin_text=stdin_text, timeout=timeout,
        )

    def _clone_or_update(self, url: str, target: Path,
                         ref: str = "", force: bool = False) -> None:
        """Put *target* on *ref* ("" = the remote's default branch).

        *force* permits discarding uncommitted local changes; the GUI collects
        that confirmation per-repo before the worker starts, and this method
        re-checks rather than trusting it, so it stays safe to call directly.
        """
        try:
            ref = _validate_ref(ref)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc

        # .git can be a directory (normal clone) OR a file pointing at
        # .git/modules/<name>/ (submodule or worktree checkout); both are
        # valid git repos.
        is_repo = (target / ".git").exists()

        # Hoisted above the ref dispatch: the pinned path runs `git init`,
        # which would otherwise happily initialise over a non-empty foreign
        # directory. If the dir is empty (a common leftover from an aborted
        # install) we can safely use it. Otherwise refuse — silently skipping
        # would leave the rest of the install referencing a bad checkout.
        if not is_repo and target.is_dir() and any(target.iterdir()):
            raise RuntimeError(
                f"{target} exists but is not a git repo and is not empty. "
                f"Remove or rename it and re-run the install."
            )

        if ref:
            self._checkout_ref(url, target, ref, force)
        elif is_repo:
            self._update_default_branch(url, target, force)
        else:
            self._run(["git", "clone", "--depth", "1", url, str(target)])

        self._log_head(target)

    # ── git plumbing ─────────────────────────────────────────────────────────

    def _checkout_ref(self, url: str, target: Path, ref: str,
                      force: bool) -> None:
        """Move *target* onto *ref*, cloning from scratch if need be."""
        if not (target / ".git").exists():
            # `init` + `remote add` + `fetch <ref>` reaches an arbitrary commit,
            # which `git clone --branch` cannot; it also collapses the "no
            # clone yet" and "clone exists" cases into one path.
            self._run(["git", "init", str(target)])
            self._run(["git", "-C", str(target), "remote", "add", "origin", url])
        else:
            self._check_origin(target, url)

        # An abbreviated SHA cannot be fetched by name, so don't pay for a
        # round trip that is guaranteed to fail.
        ok = (not _looks_like_abbrev_sha(ref)
              and self._try_fetch(target, ref, depth=1))

        if ok:
            # FETCH_HEAD is only meaningful after a *successful* fetch (a failed
            # one truncates it), so the checkout happens here and nowhere else.
            want = _git(["-C", str(target), "rev-parse", "FETCH_HEAD"]).stdout.strip()
            if self._already_at(target, ref, want):
                return
            self._make_room(target, force)
            if self._has_remote_branch(target, ref):
                # A branch fetch also creates refs/remotes/origin/<ref>, so this
                # tells branch from tag/commit with no extra network call.
                self._run(["git", "-C", str(target), "checkout", "-f", "-B",
                           ref, "FETCH_HEAD"])
                _git(["-C", str(target), "branch", "--set-upstream-to",
                      f"origin/{ref}", ref])
            else:
                self._run(["git", "-C", str(target), "checkout", "-f",
                           "--detach", "FETCH_HEAD"])
            return

        # Fallback: an abbreviated SHA, or a commit that is not at any ref tip.
        # Only a full-history fetch can resolve those locally — and only a hex
        # commit id ever needs one, so a mistyped branch name fails immediately
        # instead of downloading the whole history first.
        if not re.fullmatch(r"[0-9a-fA-F]{4,40}", ref):
            raise RuntimeError(
                f"'{ref}' is not a branch or tag in {url}. Check the spelling, "
                f"or use the ↻ button to reload the ref list."
            )
        self.log.emit(
            f"'{ref}' cannot be fetched directly — falling back to a full "
            f"history fetch (slow; paste the full 40-character SHA to avoid "
            f"this).\n",
            "yellow",
        )
        self._deepen(target)
        self._run(["git", "-C", str(target), "fetch", "--tags", "--force",
                   "origin"], timeout=900)
        self._checkout_local(target, url, ref, force)

    def _update_default_branch(self, url: str, target: Path,
                               force: bool) -> None:
        """Blank ref: fast-forward the checked-out branch, as MTR always has."""
        self._run(["git", "-C", str(target), "fetch", "--all", "--prune"])

        branch = self._current_branch(target)
        if branch is None:
            # Detached HEAD left behind by an earlier pinned install. There is
            # no upstream to fast-forward, and blank means "back to normal".
            default = self._remote_default_branch(url) or "HEAD"
            self.log.emit(
                f"Clone is on a detached HEAD; returning to the remote's "
                f"default branch ({default}).\n",
                "yellow",
            )
            self._checkout_ref(url, target, default, force)
            return

        cp = _git(["-C", str(target), "pull", "--ff-only"])
        self.log.emit(cp.stdout + cp.stderr, "")
        if cp.returncode == 0:
            return
        if not force:
            raise RuntimeError(
                f"'git pull --ff-only' failed in {target} (see above). If you "
                f"have local commits, push or drop them; if you have local "
                f"edits, re-run the install and confirm the reset when asked."
            )
        self._make_room(target, force)
        self._run(["git", "-C", str(target), "pull", "--ff-only"])

    def _make_room(self, target: Path, force: bool) -> None:
        """Discard local modifications so a checkout can move HEAD."""
        if not (target / ".git").exists() or not _dirty_files(target):
            return
        if not force:
            raise RuntimeError(
                f"{target} has uncommitted changes and the reset was not "
                f"confirmed. Re-run the install and confirm when asked."
            )
        self._run(["git", "-C", str(target), "reset", "--hard"])
        # No -x: gitignored build artefacts, simulation *.fits output and
        # inst_pkgs/ are user data and must survive. Single -f also makes git
        # refuse to recurse into a nested repository.
        self._run(["git", "-C", str(target), "clean", "-fd"])

    def _try_fetch(self, target: Path, ref: str, depth: int = 1) -> bool:
        """Fetch exactly *ref*; return False instead of raising on failure."""
        args = ["-C", str(target), "fetch"]
        if depth:
            args += ["--depth", str(depth)]
        args += ["--end-of-options", "origin", ref]
        self.log.emit(f"$ git {' '.join(args)}\n", "")
        cp = _git(args, timeout=600)
        self.log.emit(cp.stdout + cp.stderr, "")
        return cp.returncode == 0

    def _deepen(self, target: Path) -> None:
        """Un-shallow *target*, but only if it actually is shallow."""
        # `fetch --unshallow` errors out on an already-complete repo, and a
        # freshly `git init`ed one is not shallow either.
        if _git(["-C", str(target), "rev-parse",
                 "--is-shallow-repository"]).stdout.strip() == "true":
            self._run(["git", "-C", str(target), "fetch", "--unshallow",
                       "origin"], timeout=900)

    def _has_remote_branch(self, target: Path, ref: str) -> bool:
        return _git(["-C", str(target), "rev-parse", "--verify", "--quiet",
                     f"refs/remotes/origin/{ref}"]).returncode == 0

    def _already_at(self, target: Path, ref: str, want: str) -> bool:
        """True when the checkout already matches, so nothing need be touched."""
        if not want:
            return False
        head = _git(["-C", str(target), "rev-parse", "HEAD"]).stdout.strip()
        if head != want:
            return False
        branch = self._current_branch(target)
        if self._has_remote_branch(target, ref) and branch != ref:
            return False
        self.log.emit(
            f"Already at {want[:8]}; working tree left untouched.\n", "")
        return True

    def _checkout_local(self, target: Path, url: str, ref: str,
                        force: bool) -> None:
        """Check out a ref that is already present in the local object store."""
        # ^{commit} peels annotated tags and rejects a ref naming a tree/blob.
        cp = _git(["-C", str(target), "rev-parse", "--verify", "--quiet",
                   "--end-of-options", f"{ref}^{{commit}}"])
        if cp.returncode != 0:
            raise RuntimeError(
                f"'{ref}' is not a branch, tag or commit in {url}. Check the "
                f"spelling, or use the ↻ button to reload the ref list."
            )
        want = cp.stdout.strip()
        if self._already_at(target, ref, want):
            return
        self._make_room(target, force)
        if self._has_remote_branch(target, ref):
            self._run(["git", "-C", str(target), "checkout", "-f", "-B", ref,
                       f"origin/{ref}"])
        else:
            self._run(["git", "-C", str(target), "checkout", "-f", "--detach",
                       want])

    def _check_origin(self, target: Path, url: str) -> None:
        """Warn — never fail — when the clone points somewhere unexpected.

        Developers on these repos legitimately point the clone at their own
        fork, which is the whole reason this feature exists, so a mismatch is
        reported and honoured rather than "corrected".
        """
        cp = _git(["-C", str(target), "remote", "get-url", "origin"])
        if cp.returncode != 0:
            self._run(["git", "-C", str(target), "remote", "add", "origin", url])
            return
        have = cp.stdout.strip()
        if not _same_remote(have, url):
            self.log.emit(
                f"⚠ {target.name}: origin is {have}, not {url}. Fetching the "
                f"requested ref from that remote instead.\n",
                "yellow",
            )

    def _current_branch(self, target: Path) -> str | None:
        """Checked-out branch name, or None when HEAD is detached."""
        cp = _git(["-C", str(target), "symbolic-ref", "--quiet", "--short",
                   "HEAD"])
        return cp.stdout.strip() if cp.returncode == 0 else None

    def _remote_default_branch(self, url: str) -> str | None:
        cp = _git(["ls-remote", "--symref", "--end-of-options", url, "HEAD"],
                  timeout=20)
        for line in cp.stdout.splitlines():
            if line.startswith("ref: refs/heads/"):
                return line[len("ref: refs/heads/"):].split("\t")[0].strip()
        return None

    def _log_head(self, target: Path) -> None:
        """Log exactly which commit landed, so it can be quoted in a bug report."""
        if not (target / ".git").exists():
            return
        self.log.emit(f"→ {target.name}: {_describe_head(target)}\n", "green")
        cp = _git(["-C", str(target), "log", "-1", "--format=%H  %cs  %s"])
        if cp.returncode == 0:
            self.log.emit(f"   {cp.stdout.strip()}\n", "")

    def _check_layout(self) -> None:
        """Fail early when the selected refs predate the current repo layout.

        Without this, an old ref sails through checkout and then either dies
        inside `pip install --editable` with an opaque backend traceback, or —
        worse — silently yields a pipeline with zero recipes because
        PYCPL_RECIPE_DIR points at a directory that does not exist.
        """
        for needed in (TARGET_A / "metisp" / "pymetis" / "pyproject.toml",
                       TARGET_A / "metisp" / "pyrecipes",
                       TARGET_B / "pyproject.toml"):
            if not needed.exists():
                raise RuntimeError(
                    f"{needed} does not exist at the selected ref. That ref "
                    f"probably predates the current repository layout — pick "
                    f"a newer one."
                )

    def _backup_edps_config(self) -> None:
        """Back up an existing application.properties, once.

        Only the *first* backup is the user's own file: on a re-install the
        file in place is MTR's own, so overwriting the backup with it would
        destroy the original for good (and Uninstall would then "restore"
        MTR's config as if it were theirs).
        """
        props = Path.home() / ".edps" / "application.properties"
        if not props.exists():
            return
        backup = props.with_name("application.properties_backup")
        if backup.exists():
            props.unlink()
            self.log.emit(
                f"{backup} already exists — keeping the original backup "
                "and discarding the current config\n",
                "yellow",
            )
        else:
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
        edps_bin = Path(sys.executable).parent / "edps"
        base = [str(edps_bin), "-P", "4444"]
        try:
            self._run(base, cwd=REPO_ROOT, stdin_text="\n", timeout=60)
        finally:
            # Guarded: if edps is missing, the try raises FileNotFoundError and
            # an unguarded stop here would raise a *second* one that replaces
            # it — the user would see the stop command's error, not the cause.
            try:
                subprocess.run(base + ["-s"], cwd=str(REPO_ROOT),
                               capture_output=True, timeout=15,
                               env=_child_env())
            except Exception as exc:
                self.log.emit(f"(could not stop the EDPS server: {exc})\n",
                              "yellow")

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
            # Hardlink products into the per-run output dir instead of copying
            # them, so they don't consume disk twice (once in the EDPS working
            # store ~/EDPS_data, once under the run's output folder). Requires
            # both to be on the same filesystem (true by default — both under
            # $HOME); use "symlink" instead if the output dir is on another mount.
            "mode": (r"^mode=.*", "mode=link"),
            # Wipe EDPS bookkeeping on every server startup. Without this, a
            # stale db.json entry whose on-disk outputs have been removed will
            # collide with a fresh submission's deterministic job UUID and make
            # EDPS short-circuit the run with a FileNotFoundError.
            "truncate": (r"^truncate=.*", "truncate=True"),
        }
        for key, (pattern, replacement) in patches.items():
            # A *function* replacement, because re.subn interprets backslashes
            # and \g<...> in a string replacement: a data dir containing a
            # backslash would corrupt the config or raise re.error. The paths
            # here come from METIS_DATA_DIR, which is arbitrary user input.
            text, count = re.subn(
                pattern, lambda _m, r=replacement: r, text, flags=re.MULTILINE,
            )
            if count == 0:
                raise RuntimeError(
                    f"{props} has no '{key}=' line to patch — EDPS config "
                    f"format may have changed; re-run EDPS initialisation."
                )
        paths.write_text_atomic(props, text)
        self.log.emit(f"Patched {props}\n", "")


# ---------------------------------------------------------------------------
# Uninstall worker (background thread)
# ---------------------------------------------------------------------------

class UninstallWorker(QThread):
    """Reverses every change made by InstallWorker (and the Archive-tab
    MetisWISE install).

    Steps: pip-uninstall the installed packages, delete the whole user data
    directory, restore or remove the EDPS configuration, and clear the stored
    archive credentials from the OS keyring.

    Each step is isolated in its own ``try/except`` so a single failure (e.g. a
    keyring backend that is unavailable, or a directory that is busy) is logged
    but does not abort the remaining steps.  ``done(False)`` is emitted if any
    step failed.
    """

    log  = pyqtSignal(str, str)   # (text, colour)
    done = pyqtSignal(bool)       # success (False if any step failed)

    # Top-level packages the Install tab installs: the pipeline deps plus the
    # two editable installs, by their distribution names (the pymetis clone
    # registers as ``eso-pymetis``; METIS_Simulations as ``metis_simulations``).
    PIPELINE_PACKAGES = [
        "pycpl", "edps", "pyesorex", "adari_core",
        "scopesim", "scopesim_templates",
        "eso-pymetis", "metis_simulations",
    ]

    def run(self) -> None:
        from . import credentials as credstore

        # _METISWISE_RUNTIME_DEPS is the single source of truth for what the
        # Archive tab installs; reuse it so the two lists never drift apart.
        from .archive import _METISWISE_RUNTIME_DEPS

        ok = True

        # ── pip uninstall ─────────────────────────────────────────────────
        self._step("Uninstalling Python packages via pip…")
        self.log.emit(
            "Only the explicitly-installed packages are removed; their "
            "transitive sub-dependencies are left in place.\n", "yellow",
        )
        packages = [*self.PIPELINE_PACKAGES, "metiswise", *_METISWISE_RUNTIME_DEPS]
        try:
            # pipx venvs have no pip; bootstrap it so the uninstall can run
            # (the packages live in the venv even when pip itself is absent).
            self._ensure_pip()
            # pip exits 0 for not-installed names ("WARNING: Skipping …"), so a
            # single call is safe whether or not MetisWISE was ever installed.
            self._run([sys.executable, "-m", "pip", "uninstall", "-y", *packages])
        except Exception as exc:
            ok = False
            self.log.emit(f"✗ pip uninstall failed: {exc}\n", "red")

        # ── delete the user data directory ────────────────────────────────
        self._step(f"Removing the METIS data directory  →  {REPO_ROOT}")
        try:
            self._remove_data_dir()
        except Exception as exc:
            ok = False
            self.log.emit(f"✗ Failed to remove data directory: {exc}\n", "red")

        # ── restore / remove EDPS configuration ───────────────────────────
        self._step("Cleaning up EDPS configuration…")
        try:
            self._cleanup_edps()
        except Exception as exc:
            ok = False
            self.log.emit(f"✗ EDPS cleanup failed: {exc}\n", "red")

        # ── clear keyring credentials ─────────────────────────────────────
        self._step("Clearing stored archive credentials…")
        for label, deleter in (
            ("OmegaCEN pip", credstore.delete_pip_credentials),
            ("archive DB",   credstore.delete_db_credentials),
        ):
            try:
                deleter()
                self.log.emit(f"Removed {label} credentials from the keyring.\n", "")
            except credstore.CredentialsUnavailable as exc:
                # A missing keyring backend is not fatal — there is simply
                # nothing persisted to clear.
                self.log.emit(
                    f"Could not clear {label} credentials: {exc}\n", "yellow",
                )

        if ok:
            self.log.emit("\n✓ Uninstall complete.\n", "green")
        else:
            self.log.emit(
                "\n⚠ Uninstall finished with errors (see above).\n", "yellow",
            )
        self.done.emit(ok)

    # ── private helpers ───────────────────────────────────────────────────

    def _step(self, msg: str) -> None:
        self.log.emit(f"\n── {msg}\n", "cyan")

    def _ensure_pip(self) -> None:
        """Bootstrap pip into MTR's interpreter if it is missing (pipx venvs)."""
        boot = ensurepip_command_if_needed()
        if boot:
            self._step("Bootstrapping pip (pipx app venvs ship without it)…")
            self._run(boot)

    def _run(self, cmd: list, timeout: int = 300) -> None:
        stream_subprocess(cmd, on_line=self.log.emit, timeout=timeout)

    def _remove_data_dir(self) -> None:
        """Delete the whole user data dir, plus an externally-relocated
        simulations clone if one exists outside the data dir."""
        targets = [REPO_ROOT]
        # If METIS_SIMULATIONS_DIR points outside the data dir, removing
        # REPO_ROOT won't catch the clone — remove it explicitly.
        if TARGET_B != REPO_ROOT and REPO_ROOT not in TARGET_B.parents:
            targets.append(TARGET_B)
        for target in targets:
            if not target.exists():
                self.log.emit(f"{target} does not exist — nothing to remove.\n", "")
                continue
            try:
                _assert_safe_to_remove(target)
            except RuntimeError as exc:
                self.log.emit(f"✗ {exc}\n", "red")
                continue
            failures: list[str] = []
            shutil.rmtree(
                target,
                onexc=lambda _f, path, exc, _acc=failures: _acc.append(
                    f"{path}: {exc}"),
            )
            if failures:
                self.log.emit(
                    f"✗ Could not fully remove {target} "
                    f"({len(failures)} item(s) left):\n", "red",
                )
                for line in failures[:10]:
                    self.log.emit(f"    {line}\n", "red")
            else:
                self.log.emit(f"Removed {target}\n", "")

    def _cleanup_edps(self) -> None:
        """If the install backed up a pre-existing config, restore it; otherwise
        the install created the whole EDPS state, so remove it entirely
        (config dir + the base_dir bookkeeping/data directory)."""
        edps_dir = Path.home() / ".edps"
        props = edps_dir / "application.properties"
        backup = props.with_name("application.properties_backup")
        if backup.exists():
            if props.exists():
                props.unlink()
            backup.rename(props)
            self.log.emit(f"Restored original {props} from backup.\n", "green")
            return
        # No backup → install owns everything here. Resolve the bookkeeping dir
        # from the config *before* deleting it.
        base_dir = self._edps_base_dir(props)
        for target in (edps_dir, base_dir):
            if target.exists():
                shutil.rmtree(target)
                self.log.emit(f"Removed {target}\n", "")
            else:
                self.log.emit(f"{target} does not exist — nothing to remove.\n", "")

    @staticmethod
    def _edps_base_dir(props: Path) -> Path:
        """Read ``base_dir=`` from the EDPS config; fall back to ~/EDPS_data."""
        default = Path.home() / "EDPS_data"
        if not props.exists():
            return default
        for line in props.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith("base_dir="):
                value = stripped.split("=", 1)[1].strip()
                if value:
                    return Path(value).expanduser()
        return default


def _resolve_run_metis_command() -> list[str]:
    """Build the `python -m metis_test_runner.run_metis` invocation list."""
    return [sys.executable, "-u", "-m", "metis_test_runner.run_metis"]


# ---------------------------------------------------------------------------
# Ref combo box
# ---------------------------------------------------------------------------

class RefComboBox(QComboBox):
    """Editable combo that also opens its list when the text field is clicked.

    A plain editable QComboBox only opens its popup from the arrow button — a
    click in the text area just places the cursor — which makes the list feel
    unreachable by mouse, unlike every non-editable combo in the app.

    So a click opens the list whenever the field holds nothing custom: blank
    (the default) or a value that came from the list itself.  Once something
    hand-typed is in there — a commit SHA — clicks place the cursor instead, so
    it stays editable.  The native arrow always opens the list either way.
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setEditable(True)
        # Enter must not permanently append a typed SHA to the dropdown.
        self.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.lineEdit().installEventFilter(self)
        if self.completer() is not None:
            self.completer().setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
            self.completer().setFilterMode(Qt.MatchFlag.MatchContains)

    def _holds_custom_text(self) -> bool:
        text = self.currentText().strip()
        return bool(text) and self.findText(text) < 0

    def eventFilter(self, obj, event) -> bool:
        if (obj is self.lineEdit() and self.count()
                and not self._holds_custom_text()):
            # Open on RELEASE, not press. Showing the popup from the press
            # handler leaves the matching release to land on the freshly-shown
            # list, which reads it as "released over an item" and closes again
            # immediately — the popup would only survive while the button was
            # held down. Swallowing both events keeps it open on a normal click.
            if event.type() == QEvent.Type.MouseButtonPress:
                return True
            if event.type() == QEvent.Type.MouseButtonRelease:
                if not self.view().isVisible():
                    self.showPopup()
                return True
        return super().eventFilter(obj, event)


# ---------------------------------------------------------------------------
# Ref discovery (background thread)
# ---------------------------------------------------------------------------

class RefWorker(QThread):
    """Resolve one repo's local HEAD and its remote branch/tag list.

    The local part is instant and always succeeds; the ``ls-remote`` is a
    network call that is allowed to fail — the ref combo stays usable as a
    plain text field when offline.
    """

    status = pyqtSignal(str, str)    # (key, "main @ d2d257c5")  — local, instant
    refs   = pyqtSignal(str, list)   # (key, ["main", "v0.4.2", …])
    failed = pyqtSignal(str, str)    # (key, reason) — status line only, never modal

    def __init__(self, key: str, url: str, target: Path) -> None:
        super().__init__()
        self._key, self._url, self._target = key, url, target

    def run(self) -> None:
        self.status.emit(self._key, _describe_head(self._target))
        cp = _git(["ls-remote", "--heads", "--tags", "--end-of-options",
                   self._url], timeout=20)
        if cp.returncode != 0:
            reason = (cp.stderr.strip().splitlines()
                      or ["git ls-remote failed"])[-1]
            self.failed.emit(self._key, reason)
            return
        self.refs.emit(self._key, _parse_ls_remote(cp.stdout))


# ---------------------------------------------------------------------------
# Install tab
# ---------------------------------------------------------------------------

class InstallTab(WorkerHost, QWidget):

    # (settings key, label, repo URL, clone target)
    REPOS = (
        ("pipeline_ref", "METIS_Pipeline", REPO_A_URL, TARGET_A),
        ("simulations_ref", "METIS_Simulations", REPO_B_URL, TARGET_B),
    )

    def __init__(self) -> None:
        super().__init__()
        self._worker: InstallWorker | UninstallWorker | None = None
        self._settings = QSettings("METIS", "TestRunner")
        # Held so Python doesn't garbage-collect a running QThread mid-flight.
        self._ref_workers: dict[str, RefWorker] = {}
        self._refs_loaded: set[str] = set()
        self._last_action = ""
        self._build_ui()
        self._load_settings()

    def showEvent(self, event) -> None:
        super().showEvent(event)
        # Unlike ArchiveTab.showEvent this is a NETWORK probe, so it runs once
        # per session rather than on every tab switch. Deferred via a timer
        # because MainWindow.__init__ selects this tab during construction when
        # nothing is installed — we must not delay the first paint.
        QTimer.singleShot(0, lambda: self._refresh_refs(only_missing=True))

    # ── UI construction ─────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(14)
        layout.setContentsMargins(20, 20, 20, 20)

        desc = QLabel(
            "<b>METIS Pipeline Installation</b><br><br>"
            "Skip this tab if you have already installed the pipeline via one of "
            "these methods and go straight to <b>Run</b>:<br>"
            "<ul>"
            "<li><b>Bare-metal / ESO docs</b> — select runner <i>native</i></li>"
            "<li><b>Pipeline container</b> — select runner <i>docker</i> or "
            "<i>podman</i> and enter the container name</li>"
            "</ul>"
            "Otherwise, clicking <i>Install / Update</i> will perform the following "
            "steps:<br>"
            "<ol>"
            f"<li>Clone or update <b>METIS_Pipeline</b> and <b>METIS_Simulations</b> "
            f"into <code>{REPO_ROOT}</code></li>"
            "<li>Install all Python dependencies (pycpl, edps, pyesorex, adari_core, "
            "scopesim, scopesim_templates) into MTR's own pipx/venv</li>"
            "<li>Initialise and configure EDPS on port 4444</li>"
            "</ol>"
            "Leave the version fields blank and re-running is safe — existing "
            "repositories are fast-forwarded, not re-cloned."
        )
        desc.setWordWrap(True)
        desc.setTextFormat(Qt.TextFormat.RichText)
        layout.addWidget(desc)

        layout.addWidget(self._build_version_group())

        btn_row = QHBoxLayout()

        self.install_btn = QPushButton("Install / Update")
        self.install_btn.setProperty("role", "success")
        self.install_btn.setMinimumHeight(36)
        self.install_btn.setMaximumWidth(200)
        self.install_btn.clicked.connect(self._start)
        btn_row.addWidget(self.install_btn)

        self.uninstall_btn = QPushButton("Uninstall")
        self.uninstall_btn.setProperty("role", "danger")
        self.uninstall_btn.setMinimumHeight(36)
        self.uninstall_btn.setMaximumWidth(200)
        self.uninstall_btn.clicked.connect(self._uninstall)
        btn_row.addWidget(self.uninstall_btn)

        btn_row.addStretch()
        layout.addLayout(btn_row)

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Monospace", 9))
        layout.addWidget(self.log_view, stretch=1)

    def _build_version_group(self) -> QGroupBox:
        grp = QGroupBox("Repository version (advanced)")
        lay = QVBoxLayout(grp)

        hint = QLabel(
            "Leave blank to track each repository's default branch. Developers "
            "can pick a branch or tag from the list, or paste a full "
            "40-character commit SHA. An existing clone is <b>overwritten</b> "
            "with the selection."
        )
        hint.setWordWrap(True)
        hint.setTextFormat(Qt.TextFormat.RichText)
        hint.setProperty("hint", "note")
        lay.addWidget(hint)

        self.ref_combos: dict[str, RefComboBox] = {}
        self.ref_status: dict[str, QLabel] = {}
        self.ref_buttons: dict[str, QPushButton] = {}

        for key, label, _url, _target in self.REPOS:
            combo = RefComboBox()
            combo.lineEdit().setPlaceholderText("default branch")
            self.ref_combos[key] = combo

            btn = QPushButton("↻")
            btn.setProperty("role", "info")
            btn.setToolTip("Reload branches and tags")
            btn.setMaximumWidth(40)
            btn.clicked.connect(lambda _checked=False, k=key: self._reload(k))
            self.ref_buttons[key] = btn

            # Kept short: _labeled pins the label column at LABEL_W, and
            # "(branch / tag / commit)" overflows it. The hint above says it.
            lay.addWidget(_labeled(f"{label} version:", combo, btn))

            status = QLabel("currently: …")
            status.setProperty("hint", "true")
            status.setIndent(LABEL_W)
            self.ref_status[key] = status
            lay.addWidget(status)

        return grp

    # ── ref discovery ────────────────────────────────────────────────────────

    def _refresh_refs(self, only_missing: bool = False) -> None:
        if SMOKE_TEST or not shutil.which("git"):
            return
        for key, _label, url, target in self.REPOS:
            if only_missing and key in self._refs_loaded:
                continue
            if key in self._ref_workers:
                continue
            self._refs_loaded.add(key)
            self.ref_buttons[key].setEnabled(False)
            w = RefWorker(key, url, target)
            w.status.connect(self._on_ref_status)
            w.refs.connect(self._on_refs)
            w.failed.connect(self._on_ref_failed)
            w.finished.connect(lambda k=key: self._ref_worker_done(k))
            self._ref_workers[key] = w
            w.start()

    def _reload(self, key: str) -> None:
        self._refs_loaded.discard(key)
        self._refresh_refs(only_missing=True)

    def _ref_worker_done(self, key: str) -> None:
        self._ref_workers.pop(key, None)
        self.ref_buttons[key].setEnabled(True)

    def _on_ref_status(self, key: str, text: str) -> None:
        self.ref_status[key].setText(f"currently: {text}")

    def _on_refs(self, key: str, items: list) -> None:
        self._populate(self.ref_combos[key], items)

    def _on_ref_failed(self, key: str, reason: str) -> None:
        # Never modal, and never in log_view — that widget is the install
        # transcript, and pre-install noise there is confusing.
        lbl = self.ref_status[key]
        lbl.setText(f"{lbl.text()}   (ref list unavailable — offline?)")
        self.ref_combos[key].setToolTip(reason)

    @staticmethod
    def _populate(combo: QComboBox, items: list) -> None:
        """Refill the dropdown without disturbing what the user has typed."""
        edit = combo.lineEdit()
        typed, focused = combo.currentText(), edit.hasFocus()
        pos = edit.cursorPosition()
        # addItems() on an editable combo with currentIndex == -1 silently
        # snaps the line edit to items[0], so the text must be restored.
        combo.blockSignals(True)
        combo.clear()
        combo.addItems([str(i) for i in items])
        combo.setCurrentText(typed)
        combo.blockSignals(False)
        if focused:
            edit.setCursorPosition(min(pos, len(typed)))

    def _refresh_status_labels(self) -> None:
        for key, _label, _url, target in self.REPOS:
            self.ref_status[key].setText(f"currently: {_describe_head(target)}")

    # ── settings ─────────────────────────────────────────────────────────────

    def _load_settings(self) -> None:
        for key, _label, _url, _target in self.REPOS:
            self.ref_combos[key].setCurrentText(
                self._settings.value(f"install/{key}", "", type=str))

    def _save_settings(self) -> None:
        for key, _label, _url, _target in self.REPOS:
            self._settings.setValue(f"install/{key}",
                                    self.ref_combos[key].currentText().strip())

    def stop_ref_workers(self) -> None:
        """Let a pending ls-remote finish before the window goes away."""
        for w in list(self._ref_workers.values()):
            w.quit()
            w.wait(2000)

    # ── actions ──────────────────────────────────────────────────────────────

    def _start(self) -> None:
        try:
            refs = {target: _validate_ref(self.ref_combos[key].currentText())
                    for key, _label, _url, target in self.REPOS}
        except ValueError as exc:
            QMessageBox.warning(self, "Invalid ref", str(exc))
            return

        force = self._confirm_discard()
        if force is None:
            return

        self._save_settings()
        self._last_action = "install"
        self.log_view.clear()
        self.install_btn.setEnabled(False)
        self.uninstall_btn.setEnabled(False)
        self._worker = self.track_worker(InstallWorker(refs=refs, force=force))
        self._worker.log.connect(lambda text, color: log_append(self.log_view, text, color))
        self._worker.done.connect(self._on_done)
        self._worker.start()

    def _confirm_discard(self) -> set | None:
        """Targets whose local changes may be discarded, or None to abort.

        Checked for every dirty clone unconditionally — `checkout -f` discards
        tracked edits whether or not the selected ref actually changed, so
        gating this on a changed ref would destroy work silently.
        """
        force: set[Path] = set()
        for key, label, _url, target in self.REPOS:
            try:
                entries = _dirty_files(target)
            except RuntimeError as exc:
                # Assuming "clean" here risks silent data loss, so ask.
                if QMessageBox.question(
                    self, "Could not check for local changes",
                    f"Could not determine whether {label} ({target}) has local "
                    f"changes:\n\n{exc}\n\nContinue anyway?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No,
                ) != QMessageBox.StandardButton.Yes:
                    return None
                continue
            if not entries:
                continue
            if QMessageBox.question(
                self, "Discard local changes?",
                self._discard_text(label, target, entries),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            ) != QMessageBox.StandardButton.Yes:
                return None
            force.add(target)
        return force

    @staticmethod
    def _discard_text(label: str, target: Path, entries: list) -> str:
        modified = [e for e in entries if not e.startswith("?")]
        untracked = [e for e in entries if e.startswith("?")]
        parts = [f"{label} ({target}) has uncommitted changes.\n"]
        for title, group in (("Will be reverted:", modified),
                             ("Will be deleted:", untracked)):
            if not group:
                continue
            parts.append(title)
            parts.extend(f"  {e}" for e in group[:20])
            if len(group) > 20:
                parts.append(f"  … and {len(group) - 20} more")
            parts.append("")
        parts.append(
            "Ignored build artefacts (__pycache__, build/, *.fits, inst_pkgs/) "
            "are kept.\n"
            "These changes are discarded only if the checkout has to move.\n\n"
            "Continue?"
        )
        return "\n".join(parts)

    def _uninstall(self) -> None:
        reply = QMessageBox.question(
            self, "Confirm uninstall",
            "This will permanently:\n"
            "• pip-uninstall all pipeline and MetisWISE packages\n"
            f"• delete the entire data directory ({REPO_ROOT})\n"
            "• restore or remove the EDPS configuration\n"
            "• delete stored archive credentials from the OS keyring\n\n"
            "Transitive sub-dependencies are not removed. Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._last_action = "uninstall"
        self.log_view.clear()
        self.install_btn.setEnabled(False)
        self.uninstall_btn.setEnabled(False)
        self._worker = self.track_worker(UninstallWorker())
        self._worker.log.connect(lambda text, color: log_append(self.log_view, text, color))
        self._worker.done.connect(self._on_done)
        self._worker.start()

    def _on_done(self, success: bool) -> None:
        self.install_btn.setEnabled(True)
        self.uninstall_btn.setEnabled(True)
        if self._last_action == "uninstall" and success:
            # The clones are gone; leaving the pins behind would silently
            # re-pin the next install to a long-forgotten ref.
            for key, _label, _url, _target in self.REPOS:
                self.ref_combos[key].setCurrentText("")
                self._settings.remove(f"install/{key}")
        # HEAD just moved (or the clones vanished), so the labels are stale.
        self._refresh_status_labels()


# ---------------------------------------------------------------------------
# Archive workers (background threads)
# ---------------------------------------------------------------------------

class MetisWISEInstallWorker(QThread):
    """Install MetisWISE into MTR's venv via `sys.executable -m pip install`."""

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)
    # Emitted when neither the field nor the keyring provides credentials;
    # the slot runs on the main thread (queued connection) and shows a dialog.
    needs_input = pyqtSignal(str)

    def __init__(self, pip_credentials: str | None) -> None:
        """*pip_credentials* is the typed ``user:pass``, or None to use the
        entry stored in the OS keyring (resolved here, on the worker thread,
        so a blocking keyring-unlock prompt never freezes the UI)."""
        super().__init__()
        self._credentials = pip_credentials

    def run(self) -> None:
        from . import credentials as credstore
        from .archive import install_metiswise_command

        creds = self._credentials
        if creds is None:
            try:
                creds = credstore.get_pip_credentials()
            except credstore.CredentialsUnavailable:
                creds = None
            if not creds:
                self.needs_input.emit(
                    "No OmegaCEN credentials stored in the OS keyring — "
                    "enter username:password in the field above.",
                )
                self.done.emit(False)
                return
            self.log.emit("Using OmegaCEN credentials from keyring.\n", "")

        try:
            # install_metiswise_command returns a SEQUENCE of pip commands
            # (deps first, then metiswise --no-deps — see archive.py for why)
            # plus env overrides carrying the credentialed index URL, which
            # must stay out of argv (and out of this log).
            cmds, env_overrides = install_metiswise_command(creds)
            # pipx venvs ship without pip; bootstrap it first (no credentialed
            # index needed for ensurepip, so run it with the plain environment).
            boot = ensurepip_command_if_needed()
            if boot:
                cmds = [boot, *cmds]
            for cmd in cmds:
                self.log.emit(f"$ {' '.join(cmd)}\n", "cyan")
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, env=os.environ | env_overrides,
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
            if self._credentials is not None:
                # Persist typed credentials only after a successful install.
                try:
                    credstore.set_pip_credentials(self._credentials)
                    self.log.emit(
                        "Saved OmegaCEN credentials to the OS keyring.\n",
                        "green",
                    )
                except credstore.CredentialsUnavailable:
                    self.log.emit(
                        "Keyring unavailable — credentials kept for this "
                        "session only.\n", "yellow",
                    )
            self.log.emit("\n✓ MetisWISE installed successfully.\n", "green")
            self.done.emit(True)
        except Exception as exc:
            _log_exception(self.log, "Failed", exc)
            self.done.emit(False)


class TestConnectionWorker(QThread):
    """Probe the remote archive; on success store the fields in the keyring.

    Typed field values win; blanks are filled from the keyring entry (read
    here, on the worker thread, so an unlock prompt never freezes the UI).
    Nothing is persisted unless the connection probe succeeds.
    """

    log  = pyqtSignal(str, str)
    done = pyqtSignal(bool)
    # Emitted when fields are missing from both the form and the keyring;
    # the slot runs on the main thread (queued connection) and shows a dialog.
    needs_input = pyqtSignal(str)

    def __init__(self, fields: dict[str, str]) -> None:
        super().__init__()
        self._fields = fields

    def run(self) -> None:
        from . import credentials as credstore
        from .archive import (
            apply_db_credentials,
            query_archive,
            reset_db_connection,
            scrub_env_cfg,
        )

        fields = dict(self._fields)
        blanks = [k for k, v in fields.items() if not v]
        if blanks:
            try:
                stored = credstore.get_db_credentials() or {}
            except credstore.CredentialsUnavailable:
                stored = {}
            filled = [k for k in blanks if stored.get(k)]
            for k in filled:
                fields[k] = stored[k]
            if filled:
                self.log.emit(
                    f"Loaded {len(filled)} field(s) from keyring.\n", "",
                )
        missing = [k for k, v in fields.items() if not v]
        if missing:
            self.needs_input.emit(
                "These fields are neither filled in nor stored in the OS "
                "keyring:\n  " + "\n  ".join(missing),
            )
            self.done.emit(False)
            return

        try:
            apply_db_credentials(fields)
            reset_db_connection()
            self.log.emit("Probing archive connection…\n", "cyan")
            items = query_archive(
                on_log=lambda msg: self.log.emit(msg + "\n", ""),
            )
        except Exception as exc:
            # Nothing persisted on failure — keyring and legacy file untouched.
            _log_exception(self.log, "Connection failed", exc)
            self.done.emit(False)
            return

        try:
            credstore.set_db_credentials(fields)
            self.log.emit("Saved credentials to the OS keyring.\n", "green")
            if scrub_env_cfg():
                self.log.emit(
                    "Migrated: removed credentials from "
                    "~/.awe/Environment.cfg\n", "",
                )
        except credstore.CredentialsUnavailable:
            # Keep the legacy file intact — without a keyring backend it is
            # the only persistent store the user has.
            self.log.emit(
                "Keyring unavailable — credentials active for this session "
                "only (not saved).\n", "yellow",
            )
        self.log.emit(
            f"\n✓ Connected — {len(items)} item(s) visible.\n", "green",
        )
        self.done.emit(True)


class QueryWorker(QThread):
    """Query the archive for available files."""

    log     = pyqtSignal(str, str)
    results = pyqtSignal(list)
    done    = pyqtSignal(bool)

    def __init__(self, category: str | None = None) -> None:
        super().__init__()
        self._category = category

    def run(self) -> None:
        from .archive import query_archive
        try:
            items = query_archive(
                category=self._category,
                on_log=lambda msg: self.log.emit(msg + "\n", ""),
            )
            self.results.emit(items)
            self.log.emit(f"Found {len(items)} item(s).\n", "green")
            self.done.emit(True)
        except Exception as exc:
            _log_exception(self.log, "Query failed", exc)
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
        from .archive import download_file
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
            _log_exception(self.log, "Download failed", exc)
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
        from .archive import upload_file
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
            _log_exception(self.log, "Upload failed", exc)
            self.done.emit(False)


# ---------------------------------------------------------------------------
# Archive tab
# ---------------------------------------------------------------------------

class ArchiveTab(WorkerHost, QWidget):
    """Archive tab: install MetisWISE + configure remote archive, then
    query / download / upload files.

    Page 0 — Install & Configure: pip-install MetisWISE into the project
    venv, fill the five DB fields (stored in the OS keyring after a
    successful test, injected into the process environment for
    commonwise), and run a test connection.
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

    def showEvent(self, event) -> None:
        # Re-check whether MetisWISE is still installed every time the tab
        # becomes visible — re-running the Install tab can remove it (its pip
        # reinstall of the pipeline deps doesn't include MetisWISE), and the
        # user needs the Install button to re-enable in that case.
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

        # Download/upload workers have always emitted `progress`; nothing was
        # ever connected to it, so multi-GB transfers showed no feedback at all.
        self._progress = QProgressBar()
        self._progress.setTextVisible(True)
        self._progress.setFormat("%v / %m files")
        self._progress.hide()
        outer.addWidget(self._progress)

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
            "After a successful test all credentials are stored in your OS "
            "keyring (never on disk) — on later runs, leave fields blank to "
            "use the stored values. <code>data_server</code>, port and "
            "protocol are inherited from the MetisWISE-packaged default "
            "(<code>metis-ds.hpc.rug.nl:8013</code>, https)."
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
        self._cred_edit.setPlaceholderText(
            "username:password (blank = use stored keyring entry)",
        )
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
            "Stored in your OS keyring after a successful test; leave a "
            "field blank to use its stored value."
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
            edit.setPlaceholderText("blank = use stored keyring value")
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

        from .archive import TASK_PRODUCTS
        all_produces = set(_chain.from_iterable(p.produces for p in TASK_PRODUCTS.values()))
        for catg in sorted(all_produces):
            self._catg_combo.addItem(catg)
        self._catg_combo.insertSeparator(self._catg_combo.count())
        from .run_metis import DPR_TO_TAG
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
        from .archive import metiswise_available
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
        from .archive import metiswise_available
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

    def _begin_progress(self, total: int) -> None:
        self._progress.setRange(0, total)
        self._progress.setValue(0)
        self._progress.show()

    def _on_progress(self, done: int, total: int) -> None:
        self._progress.setRange(0, total)
        self._progress.setValue(done)

    def _end_progress(self) -> None:
        self._progress.hide()

    def _reject_if_busy(self) -> bool:
        """True (and warns) if an archive worker is already running.

        The tab keeps one ``self._worker`` slot, so starting a second action
        used to rebind it and drop the last reference to a *running* QThread.
        Only the button for the action in flight was disabled, so e.g. "Save &
        Test" during a MetisWISE install aborted the process.
        """
        if not self.busy():
            return False
        QMessageBox.information(
            self, "Archive busy",
            "Another archive operation is still running. "
            "Please wait for it to finish.",
        )
        return True

    def _on_install_metiswise(self) -> None:
        if self._reject_if_busy():
            return
        # NB: not .strip() — a credential with leading/trailing whitespace is
        # rejected below rather than silently altered.
        creds = self._cred_edit.text()
        if creds:
            from .archive import encode_pip_credentials
            try:
                encode_pip_credentials(creds)
            except ValueError as exc:
                QMessageBox.warning(
                    self, "Malformed credentials",
                    f"{exc}\n\nLeave the field blank to use the keyring entry.",
                )
                return
        self._log.clear()
        self._install_btn.setEnabled(False)
        # Blank field → the worker resolves the keyring entry on its own
        # thread, so a keyring-unlock prompt never blocks the GUI.
        self._worker = self.track_worker(MetisWISEInstallWorker(creds or None))
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
        self._worker.needs_input.connect(
            lambda msg: QMessageBox.warning(self, "Missing credentials", msg),
        )
        self._worker.done.connect(self._on_metiswise_installed)
        self._worker.start()

    def _on_metiswise_installed(self, _success: bool) -> None:
        self._save_settings()
        self._refresh_install_status()

    # ── Save & Test Connection ─────────────────────────────────────────────

    def _on_save_and_test(self) -> None:
        if self._reject_if_busy():
            return
        from .archive import metiswise_available
        if not metiswise_available():
            QMessageBox.warning(
                self, "MetisWISE not installed",
                "Install MetisWISE first — the test connection needs it.",
            )
            return
        fields = {k: e.text().strip() for k, e in self._cfg_edits.items()}
        # Blank fields are allowed — the worker fills them from the keyring
        # (only it can know what is stored without prompting on this thread)
        # and reports genuinely missing ones via needs_input.
        self._save_test_btn.setEnabled(False)
        self._cfg_status.setText("Testing…")
        self._cfg_status.setStyleSheet("color: gray;")
        self._worker = self.track_worker(TestConnectionWorker(fields))
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
        self._worker.needs_input.connect(
            lambda msg: QMessageBox.warning(self, "Missing fields", msg),
        )
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
        if self._reject_if_busy():
            return
        self._archive_list.clear()
        self._refresh_btn.setEnabled(False)
        catg_text = self._catg_combo.currentText().strip()
        category = None if catg_text in ("", "(all)") else catg_text
        if category:
            log_append(self._log, f"Querying archive for {category}…\n", "cyan")
        else:
            log_append(self._log, "Querying archive (all items)…\n", "cyan")
        self._worker = self.track_worker(QueryWorker(category=category))
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
        if self._reject_if_busy():
            return
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
        self._worker = self.track_worker(DownloadWorker(filenames, Path(dest)))
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
        self._begin_progress(len(filenames))
        self._worker.progress.connect(self._on_progress)
        self._worker.done.connect(lambda _ok: self._end_progress())
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
        from .run_metis import classify_fits_file
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

        from .archive import TASK_PRODUCTS
        from .run_metis import DPR_TO_TAG
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
        if self._reject_if_busy():
            return
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
        self._worker = self.track_worker(UploadWorker(entries))
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
        self._begin_progress(len(entries))
        self._worker.progress.connect(self._on_progress)
        self._worker.done.connect(lambda _ok: self._end_progress())
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

        # Pre-populate from a legacy (pre-keyring) ~/.awe/Environment.cfg if
        # present.  Deliberately NO keyring read here: that could pop an
        # unlock prompt at GUI startup, annoying users who never touch the
        # archive.  Stored keyring values are resolved lazily by the workers;
        # post-migration the file is scrubbed and the fields stay blank
        # (placeholders explain that blank = stored keyring value).
        from .archive import read_env_cfg
        existing = read_env_cfg()
        for key, edit in self._cfg_edits.items():
            edit.setText(existing.get(key, ""))

    def _save_settings(self) -> None:
        # No archive fields are persisted to QSettings. All credentials live
        # in the OS keyring, written by the workers after a successful
        # install / connection test.
        return


# ---------------------------------------------------------------------------
# Run tab
# ---------------------------------------------------------------------------

class RunTab(WorkerHost, QWidget):
    #: How long to let run_metis clean up (stop EDPS, restore config) after
    #: SIGTERM before resorting to SIGKILL.
    STOP_GRACE_MS = 10_000


    def __init__(self) -> None:
        super().__init__()
        self._process: QProcess | None = None
        self._stopping = False
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
        csv_main_lbl.setAlignment(
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
        )
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

        # CSV → YAML translation (dry run): no simulation, no pipeline.
        cty_row = QWidget()
        cty_h = QHBoxLayout(cty_row)
        cty_h.setContentsMargins(0, 0, 0, 0)
        cty_lbl = QLabel("")
        cty_lbl.setFixedWidth(LABEL_W)
        cty_h.addWidget(cty_lbl)
        self.csv_to_yaml_cb = QCheckBox(
            "Translate CSV → YAML, no simulation  (--csv-to-yaml)"
        )
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
        self.input_list.model().rowsInserted.connect(
            lambda *_a: self._refresh_input_status())
        self.input_list.model().rowsRemoved.connect(
            lambda *_a: self._refresh_input_status())

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
        pipe_in_content.setAlignment(
            self.pipeline_input_list, Qt.AlignmentFlag.AlignTop)

        self.pipeline_input_row = QWidget()
        pi_outer = QVBoxLayout(self.pipeline_input_row)
        pi_outer.setContentsMargins(0, 0, 0, 0)
        pi_lbl = QLabel("Pipeline input dirs  (--pipeline-input):")
        pi_outer.addWidget(pi_lbl)
        pi_outer.addLayout(pipe_in_content)
        # Cap the list at the button column's height (computed after the
        # layouts are parented so style spacing resolves) so the list's
        # bottom edge lines up with the Clear button.
        self.pipeline_input_list.setMaximumHeight(
            pipe_in_btns.sizeHint().height())
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
        self.input_grp.setVisible(not pipe_only)
        self.pipeline_input_row.setVisible(pipe_only)
        self._update_output_info()

    def _update_csv_to_yaml_state(self) -> None:
        """CSV→YAML is a translate-only dry run, so the simulation/pipeline
        options don't apply — disable them while it's ticked."""
        on = self.csv_to_yaml_cb.isChecked()
        for w in (self.rb_both, self.rb_sim_only, self.rb_pipe_only,
                  self.calib_cb, self.static_cb, self.auto_fetch_cb,
                  self.cores_spin):
            w.setEnabled(not on)

    # ── Runner-dependent field visibility ────────────────────────────────────

    def _update_runner_fields(self) -> None:
        runner = self.runner_combo.currentText()
        self.container_row.setVisible(runner in ("docker", "podman"))
        if runner in ("docker", "podman"):
            ph = "(resolved inside container)"
        else:
            ph = str(REPO_ROOT / "inst_pkgs")
        self.inst_edit.setPlaceholderText(ph)

    # ── Input list (YAML / CSV) ──────────────────────────────────────────────

    def _add_input(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self, "Select input files", str(REPO_ROOT),
            "Observation inputs (*.yaml *.yml *.csv);;"
            "YAML (*.yaml *.yml);;CSV (*.csv);;All files (*)"
        )
        existing = {self.input_list.item(i).text()
                    for i in range(self.input_list.count())}
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

    def _clear_pipeline_inputs(self) -> None:
        self.pipeline_input_list.clear()
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
                args += ["--pipeline-input",
                         self.pipeline_input_list.item(i).text()]

        runner = self.runner_combo.currentText()
        args += ["--runner", runner]
        if runner in ("docker", "podman") and self.container_edit.text().strip():
            args += ["--container", self.container_edit.text().strip()]
        if self.sim_dir_edit.text().strip():
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

        for i in range(self.input_list.count()):
            args.append(self.input_list.item(i).text())

        return args

    def _run(self) -> None:
        if not self.rb_pipe_only.isChecked() and self.input_list.count() == 0:
            QMessageBox.warning(
                self, "No input files",
                "Add at least one input file (YAML or CSV)."
            )
            return

        self._save_settings()
        self.log_view.clear()
        self._stopping = False

        args = self._build_cmd_args()

        # A new QProcess per run, parented to the tab: without this the old
        # ones accumulate for the lifetime of the window.
        if self._process is not None:
            self._process.deleteLater()
        self._process = QProcess(self)
        self._process.setWorkingDirectory(str(REPO_ROOT))
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
        """Ask the run to stop, then insist.

        SIGKILL alone (the old behaviour) skipped run_metis's own cleanup, so
        the EDPS server stayed up on its port, ~/.edps/application.properties
        stayed patched by --prefer-masters, and the temp sim script leaked.
        SIGTERM lets that `finally` run; the kill is the fallback.
        """
        if not self._process:
            return
        if self._process.state() == QProcess.ProcessState.NotRunning:
            return
        self._stopping = True
        log_append(self.log_view, "\nStopping — waiting for cleanup…\n", "yellow")
        self._process.terminate()
        if not self._process.waitForFinished(self.STOP_GRACE_MS):
            log_append(
                self.log_view,
                "Cleanup did not finish in time; killing the process.\n"
                "Check for a stray EDPS server if the next run misbehaves.\n",
                "red",
            )
            self._process.kill()
            self._process.waitForFinished(2000)

    @staticmethod
    def _strip_ansi(text: str) -> str:
        return strip_ansi(text)

    def _on_stdout(self) -> None:
        data = self._process.readAllStandardOutput().data().decode(errors="replace")
        log_append(self.log_view, self._strip_ansi(data))

    def _on_stderr(self) -> None:
        data = self._process.readAllStandardError().data().decode(errors="replace")
        log_append(self.log_view, self._strip_ansi(data), "orange")

    def _on_finished(self, exit_code: int, status) -> None:
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
        self.csv_start_spin.setValue(s.value("csv_start", 0, type=int))
        self.csv_end_spin.setValue(s.value("csv_end", 0, type=int))
        mode = s.value("pipeline_mode", "both")
        {"sim_only": self.rb_sim_only, "pipe_only": self.rb_pipe_only}.get(
            mode, self.rb_both
        ).setChecked(True)
        self.runner_combo.setCurrentText(s.value("runner", "default"))
        self.container_edit.setText(s.value("container", ""))
        self.sim_dir_edit.setText(s.value("sim_dir", ""))
        self.inst_edit.setText(s.value("inst_pkgs", ""))
        self.auto_fetch_cb.setChecked(s.value("auto_fetch", False, type=bool))
        self.csv_to_yaml_cb.setChecked(s.value("csv_to_yaml", False, type=bool))
        self._update_csv_to_yaml_state()
        for f in (s.value("pipeline_input_dirs") or []):
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
        s.setValue("pipeline_input_dirs", [
            self.pipeline_input_list.item(i).text()
            for i in range(self.pipeline_input_list.count())
        ])
        s.setValue("input_files", [
            self.input_list.item(i).text() for i in range(self.input_list.count())
        ])
        # Remove legacy keys after successful migration to avoid confusion.
        s.remove("yaml_files")
        s.remove("workflow")


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
        self._install_tab = InstallTab()
        tabs.addTab(self._run_tab, "Run")
        tabs.addTab(self._install_tab, "Install")
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

    def _busy_jobs(self) -> list[str]:
        """Names of jobs still running, for the close confirmation."""
        jobs = []
        proc = getattr(self._run_tab, "_process", None)
        if proc is not None and proc.state() != QProcess.ProcessState.NotRunning:
            jobs.append("a pipeline run")
        for tab, label in ((self._install_tab, "an install/uninstall"),
                           (self._archive_tab, "an archive operation")):
            for worker in tab.live_workers():
                jobs.append(label)
                break
        return jobs

    def closeEvent(self, event) -> None:
        # Destroying a tab while one of its QThreads is still running aborts
        # with "QThread: Destroyed while thread is still running", and the
        # worker's queued log signal targets a QTextEdit that is going away.
        busy = self._busy_jobs()
        if busy and not SMOKE_TEST:
            reply = QMessageBox.question(
                self, "Job still running",
                "There is still " + " and ".join(busy) + " in progress.\n\n"
                "Quit anyway? The job will be stopped.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                event.ignore()
                return

        self._run_tab._save_settings()
        self._install_tab._save_settings()
        self._archive_tab._save_settings()

        self._run_tab.stop_process()
        self._install_tab.stop_ref_workers()
        for tab in (self._install_tab, self._archive_tab):
            tab.stop_workers()
        super().closeEvent(event)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    global SMOKE_TEST
    pink = "--pink" in sys.argv
    smoke_test = "--smoke-test" in sys.argv or os.environ.get("SMOKE_TEST")
    SMOKE_TEST = bool(smoke_test)
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
