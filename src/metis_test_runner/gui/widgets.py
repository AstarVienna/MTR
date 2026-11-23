"""Small helpers and widgets shared by the GUI tabs."""

# PyQt6 stubs type most widget getters as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

import html
import re
import sys
from pathlib import Path

from PyQt6.QtCore import (
    QEvent,
    Qt,
)
from PyQt6.QtGui import QColor, QTextCharFormat, QTextCursor
from PyQt6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTabBar,
    QTextEdit,
    QWidget,
)

from .. import paths
from ..env import resolve_runtime_env
from . import theme

_URL_RE = re.compile(r"https?://[^\s<>\"']+")


def _linkify(text: str) -> str:
    """Plain *text* as rich text with clickable URLs and its line breaks kept."""
    linked = _URL_RE.sub(lambda m: f'<a href="{m[0]}">{m[0]}</a>', html.escape(text, quote=False))
    return f'<p style="white-space:pre-wrap">{linked}</p>'


LABEL_W = 280  # fixed label column width in the Run options form

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
    A user-owned checkout (METIS_PIPELINE_DIR) need not be a git repo.
    """
    return paths.pipeline_present()


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
        resolved = theme.LOG_COLORS.get(f"log_{color}", color)
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
            cursor.movePosition(QTextCursor.MoveOperation.StartOfLine, QTextCursor.MoveMode.KeepAnchor)
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
            QFileDialog.getExistingDirectory(parent, "Select directory", str(Path.cwd())) or edit.text()
        )
    )
    return btn


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

    def eventFilter(self, obj, event) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
        if obj is self.lineEdit() and self.count() and not self._holds_custom_text():
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
# Helpers
# ---------------------------------------------------------------------------


class _ExpandingTabBar(QTabBar):
    """Tab bar that divides its width equally among all tabs, overriding QSS."""

    def resizeEvent(self, event):  # pyright: ignore[reportIncompatibleMethodOverride]
        super().resizeEvent(event)
        count = self.count()
        if count > 0:
            new_ss = f"QTabBar::tab {{ width: {event.size().width() // count}px; }}"
            if self.styleSheet() != new_ss:
                self.setStyleSheet(new_ss)

    def sizeHint(self):
        sh = super().sizeHint()
        parent = self.parentWidget()
        if parent and parent.width() > 0:
            sh.setWidth(parent.width())
        return sh

    def minimumSizeHint(self):
        sh = super().minimumSizeHint()
        sh.setWidth(0)
        return sh
