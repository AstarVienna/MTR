"""First-run guided tour: dims the window, highlights one widget per step and explains it."""

# PyQt6 stubs type most widget getters as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

import math
from collections.abc import Callable
from dataclasses import dataclass

from PyQt6.QtCore import QEvent, QObject, QPoint, QPointF, QRect, QRectF, QSettings, Qt, QTimer
from PyQt6.QtGui import QColor, QKeySequence, QPainter, QPainterPath, QPen, QPolygonF, QShortcut
from PyQt6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

SETTINGS_KEY = "tour/completed"


def completed() -> bool:
    return QSettings("METIS", "TestRunner").value(SETTINGS_KEY, False, type=bool)


def mark_completed() -> None:
    QSettings("METIS", "TestRunner").setValue(SETTINGS_KEY, True)


@dataclass(frozen=True)
class Step:
    title: str
    text: str
    #: Run before the step is shown, e.g. to switch to the tab it explains
    prepare: Callable[[], None] | None = None
    #: Area to highlight in window coordinates; None or empty centres the popup
    target: Callable[[], QRect | None] | None = None


def widgets_rect(window: QWidget, *widgets: QWidget) -> QRect | None:
    """Bounding box of the visible *widgets* in *window*'s coordinates."""
    rect = QRect()
    for w in widgets:
        if w.isVisibleTo(window):
            rect = rect.united(QRect(w.mapTo(window, QPoint(0, 0)), w.size()))
    return None if rect.isNull() else rect


def tab_rect(window: QWidget, tabs, page: QWidget) -> QRect:
    """The tab label of *page* in *tabs*, in *window*'s coordinates."""
    bar = tabs.tabBar()
    r = bar.tabRect(tabs.indexOf(page))
    return QRect(bar.mapTo(window, r.topLeft()), r.size())


def _edge_point(rect: QRectF, toward: QPointF) -> QPointF:
    """Where the ray from *rect*'s centre to *toward* leaves *rect*."""
    c = rect.center()
    dx, dy = toward.x() - c.x(), toward.y() - c.y()
    tx = rect.width() / 2 / abs(dx) if dx else math.inf
    ty = rect.height() / 2 / abs(dy) if dy else math.inf
    t = min(tx, ty, 1.0)
    return QPointF(c.x() + dx * t, c.y() + dy * t)


class TourOverlay(QWidget):
    """Covers *window*, blocks input to it and walks through *steps*."""

    PAD = 6  # space between a target and its highlight frame
    GAP = 44  # popup distance from the highlight, room for the arrow
    MARGIN = 12
    WIDTH = 480

    def __init__(
        self,
        window: QWidget,
        steps: list[Step],
        colors: dict[str, str],
        on_finish: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(window)
        self._window = window
        self._steps = steps
        self._accent = QColor(colors["accent"])
        self._on_finish = on_finish
        self._index = 0
        self._hole: QRect | None = None

        self._popup = QFrame(self)
        self._popup.setObjectName("tourPopup")
        self._popup.setStyleSheet(
            f"QFrame#tourPopup {{ background: {colors['window']}; "
            f"border: 2px solid {colors['accent']}; border-radius: 10px; }}"
        )
        self._title = QLabel()
        self._title.setStyleSheet(f"font-size: 15pt; font-weight: bold; color: {colors['accent']};")
        self._title.setWordWrap(True)
        self._body = QLabel()
        self._body.setWordWrap(True)
        self._body.setTextFormat(Qt.TextFormat.RichText)
        self._body.setStyleSheet("font-size: 12pt;")
        self._counter = QLabel()
        self._counter.setProperty("hint", "true")
        self._skip_btn = QPushButton("Skip tour")
        self._back_btn = QPushButton("Back")
        self._next_btn = QPushButton("Next")
        for b in (self._skip_btn, self._back_btn):
            b.setProperty("role", "browse")
        self._next_btn.setProperty("role", "accent")
        self._skip_btn.clicked.connect(self.finish)
        self._back_btn.clicked.connect(self.back)
        self._next_btn.clicked.connect(self.next)

        buttons = QHBoxLayout()
        buttons.addWidget(self._counter)
        buttons.addStretch(1)
        for b in (self._skip_btn, self._back_btn, self._next_btn):
            buttons.addWidget(b)
        lay = QVBoxLayout(self._popup)
        lay.setContentsMargins(16, 14, 16, 12)
        lay.setSpacing(10)
        lay.addWidget(self._title)
        lay.addWidget(self._body)
        lay.addLayout(buttons)

        for key, slot in (
            (Qt.Key.Key_Escape, self.finish),
            (Qt.Key.Key_Right, self.next),
            (Qt.Key.Key_Left, self.back),
        ):
            QShortcut(QKeySequence(key), self, slot, context=Qt.ShortcutContext.WindowShortcut)
        window.installEventFilter(self)

    # ── navigation ───────────────────────────────────────────────────────────

    def start(self) -> None:
        self.setGeometry(self._window.rect())
        self.show()
        self.raise_()
        self._show_step(0)

    def next(self) -> None:
        if self._index + 1 < len(self._steps):
            self._show_step(self._index + 1)
        else:
            self.finish()

    def back(self) -> None:
        if self._index > 0:
            self._show_step(self._index - 1)

    def finish(self) -> None:
        mark_completed()
        self._window.removeEventFilter(self)
        self.hide()
        self.deleteLater()
        if self._on_finish is not None:
            self._on_finish()

    def current_step(self) -> int:
        return self._index

    def _show_step(self, index: int) -> None:
        self._index = index
        step = self._steps[index]
        if step.prepare is not None:
            step.prepare()
        last = index == len(self._steps) - 1
        self._title.setText(step.title)
        self._body.setText(step.text)
        self._counter.setText(f"{index + 1} / {len(self._steps)}")
        self._back_btn.setEnabled(index > 0)
        self._skip_btn.setVisible(not last)
        self._next_btn.setText("Finish" if last else "Next")
        self._next_btn.setFocus()
        self._place()
        # A freshly shown tab lays out its children in the event loop: place again after.
        QTimer.singleShot(0, self._place)

    # ── geometry ─────────────────────────────────────────────────────────────

    def eventFilter(self, obj: QObject, event: QEvent) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
        if obj is self._window and event.type() == QEvent.Type.Resize:
            self.setGeometry(self._window.rect())
            self._place()
        return False

    def _place(self) -> None:
        if not self.isVisible():
            return
        step = self._steps[self._index]
        target = step.target() if step.target is not None else None
        if target is not None and not target.isEmpty():
            p = self.PAD
            hole = target.adjusted(-p, -p, p, p).intersected(self.rect())
            self._hole = hole if not hole.isEmpty() else None
        else:
            self._hole = None
        width = max(200, min(self.WIDTH, self.width() - 2 * self.MARGIN))
        self._popup.setFixedWidth(width)
        lay = self._popup.layout()
        height = lay.totalHeightForWidth(width) if lay.hasHeightForWidth() else -1
        if height <= 0:
            height = self._popup.sizeHint().height()
        self._popup.setGeometry(self._popup_rect(width, height))
        self._popup.show()
        self._popup.raise_()
        self.update()

    def _popup_rect(self, w: int, h: int) -> QRect:
        area = self.rect().adjusted(self.MARGIN, self.MARGIN, -self.MARGIN, -self.MARGIN)
        hole, gap = self._hole, self.GAP
        if hole is None:
            return QRect(area.center().x() - w // 2, area.center().y() - h // 2, w, h)
        cx, cy = hole.center().x() - w // 2, hole.center().y() - h // 2
        candidates = (  # (room needed fits, top-left)
            (area.bottom() - hole.bottom() - gap >= h, QPoint(cx, hole.bottom() + gap)),
            (hole.top() - gap - area.top() >= h, QPoint(cx, hole.top() - gap - h)),
            (area.right() - hole.right() - gap >= w, QPoint(hole.right() + gap, cy)),
            (hole.left() - gap - area.left() >= w, QPoint(hole.left() - gap - w, cy)),
        )
        # Nothing fits beside a huge target: sit inside it, near the bottom.
        pos = next((p for fits, p in candidates if fits), QPoint(cx, hole.bottom() - h - gap))
        x = min(max(pos.x(), area.left()), area.right() - w)
        y = min(max(pos.y(), area.top()), area.bottom() - h)
        return QRect(x, y, w, h)

    # ── painting / input ─────────────────────────────────────────────────────

    def paintEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        shade = QPainterPath()
        shade.addRect(QRectF(self.rect()))
        if self._hole is not None:
            cut = QPainterPath()
            cut.addRoundedRect(QRectF(self._hole), 8, 8)
            shade = shade.subtracted(cut)
        p.fillPath(shade, QColor(0, 0, 0, 165))
        if self._hole is None:
            return
        p.setPen(QPen(self._accent, 3))
        p.setBrush(Qt.BrushStyle.NoBrush)
        p.drawRoundedRect(QRectF(self._hole), 8, 8)
        popup = QRectF(self._popup.geometry())
        hole = QRectF(self._hole)
        if not popup.intersects(hole):
            self._draw_arrow(p, _edge_point(popup, hole.center()), _edge_point(hole, popup.center()))

    def _draw_arrow(self, p: QPainter, start: QPointF, end: QPointF) -> None:
        angle = math.atan2(end.y() - start.y(), end.x() - start.x())
        end = QPointF(end.x() - 4 * math.cos(angle), end.y() - 4 * math.sin(angle))
        size = 14
        pen = QPen(self._accent, 3)
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        p.setPen(pen)
        shaft_end = QPointF(end.x() - size * 0.8 * math.cos(angle), end.y() - size * 0.8 * math.sin(angle))
        p.drawLine(start, shaft_end)
        head = QPolygonF(
            [
                end,
                QPointF(end.x() - size * math.cos(angle - 0.45), end.y() - size * math.sin(angle - 0.45)),
                QPointF(end.x() - size * math.cos(angle + 0.45), end.y() - size * math.sin(angle + 0.45)),
            ]
        )
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(self._accent)
        p.drawPolygon(head)

    def mousePressEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        event.accept()  # the window stays inert while the tour runs

    def wheelEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        event.accept()


def build_steps(win) -> list[Step]:
    """The tour of *win* (a MainWindow): toolbar, then each tab and its central widgets."""
    tabs = win._tabs
    run, inst, arch, rep = win._run_tab, win._install_tab, win._archive_tab, win._reports_tab

    def show(page: QWidget) -> Callable[[], None]:
        return lambda: tabs.setCurrentWidget(page)

    def area(*widgets: QWidget) -> Callable[[], QRect | None]:
        return lambda: widgets_rect(win, *widgets)

    def label(page: QWidget) -> Callable[[], QRect]:
        return lambda: tab_rect(win, tabs, page)

    return [
        Step(
            "Welcome to the METIS Test Runner",
            "MTR simulates METIS observations with ScopeSim and reduces them with the METIS "
            "pipeline. This short tour shows what each tab is for.<br><br>"
            "Use <b>Next</b> / <b>Back</b> or the arrow keys; <b>Esc</b> skips the tour. "
            "You can replay it any time with the <b>Tour</b> button.",
        ),
        Step(
            "Four tabs",
            "<b>Run</b> simulates and reduces observations, <b>Install</b> sets up the pipeline, "
            "<b>Archive</b> exchanges files with the METIS archive, and <b>Reports</b> shows the "
            "plots a run produced.",
            target=area(tabs.tabBar()),
        ),
        Step(
            "Toolbar",
            "Switch between the dark and light theme, open the <b>Data flow</b> diagram of how "
            "simulated data moves through the pipeline tasks (you can run a single task or open its "
            "reports from there), and replay this <b>Tour</b>.",
            target=area(win._theme_btn, win._flow_btn, win._tour_btn),
        ),
        # ── Install ──
        Step(
            "Install tab",
            "Start here on a fresh machine. It clones <b>METIS_Pipeline</b> and "
            "<b>METIS_Simulations</b> and installs all Python dependencies into MTR's own "
            "environment.<br><br>Skip it if you already have the pipeline natively or in a "
            "container; then pick that runner in the <b>Run</b> tab.",
            show(inst),
            label(inst),
        ),
        Step(
            "Repositories (advanced)",
            "Leave the version fields blank to track each repository's default branch. Developers "
            "can pick a branch, tag or commit, or select their own checkout, which MTR uses as-is "
            "and never updates or deletes.",
            show(inst),
            area(inst.repo_grp),
        ),
        Step(
            "Install / Update",
            "<b>Install / Update</b> installs everything, or brings an existing install up to date; "
            "re-running it is safe. <b>Uninstall</b> removes what MTR installed, after asking.",
            show(inst),
            area(inst.install_btn, inst.uninstall_btn),
        ),
        Step(
            "Install log",
            "Progress and any errors of the install or uninstall appear here.",
            show(inst),
            area(inst.log_view),
        ),
        # ── Run ──
        Step(
            "Run tab",
            "Run test observations: simulate raw frames from your input files, then reduce them "
            "with the pipeline.",
            show(run),
            label(run),
        ),
        Step(
            "Input files",
            "Add YAML or CSV files describing the observations to simulate. For CSV files you can "
            "restrict the run to a range of lines.",
            show(run),
            area(run.input_grp),
        ),
        Step(
            "Options",
            "Set the output directory, calibration handling and CPU cores. The <b>runner</b> picks "
            "the pipeline: <i>default</i> uses the Install tab's, <i>native</i> a bare-metal install, "
            "<i>docker</i>/<i>podman</i> a container. The <b>pipeline mode</b> chooses whether to "
            "simulate, reduce, or both.",
            show(run),
            area(run.opts_grp),
        ),
        Step(
            "Run, Stop, Open Folder",
            "<b>Run</b> starts the run, <b>Stop</b> ends it cleanly, and <b>Open Folder</b> opens "
            "its output directory.",
            show(run),
            area(run.run_btn, run.stop_btn, run.open_folder_btn),
        ),
        Step(
            "Run log",
            "Live output of the simulation and the pipeline. When a run finishes, its reports are "
            "loaded in the <b>Reports</b> tab.",
            show(run),
            area(run.log_view),
        ),
        # ── Archive ──
        Step(
            "Archive tab",
            "Talks to the METIS AIT archive through MetisWISE: download calibrations and data, or "
            "upload your own FITS files.",
            show(arch),
            label(arch),
        ),
        Step(
            "Archive pages",
            "This area has three pages: <b>setup</b> installs MetisWISE and stores your archive "
            "credentials, <b>Query / Download</b> searches the archive by category and filename, "
            "and <b>Upload</b> sends FITS files to it. The arrow buttons move between them.",
            show(arch),
            area(arch._stack),
        ),
        Step(
            "Archive log",
            "Results of each archive operation; a progress bar appears below it during transfers.",
            show(arch),
            area(arch._log),
        ),
        # ── Reports ──
        Step(
            "Reports tab",
            "Browse the ADARI reports (diagnostic plots) that EDPS made during a run.",
            show(rep),
            label(rep),
        ),
        Step(
            "Choose a run",
            "The newest run is selected. <b>Browse…</b> opens any other run folder and "
            "<b>Refresh</b> rescans the selected one.",
            show(rep),
            area(rep.run_combo, rep.refresh_btn),
        ),
        Step(
            "Report tree",
            "The run's reports, grouped by EDPS job. Select a panel to view it.",
            show(rep),
            area(rep.viewer.tree),
        ),
        Step(
            "Report viewer",
            "The selected plot with its description. Toggle <b>Fit</b>, zoom with \u2212 / +, or open "
            "the image or its folder.",
            show(rep),
            area(rep.viewer.description, rep.viewer.scroll, rep.viewer.fit_cb, rep.viewer.folder_btn),
        ),
        Step(
            "You're all set",
            "On a fresh machine, begin in the <b>Install</b> tab. Replay this tour any time with "
            "the <b>Tour</b> button in the toolbar.",
        ),
    ]
