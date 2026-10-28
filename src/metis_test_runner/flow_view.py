"""The "Data flow" window: :mod:`flow`'s graph in a zoomable, pannable view."""

# PyQt6 stubs type painters and events as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable

from PyQt6.QtCore import QPointF, QRectF, Qt, QTimer
from PyQt6.QtGui import (
    QColor,
    QFont,
    QFontMetricsF,
    QKeySequence,
    QPainter,
    QPainterPath,
    QPainterPathStroker,
    QPen,
    QPolygonF,
    QShortcut,
)
from PyQt6.QtWidgets import (
    QCheckBox,
    QGraphicsItem,
    QGraphicsPathItem,
    QGraphicsRectItem,
    QGraphicsScene,
    QGraphicsSimpleTextItem,
    QGraphicsView,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from . import flow

NODE_H = 38
RAW_H = 26
ROW_H = 52
COL_GAP = 44
PAD = 8
MIN_W = 90
LANE_HEAD = flow.LANE_HEAD * ROW_H  # an expanded lane's header, above its first row
FRAME_PAD = 8
SIM_HEAD = 20  # simulations frame header above its first raw frame
MIN_ZOOM, MAX_ZOOM = 0.05, 4.0
MAX_FIT = 2.0  # Fit never magnifies beyond this
HOVER_MS = 100  # how long the pointer rests on a node before its trace shows
RESIZE_MS = 80  # re-fit once a window resize pauses


def _color(colors: dict[str, str], key: str, alpha: int = 255) -> QColor:
    c = QColor(colors[key])
    c.setAlpha(alpha)
    return c


class _NodeItem(QGraphicsItem):
    """A node box, or for a "sim" node with a span, the frame around its raw frames."""

    def __init__(self, node: flow.Node, width: float, window: FlowWindow):
        super().__init__()
        self.node = node
        self._w = width
        self.height = RAW_H if node.kind == "raw" else NODE_H
        self._window = window
        self.setToolTip(node.tooltip)
        self.setAcceptHoverEvents(True)
        if node.kind == "group":
            self.setCursor(Qt.CursorShape.PointingHandCursor)
        if node.span:
            self.setZValue(-1.5)

    def boundingRect(self) -> QRectF:
        if self.node.span:
            height = (self.node.span - 1) * ROW_H + RAW_H + SIM_HEAD + 6
            return QRectF(-6, -SIM_HEAD, self._w + 12, height)
        return QRectF(0, 0, self._w, self.height)

    def shape(self) -> QPainterPath:
        # A frame reacts to its header only, so the raw frames inside it get their own hover.
        path = QPainterPath()
        rect = self.boundingRect()
        path.addRect(QRectF(rect.left(), rect.top(), rect.width(), SIM_HEAD) if self.node.span else rect)
        return path

    def _style(self) -> tuple[QColor, QColor, QColor, float]:
        """Fill, border, text and border width for the node's kind and overlay state."""
        c = self._window.colors
        state, kind = self.node.state, self.node.kind
        if state in ("running", "target"):
            return (
                _color(c, "btn_success_bg"),
                _color(c, "accent") if state == "target" else _color(c, "btn_success_bg"),
                _color(c, "btn_success_fg"),
                3.0 if state == "target" else 1.5,
            )
        if state == "missing":
            return _color(c, "btn_danger_bg"), _color(c, "btn_danger_bg"), _color(c, "btn_danger_fg"), 1.5
        fill, border = {
            "sim": ("accent_dim", "accent"),
            "static": ("alt_base", "log_orange"),
            "raw": ("base", "log_cyan"),
            "task": ("alt_base", "border"),
            "group": ("alt_base", "accent"),
            "archive": ("accent", "accent"),
        }[kind]
        text = _color(c, "highlight_text" if kind == "archive" else "window_text")
        if state == "present":
            return _color(c, fill), _color(c, "log_green"), text, 2.5
        return _color(c, fill), _color(c, border), text, 1.8 if kind == "group" else 1.2

    def paint(self, painter, option, widget=None) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        fill, border, text, width = self._style()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        pen = QPen(border, width)
        if self.node.kind == "static":
            pen.setStyle(Qt.PenStyle.DashLine)
        painter.setPen(pen)
        lines = self.node.label.split("\n")
        if self.node.kind == "group":
            lines[0] = f"▸ {lines[0]}"
        if self.node.span:
            fill.setAlpha(90)
            painter.setBrush(fill)
            painter.drawRoundedRect(self.boundingRect().adjusted(1, 1, -1, -1), 8, 8)
            painter.setPen(text)
            painter.setFont(self._window.small_bold_font)
            header = QRectF(PAD - 6, -SIM_HEAD + 3, self._w, SIM_HEAD - 4)
            painter.drawText(header, Qt.AlignmentFlag.AlignLeft, " · ".join(lines))
            return
        painter.setBrush(fill)
        painter.drawRoundedRect(self.boundingRect().adjusted(1, 1, -1, -1), 6, 6)
        painter.setPen(text)
        painter.setFont(self._window.bold_font)
        inner = self._w - 2 * PAD
        if len(lines) == 1 and not self.node.badges:
            align = Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
            painter.drawText(QRectF(PAD, 0, inner, self.height), align, lines[0])
            return
        painter.drawText(QRectF(PAD, 3, inner, 18), Qt.AlignmentFlag.AlignLeft, lines[0])
        painter.setFont(self._window.small_font)
        if len(lines) > 1:
            painter.drawText(QRectF(PAD, 20, inner, 15), Qt.AlignmentFlag.AlignLeft, lines[1])
        if self.node.badges:
            painter.setPen(_color(self._window.colors, "log_yellow") if self.node.state is None else text)
            painter.drawText(
                QRectF(PAD, 20, inner, 15), Qt.AlignmentFlag.AlignRight, "  ".join(self.node.badges)
            )

    def hoverEnterEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        self._window.hover(self.node.id)

    def hoverLeaveEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        self._window.hover(None)


class _LaneHeader(QGraphicsSimpleTextItem):
    """An expanded lane's name; clicking it collapses the lane."""

    def __init__(self, lane: flow.Lane):
        super().__init__(f"▾ {lane.label}")
        self.lane = lane.id
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setToolTip("Click to collapse")


class _EdgeItem(QGraphicsPathItem):
    def __init__(self, edge: flow.Edge, p1: QPointF, p2: QPointF, pen: QPen):
        sign = 1 if p2.x() >= p1.x() else -1
        dx = sign * max(abs(p2.x() - p1.x()) / 2, 30)
        path = QPainterPath(p1)
        path.cubicTo(p1 + QPointF(dx, 0), p2 - QPointF(dx, 0), p2)
        super().__init__(path)
        self.edge = edge
        self.setPen(pen)
        self.setZValue(-1)
        self._head = QPolygonF([p2, p2 + QPointF(-sign * 9, -4.5), p2 + QPointF(-sign * 9, 4.5)])
        tip = {
            "sim": "simulated",
            "main": "main input",
            "assoc": "associated input",
            "static": "static input",
        }
        tip |= {"upload": "upload (Archive tab)", "fetch": "download (--auto-fetch-calibrations)"}
        self.setToolTip(f"{tip[edge.kind]}: {', '.join(edge.tags)}" if edge.tags else tip[edge.kind])

    def boundingRect(self) -> QRectF:
        return super().boundingRect().adjusted(-10, -10, 10, 10)

    def shape(self) -> QPainterPath:
        stroker = QPainterPathStroker()
        stroker.setWidth(8)
        return stroker.createStroke(self.path())

    def paint(self, painter, option, widget=None) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setPen(self.pen())
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawPath(self.path())
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(self.pen().color())
        painter.drawPolygon(self._head)


class _View(QGraphicsView):
    def __init__(self, scene: QGraphicsScene, window: FlowWindow):
        super().__init__(scene)
        self._window = window
        self._press: QPointF | None = None
        self.setDragMode(QGraphicsView.DragMode.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        self.setRenderHints(QPainter.RenderHint.Antialiasing | QPainter.RenderHint.TextAntialiasing)
        # Dragging pans; scrollbars would only take room from Fit.
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

    def zoom(self, factor: float) -> None:
        self._window.set_auto_fit(False)
        scale = self.transform().m11() * factor
        if MIN_ZOOM <= scale <= MAX_ZOOM:
            self.scale(factor, factor)

    def resizeEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        super().resizeEvent(event)
        self._window.view_resized()

    def wheelEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        self.zoom(1.15 ** (event.angleDelta().y() / 120))

    def mousePressEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        self._press = event.position()
        super().mousePressEvent(event)

    def mouseReleaseEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        super().mouseReleaseEvent(event)
        # A click toggles a lane, pins the trace of a node, or clears the pin; a drag pans.
        if self._press is not None:
            if (event.position() - self._press).manhattanLength() < 4:
                self._window.click(self.itemAt(event.position().toPoint()))
            else:
                self._window.set_auto_fit(False)
        self._press = None


class FlowWindow(QWidget):
    """Simulations -> raw frames -> Pipeline tasks -> archive, derived from the workflow code."""

    def __init__(
        self,
        colors: dict[str, str],
        run_input: Callable[[], flow.RunInput] | None = None,
        parent: QWidget | None = None,
    ):
        super().__init__(parent, Qt.WindowType.Window)
        self.setWindowTitle("METIS Test Runner — data flow")
        self.resize(1200, 800)
        self.colors = colors
        self.expanded: set[str] = set()
        self._run_input = run_input
        self._hover: str | None = None
        # Passing over nodes would flash their traces; show one only once the pointer rests
        # on it, but clear it as soon as the pointer leaves.
        self._hover_next: str | None = None
        self._hover_timer = QTimer(self)
        self._hover_timer.setSingleShot(True)
        self._hover_timer.setInterval(HOVER_MS)
        self._hover_timer.timeout.connect(self._hover_now)
        self._pinned: str | None = None
        self._nodes: dict[str, _NodeItem] = {}
        self._edges: list[_EdgeItem] = []
        self._lane_rects: dict[str, QRectF] = {}
        # Fit (columns spread to the window's shape, everything shown) holds until you zoom or pan.
        self._auto_fit = True
        self._col_gap = float(COL_GAP)
        self._resize_timer = QTimer(self)
        self._resize_timer.setSingleShot(True)
        self._resize_timer.setInterval(RESIZE_MS)
        self._resize_timer.timeout.connect(self.fit)
        self._up: dict[str, set[str]] = defaultdict(set)
        self._down: dict[str, set[str]] = defaultdict(set)
        self.flow: flow.Flow | None = None

        self.bold_font = QFont(self.font())
        self.bold_font.setBold(True)
        self.small_font = QFont(self.font())
        self.small_font.setPointSizeF(self.font().pointSizeF() * 0.85)
        self.small_bold_font = QFont(self.small_font)
        self.small_bold_font.setBold(True)

        expand_btn = QPushButton("Expand all")
        expand_btn.clicked.connect(lambda: self.set_expanded(set(flow.lanes())))
        collapse_btn = QPushButton("Collapse all")
        collapse_btn.clicked.connect(lambda: self.set_expanded(set()))
        self.overlay_cb = QCheckBox("Highlight Run tab input")
        self.overlay_cb.setEnabled(run_input is not None)
        self.overlay_cb.toggled.connect(lambda _on: self.rebuild())
        refresh_btn = QPushButton("Refresh")
        refresh_btn.setToolTip("Re-read the Run tab and the archive install state")
        refresh_btn.clicked.connect(lambda: self.rebuild())
        self.fit_btn = QPushButton("Fit")
        self.fit_btn.setCheckable(True)
        self.fit_btn.setChecked(True)
        self.fit_btn.setToolTip("Show the whole flow, filling the window; stays on until you zoom or pan")
        self.fit_btn.clicked.connect(lambda: self.fit())
        one_btn = QPushButton("100%")
        one_btn.clicked.connect(self.view_actual_size)

        controls = QHBoxLayout()
        controls.addWidget(expand_btn)
        controls.addWidget(collapse_btn)
        controls.addWidget(self.overlay_cb)
        controls.addWidget(refresh_btn)
        controls.addStretch(1)
        controls.addWidget(self.fit_btn)
        controls.addWidget(one_btn)

        self.scene = QGraphicsScene(self)
        self.view = _View(self.scene, self)
        self.legend = QLabel()
        self.status = QLabel()
        self.status.setWordWrap(True)
        self.status.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        layout = QVBoxLayout(self)
        layout.addLayout(controls)
        layout.addWidget(self.legend)
        layout.addWidget(self.view, 1)
        layout.addWidget(self.status)

        QShortcut(QKeySequence.StandardKey.ZoomIn, self, lambda: self.view.zoom(1.25))
        QShortcut(QKeySequence.StandardKey.ZoomOut, self, lambda: self.view.zoom(0.8))
        QShortcut(QKeySequence("Ctrl+0"), self, self.fit)

    # -- public ---------------------------------------------------------------

    def apply_theme(self, colors: dict[str, str]) -> None:
        self.colors = colors
        self.rebuild()

    def showEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        super().showEvent(event)
        # The archive may have been installed since the window was last open.
        if not event.spontaneous():
            self.rebuild()
            QTimer.singleShot(0, self.fit)

    def fit(self) -> None:
        """Spread the columns to the window's shape, show everything, and keep doing so."""
        self.set_auto_fit(True)
        if self.flow is not None:
            self._draw(self.flow, spread=True)
        self._fit_rect(self.scene.itemsBoundingRect())

    def set_auto_fit(self, on: bool) -> None:
        self._auto_fit = on
        self.fit_btn.setChecked(on)
        if not on:
            self._resize_timer.stop()

    def view_resized(self) -> None:
        if self._auto_fit and self.flow is not None:
            self._resize_timer.start()

    def view_actual_size(self) -> None:
        self.set_auto_fit(False)
        self.view.resetTransform()

    def set_expanded(self, lanes: set[str]) -> None:
        self.expanded = lanes
        self.rebuild(fit=True)

    def toggle_lane(self, lane: str) -> None:
        self.set_expanded(self.expanded ^ {lane})

    def rebuild(self, fit: bool = False) -> None:
        from .archive import metiswise_available

        overlay = None
        if self.overlay_cb.isChecked() and self._run_input is not None:
            overlay = flow.overlay_for(self._run_input())
        archive_installed = metiswise_available()
        self.flow = flow.build_flow(self.expanded, archive_installed, overlay)
        self._hover = self._hover_next = None
        self._hover_timer.stop()
        if self._pinned not in self.flow.nodes:
            self._pinned = None
        if fit:
            self.set_auto_fit(True)
        self._draw(self.flow, spread=self._auto_fit)
        self._update_legend()
        notes = [f"Source: {self.flow.source}"]
        if not archive_installed:
            notes.append("Archive not shown: MetisWISE is not installed (Archive tab).")
        notes += self.flow.notes
        notes.append(
            "Wheel: zoom · drag: pan · Fit: back to the whole flow · "
            "click a sub-workflow to show or hide its tasks · hover a node to trace it, click to pin."
        )
        self.status.setText("\n".join(notes))
        if self._auto_fit:
            self._fit_rect(self.scene.itemsBoundingRect())

    def click(self, item: QGraphicsItem | None) -> None:
        if isinstance(item, _LaneHeader) and item.lane:
            self.toggle_lane(item.lane)
        elif isinstance(item, _NodeItem) and item.node.kind == "group" and item.node.lane:
            self.toggle_lane(item.node.lane)
        else:
            self.pin(item.node.id if isinstance(item, _NodeItem) else None)

    def hover(self, node_id: str | None) -> None:
        self._hover_next = node_id
        if node_id is None:
            self._hover_timer.stop()
            if self._hover is not None:
                self._hover_now()
        elif node_id != self._hover:
            self._hover_timer.start()

    def _hover_now(self) -> None:
        self._hover = self._hover_next
        self._apply_focus()

    def pin(self, node_id: str | None) -> None:
        self._pinned = None if node_id == self._pinned else node_id
        self._apply_focus()

    # -- drawing --------------------------------------------------------------

    def _gap(self, f: flow.Flow, col_w: list[float]) -> float:
        """The column gap that widens the layout to the window's shape, within limits."""
        n = len(col_w)
        if n < 2 or not f.lanes:
            return COL_GAP
        first, last = f.lanes[0], f.lanes[-1]
        top = first.top * ROW_H - (LANE_HEAD if first.expanded else FRAME_PAD)
        bottom = (last.top + last.rows - 1) * ROW_H + NODE_H + FRAME_PAD
        vp = self.view.viewport().size()
        wanted = (bottom - top + 40) * vp.width() / max(vp.height(), 1) - 40
        spare = wanted - (sum(col_w) + (n - 1) * COL_GAP + 16)
        return COL_GAP + min(max(spare, 0) / (n - 1), 3 * COL_GAP)

    def _fit_rect(self, rect: QRectF) -> None:
        self.view.fitInView(rect.adjusted(-20, -20, 20, 20), Qt.AspectRatioMode.KeepAspectRatio)
        scale = self.view.transform().m11()
        if scale > MAX_FIT:
            self.view.scale(MAX_FIT / scale, MAX_FIT / scale)
            self.view.centerOn(rect.center())

    def _width(self, node: flow.Node) -> float:
        bold, small = QFontMetricsF(self.bold_font), QFontMetricsF(self.small_font)
        title, *rest = node.label.split("\n")
        if node.kind == "group":
            title = f"▸ {title}"
        if node.span:
            return QFontMetricsF(self.small_bold_font).horizontalAdvance(" · ".join([title, *rest])) + PAD
        second = small.horizontalAdvance(rest[0]) if rest else 0
        if node.badges:
            second += small.horizontalAdvance("  ".join(node.badges)) + (12 if rest else 0)
        return max(MIN_W, bold.horizontalAdvance(title) + 2 * PAD, second + 2 * PAD)

    def _draw(self, f: flow.Flow, spread: bool = False) -> None:
        """Lay out and draw *f*; *spread* recomputes the column gap, else the last one is kept."""
        self.scene.clear()
        self._nodes.clear()
        self._edges.clear()
        self._lane_rects.clear()
        self._up.clear()
        self._down.clear()
        self.view.setBackgroundBrush(_color(self.colors, "base"))

        widths = {nid: self._width(node) for nid, node in f.nodes.items()}
        n_cols = max((node.col for node in f.nodes.values()), default=0) + 1
        col_w = [
            max((widths[nid] for nid, n in f.nodes.items() if n.col == col), default=MIN_W)
            for col in range(n_cols)
        ]
        if spread:
            self._col_gap = self._gap(f, col_w)
        gap = self._col_gap
        col_x = [sum(col_w[:col]) + col * gap for col in range(n_cols)]
        for nid, node in f.nodes.items():
            if node.span or node.kind == "raw":  # raw frames line up inside their simulations frame
                widths[nid] = col_w[node.col]

        # Lane frames, behind everything, spanning every column but the archive's
        last = n_cols - (2 if flow.ARCHIVE_ID in f.nodes else 1)
        right = col_x[last] + col_w[last] + 16
        for lane in f.lanes:
            # Only an expanded lane has a header; a collapsed one is named by its box.
            head = LANE_HEAD if lane.expanded else FRAME_PAD
            height = (lane.rows - 1) * ROW_H + NODE_H + head + FRAME_PAD
            rect = QRectF(-16, lane.top * ROW_H - head, right + 16, height)
            self._lane_rects[lane.id] = rect
            frame = QGraphicsRectItem(rect)
            frame.setPen(QPen(_color(self.colors, "border"), 1))
            frame.setBrush(_color(self.colors, "window", 120))
            frame.setZValue(-2)
            self.scene.addItem(frame)
            if lane.expanded:
                header = _LaneHeader(lane)
                header.setFont(self.bold_font)
                header.setBrush(_color(self.colors, "accent"))
                header.setPos(rect.left() + 8, rect.top() + 4)
                self.scene.addItem(header)

        for nid, node in f.nodes.items():
            item = _NodeItem(node, widths[nid], self)
            item.setPos(col_x[node.col], node.row * ROW_H)
            self.scene.addItem(item)
            self._nodes[nid] = item

        def anchor(end: str, outgoing: bool, offset: float) -> QPointF:
            if end.startswith("lane:"):
                rect = self._lane_rects[end[5:]]
                return QPointF(rect.right(), rect.center().y() + offset)
            item = self._nodes[end]
            x = item.pos().x() + widths[end] if outgoing else item.pos().x()
            return QPointF(x, item.pos().y() + item.height / 2 + offset)

        styles = {
            "sim": (QPen(_color(self.colors, "log_cyan"), 1.4), False),
            "main": (QPen(_color(self.colors, "accent"), 1.8), False),
            "assoc": (QPen(_color(self.colors, "window_text", 110), 1.1), False),
            "static": (QPen(_color(self.colors, "log_orange"), 1.1), True),
            "upload": (QPen(_color(self.colors, "log_green"), 2.0), False),
            "fetch": (QPen(_color(self.colors, "log_yellow"), 2.0), True),
        }
        for edge in f.edges:
            if not edge.src.startswith("lane:") and not edge.dst.startswith("lane:"):
                self._down[edge.src].add(edge.dst)
                self._up[edge.dst].add(edge.src)
            if f.nodes.get(edge.src) and f.nodes[edge.src].span:
                continue  # the frame around the raw frames already shows this link
            pen, dashed = styles[edge.kind]
            pen = QPen(pen)
            if dashed:
                pen.setStyle(Qt.PenStyle.DashLine)
            if edge.kind == "upload":
                p1, p2 = anchor(edge.src, True, 6), anchor(edge.dst, False, 6)
            elif edge.kind == "fetch":
                p1, p2 = anchor(edge.src, False, -6), anchor(edge.dst, True, -6)
            else:
                p1, p2 = anchor(edge.src, True, 0), anchor(edge.dst, False, 0)
            item = _EdgeItem(edge, p1, p2, pen)
            self.scene.addItem(item)
            self._edges.append(item)
        self.scene.setSceneRect(self.scene.itemsBoundingRect().adjusted(-40, -40, 40, 40))
        self._apply_focus()

    def _update_legend(self) -> None:
        c = self.colors
        parts = [
            ("accent", "simulations / sub-workflow"),
            ("log_cyan", "raw frames"),
            ("log_orange", "static calibrations"),
            ("border", "Pipeline task"),
            ("log_yellow", "meta target"),
        ]
        if flow.ARCHIVE_ID in (self.flow.nodes if self.flow else {}):
            parts += [("log_green", "upload"), ("log_yellow", "auto-fetch (dashed)")]
        if self.overlay_cb.isChecked():
            parts += [("btn_success_bg", "would run"), ("btn_danger_bg", "missing master")]
        self.legend.setText("  ".join(f"<span style='color:{c[key]}'>■</span> {text}" for key, text in parts))

    def _trace(self, node_id: str) -> set[str]:
        """*node_id* with everything upstream and downstream of it."""
        seen = {node_id}
        for links in (self._up, self._down):
            todo = [node_id]
            while todo:
                for nxt in links[todo.pop()]:
                    if nxt not in seen:
                        seen.add(nxt)
                        todo.append(nxt)
        return seen

    def _apply_focus(self) -> None:
        focus = self._pinned or self._hover
        if focus is None or focus not in self._nodes:
            for item in self._nodes.values():
                item.setOpacity(0.35 if item.node.state == "idle" else 1.0)
            for edge in self._edges:
                edge.setVisible(edge.edge.kind != "static")
                ends = [self._nodes.get(e) for e in (edge.edge.src, edge.edge.dst)]
                idle = any(n is not None and n.node.state == "idle" for n in ends)
                edge.setOpacity(0.2 if idle else 1.0)
            return
        related = self._trace(focus)
        for nid, item in self._nodes.items():
            item.setOpacity(1.0 if nid in related else 0.12)
        for edge in self._edges:
            src, dst = edge.edge.src, edge.edge.dst
            if src.startswith("lane:") or dst.startswith("lane:"):
                inside = focus == flow.ARCHIVE_ID
            else:
                inside = src in related and dst in related
            edge.setVisible(edge.edge.kind != "static" or inside)
            edge.setOpacity(1.0 if inside else 0.06)
