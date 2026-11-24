"""The Reports tab and the Data flow window's report dialog: ADARI report images of a run."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from pathlib import Path

from PyQt6.QtCore import QObject, QRunnable, Qt, QThreadPool, QUrl, pyqtSignal
from PyQt6.QtGui import QDesktopServices, QPixmap
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from . import edps_config, reports
from .reports import Panel, Report

_DATA = Qt.ItemDataRole.UserRole


class _Signals(QObject):
    done = pyqtSignal(int, object, object)  # token, result, exception


class _Job(QRunnable):
    """Run *fn* on the global thread pool and emit ``signals.done(token, result, error)``."""

    def __init__(self, fn: Callable[[], object], token: int):
        super().__init__()
        self.setAutoDelete(False)
        self.fn, self.token, self.signals = fn, token, _Signals()

    def run(self) -> None:
        try:
            result, error = self.fn(), None
        except Exception as e:  # reported to the GUI, never raised in a slot
            result, error = None, e
        self.signals.done.emit(self.token, result, error)


_LIVE: set[_Job] = set()


def start_job(fn: Callable[[], object], token: int, slot: Callable[[int, object, object], None]) -> None:
    """Run *fn* off the GUI thread and call *slot* there with the result.

    Jobs use the global pool and capture no widgets, so a closing window never waits on or kills a
    thread; a result for a destroyed receiver is simply dropped (queued signals disconnect with it).
    """
    job = _Job(fn, token)
    _LIVE.add(job)
    job.signals.done.connect(slot)
    job.signals.done.connect(lambda *_: _LIVE.discard(job))
    QThreadPool.globalInstance().start(job)


def edps_base_dir(run: Path) -> Path | None:
    """EDPS's job folder for *run*, from ~/.edps/application.properties (None if unreadable)."""
    return edps_config.base_dir(run / "pipeline")


class ReportViewer(QWidget):
    """Tree of job → report → panel, and the selected panel's image."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self._pixmap: QPixmap | None = None
        self._zoom = 1.0
        self._path: Path | None = None

        self.tree = QTreeWidget()
        self.tree.setHeaderHidden(True)
        self.tree.setProperty("role", "reports")
        self.tree.setIndentation(14)
        self.tree.currentItemChanged.connect(self._on_select)

        self.description = QLabel()
        self.description.setProperty("fontsize", "large")
        self.description.setWordWrap(True)
        self.description.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.image = QLabel()
        self.image.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.scroll = QScrollArea()
        self.scroll.setWidget(self.image)
        self.scroll.setWidgetResizable(True)

        self.fit_cb = QCheckBox("Fit")
        self.fit_cb.setChecked(True)
        self.fit_cb.toggled.connect(self._render)
        zoom_out, zoom_in = QPushButton("\u2212"), QPushButton("+")
        zoom_out.clicked.connect(lambda: self._set_zoom(self._zoom / 1.25))
        zoom_in.clicked.connect(lambda: self._set_zoom(self._zoom * 1.25))
        for b in (zoom_out, zoom_in):
            b.setProperty("symbol", "true")
        self.open_btn = QPushButton("Open image")
        self.open_btn.clicked.connect(lambda: self._open(self._path))
        self.folder_btn = QPushButton("Open folder")
        self.folder_btn.clicked.connect(lambda: self._open(self._path.parent if self._path else None))
        for b in (zoom_out, zoom_in, self.open_btn, self.folder_btn):
            b.setProperty("role", "browse")
        controls = QHBoxLayout()
        for w in (self.fit_cb, zoom_out, zoom_in):
            controls.addWidget(w)
        controls.addStretch(1)
        controls.addWidget(self.open_btn)
        controls.addWidget(self.folder_btn)

        right = QWidget()
        rlay = QVBoxLayout(right)
        rlay.setContentsMargins(0, 0, 0, 0)
        rlay.addWidget(self.description)
        rlay.addWidget(self.scroll, 1)
        rlay.addLayout(controls)

        splitter = QSplitter()
        splitter.addWidget(self.tree)
        splitter.addWidget(right)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([280, 700])
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.addWidget(splitter)
        self._show(None)

    def set_reports(self, items: list[Report]) -> None:
        self.tree.clear()
        jobs: dict[Path, list[Report]] = {}
        for r in items:
            jobs.setdefault(r.job_dir, []).append(r)
        per_task = Counter(job.parent.name for job in jobs)
        first = None
        for job, job_reports in jobs.items():
            label = job.parent.name + (f" · {job.name[:8]}" if per_task[job.parent.name] > 1 else "")
            top = QTreeWidgetItem([label])
            top.setData(0, _DATA, job)
            top.setToolTip(0, str(job))
            font = top.font(0)
            font.setBold(True)
            top.setFont(0, font)
            self.tree.addTopLevelItem(top)
            for r in job_reports:
                rep = QTreeWidgetItem([r.name + (" (failed)" if r.error else "")])
                rep.setData(0, _DATA, r)
                top.addChild(rep)
                for p in r.panels:
                    item = QTreeWidgetItem([p.prodcatg or p.path.stem])
                    item.setData(0, _DATA, p)
                    item.setToolTip(0, p.description)
                    rep.addChild(item)
                    first = first or item
        self.tree.expandAll()
        if first is not None:
            self.tree.setCurrentItem(first)
        else:
            self._show(None)

    def _on_select(self, item: QTreeWidgetItem | None, _prev=None) -> None:
        data = item.data(0, _DATA) if item is not None else None
        if isinstance(data, Panel):
            self._show(data.path, data.description or data.path.name)
        elif isinstance(data, Report):
            text = (
                f"genreport failed:\n{data.error}"
                if data.error
                else f"{data.name}: {len(data.panels)} image(s)"
            )
            self._show(data.job_dir / data.name / "output.json", text, image=False)
        elif isinstance(data, Path):
            self._show(data / "input.sof", f"EDPS job folder: {data}", image=False)
        else:
            self._show(None)

    def _show(self, path: Path | None, text: str = "", image: bool = True) -> None:
        self._path = path
        self._pixmap = QPixmap(str(path)) if image and path is not None and path.is_file() else None
        if image and path is not None and self._pixmap is None:
            text += f"\n(image not found: {path})"
        self.description.setText(text)
        self.open_btn.setEnabled(self._pixmap is not None)
        self.folder_btn.setEnabled(path is not None)
        self._render()

    def _set_zoom(self, zoom: float) -> None:
        self._zoom = min(max(zoom, 0.1), 8.0)
        self.fit_cb.setChecked(False)
        self._render()

    def _render(self) -> None:
        if self._pixmap is None or self._pixmap.isNull():
            self.image.clear()
            return
        if self.fit_cb.isChecked():
            size = self.scroll.viewport().size()
            pm = self._pixmap.scaled(
                size, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation
            )
            self._zoom = pm.width() / max(self._pixmap.width(), 1)
        else:
            pm = self._pixmap.scaledToWidth(
                max(1, round(self._pixmap.width() * self._zoom)), Qt.TransformationMode.SmoothTransformation
            )
        self.image.setPixmap(pm)

    def resizeEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        super().resizeEvent(event)
        if self.fit_cb.isChecked():
            self._render()

    @staticmethod
    def _open(path: Path | None) -> None:
        if path is not None and path.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))


class ReportsTab(QWidget):
    """Pick a run and browse the ADARI reports EDPS made for it."""

    def __init__(self, output_dir: Callable[[], Path], parent: QWidget | None = None):
        super().__init__(parent)
        self._output_dir = output_dir
        self._reports: list[Report] = []
        self._token = 0
        self._request: tuple[Path, tuple] = (Path(), ())
        self._cache: dict[Path, tuple[tuple, list[Report]]] = {}

        self.run_combo = QComboBox()
        self.run_combo.setProperty("fontsize", "large")
        self.run_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon)
        self.run_combo.currentIndexChanged.connect(self._load)
        browse = QPushButton("Browse…")
        browse.setProperty("role", "browse")
        browse.clicked.connect(self._browse)
        self.refresh_btn = refresh = QPushButton("Refresh")
        refresh.setProperty("role", "browse")
        refresh.clicked.connect(lambda: self.show_run(self.current_run(), force=True))
        top = QHBoxLayout()
        run_label = QLabel("Run:")
        run_label.setProperty("fontsize", "large")
        top.addWidget(run_label)
        top.addWidget(self.run_combo, 1)
        top.addWidget(browse)
        top.addWidget(refresh)

        self.status = QLabel()
        self.status.setProperty("fontsize", "large")
        self.status.setWordWrap(True)
        self.viewer = ReportViewer()
        lay = QVBoxLayout(self)
        lay.addLayout(top)
        lay.addWidget(self.status)
        lay.addWidget(self.viewer, 1)

    def current_run(self) -> Path | None:
        data = self.run_combo.currentData()
        return Path(data) if data else None

    def show_run(self, run: Path | None = None, force: bool = False) -> None:
        """Re-list the runs and show *run* (default: the newest); *force* rescans a cached run."""
        runs = reports.list_runs(self._output_dir())
        if run is not None and run not in runs:
            runs.insert(0, run)
        self.run_combo.blockSignals(True)
        self.run_combo.clear()
        for r in runs:
            self.run_combo.addItem(r.name, str(r))
            self.run_combo.setItemData(self.run_combo.count() - 1, str(r), Qt.ItemDataRole.ToolTipRole)
        if run is not None:
            self.run_combo.setCurrentIndex(runs.index(run))
        self.run_combo.blockSignals(False)
        self._load(force)

    def reports_for(self, task: str) -> list[Report]:
        return [r for r in self._reports if r.task == task]

    def showEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        super().showEvent(event)
        if self.run_combo.count() == 0:
            self.show_run()

    def _browse(self) -> None:
        start = str(self.current_run() or self._output_dir())
        chosen = QFileDialog.getExistingDirectory(self, "Select a run folder", start)
        if chosen:
            self.show_run(Path(chosen))

    @staticmethod
    def _stamp(run: Path) -> tuple:
        """What *run*'s reports depend on: its index, or lacking one EDPS's job folders."""
        try:
            return ((run / reports.INDEX_NAME).stat().st_mtime,)
        except OSError:
            pass
        base = edps_base_dir(run)
        return (str(base), reports.jobs_stamp(base)) if base is not None else (None,)

    def _load(self, force: bool = False) -> None:
        self._token += 1  # supersedes any scan still running
        run = self.current_run()
        if run is None:
            self._apply(None, [])
            return
        stamp = self._stamp(run)
        cached = self._cache.get(run)
        if not force and cached is not None and cached[0] == stamp:
            self._apply(run, cached[1])
            return
        self.status.setText("Loading reports…")
        self._reports = []  # reports_for() must not answer with the previous run's
        self.viewer.set_reports([])

        def scan() -> list[Report]:  # the EDPS config is only needed for runs made before indexing
            indexed = (run / reports.INDEX_NAME).is_file()
            return reports.load_run(run, None if indexed else edps_base_dir(run))

        self._request = (run, stamp)
        start_job(scan, self._token, self._loaded)

    def _loaded(self, token: int, result: object, error: object) -> None:
        if token != self._token:  # a newer request superseded this one
            return
        run, stamp = self._request
        if error is not None:
            self._reports = []
            self.status.setText(f"Could not load the reports of {run.name}: {error}")
            return
        self._cache[run] = (stamp, result)  # pyright: ignore[reportArgumentType]
        self._apply(run, result)  # pyright: ignore[reportArgumentType]

    def _apply(self, run: Path | None, items: list[Report]) -> None:
        self._reports = items
        images = sum(len(r.panels) for r in items)
        failed = sum(bool(r.error) for r in items)
        if run is None:
            self.status.setText(f"No runs in {self._output_dir()} yet.")
        elif not items:
            self.status.setText(
                "No ADARI reports for this run. EDPS makes them when the pipeline's workflows declare "
                "reports and the run's report type is not 'none'."
            )
        else:
            jobs = len({r.job_dir for r in items})
            self.status.setText(
                f"{images} image(s) from {len(items)} report(s) in {jobs} EDPS job(s)"
                + (f"; {failed} report(s) failed" if failed else "")
            )
        self.viewer.set_reports(items)


class ReportsDialog(QDialog):
    """One task's reports, opened from the Data flow window."""

    def __init__(self, task: str, run: Path | None, items: list[Report], parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle(f"Reports — {task}" + (f" ({run.name})" if run else ""))
        self.resize(1000, 750)
        self.viewer = ReportViewer()
        lay = QVBoxLayout(self)
        if not items:
            lay.addWidget(
                QLabel(f"No ADARI reports for {task} in " + (str(run) if run else "the selected run") + ".")
            )
        lay.addWidget(self.viewer, 1)
        self.viewer.set_reports(items)
