"""The Archive tab."""

# PyQt6 stubs type most widget getters as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

from pathlib import Path

from PyQt6.QtCore import (
    QSettings,
    Qt,
    QThread,
)
from PyQt6.QtGui import QColor, QFont, QPalette
from PyQt6.QtWidgets import (
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
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSplitter,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from .. import tables
from .widgets import log_append
from .workers import (
    DownloadWorker,
    MetisWISEInstallWorker,
    QueryWorker,
    StageWorker,
    TestConnectionWorker,
    UploadWorker,
    WorkerHost,
)

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
        self._staged: set[str] = set()
        self._stage_worker: StageWorker | None = None
        self._astropy: bool | None = None
        self._categories_stale = True
        self._build_ui()
        self._load_settings()
        self._refresh_install_status()

    def showEvent(self, event) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        # Re-check whether MetisWISE is still installed every time the tab
        # becomes visible — re-running the Install tab can remove it (its pip
        # reinstall of the pipeline deps doesn't include MetisWISE), and the
        # user needs the Install button to re-enable in that case.
        super().showEvent(event)
        self._refresh_install_status()
        self._ensure_categories()

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
        self._stack.currentChanged.connect(lambda _i: self._ensure_categories())

    def _build_page_install(self) -> QWidget:
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setSpacing(12)

        desc = QLabel(
            "<b>Install MetisWISE & connect to the remote archive</b><br><br>"
            "The Archive tab talks to the METIS AIT archive via the "
            "<a href='https://github.com/AstarVienna/MetisWISE'>MetisWISE</a> "
            "Python client. Install the package, then fill in the five "
            "database fields from the "
            "<a href='https://metis.strw.leidenuniv.nl/wiki/doku.php?id=ait:archive'>"
            "METIS wiki</a> and click Save &amp; Test. "
            "After a successful test the credentials are stored in your OS "
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
        mw_btn_row = QHBoxLayout()
        self._install_btn = QPushButton("Install MetisWISE")
        self._install_btn.setProperty("role", "success")
        self._install_btn.setMinimumSize(260, 44)
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
            "Stored in your OS keyring after a successful test; leave a field blank to use its stored value."
        )
        cfg_desc.setWordWrap(True)
        cfg_desc.setTextFormat(Qt.TextFormat.RichText)
        cfg_lay.addWidget(cfg_desc)

        self._cfg_edits: dict[str, QLineEdit] = {}
        # fmt: off
        labels = {
            "database_user":            "database_user:",
            "database_password":        "database_password:",
            "project":                  "project:",
            "database_tablespacename":  "database_tablespacename:",
            "database_name":            "database_name:",
        }
        # fmt: on
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

    def _ensure_categories(self) -> None:
        """Fill the category combo from the Pipeline tables once its page is showing."""
        if not self._categories_stale or self._stack.currentIndex() != 1 or not self.isVisible():
            return
        self._categories_stale = False
        text = self._catg_combo.currentText()
        self._catg_combo.clear()
        self._catg_combo.addItem("(all)")
        t = tables.get()
        for catg in sorted(t.product_tags()):
            self._catg_combo.addItem(catg)
        self._catg_combo.insertSeparator(self._catg_combo.count())
        for tag in sorted(t.raw_tags()):
            self._catg_combo.addItem(tag)
        self._catg_combo.setCurrentText(text)

    def refresh_categories(self) -> None:
        """The Pipeline changed: rebuild the category list when next (or now, if) shown."""
        self._categories_stale = True
        self._ensure_categories()

    def _refresh_install_status(self) -> None:
        """Update the MetisWISE status label + Continue button gate."""
        from ..archive import metiswise_available

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
        from ..archive import metiswise_available

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
            self,
            "Archive busy",
            "Another archive operation is still running. Please wait for it to finish.",
        )
        return True

    def _on_install_metiswise(self) -> None:
        if self._reject_if_busy() or self.reject_if_blocked("the MetisWISE install"):
            return
        from ..archive import pipeline_pymetis_installed

        if not pipeline_pymetis_installed():
            QMessageBox.warning(
                self,
                "Install the pipeline first",
                "MetisWISE needs the pipeline's pymetis. Run Install on the Install tab first.",
            )
            return
        self._log.clear()
        self._install_btn.setEnabled(False)
        self._worker = self.track_worker(MetisWISEInstallWorker())
        self._worker.log.connect(lambda t, c: log_append(self._log, t, c))
        self._worker.done.connect(self._on_metiswise_installed)
        self._worker.start()

    def _on_metiswise_installed(self, _success: bool) -> None:
        self._save_settings()
        self._refresh_install_status()

    # ── Save & Test Connection ─────────────────────────────────────────────

    def _on_save_and_test(self) -> None:
        if self._reject_if_busy():
            return
        from ..archive import metiswise_available

        if not metiswise_available():
            QMessageBox.warning(
                self,
                "MetisWISE not installed",
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
            self,
            "Download destination",
            str(Path.cwd()),
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

    def _have_astropy(self) -> bool:
        """Whether files can be classified; says so in the log once if not."""
        if self._astropy is None:
            try:
                import astropy.io.fits  # noqa: F401

                self._astropy = True
            except ImportError:
                self._astropy = False
                log_append(
                    self._log,
                    "astropy is not installed: files cannot be classified, pick classes by hand.\n",
                    "yellow",
                )
        return self._astropy

    def _add_staged_file(self, path: Path) -> None:
        """Classify one file and stage it (synchronous; folders use StageWorker)."""
        if str(path) in self._staged:
            return
        from ..run_metis import classify_fits_file

        self._append_staged(path, classify_fits_file(path) if self._have_astropy() else None)

    def _append_staged(self, path: Path, tag: str | None) -> None:
        if str(path) in self._staged:
            return
        self._staged.add(str(path))
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
            self,
            "Stage FITS files for upload",
            str(Path.cwd()),
            "FITS files (*.fits *.fits.gz)",
        )
        for f in files:
            self._add_staged_file(Path(f))

    def _on_add_upload_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(
            self,
            "Stage folder of FITS files",
            str(Path.cwd()),
        )
        if not folder:
            return
        if self._reject_if_busy():
            return
        log_append(self._log, f"Scanning {folder}…\n", "cyan")
        self._set_staging_busy(True)
        w = self._stage_worker = self.track_worker(
            StageWorker(Path(folder), set(self._staged), self._have_astropy())
        )
        w.log.connect(lambda t, c: log_append(self._log, t, c))
        w.batch.connect(self._on_stage_batch)
        self._begin_progress(0)
        w.progress.connect(self._on_progress)
        w.done.connect(lambda n, f=folder: self._on_stage_done(n, f))
        w.start()

    def _set_staging_busy(self, busy: bool) -> None:
        for btn in (
            self._add_files_btn,
            self._add_folder_btn,
            self._remove_staged_btn,
            self._set_class_btn,
            self._upload_btn,
        ):
            btn.setEnabled(not busy)

    def _on_stage_batch(self, batch: list) -> None:
        for path, tag in batch:
            self._append_staged(path, tag)

    def _on_stage_done(self, found: int, folder: str) -> None:
        self._end_progress()
        self._set_staging_busy(False)
        log_append(self._log, f"Staged {found} file(s) from {folder}\n", "cyan")

    def _on_remove_staged(self) -> None:
        rows = sorted(
            {idx.row() for idx in self._stage_table.selectedIndexes()},
            reverse=True,
        )
        for r in rows:
            self._staged.discard(self._stage_table.item(r, 2).text())
            self._stage_table.removeRow(r)

    def _candidate_class_names(self) -> list[str]:
        from .. import tables

        return sorted(tables.get().raw_tags() | tables.get().product_tags())

    def _on_set_class_selected(self) -> None:
        rows = sorted({idx.row() for idx in self._stage_table.selectedIndexes()})
        if not rows:
            QMessageBox.warning(
                self,
                "No selection",
                "Select one or more rows, then choose a class to apply.",
            )
            return

        dlg = QDialog(self)
        dlg.setWindowTitle("Set DataItem class")
        v = QVBoxLayout(dlg)
        v.addWidget(
            QLabel(
                f"Apply to {len(rows)} selected row(s):",
            )
        )
        combo = QComboBox()
        combo.setEditable(True)
        combo.addItems(self._candidate_class_names())
        v.addWidget(combo)
        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
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
                self,
                "Nothing to upload",
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
                self,
                "Unresolved rows",
                "These files have no DataItem class set. Use "
                "“Set class for selected…” first:\n  " + "\n  ".join(unresolved),
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
        for stale in (
            "archive_cred",
            "archive_cfg_database_user",
            "archive_cfg_database_password",
            "archive_cfg_project",
            "archive_cfg_database_tablespacename",
            "archive_cfg_database_name",
            "archive_db_user",
            "archive_db_pass",
        ):
            self._settings.remove(stale)

        # Pre-populate from a legacy (pre-keyring) ~/.awe/Environment.cfg if
        # present.  Deliberately NO keyring read here: that could pop an
        # unlock prompt at GUI startup, annoying users who never touch the
        # archive.  Stored keyring values are resolved lazily by the workers;
        # post-migration the file is scrubbed and the fields stay blank
        # (placeholders explain that blank = stored keyring value).
        from ..archive import read_env_cfg

        existing = read_env_cfg()
        for key, edit in self._cfg_edits.items():
            edit.setText(existing.get(key, ""))

    def _save_settings(self) -> None:
        # No archive fields are persisted to QSettings. All credentials live
        # in the OS keyring, written by the workers after a successful
        # install / connection test.
        return
