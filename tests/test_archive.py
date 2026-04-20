"""
Unit tests for archive.py.

Covers:
  - MetisWISE availability check
  - Install command generation
  - Database connection setup
  - Environment.cfg read/write helpers
  - Query / download with mocked MetisWISE
  - Missing calibration identification
"""

from unittest.mock import patch, MagicMock

import archive

# Mocks for the commonwise database modules imported by _ensure_db_connection().
_DB_MOCKS = {
    "common": MagicMock(),
    "common.config": MagicMock(),
    "common.config.Profile": MagicMock(),
    "common.database": MagicMock(),
    "common.database.Database": MagicMock(),
}

# Mocks for the modules imported by _ensure_metiswise_imports().
_IMPORT_MOCKS = {
    "codes": MagicMock(),
    "codes.drld_parser": MagicMock(),
    "codes.drld_parser.data_reduction_library_design": MagicMock(),
    "metiswise.main.aweimports": MagicMock(),
}


# ---------------------------------------------------------------------------
# MetisWISE availability
# ---------------------------------------------------------------------------


class TestMetisWISEAvailable:
    def test_available(self):
        with patch.dict("sys.modules", {"metiswise": MagicMock()}):
            assert archive.metiswise_available() is True

    def test_not_installed(self):
        with patch.dict("sys.modules", {"metiswise": None}):
            assert archive.metiswise_available() is False


# ---------------------------------------------------------------------------
# Install command
# ---------------------------------------------------------------------------


class TestInstallMetisWiseCommand:
    def test_command_structure(self):
        cmd = archive.install_metiswise_command("user:secret")
        assert cmd[0:3] == ["uv", "pip", "install"]
        assert "--python" in cmd
        assert "metiswise" in cmd

    def test_credentials_in_url(self):
        cmd = archive.install_metiswise_command("alice:p4ss")
        urls = [arg for arg in cmd if "entropynaut" in arg]
        assert len(urls) == 1
        assert "alice:p4ss@pip.entropynaut.com" in urls[0]

    def test_extra_index_urls(self):
        cmd = archive.install_metiswise_command("u:p")
        idx_flags = [i for i, a in enumerate(cmd) if a == "--extra-index-url"]
        assert len(idx_flags) == 3
        urls = [cmd[i + 1] for i in idx_flags]
        assert any("ftp.eso.org" in u for u in urls)
        assert any("ivh.github.io" in u for u in urls)
        assert any("entropynaut" in u for u in urls)

    def test_uses_unsafe_best_match(self):
        cmd = archive.install_metiswise_command("u:p")
        assert "--index-strategy" in cmd
        idx = cmd.index("--index-strategy")
        assert cmd[idx + 1] == "unsafe-best-match"

    def test_targets_project_venv(self):
        cmd = archive.install_metiswise_command("u:p")
        python_idx = cmd.index("--python")
        python_path = cmd[python_idx + 1]
        assert ".venv" in python_path
        assert python_path.endswith("python")

    def test_does_not_include_pymetis(self):
        cmd = archive.install_metiswise_command("u:p")
        git_args = [a for a in cmd if a.startswith("git+")]
        assert len(git_args) == 0


# ---------------------------------------------------------------------------
# Database connection setup
# ---------------------------------------------------------------------------


class TestEnsureDbConnection:
    def test_creates_profile_and_connects(self):
        mock_profiles = MagicMock()
        mock_database = MagicMock()

        with patch.dict("sys.modules", {
            "common": MagicMock(),
            "common.config": MagicMock(),
            "common.config.Profile": MagicMock(profiles=mock_profiles),
            "common.database": MagicMock(),
            "common.database.Database": MagicMock(database=mock_database),
        }):
            import importlib
            importlib.reload(archive)

            archive._ensure_db_connection()
            mock_profiles.create_profile.assert_called_once()
            mock_database.connect.assert_called_once()

            importlib.reload(archive)

    def test_idempotent_within_thread(self):
        mock_profiles = MagicMock()
        mock_database = MagicMock()

        with patch.dict("sys.modules", {
            "common": MagicMock(),
            "common.config": MagicMock(),
            "common.config.Profile": MagicMock(profiles=mock_profiles),
            "common.database": MagicMock(),
            "common.database.Database": MagicMock(database=mock_database),
        }):
            import importlib
            importlib.reload(archive)

            archive._ensure_db_connection()
            archive._ensure_db_connection()
            mock_profiles.create_profile.assert_called_once()
            mock_database.connect.assert_called_once()

            importlib.reload(archive)

    def test_noop_when_commonwise_missing(self):
        # Force ImportError on any `import common...` even though the package
        # may happen to be installed in the test environment.
        with patch.dict("sys.modules", {
            "common": None,
            "common.config": None,
            "common.config.Profile": None,
            "common.database": None,
            "common.database.Database": None,
        }):
            import importlib
            importlib.reload(archive)

            archive._ensure_db_connection()
            assert archive._thread_local.db_ready is True

            importlib.reload(archive)


class TestResetDbConnection:
    def test_clears_flag(self):
        archive._thread_local.db_ready = True
        archive.reset_db_connection()
        assert not getattr(archive._thread_local, "db_ready", False)


# ---------------------------------------------------------------------------
# Environment.cfg read/write helpers
# ---------------------------------------------------------------------------

_FIVE = {
    "database_user":            "AWTEST",
    "database_password":        "lmno",
    "project":                  "SIM",
    "database_tablespacename":  "metis_data",
    "database_name":            "metis.example.com:5436/pgmetis",
}


class TestWriteEnvCfg:
    def test_creates_new_file(self, tmp_path):
        with patch("archive.Path.home", return_value=tmp_path):
            cfg = archive.write_env_cfg(**_FIVE)
        assert cfg.exists()
        text = cfg.read_text()
        assert text.startswith("[global]")
        for key, value in _FIVE.items():
            assert f"{key} : {value}" in text

    def test_creates_awe_dir(self, tmp_path):
        with patch("archive.Path.home", return_value=tmp_path):
            archive.write_env_cfg(**_FIVE)
        assert (tmp_path / ".awe").is_dir()

    def test_only_five_keys_when_creating(self, tmp_path):
        """Nothing else is written — data_server, port, protocol, etc.
        inherit from the MetisWISE default."""
        with patch("archive.Path.home", return_value=tmp_path):
            cfg = archive.write_env_cfg(**_FIVE)
        text = cfg.read_text()
        assert "data_server" not in text
        assert "data_port" not in text
        assert "data_protocol" not in text
        assert "database_engine" not in text

    def test_patches_existing_global_section(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        cfg = awe / "Environment.cfg"
        cfg.write_text(
            "# a comment\n"
            "[global]\n"
            "database_user : OLDUSER\n"
            "database_password : OLDPASS\n"
            "project : OLDPROJ\n"
            "database_tablespacename : oldspace\n"
            "database_name : old.example.com/db\n"
            "data_server : remote.example.com\n"
            "data_port : 8013\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            archive.write_env_cfg(**_FIVE)
        text = cfg.read_text()
        # Updated values present
        for key, value in _FIVE.items():
            assert f"{key} : {value}" in text
        # Unrelated keys preserved
        assert "data_server : remote.example.com" in text
        assert "data_port : 8013" in text
        # Comment preserved
        assert "# a comment" in text
        # Old values gone
        assert "OLDUSER" not in text
        assert "old.example.com" not in text

    def test_appends_missing_keys_to_existing_global(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        cfg = awe / "Environment.cfg"
        cfg.write_text(
            "[global]\n"
            "database_user : A\n"
            "data_server : remote.example.com\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            archive.write_env_cfg(**_FIVE)
        text = cfg.read_text()
        for key, value in _FIVE.items():
            assert f"{key} : {value}" in text
        assert "data_server : remote.example.com" in text

    def test_creates_global_when_absent(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        cfg = awe / "Environment.cfg"
        cfg.write_text("[other]\nkey : value\n")
        with patch("archive.Path.home", return_value=tmp_path):
            archive.write_env_cfg(**_FIVE)
        text = cfg.read_text()
        assert "[other]" in text
        assert "[global]" in text
        for key, value in _FIVE.items():
            assert f"{key} : {value}" in text

    def test_handles_equals_separator(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        cfg = awe / "Environment.cfg"
        cfg.write_text(
            "[global]\n"
            "database_user = OLDUSER\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            archive.write_env_cfg(**_FIVE)
        text = cfg.read_text()
        assert "database_user = AWTEST" in text


class TestReadEnvCfg:
    def test_missing_file(self, tmp_path):
        with patch("archive.Path.home", return_value=tmp_path):
            values = archive.read_env_cfg()
        assert values == {k: "" for k in archive.ENV_CFG_FIELDS}

    def test_populated_file(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        (awe / "Environment.cfg").write_text(
            "[global]\n"
            "database_user : AWTEST\n"
            "database_password : lmno\n"
            "project : SIM\n"
            "database_tablespacename : ts\n"
            "database_name : metis.example.com:5436/pgmetis\n"
            "data_server : remote.example.com\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            values = archive.read_env_cfg()
        assert values["database_user"] == "AWTEST"
        assert values["database_password"] == "lmno"
        assert values["project"] == "SIM"
        assert values["database_tablespacename"] == "ts"
        assert values["database_name"] == "metis.example.com:5436/pgmetis"

    def test_ignores_other_sections(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        (awe / "Environment.cfg").write_text(
            "[other]\n"
            "database_user : LEAKED\n"
            "[global]\n"
            "database_user : CORRECT\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            values = archive.read_env_cfg()
        assert values["database_user"] == "CORRECT"

    def test_round_trip(self, tmp_path):
        with patch("archive.Path.home", return_value=tmp_path):
            archive.write_env_cfg(**_FIVE)
            values = archive.read_env_cfg()
        assert values == _FIVE


# ---------------------------------------------------------------------------
# Query archive (mocked MetisWISE)
# ---------------------------------------------------------------------------


class TestQueryArchive:
    def test_query_all(self):
        mock_item = MagicMock()
        mock_item.filename = "test.fits"
        mock_item.pro_catg = "MASTER_DARK"
        type(mock_item).__name__ = "Pro"

        mock_dataitem = MagicMock()
        mock_dataitem.select_all.return_value = [mock_item]

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=mock_dataitem),
            "metiswise.main.pro": MagicMock(),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive()
            assert len(items) == 1
            assert items[0]["filename"] == "test.fits"

            importlib.reload(archive)

    def test_query_by_category(self):
        mock_item = MagicMock()
        mock_item.filename = "master.fits"
        mock_item.pro_catg = "MASTER_DARK"

        class _DataItem:
            pass

        class MASTER_DARK(_DataItem):
            @classmethod
            def select_all(cls):
                return [mock_item]

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=_DataItem),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive(category="MASTER_DARK")
            assert len(items) == 1
            assert items[0]["filename"] == "master.fits"

            importlib.reload(archive)

    def test_query_raw_category(self):
        mock_item = MagicMock()
        mock_item.filename = "raw.fits"
        mock_item.pro_catg = AttributeError  # raw items have no pro_catg
        del mock_item.pro_catg

        class _DataItem:
            pass

        class IFU_SCI_RAW(_DataItem):
            @classmethod
            def select_all(cls):
                return [mock_item]

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=_DataItem),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive(category="IFU_SCI_RAW")
            assert len(items) == 1
            assert items[0]["filename"] == "raw.fits"
            assert items[0]["pro_catg"] == ""

            importlib.reload(archive)

    def test_query_unknown_category(self):
        class _DataItem:
            pass

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=_DataItem),
        }):
            import importlib
            importlib.reload(archive)

            logs = []
            items = archive.query_archive(
                category="NONEXISTENT", on_log=logs.append,
            )
            assert items == []
            assert any("Unknown category" in msg for msg in logs)

            importlib.reload(archive)

    def test_query_resolves_nested_subclass(self):
        mock_item = MagicMock()
        mock_item.filename = "nested.fits"
        mock_item.pro_catg = "LINEARITY_2RG"

        class _DataItem:
            pass

        class _Intermediate(_DataItem):
            pass

        class LINEARITY_2RG(_Intermediate):
            @classmethod
            def select_all(cls):
                return [mock_item]

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=_DataItem),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive(category="LINEARITY_2RG")
            assert len(items) == 1
            assert items[0]["filename"] == "nested.fits"

            importlib.reload(archive)


# ---------------------------------------------------------------------------
# Download file (mocked MetisWISE)
# ---------------------------------------------------------------------------


class TestDownloadFile:
    def test_download_success(self, tmp_path):
        src_dir = tmp_path / "retrieve_dir"
        src_dir.mkdir()
        (src_dir / "data.fits").write_bytes(b"fits data")

        mock_item = MagicMock()
        mock_item.filename = "data.fits"
        mock_item.pathname = str(src_dir)
        mock_item.retrieve = MagicMock()

        mock_dataitem = MagicMock()
        mock_dataitem.filename.__eq__ = MagicMock(return_value=[mock_item])

        dest_dir = tmp_path / "downloads"

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=mock_dataitem),
        }):
            import importlib
            importlib.reload(archive)

            result = archive.download_file("data.fits", dest_dir)
            assert result is not None
            assert result == dest_dir / "data.fits"
            assert result.exists()
            mock_item.retrieve.assert_called_once()

            importlib.reload(archive)

    def test_download_not_found(self, tmp_path):
        mock_dataitem = MagicMock()
        mock_dataitem.filename.__eq__ = MagicMock(return_value=[])

        dest_dir = tmp_path / "downloads"

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=mock_dataitem),
        }):
            import importlib
            importlib.reload(archive)

            result = archive.download_file("missing.fits", dest_dir)
            assert result is None

            importlib.reload(archive)


# ---------------------------------------------------------------------------
# Missing calibration identification
# ---------------------------------------------------------------------------


class TestIdentifyMissingCalibrations:
    """Test the pure-logic calibration gap detection."""

    def test_no_gaps_when_all_present(self):
        all_tags = {
            "DETLIN_IFU_RAW", "DARK_IFU_RAW", "IFU_DISTORTION_RAW",
            "IFU_WAVE_RAW", "IFU_RSRF_RAW", "IFU_STD_RAW",
        }
        missing = archive.identify_missing_calibrations(
            "metis.metis_ifu_wkf", all_tags, has_science=False,
        )
        assert missing == []

    def test_detects_upstream_gap(self):
        missing = archive.identify_missing_calibrations(
            "metis.metis_ifu_wkf",
            data_tags={"IFU_RSRF_RAW"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert "metis_ifu_lingain" in task_names
        assert "metis_ifu_dark" in task_names
        assert "metis_ifu_distortion" in task_names
        assert "metis_ifu_wavecal" in task_names
        assert "metis_ifu_rsrf" not in task_names

    def test_lm_img_partial(self):
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_FLAT_LAMP_RAW"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert "metis_lm_img_lingain" in task_names
        assert "metis_lm_img_dark" in task_names
        assert "metis_lm_img_flat" not in task_names

    def test_empty_data_tags(self):
        missing = archive.identify_missing_calibrations(
            "metis.metis_ifu_wkf", set(), has_science=False,
        )
        assert missing == []

    def test_unknown_workflow(self):
        missing = archive.identify_missing_calibrations(
            "metis.nonexistent_wkf", {"FOO"}, has_science=False,
        )
        assert missing == []

    def test_science_tasks_ignored(self):
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_DISTORTION_RAW", "LM_IMAGE_SCI_RAW"},
            has_science=True,
        )
        task_names = [t for t, _ in missing]
        assert "metis_lm_img_lingain" in task_names
        assert "metis_lm_img_dark" in task_names
        assert "metis_lm_img_flat" in task_names
        assert "metis_lm_img_basic_reduce_sci" not in task_names
        assert "metis_lm_img_basic_reduce_std" not in task_names

    def test_master_pro_catg_covers_task(self):
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_FLAT_LAMP_RAW", "MASTER_DARK_2RG", "LINEARITY_2RG"},
            has_science=False,
        )
        assert missing == []

    def test_master_fills_partial_gap(self):
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_FLAT_LAMP_RAW", "MASTER_DARK_2RG"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert task_names == ["metis_lm_img_lingain"]
        assert missing == [("metis_lm_img_lingain", "LINEARITY_2RG")]

    def test_only_masters_no_raw(self):
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={
                "MASTER_DARK_2RG",
                "LINEARITY_2RG",
                "MASTER_IMG_FLAT_LAMP_LM",
            },
            has_science=False,
        )
        assert missing == []

    def test_raw_and_master_same_task(self):
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"DARK_2RG_RAW", "MASTER_DARK_2RG"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert task_names == ["metis_lm_img_lingain"]
        assert "metis_lm_img_dark" not in task_names
