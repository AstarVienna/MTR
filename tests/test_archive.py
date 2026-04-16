"""
Unit tests for archive.py.

Covers:
  - MetisWISE availability check
  - Install command generation
  - Stale Environment.cfg detection
  - Upload / query / download with mocked MetisWISE
  - Missing calibration identification
"""

import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import archive


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
# Stale Environment.cfg detection
# ---------------------------------------------------------------------------


class TestCheckStaleEnvironmentCfg:
    def test_no_file(self, tmp_path):
        with patch("archive.Path.home", return_value=tmp_path):
            assert archive.check_stale_environment_cfg() is None

    def test_production_config_no_warning(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        (awe / "Environment.cfg").write_text(
            "data_server : metis-ds.hpc.rug.nl\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            assert archive.check_stale_environment_cfg() is None

    def test_stale_container_config_warns(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        (awe / "Environment.cfg").write_text(
            "data_server : dataserver\ndata_port : 8013\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            warning = archive.check_stale_environment_cfg()
            assert warning is not None
            assert "dataserver" in warning


# ---------------------------------------------------------------------------
# Upload files (mocked MetisWISE)
# ---------------------------------------------------------------------------


class TestUploadFiles:
    def test_upload_raw_file(self, tmp_path):
        fits_file = tmp_path / "raw.fits"
        fits_file.write_bytes(b"dummy")

        mock_fits = MagicMock()
        mock_hdus = MagicMock()
        mock_hdus.__getitem__ = MagicMock(
            return_value=MagicMock(header={"ESO DPR CATG": "FLAT"})
        )
        mock_fits.open.return_value = mock_hdus

        mock_raw_cls = MagicMock()
        mock_raw_inst = MagicMock()
        mock_raw_cls.return_value = mock_raw_inst

        mock_astropy_io = MagicMock(fits=mock_fits)
        mock_mw_main = MagicMock()
        mock_mw_main.raw = MagicMock(Raw=mock_raw_cls)
        mock_mw_main.pro = MagicMock()

        with patch.dict("sys.modules", {
            "astropy": MagicMock(),
            "astropy.io": mock_astropy_io,
            "astropy.io.fits": mock_fits,
            "metiswise": MagicMock(),
            "metiswise.main": mock_mw_main,
            "metiswise.main.raw": MagicMock(Raw=mock_raw_cls),
            "metiswise.main.pro": MagicMock(),
        }):
            import importlib
            importlib.reload(archive)

            result = archive.upload_files([fits_file])
            assert result == ["raw.fits"]
            mock_raw_inst.store.assert_called_once()
            mock_raw_inst.commit.assert_called_once()

            importlib.reload(archive)

    def test_upload_skips_unknown_header(self, tmp_path):
        fits_file = tmp_path / "unknown.fits"
        fits_file.write_bytes(b"dummy")

        mock_fits = MagicMock()
        mock_hdus = MagicMock()
        mock_hdus.__getitem__ = MagicMock(
            return_value=MagicMock(header={})
        )
        mock_fits.open.return_value = mock_hdus

        with patch.dict("sys.modules", {
            "astropy": MagicMock(),
            "astropy.io": MagicMock(),
            "astropy.io.fits": mock_fits,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.raw": MagicMock(),
            "metiswise.main.pro": MagicMock(),
        }):
            import importlib
            importlib.reload(archive)

            logs = []
            result = archive.upload_files([fits_file], on_log=logs.append)
            assert result == []
            assert any("Skipping" in msg for msg in logs)

            importlib.reload(archive)


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
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.aweimports": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=mock_dataitem),
            "metiswise.main.pro": MagicMock(),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive()
            assert len(items) == 1
            assert items[0]["filename"] == "test.fits"

            importlib.reload(archive)

    def test_query_by_pro_catg(self):
        mock_item = MagicMock()
        mock_item.filename = "master.fits"
        mock_item.pro_catg = "MASTER_DARK"
        type(mock_item).__name__ = "Pro"

        mock_pro = MagicMock()
        mock_pro.pro_catg.__eq__ = MagicMock(return_value=[mock_item])

        with patch.dict("sys.modules", {
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.aweimports": MagicMock(),
            "metiswise.main.dataitem": MagicMock(),
            "metiswise.main.pro": MagicMock(Pro=mock_pro),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive(pro_catg="MASTER_DARK")
            assert len(items) == 1
            assert items[0]["filename"] == "master.fits"

            importlib.reload(archive)


# ---------------------------------------------------------------------------
# Download file (mocked MetisWISE)
# ---------------------------------------------------------------------------


class TestDownloadFile:
    def test_download_success(self, tmp_path):
        # Create a fake source file that MetisWISE would "retrieve"
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
        # IFU workflow: provide all raw tags
        all_tags = {
            "DETLIN_IFU_RAW", "DARK_IFU_RAW", "IFU_DISTORTION_RAW",
            "IFU_WAVE_RAW", "IFU_RSRF_RAW", "IFU_STD_RAW",
        }
        missing = archive.identify_missing_calibrations(
            "metis.metis_ifu_wkf", all_tags, has_science=False,
        )
        assert missing == []

    def test_detects_upstream_gap(self):
        # Only have the rsrf raw — lingain, dark, distortion, wavecal
        # are upstream and missing.
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
        # rsrf itself is present, so it should NOT be listed
        assert "metis_ifu_rsrf" not in task_names

    def test_lm_img_partial(self):
        # Only have flat raw — lingain and dark are missing
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
        # Science tasks should not contribute to the missing list
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_DISTORTION_RAW", "LM_IMAGE_SCI_RAW"},
            has_science=True,
        )
        task_names = [t for t, _ in missing]
        # lingain, dark, flat are upstream of distortion
        assert "metis_lm_img_lingain" in task_names
        assert "metis_lm_img_dark" in task_names
        assert "metis_lm_img_flat" in task_names
        # Science tasks should not appear
        assert "metis_lm_img_basic_reduce_sci" not in task_names
        assert "metis_lm_img_basic_reduce_std" not in task_names

    def test_master_pro_catg_covers_task(self):
        # User has flat raw + master dark + master linearity: nothing to fetch
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_FLAT_LAMP_RAW", "MASTER_DARK_2RG", "LINEARITY_2RG"},
            has_science=False,
        )
        assert missing == []

    def test_master_fills_partial_gap(self):
        # Flat raw + master dark, but no linearity master or raw — only
        # lingain should still be listed as missing.
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_FLAT_LAMP_RAW", "MASTER_DARK_2RG"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert task_names == ["metis_lm_img_lingain"]
        assert missing == [("metis_lm_img_lingain", "LINEARITY_2RG")]

    def test_only_masters_no_raw(self):
        # All-masters coverage up through flat: flat is the deepest covered
        # task (via its PRO.CATG), and dark + lingain are also covered.
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
        # Providing both the raw and the master for dark must not confuse
        # the gap walk — lingain is still missing.
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"DARK_2RG_RAW", "MASTER_DARK_2RG"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert task_names == ["metis_lm_img_lingain"]
        assert "metis_lm_img_dark" not in task_names
