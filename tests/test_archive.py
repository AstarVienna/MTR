"""
Unit tests for archive.py.

Covers:
  - OS detection
  - Podman availability checks
  - Install command generation
  - Archive stack file generation
  - Missing calibration identification
  - Container exec command construction
"""

import json
import subprocess
import textwrap
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import archive


# ---------------------------------------------------------------------------
# OS detection
# ---------------------------------------------------------------------------


class TestDetectOs:
    def test_linux_ubuntu(self, tmp_path):
        os_release = tmp_path / "os-release"
        os_release.write_text('ID=ubuntu\nVERSION_ID="22.04"\n')
        with patch("archive.platform.system", return_value="Linux"), \
             patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "read_text",
                          return_value='ID=ubuntu\nVERSION_ID="22.04"\n'):
            family, distro = archive.detect_os()
            assert family == "linux"
            assert distro == "ubuntu"

    def test_linux_arch(self):
        with patch("archive.platform.system", return_value="Linux"), \
             patch("archive.Path.exists", return_value=True), \
             patch("archive.Path.read_text", return_value="ID=arch\n"):
            family, distro = archive.detect_os()
            assert family == "linux"
            assert distro == "arch"

    def test_linux_no_os_release(self):
        with patch("archive.platform.system", return_value="Linux"), \
             patch("archive.Path.exists", return_value=False):
            family, distro = archive.detect_os()
            assert family == "linux"
            assert distro == "unknown"

    def test_macos(self):
        with patch("archive.platform.system", return_value="Darwin"):
            family, distro = archive.detect_os()
            assert family == "darwin"
            assert distro == "macos"

    def test_windows(self):
        with patch("archive.platform.system", return_value="Windows"):
            family, distro = archive.detect_os()
            assert family == "windows"
            assert distro == "unknown"


# ---------------------------------------------------------------------------
# Podman availability
# ---------------------------------------------------------------------------


class TestPodmanAvailable:
    def test_available(self):
        with patch("archive.subprocess.run") as mock:
            mock.return_value = MagicMock(returncode=0)
            assert archive.podman_available() is True
            mock.assert_called_once()

    def test_not_installed(self):
        with patch("archive.subprocess.run", side_effect=FileNotFoundError):
            assert archive.podman_available() is False

    def test_timeout(self):
        with patch("archive.subprocess.run",
                   side_effect=subprocess.TimeoutExpired("podman", 10)):
            assert archive.podman_available() is False


class TestPodmanComposeAvailable:
    def test_available(self):
        with patch("archive.subprocess.run") as mock:
            mock.return_value = MagicMock(returncode=0)
            assert archive.podman_compose_available() is True

    def test_not_installed(self):
        with patch("archive.subprocess.run", side_effect=FileNotFoundError):
            assert archive.podman_compose_available() is False


# ---------------------------------------------------------------------------
# Install command
# ---------------------------------------------------------------------------


class TestInstallPodmanCommands:
    def test_ubuntu_as_non_root(self):
        with patch("archive.os.getuid", return_value=1000), \
             patch("archive.shutil.which", return_value="/usr/bin/sudo"):
            cmds = archive.install_podman_commands("ubuntu")
            assert len(cmds) == 2
            # First command: apt-get update
            assert cmds[0][0] == "sudo"
            assert "apt-get" in cmds[0]
            assert "update" in cmds[0]
            # Second command: apt-get install
            assert cmds[1][0] == "sudo"
            assert "apt-get" in cmds[1]
            assert "podman" in cmds[1]
            assert "podman-compose" in cmds[1]

    def test_ubuntu_as_root_skips_sudo(self):
        with patch("archive.os.getuid", return_value=0):
            cmds = archive.install_podman_commands("ubuntu")
            assert len(cmds) == 2
            assert cmds[0][0] == "apt-get"
            for cmd in cmds:
                assert "sudo" not in cmd

    def test_non_root_without_sudo_raises(self):
        with patch("archive.os.getuid", return_value=1000), \
             patch("archive.shutil.which", return_value=None):
            with pytest.raises(RuntimeError, match="Root privileges"):
                archive.install_podman_commands("ubuntu")

    def test_fedora(self):
        with patch("archive.os.getuid", return_value=0):
            cmds = archive.install_podman_commands("fedora")
            assert len(cmds) == 1
            assert "dnf" in cmds[0]
            assert "podman" in cmds[0]

    def test_arch(self):
        with patch("archive.os.getuid", return_value=0):
            cmds = archive.install_podman_commands("arch")
            assert len(cmds) == 1
            assert "pacman" in cmds[0]
            assert "podman" in cmds[0]

    def test_macos(self):
        with patch("archive.os.getuid", return_value=1000), \
             patch("archive.shutil.which", return_value="/usr/bin/sudo"):
            cmds = archive.install_podman_commands("macos")
            assert len(cmds) == 1
            assert cmds[0][0] == "brew"
            assert "podman" in cmds[0]

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unsupported distribution"):
            archive.install_podman_commands("gentoo")


# ---------------------------------------------------------------------------
# Archive stack file generation
# ---------------------------------------------------------------------------


class TestEnsureArchiveStackFiles:
    def test_creates_all_files(self, tmp_path):
        compose_dir = tmp_path / "stack"
        result = archive.ensure_archive_stack_files(compose_dir, "testimg")
        assert result == compose_dir
        assert (compose_dir / "compose.yml").exists()
        assert (compose_dir / "Containerfile").exists()
        assert (compose_dir / "scripts" / "ds.cfg").exists()
        assert (compose_dir / "scripts" / "entrypoint_dataserver.sh").exists()
        assert (compose_dir / "scripts" / "Environment.cfg").exists()
        assert (compose_dir / "space" / "inbox").is_dir()
        assert (compose_dir / "space" / "outbox").is_dir()

    def test_compose_contains_image_name(self, tmp_path):
        compose_dir = tmp_path / "stack"
        archive.ensure_archive_stack_files(compose_dir, "myimage")
        content = (compose_dir / "compose.yml").read_text()
        assert "myimage:latest" in content

    def test_does_not_overwrite_existing(self, tmp_path):
        compose_dir = tmp_path / "stack"
        archive.ensure_archive_stack_files(compose_dir, "img1")
        (compose_dir / "compose.yml").write_text("custom content")
        archive.ensure_archive_stack_files(compose_dir, "img2")
        assert (compose_dir / "compose.yml").read_text() == "custom content"

    def test_entrypoint_is_executable(self, tmp_path):
        compose_dir = tmp_path / "stack"
        archive.ensure_archive_stack_files(compose_dir)
        sh = compose_dir / "scripts" / "entrypoint_dataserver.sh"
        assert sh.stat().st_mode & 0o111  # has execute bits


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


# ---------------------------------------------------------------------------
# Archive image check
# ---------------------------------------------------------------------------


class TestArchiveImageExists:
    def test_exists(self):
        with patch("archive.subprocess.run") as mock:
            mock.return_value = MagicMock(returncode=0)
            assert archive.archive_image_exists("myimg") is True

    def test_not_exists(self):
        with patch("archive.subprocess.run") as mock:
            mock.return_value = MagicMock(returncode=1)
            assert archive.archive_image_exists("myimg") is False


# ---------------------------------------------------------------------------
# Stack status
# ---------------------------------------------------------------------------


class TestArchiveStackStatus:
    def test_parses_json_output(self, tmp_path):
        containers = [
            {"Service": "postgres", "State": "running"},
            {"Service": "dataserver", "State": "running"},
        ]
        with patch("archive.subprocess.run") as mock:
            mock.return_value = MagicMock(
                returncode=0, stdout=json.dumps(containers),
            )
            status = archive.archive_stack_status(tmp_path)
            assert status == {"postgres": "running", "dataserver": "running"}

    def test_returns_empty_on_failure(self, tmp_path):
        with patch("archive.subprocess.run") as mock:
            mock.return_value = MagicMock(returncode=1, stdout="")
            status = archive.archive_stack_status(tmp_path)
            assert status == {}

    def test_handles_not_installed(self, tmp_path):
        with patch("archive.subprocess.run", side_effect=FileNotFoundError):
            status = archive.archive_stack_status(tmp_path)
            assert status == {}


# ---------------------------------------------------------------------------
# Compose exec helper
# ---------------------------------------------------------------------------


class TestComposeExec:
    def test_calls_podman_compose_exec(self, tmp_path):
        with patch("archive.subprocess.run") as mock:
            mock.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
            rc, out, err = archive._compose_exec(
                "print('hello')", compose_dir=tmp_path,
            )
            assert rc == 0
            assert out == "ok"
            cmd = mock.call_args[0][0]
            assert cmd[:3] == ["podman-compose", "exec", "-T"]
            assert "dataserver" in cmd
            assert "python" in cmd
