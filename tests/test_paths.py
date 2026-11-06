"""Tests for the user data directory resolution in paths.py."""

import pytest

from metis_test_runner import paths


@pytest.fixture
def mac(tmp_path, monkeypatch):
    """A fake macOS home with no METIS_DATA_DIR or XDG_DATA_HOME."""
    monkeypatch.setattr(paths.sys, "platform", "darwin")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("METIS_DATA_DIR", raising=False)
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.setattr(paths, "_legacy_warned", False)
    return tmp_path


class TestMacDataDir:
    def test_default_has_no_whitespace(self, mac):
        # ~/Library/Application Support would break EDPS's .sof parsing.
        assert paths.data_dir() == mac / ".local" / "share" / "metis-test-runner"

    def test_override_still_wins(self, mac, monkeypatch):
        monkeypatch.setenv("METIS_DATA_DIR", str(mac / "data"))
        assert paths.data_dir() == mac / "data"

    def test_legacy_folder_is_mentioned_once(self, mac, capsys):
        paths.legacy_macos_data_dir().mkdir(parents=True)
        paths.data_dir()
        paths.data_dir()
        err = capsys.readouterr().err
        assert err.count("Application Support") == 1 and "Run Install again" in err

    def test_no_note_without_a_legacy_folder(self, mac, capsys):
        paths.data_dir()
        assert capsys.readouterr().err == ""

    def test_no_note_when_overridden(self, mac, monkeypatch, capsys):
        paths.legacy_macos_data_dir().mkdir(parents=True)
        monkeypatch.setenv("METIS_DATA_DIR", str(mac / "data"))
        paths.data_dir()
        assert capsys.readouterr().err == ""
