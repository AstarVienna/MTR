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


@pytest.fixture
def data(tmp_path, monkeypatch):
    """An empty data dir with no repo env vars."""
    monkeypatch.setenv("METIS_DATA_DIR", str(tmp_path))
    monkeypatch.delenv(paths.PIPELINE_ENV, raising=False)
    monkeypatch.delenv(paths.SIMULATIONS_ENV, raising=False)
    return tmp_path


class TestOwnCheckouts:
    def test_default_is_managed(self, data):
        assert paths.pipeline_dir() == data / "METIS_Pipeline"
        assert paths.user_owned_repos() == {}

    def test_install_tab_choice_round_trips(self, data, tmp_path):
        own = tmp_path / "own"
        paths.save_repo_dir("pipeline", own)
        assert paths.pipeline_dir() == own
        assert paths.simulations_dir() == data / "METIS_Simulations"
        assert paths.user_owned_repos() == {own: paths.INSTALL_TAB}
        paths.save_repo_dir("pipeline", None)
        assert paths.pipeline_dir() == data / "METIS_Pipeline"

    def test_env_var_beats_install_tab(self, data, tmp_path, monkeypatch):
        paths.save_repo_dir("simulations", tmp_path / "tab")
        monkeypatch.setenv(paths.SIMULATIONS_ENV, str(tmp_path / "env"))
        assert paths.simulations_dir() == tmp_path / "env"
        assert paths.user_owned_repos() == {tmp_path / "env": paths.SIMULATIONS_ENV}

    def test_unreadable_settings_mean_managed(self, data):
        paths.settings_file().write_text("{not json")
        assert paths.load_repo_dirs() == {}
        paths.save_repo_dir("pipeline", data / "own")  # and get overwritten cleanly
        assert paths.load_repo_dirs() == {"pipeline": data / "own"}

    def test_other_settings_survive(self, data):
        paths.settings_file().write_text('{"other": 1}')
        paths.save_repo_dir("pipeline", data / "own")
        assert '"other": 1' in paths.settings_file().read_text()

    def test_own_pipeline_need_not_be_a_git_repo(self, data):
        (data / "own").mkdir()
        paths.save_repo_dir("pipeline", data / "own")
        assert paths.pipeline_present()

    def test_change_hint(self):
        assert "Install tab" in paths.change_hint(paths.INSTALL_TAB)
        assert "unset it" in paths.change_hint(paths.PIPELINE_ENV)
