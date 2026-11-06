"""Tests for update.py and the GUI's update button and what's-new window."""

# PyQt6 stubs type most widget getters as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

import json
import sys
import time
import urllib.error
from pathlib import Path

import pytest

from metis_test_runner import gui, update
from metis_test_runner.update import WhatsNew

REAL_CHANGELOG = (Path(__file__).parent.parent / "CHANGELOG.md").read_text(encoding="utf-8")
PY = "3.12.1"


def _wheel(version, requires="<3.14,>=3.12", yanked=False):
    return {
        "filename": f"metis_test_runner-{version}-py3-none-any.whl",
        "requires-python": requires,
        "yanked": yanked,
    }


class _Response:
    def __init__(self, body):
        self._body = body

    def read(self, *args):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class TestLatestRelease:
    def test_picks_highest_numerically(self):
        files = [_wheel("0.9.0"), _wheel("0.10.0"), _wheel("0.2.0")]
        assert update.latest_release(files, PY) == "0.10.0"

    def test_reads_sdist_names(self):
        assert update.latest_release([{"filename": "metis_test_runner-0.7.2.tar.gz"}], PY) == "0.7.2"

    def test_skips_prereleases_and_dev_releases(self):
        files = [_wheel("0.7.2"), _wheel("0.8.0a1"), _wheel("0.8.0rc1"), _wheel("0.8.0.dev1")]
        assert update.latest_release(files, PY) == "0.7.2"

    @pytest.mark.parametrize("yanked", [True, "broken wheel"])
    def test_skips_yanked(self, yanked):
        files = [_wheel("0.7.2"), _wheel("0.8.0", yanked=yanked)]
        assert update.latest_release(files, PY) == "0.7.2"

    def test_skips_releases_excluded_by_requires_python(self):
        files = [_wheel("0.7.2"), _wheel("0.8.0", requires=">=3.13")]
        assert update.latest_release(files, PY) == "0.7.2"
        assert update.latest_release(files, "3.13.0") == "0.8.0"

    def test_prerelease_python_is_still_matched(self):
        assert update.latest_release([_wheel("0.7.2")], "3.13.0rc1") == "0.7.2"

    @pytest.mark.parametrize(
        "entry",
        [{"filename": "garbage.txt"}, _wheel("0.8.0", requires="not a specifier")],
    )
    def test_ignores_unusable_entries(self, entry):
        assert update.latest_release([_wheel("0.7.2"), entry], PY) == "0.7.2"

    def test_nothing_usable(self):
        assert update.latest_release([], PY) is None


class TestFetchLatest:
    def test_asks_the_index_api_for_json(self, monkeypatch):
        seen = {}

        def urlopen(request, timeout):
            seen["accept"] = request.get_header("Accept")
            seen["timeout"] = timeout
            return _Response(json.dumps({"files": [_wheel("0.7.2")]}).encode())

        monkeypatch.setattr(update.platform, "python_version", lambda: PY)
        monkeypatch.setattr(update.urllib.request, "urlopen", urlopen)
        assert update.fetch_latest(timeout=3) == "0.7.2"
        assert seen == {"accept": "application/vnd.pypi.simple.v1+json", "timeout": 3}

    @pytest.mark.parametrize(
        "failure",
        [urllib.error.URLError("offline"), TimeoutError(), ConnectionResetError()],
    )
    def test_network_errors_are_silent(self, monkeypatch, failure):
        def urlopen(request, timeout):
            raise failure

        monkeypatch.setattr(update.urllib.request, "urlopen", urlopen)
        assert update.fetch_latest() is None

    @pytest.mark.parametrize("body", [b"<html>proxy login</html>", b"{}", b'{"files": [{}]}', b"[]"])
    def test_unexpected_responses_are_silent(self, monkeypatch, body):
        monkeypatch.setattr(update.urllib.request, "urlopen", lambda request, timeout: _Response(body))
        assert update.fetch_latest() is None


class TestIsNewer:
    @pytest.mark.parametrize(
        ("latest", "current", "expected"),
        [
            ("0.10.0", "0.9.0", True),
            ("0.7.2", "0.7.2", False),
            ("0.7.1", "0.7.2", False),
            ("0.7.2", "0.0.0+unknown", True),
            (None, "0.7.2", False),
            ("not a version", "0.7.2", False),
        ],
    )
    def test_compares_versions(self, latest, current, expected):
        assert update.is_newer(latest, current) is expected


class TestCache:
    @pytest.fixture(autouse=True)
    def _data_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METIS_DATA_DIR", str(tmp_path))

    def test_empty_cache_is_due(self):
        assert update.check_due() is True
        assert update.cached_latest() is None

    def test_round_trip_and_interval(self):
        update.remember_latest("0.8.0")
        assert update.cached_latest() == "0.8.0"
        assert update.check_due() is False
        checked = json.loads(update._cache_path().read_text())["checked"]
        assert update.check_due(now=checked + update.CHECK_INTERVAL_S) is True

    @pytest.mark.parametrize("content", ["not json", "[]", '{"latest": 1, "checked": "x"}'])
    def test_corrupt_cache_is_ignored(self, content):
        update._cache_path().write_text(content)
        assert update.cached_latest() is None
        assert update.check_due() is True


class TestInstallKind:
    @pytest.fixture
    def prefix(self, tmp_path, monkeypatch):
        monkeypatch.setattr(update, "_is_editable", lambda: False)
        monkeypatch.setattr(update.sys, "prefix", str(tmp_path))
        monkeypatch.setattr(update.sys, "base_prefix", "/usr")
        return tmp_path

    def test_venv(self, prefix):
        assert update.install_kind() == "venv"

    def test_pipx_by_metadata_file_at_any_location(self, prefix):
        (prefix / "pipx_metadata.json").write_text("{}")
        assert update.install_kind() == "pipx"

    def test_editable_wins_over_pipx(self, prefix, monkeypatch):
        (prefix / "pipx_metadata.json").write_text("{}")
        monkeypatch.setattr(update, "_is_editable", lambda: True)
        assert update.install_kind() == "editable"

    def test_outside_a_venv_is_other(self, prefix, monkeypatch):
        monkeypatch.setattr(update.sys, "base_prefix", str(prefix))
        assert update.install_kind() == "other"

    def test_uv_tool_is_other(self, prefix):
        (prefix / "uv-receipt.toml").write_text("")
        assert update.install_kind() == "other"
        assert update.manual_command() == "uv tool upgrade metis-test-runner"

    def test_read_only_prefix_is_other(self, prefix, monkeypatch):
        monkeypatch.setattr(update.os, "access", lambda *a: False)
        assert update.install_kind() == "other"

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ('{"url": "file:///x", "dir_info": {"editable": true}}', True),
            ('{"url": "file:///x.whl", "archive_info": {}}', False),
            (None, False),
            ("not json", False),
        ],
    )
    def test_editable_detection(self, monkeypatch, raw, expected):
        class Dist:
            def read_text(self, name):
                assert name == "direct_url.json"
                return raw

        monkeypatch.setattr(update.metadata, "distribution", lambda name: Dist())
        assert update._is_editable() is expected

    def test_no_install_counts_as_a_developer_copy(self, monkeypatch):
        def distribution(name):
            raise update.metadata.PackageNotFoundError(name)

        monkeypatch.setattr(update.metadata, "distribution", distribution)
        assert update._is_editable() is True


class TestChecksEnabled:
    def test_env_var_disables(self, monkeypatch):
        monkeypatch.setattr(update, "install_kind", lambda: "pipx")
        monkeypatch.setenv(update.DISABLE_ENV, "1")
        assert update.checks_enabled() is False

    def test_editable_disables(self, monkeypatch):
        monkeypatch.delenv(update.DISABLE_ENV)
        monkeypatch.setattr(update, "install_kind", lambda: "editable")
        assert update.checks_enabled() is False

    def test_enabled_otherwise(self, monkeypatch):
        monkeypatch.delenv(update.DISABLE_ENV)
        monkeypatch.setattr(update, "install_kind", lambda: "venv")
        assert update.checks_enabled() is True


class TestUpgradeCommands:
    @pytest.fixture
    def pipx_venv(self, tmp_path, monkeypatch):
        venv = tmp_path / "pipxhome" / "venvs" / "metis-test-runner_dev"
        venv.mkdir(parents=True)
        (venv / "pipx_metadata.json").write_text("{}")
        monkeypatch.setattr(update, "install_kind", lambda: "pipx")
        monkeypatch.setattr(update.sys, "prefix", str(venv))
        return venv

    def test_pipx_upgrades_this_venv_by_name(self, pipx_venv, monkeypatch):
        monkeypatch.setattr(update.shutil, "which", lambda name: f"/bin/{name}")
        assert update.upgrade_commands("0.8.0") == [["/bin/pipx", "upgrade", "metis-test-runner_dev"]]

    def test_pipx_missing_from_path_is_left_to_the_user(self, pipx_venv, monkeypatch):
        monkeypatch.setattr(update.shutil, "which", lambda name: None)
        assert update.upgrade_commands("0.8.0") is None
        assert update.manual_command() == "pipx upgrade metis-test-runner_dev"

    def test_pipx_env_targets_the_running_venv(self, pipx_venv):
        assert update._upgrade_env()["PIPX_HOME"] == str(pipx_venv.parents[1])

    def test_env_has_no_secrets_or_pipeline_path(self, pipx_venv, monkeypatch):
        monkeypatch.setenv("database_password", "hunter2")
        monkeypatch.delenv("PYTHONPATH", raising=False)
        env = update._upgrade_env()
        assert "database_password" not in env
        assert "PYTHONPATH" not in env

    def test_venv_pins_the_version(self, monkeypatch):
        monkeypatch.setattr(update, "install_kind", lambda: "venv")
        monkeypatch.setattr(update, "ensurepip_command_if_needed", lambda: None)
        assert update.upgrade_commands("0.8.0") == [
            [sys.executable, "-m", "pip", "install", "--upgrade", "metis-test-runner==0.8.0"]
        ]

    def test_venv_bootstraps_pip_first(self, monkeypatch):
        monkeypatch.setattr(update, "install_kind", lambda: "venv")
        monkeypatch.setattr(update, "ensurepip_command_if_needed", lambda: ["boot"])
        commands = update.upgrade_commands("0.8.0")
        assert commands[0] == ["boot"]
        assert commands[1][2:4] == ["pip", "install"]

    @pytest.mark.parametrize("kind", ["other", "editable"])
    def test_other_kinds_are_left_to_the_user(self, monkeypatch, kind):
        monkeypatch.setattr(update, "install_kind", lambda: kind)
        assert update.upgrade_commands("0.8.0") is None

    def test_never_reinstalls(self):
        source = Path(update.__file__).read_text()
        assert '"reinstall"' not in source


class TestPerformUpgrade:
    @pytest.fixture
    def ran(self, monkeypatch):
        ran = []
        monkeypatch.setattr(update, "stream_subprocess", lambda cmd, **kw: ran.append((cmd, kw)))
        monkeypatch.setattr(update, "__version__", "0.7.2")
        return ran

    def test_returns_the_new_version(self, ran, monkeypatch):
        monkeypatch.setattr(update, "installed_version", lambda: "0.8.0")
        assert update.perform_upgrade([["a"], ["b"]], lambda *a: None) == "0.8.0"
        assert [cmd for cmd, _ in ran] == [["a"], ["b"]]
        assert ran[0][1]["timeout"] == update.UPGRADE_TIMEOUT_S

    @pytest.mark.parametrize("installed", ["0.7.2", None])
    def test_exit_zero_without_a_new_version_is_a_failure(self, ran, monkeypatch, installed):
        monkeypatch.setattr(update, "installed_version", lambda: installed)
        with pytest.raises(RuntimeError, match="not newer"):
            update.perform_upgrade([["pipx"]], lambda *a: None)

    def test_installed_version_reads_the_disk(self):
        from importlib.metadata import version

        assert update.installed_version() == version("metis-test-runner")


class TestRestart:
    def test_replaces_the_process_with_the_same_arguments(self, monkeypatch):
        calls = []
        monkeypatch.setattr(update.os, "execve", lambda *a: calls.append(a))
        monkeypatch.setattr(update.sys, "argv", ["mtr", "--pink"])
        monkeypatch.setenv("database_password", "hunter2")
        update.restart()
        exe, argv, env = calls[0]
        assert exe == sys.executable
        assert argv == [sys.executable, "-m", "metis_test_runner", "--pink"]
        assert "database_password" not in env


CHANGELOG = """# Changelog

## Unreleased

### Added
- not released yet

## 0.10.0 — 2026-12-01

### Action required
- Move the products.

### Changed
- Newest change.

## 0.9.0 — 2026-11-20

### Fixed
- A fix.

### Action required
- Rerun *Install*.

## 0.8.0

### Added
- Older thing.
"""


class TestWhatsNew:
    def _news(self, last_seen, current="0.10.0", has_history=True, changelog=CHANGELOG):
        return update.whats_new(last_seen, has_history=has_history, changelog=changelog, current=current)

    def test_fresh_install_shows_nothing(self):
        assert self._news(None, has_history=False) is None

    def test_existing_user_without_a_stored_version_sees_the_current_notes(self):
        news = self._news(None)
        assert news.title == "What's new in 0.10.0"
        assert "Newest change" in news.notes and "A fix" not in news.notes
        assert "Move the products" in news.actions

    @pytest.mark.parametrize("last_seen", ["0.10.0", "0.11.0"])
    def test_same_or_downgraded_version_shows_nothing(self, last_seen):
        assert self._news(last_seen) is None

    def test_upgrade_collects_every_skipped_release(self):
        news = self._news("0.8.0")
        assert news.title == "What's new since 0.8.0"
        assert news.notes.index("Newest change") < news.notes.index("A fix")
        assert "Older thing" not in news.notes

    def test_version_compare_is_numeric(self):
        assert "A fix" in self._news("0.8.0", current="0.10.0").notes
        assert "Newest change" not in self._news("0.8.0", current="0.9.0").notes

    def test_actions_are_moved_out_of_the_notes_and_labelled(self):
        news = self._news("0.8.0")
        assert news.actions.index("**0.10.0**") < news.actions.index("Move the products")
        assert news.actions.index("**0.9.0**") < news.actions.index("Rerun *Install*")
        assert "Action required" not in news.notes
        assert "Move the products" not in news.notes
        assert "### Fixed" in news.notes

    def test_no_actions(self):
        assert self._news("0.8.0", current="0.8.0") is None
        assert self._news("0.7.0", current="0.8.0").actions == ""

    def test_unreleased_is_never_shown(self):
        assert "not released yet" not in self._news("0.8.0").notes

    def test_unreadable_changelog_shows_nothing(self):
        assert self._news("0.8.0", changelog=None) is None

    def test_unknown_stored_version_counts_as_missing(self):
        assert self._news("garbage").title == "What's new in 0.10.0"

    def test_version_without_a_section_shows_nothing(self):
        assert self._news("0.8.0", current="0.8.1", changelog="# Changelog\n") is None

    def test_real_changelog(self):
        news = self._news("0.7.0", current="0.7.2", changelog=REAL_CHANGELOG)
        assert "## 0.7.2 — 2026-10-19" in news.notes and "## 0.7.1 — 2026-10-16" in news.notes
        assert "## 0.7.0" not in news.notes
        assert "## 0.4.5" in self._news("0.4.4", current="0.4.5", changelog=REAL_CHANGELOG).notes


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------


@pytest.fixture
def win(qapp, tmp_path, monkeypatch):
    monkeypatch.setenv("METIS_DATA_DIR", str(tmp_path))
    monkeypatch.delenv(update.DISABLE_ENV)
    monkeypatch.setattr(update, "install_kind", lambda: "pipx")
    monkeypatch.setattr(gui, "__version__", "0.7.2")
    monkeypatch.setattr(update, "__version__", "0.7.2")
    window = gui.MainWindow()
    window.show()
    yield window
    window.close()


def _wait_for(condition, timeout=3.0):
    from PyQt6.QtWidgets import QApplication

    deadline = time.monotonic() + timeout
    while not condition() and time.monotonic() < deadline:
        QApplication.processEvents()
        time.sleep(0.01)
    return condition()


def _settings():
    from PyQt6.QtCore import QSettings

    return QSettings("METIS", "TestRunner")


class TestUpdateButton:
    def test_hidden_until_there_is_something_to_offer(self, win):
        assert win._update_action.isVisible() is False
        assert win._update_btn.isVisible() is False

    def test_shown_for_a_newer_release(self, win):
        win._offer_update("0.8.0")
        assert _wait_for(win._update_btn.isVisible)
        assert win._update_btn.text() == "Update to 0.8.0"
        assert "0.7.2" in win._update_btn.toolTip() and "0.8.0" in win._update_btn.toolTip()

    @pytest.mark.parametrize("latest", ["0.7.2", "0.7.1", None])
    def test_stays_hidden_otherwise(self, win, latest):
        win._offer_update(latest)
        assert win._update_btn.isVisible() is False

    def test_cached_result_shows_without_touching_the_network(self, win, monkeypatch):
        update.remember_latest("0.8.0")

        def no_network():
            raise AssertionError("checked PyPI inside the 24 h window")

        monkeypatch.setattr(update, "fetch_latest", no_network)
        win.start_update_check()
        assert _wait_for(win._update_btn.isVisible)

    def test_stale_cache_from_before_an_upgrade_shows_nothing(self, win):
        update.remember_latest("0.7.2")
        win.start_update_check()
        assert win._update_btn.isVisible() is False

    def test_disabled_check_does_nothing(self, win, monkeypatch):
        monkeypatch.setenv(update.DISABLE_ENV, "1")
        update.remember_latest("0.8.0")
        win.start_update_check()
        assert win._update_btn.isVisible() is False

    def test_background_check_reaches_the_button(self, win, monkeypatch):
        monkeypatch.setattr(update, "fetch_latest", lambda: "0.8.0")
        win.start_update_check()
        assert _wait_for(win._update_btn.isVisible)
        assert win._update_btn.text() == "Update to 0.8.0"
        assert update.cached_latest() == "0.8.0"
        assert update.check_due() is False

    def test_failed_check_is_retried_next_start(self, win, monkeypatch):
        monkeypatch.setattr(update, "fetch_latest", lambda: None)
        win._check_for_update()
        assert update.check_due() is True


class TestUpdateFlow:
    @pytest.fixture
    def boxes(self, monkeypatch):
        from PyQt6.QtWidgets import QMessageBox

        boxes = {"info": [], "question": [], "reply": QMessageBox.StandardButton.Yes}

        def question(*args, **kwargs):
            boxes["question"].append(args)
            return boxes["reply"]

        monkeypatch.setattr(QMessageBox, "information", lambda *a: boxes["info"].append(a))
        monkeypatch.setattr(QMessageBox, "question", question)
        return boxes

    def test_refused_while_a_job_runs(self, win, boxes, monkeypatch):
        win._offer_update("0.8.0")
        monkeypatch.setattr(win, "_busy_jobs", lambda: ["a pipeline run"])
        monkeypatch.setattr(update, "upgrade_commands", lambda v: pytest.fail("started the upgrade"))
        win._update_btn.click()
        assert "a pipeline run" in boxes["info"][0][2]

    def test_declining_changes_nothing(self, win, boxes, monkeypatch):
        from PyQt6.QtWidgets import QMessageBox

        boxes["reply"] = QMessageBox.StandardButton.No
        win._offer_update("0.8.0")
        monkeypatch.setattr(update, "upgrade_commands", lambda v: [["x"]])
        monkeypatch.setattr(gui, "UpdateDialog", lambda *a: pytest.fail("started the upgrade"))
        win._update_btn.click()
        assert boxes["question"]

    def test_success_restarts_the_app(self, win, boxes, monkeypatch):
        class Dialog:
            updated = True

            def __init__(self, target, commands, parent):
                assert (target, commands) == ("0.8.0", [["x"]])

            def exec(self):
                return 1

        restarted = []
        win._offer_update("0.8.0")
        monkeypatch.setattr(update, "upgrade_commands", lambda v: [["x"]])
        monkeypatch.setattr(gui, "UpdateDialog", Dialog)
        monkeypatch.setattr(win, "_restart", lambda: restarted.append(True))
        win._update_btn.click()
        assert restarted == [True]

    def test_unsupported_install_gets_the_command(self, win, boxes, monkeypatch):
        from PyQt6.QtWidgets import QApplication, QMessageBox

        win._offer_update("0.8.0")
        monkeypatch.setattr(update, "upgrade_commands", lambda v: None)
        monkeypatch.setattr(update, "manual_command", lambda: "pipx upgrade metis-test-runner")

        def click_copy(box):
            assert box.informativeText() == "pipx upgrade metis-test-runner"
            next(b for b in box.buttons() if b.text() == "Copy command").click()

        monkeypatch.setattr(QMessageBox, "exec", click_copy)
        win._update_btn.click()
        assert QApplication.clipboard().text() == "pipx upgrade metis-test-runner"
        assert not boxes["question"]

    def test_restart_saves_settings_and_stops_the_event_loop(self, win, monkeypatch):
        from PyQt6.QtWidgets import QApplication

        quits = []
        monkeypatch.setattr(QApplication, "quit", lambda: quits.append(True))
        win._restart()
        assert win.restart_requested is True
        assert win.isVisible() is False
        assert quits == [True]

    def test_refused_close_does_not_restart(self, win, monkeypatch):
        from PyQt6.QtWidgets import QApplication

        monkeypatch.setattr(win, "close", lambda: False)
        monkeypatch.setattr(QApplication, "quit", lambda: pytest.fail("quit"))
        win._restart()
        assert win.restart_requested is False


class TestUpdateDialog:
    def _run(self, win, monkeypatch, outcome):
        def perform(commands, on_line):
            on_line("working\n", "")
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(update, "perform_upgrade", perform)
        dialog = gui.UpdateDialog("0.8.0", [["x"]], win)
        assert _wait_for(lambda: not dialog._running)
        return dialog

    def test_success_offers_only_a_restart(self, win, monkeypatch):
        dialog = self._run(win, monkeypatch, "0.8.0")
        assert dialog.updated is True
        assert dialog._button.text() == "Restart now"
        assert "working" in dialog._log.toPlainText()

    @pytest.mark.parametrize("error", [RuntimeError("pip said no"), TimeoutError("too slow")])
    def test_failure_keeps_the_old_version(self, win, monkeypatch, error):
        dialog = self._run(win, monkeypatch, error)
        assert dialog.updated is False
        assert dialog._button.text() == "Close"
        assert str(error) in dialog._status.text()

    def test_cannot_be_closed_while_running(self, win, monkeypatch):
        import threading

        release = threading.Event()

        def perform(commands, on_line):
            release.wait(5)
            return "0.8.0"

        monkeypatch.setattr(update, "perform_upgrade", perform)
        dialog = gui.UpdateDialog("0.8.0", [["x"]], win)
        dialog.show()
        dialog.reject()
        assert dialog.isVisible() is True
        assert dialog._button.isEnabled() is False
        release.set()
        dialog._worker.wait(3000)
        from PyQt6.QtWidgets import QApplication

        QApplication.processEvents()
        dialog.reject()
        assert dialog.isVisible() is False


class TestWhatsNewDialog:
    NEWS = WhatsNew("What's new since 0.7.0", "**0.8.0**\n\n- Move files", "## 0.8.0\n\n- A change")

    def _ok(self, dialog):
        from PyQt6.QtWidgets import QDialogButtonBox

        return dialog.findChild(QDialogButtonBox).button(QDialogButtonBox.StandardButton.Ok)

    def test_plain_notes_have_no_action_box(self, win):
        from PyQt6.QtWidgets import QGroupBox

        dialog = gui.WhatsNewDialog(WhatsNew("t", "", "- A change"), "#f00", win)
        assert dialog.findChild(QGroupBox) is None

    def test_actions_are_shown_but_do_not_block_closing(self, win):
        from PyQt6.QtWidgets import QGroupBox

        dialog = gui.WhatsNewDialog(self.NEWS, "#f00", win)
        dialog.show()
        assert dialog.findChild(QGroupBox).title() == "Action required before you continue"
        assert self._ok(dialog).isEnabled() is True
        dialog.reject()
        assert dialog.isVisible() is False

    def test_renders_markdown_and_opens_links_externally(self, win):
        from PyQt6.QtWidgets import QTextBrowser

        dialog = gui.WhatsNewDialog(self.NEWS, "#f00", win)
        browsers = dialog.findChildren(QTextBrowser)
        assert browsers[0].toPlainText().strip().startswith("0.8.0")
        assert all(b.openExternalLinks() for b in browsers)

    @pytest.mark.parametrize("theme", ["dark", "light"])
    def test_rendered_links_use_the_theme_colour(self, win, qapp, theme):
        from PyQt6.QtGui import QColor
        from PyQt6.QtWidgets import QTextBrowser

        gui.apply_theme(qapp, theme)
        news = WhatsNew("t", "", "See [the docs](https://example.org).")
        browser = gui.WhatsNewDialog(news, "#f00", win).findChild(QTextBrowser)
        cursor = browser.document().find("docs")
        assert not cursor.isNull()
        assert cursor.charFormat().foreground().color() == QColor(gui.THEMES[theme]["accent"])


class TestShowWhatsNew:
    @pytest.fixture
    def shown(self, win, monkeypatch):
        settings = _settings()
        settings.clear()
        settings.sync()
        shown = []

        class Dialog:
            def __init__(self, news, color, parent):
                shown.append(news)
                assert _settings().value("last_seen_version") != "0.8.0"

            def exec(self):
                return 1

        monkeypatch.setattr(gui, "WhatsNewDialog", Dialog)
        monkeypatch.setattr(gui, "__version__", "0.8.0")
        monkeypatch.setattr(update, "load_changelog", lambda: CHANGELOG)
        yield shown
        settings.clear()
        settings.sync()

    def test_fresh_install_shows_nothing_but_remembers(self, win, shown):
        win.show_whats_new()
        assert shown == []
        assert _settings().value("last_seen_version") == "0.8.0"

    def test_first_release_with_this_feature_reaches_existing_users(self, win, shown):
        settings = _settings()
        settings.setValue("install/some_ref", "main")
        settings.sync()
        win.show_whats_new()
        assert shown[0].title == "What's new in 0.8.0"

    def test_upgrade_shows_notes_then_stores_the_version(self, win, shown):
        settings = _settings()
        settings.setValue("last_seen_version", "0.7.0")
        settings.sync()
        win.show_whats_new()
        assert shown[0].title == "What's new since 0.7.0"
        assert _settings().value("last_seen_version") == "0.8.0"

    def test_downgrade_lowers_the_stored_version(self, win, shown):
        settings = _settings()
        settings.setValue("last_seen_version", "0.9.0")
        settings.sync()
        win.show_whats_new()
        assert shown == []
        assert _settings().value("last_seen_version") == "0.8.0"

    def test_editable_install_leaves_the_history_alone(self, win, shown, monkeypatch):
        monkeypatch.setattr(update, "install_kind", lambda: "editable")
        settings = _settings()
        settings.setValue("last_seen_version", "0.7.0")
        settings.sync()
        win.show_whats_new()
        assert shown == []
        assert _settings().value("last_seen_version") == "0.7.0"
