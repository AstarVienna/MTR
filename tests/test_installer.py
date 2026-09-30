"""
Unit tests for installer.py — the Qt-free install/uninstall logic behind the
GUI's Install tab and the ``mtr-install`` / ``mtr-uninstall`` commands.

Nothing here needs a QApplication: that the module imports and runs without
PyQt6 is the point (see TestNoQt).
"""

import subprocess
import sys
import time
from pathlib import Path

import pytest

from metis_test_runner import installer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _argv_text(args):
    """argv as a string with absolute paths dropped.

    tmp_path names contain the very words the assertions look for (a test
    called test_unshallow_… produces a dir with "unshallow" in it), so paths
    must never take part in matching.
    """
    return " ".join(str(a) for a in args if not str(a).startswith("/"))


def _cp(stdout="", returncode=0, stderr=""):
    """A CompletedProcess standing in for one installer._git() call."""
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def _fake_git(responses, recorder=None):
    """Replacement for installer._git driven by a {substring: CompletedProcess} map.

    The first key that appears anywhere in the argv wins; anything unmatched
    comes back as a successful empty result. Pass *recorder* to capture argv.
    """

    def fake(args, cwd=None, timeout=30):
        if recorder is not None:
            recorder.append(list(args))
        joined = _argv_text(args)
        for needle, result in responses.items():
            if needle in joined:
                return result
        return _cp("")

    return fake


# ---------------------------------------------------------------------------
# Installer._patch_edps_config
# ---------------------------------------------------------------------------


class TestPatchEdpsConfig:
    # All four keys must be present for _patch_edps_config to succeed — it
    # raises if any pattern matches zero times.
    FULL_PROPS = (
        "port=5000\nworkflow_dir=/old\nesorex_path=esorex\n"
        "association_preference=raw_per_quality_level\n"
        "categories=\npattern=$DATASET/$TIMESTAMP/$object$_$pro.catg$.$EXT\n"
        "mode=copy\n"
        "truncate=False\n"
    )

    def _make_worker(self):
        from metis_test_runner.installer import Installer

        return Installer()

    def _seed(self, tmp_path, content):
        edps = tmp_path / ".edps"
        edps.mkdir()
        (edps / "application.properties").write_text(content)
        return edps / "application.properties"

    def test_patches_port(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker()._patch_edps_config()
        assert "port=4444" in props.read_text()

    def test_patches_workflow_dir(self, tmp_path, monkeypatch):
        from metis_test_runner.installer import TARGET_A

        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker()._patch_edps_config()
        assert f"{TARGET_A}/metisp/workflows" in props.read_text()

    def test_patches_esorex_path(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker()._patch_edps_config()
        assert "esorex_path=pyesorex" in props.read_text()

    def test_preserves_unrelated_lines(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(
            tmp_path,
            "port=5000\nsome.other.key=value\n"
            "workflow_dir=/old\nesorex_path=esorex\n"
            "association_preference=raw_per_quality_level\n"
            "categories=\npattern=$DATASET/$TIMESTAMP/$object$_$pro.catg$.$EXT\n"
            "mode=copy\ntruncate=False\n",
        )
        self._make_worker()._patch_edps_config()
        assert "some.other.key=value" in props.read_text()

    def test_raises_when_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        # No .edps directory created
        with pytest.raises(RuntimeError, match="not found"):
            self._make_worker()._patch_edps_config()

    def test_patches_all_three_keys_at_once(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker()._patch_edps_config()
        content = props.read_text()
        assert "port=4444" in content
        assert "esorex_path=pyesorex" in content
        assert "workflow_dir=/old" not in content

    def test_patches_association_preference(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker()._patch_edps_config()
        assert "association_preference=master_per_quality_level" in props.read_text()

    def test_patches_categories(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker()._patch_edps_config()
        assert "categories=.*" in props.read_text()

    def test_patches_pattern_with_task(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker()._patch_edps_config()
        assert "$TASK/" in props.read_text()

    def test_patches_mode_to_link(self, tmp_path, monkeypatch):
        # Products are hardlinked into the per-run output dir, not copied, so
        # they don't consume disk twice (working store + output folder).
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker()._patch_edps_config()
        content = props.read_text()
        assert "mode=link" in content
        assert "mode=copy" not in content

    def test_raises_when_mode_key_absent(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        self._seed(
            tmp_path,
            "port=5000\nworkflow_dir=/old\nesorex_path=esorex\n"
            "association_preference=raw\ncategories=\npattern=x\ntruncate=False\n",
        )
        with pytest.raises(RuntimeError, match="mode"):
            self._make_worker()._patch_edps_config()

    def test_raises_when_port_key_absent(self, tmp_path, monkeypatch):
        # If EDPS drifts its config format, we want a loud error that names
        # the missing key, not a silent no-op that rewrites the file unchanged.
        monkeypatch.setenv("HOME", str(tmp_path))
        self._seed(
            tmp_path,
            "workflow_dir=/old\nesorex_path=esorex\nassociation_preference=raw\n",
        )
        with pytest.raises(RuntimeError, match="port"):
            self._make_worker()._patch_edps_config()

    def test_raises_when_workflow_dir_key_absent(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        self._seed(
            tmp_path,
            "port=5000\nesorex_path=esorex\nassociation_preference=raw\n",
        )
        with pytest.raises(RuntimeError, match="workflow_dir"):
            self._make_worker()._patch_edps_config()

    def test_patches_truncate_to_true(self, tmp_path, monkeypatch):
        # EDPS wipes db.json on server startup only when truncate=True.
        # Without this, stale "complete" job UUIDs from previous runs whose
        # on-disk outputs have been deleted will collide with fresh submissions
        # and cause cascading FileNotFoundError failures. The installer must
        # pin this to True, not merely rewrite it to whatever EDPS defaults to.
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, self.FULL_PROPS)
        self._make_worker()._patch_edps_config()
        content = props.read_text()
        assert "truncate=True" in content
        assert "truncate=False" not in content

    def test_raises_when_truncate_key_absent(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        self._seed(
            tmp_path,
            "port=5000\nworkflow_dir=/old\nesorex_path=esorex\n"
            "association_preference=raw\ncategories=\npattern=x\nmode=copy\n",
        )
        with pytest.raises(RuntimeError, match="truncate"):
            self._make_worker()._patch_edps_config()


# ---------------------------------------------------------------------------
# Installer._backup_edps_config
# ---------------------------------------------------------------------------


class TestBackupEdpsConfig:
    def _make_worker(self):
        from metis_test_runner.installer import Installer

        return Installer()

    def _seed(self, tmp_path, content):
        edps = tmp_path / ".edps"
        edps.mkdir()
        props = edps / "application.properties"
        props.write_text(content)
        return props

    def test_backs_up_existing_config(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, "port=5000\n")
        self._make_worker()._backup_edps_config()
        assert not props.exists()
        backup = props.with_name("application.properties_backup")
        assert backup.exists()
        assert backup.read_text() == "port=5000\n"

    def test_noop_when_no_config(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        self._make_worker()._backup_edps_config()
        edps = tmp_path / ".edps"
        assert not edps.exists()

    def test_preserves_previous_backup(self, tmp_path, monkeypatch):
        """A re-install must not clobber the user's original config.

        On the second install the file in place is MTR's own, so overwriting
        the backup with it would lose the user's original permanently.
        """
        monkeypatch.setenv("HOME", str(tmp_path))
        # fmt: off
        props = self._seed(tmp_path, "port=9999\n")          # MTR's own config
        old_backup = props.with_name("application.properties_backup")
        old_backup.write_text("port=1111\n")                 # the user's original
        # fmt: on
        self._make_worker()._backup_edps_config()
        assert not props.exists()
        assert old_backup.read_text() == "port=1111\n"

    def test_repeated_backups_keep_the_first(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, "port=1111\n")
        worker = self._make_worker()
        backup = props.with_name("application.properties_backup")
        for content in ("port=2222\n", "port=3333\n"):
            worker._backup_edps_config()
            props.write_text(content)  # MTR rewrites it
        assert backup.read_text() == "port=1111\n"


# ---------------------------------------------------------------------------
# Installer._pip_deps_command — pipeline dependency pip argv
# ---------------------------------------------------------------------------


class TestPipDepsCommand:
    def _cmd(self):
        from metis_test_runner.installer import Installer

        return Installer._pip_deps_command()

    def test_runs_pip_in_mtrs_own_interpreter(self):
        cmd = self._cmd()
        assert cmd[0] == sys.executable
        assert cmd[1:4] == ["-m", "pip", "install"]

    def test_pycpl_is_unpinned(self):
        # pycpl is deliberately unpinned while ivh's index churns; a bare
        # "pycpl" token (no ==/>=/~=) is the whole point of the change.
        cmd = self._cmd()
        assert "pycpl" in cmd
        assert not any(a.startswith("pycpl") and a != "pycpl" for a in cmd)

    def test_upgrade_flag_present(self):
        # Without --upgrade an already-installed older pycpl would survive as
        # "Requirement already satisfied", defeating the unpin.
        assert "--upgrade" in self._cmd()

    def test_both_extra_indexes_present(self):
        from metis_test_runner.installer import ESO_INDEX, PYCPL_INDEX

        cmd = self._cmd()
        assert cmd.count("--extra-index-url") == 2
        assert PYCPL_INDEX in cmd
        assert ESO_INDEX in cmd

    def test_scopesim_stays_pinned(self):
        # Only pycpl was unpinned; scopesim must still carry an exact pin. The
        # version itself is not asserted so a routine bump needn't touch tests.
        cmd = self._cmd()
        for dist in ("scopesim", "scopesim_templates"):
            assert any(a.startswith(f"{dist}==") for a in cmd)

    def test_all_pipeline_deps_requested(self):
        cmd = self._cmd()
        for dist in ("pycpl", "edps", "pyesorex", "adari_core"):
            assert any(a == dist or a.startswith(f"{dist}==") for a in cmd)


# ---------------------------------------------------------------------------
# Installer._clone_or_update — submodule (.git as file) handling
# ---------------------------------------------------------------------------


class TestCloneOrUpdateSubmodule:
    def _make_worker(self):
        from metis_test_runner.installer import Installer

        return Installer()

    def test_submodule_checkout_takes_update_branch(self, tmp_path, monkeypatch):
        # Submodules store .git as a FILE pointing at the parent's
        # .git/modules/<name>/, not a directory. The old is_dir() check would
        # misclassify this as "not a git repo" and refuse to update.
        target = tmp_path / "submodule_checkout"
        target.mkdir()
        (target / ".git").write_text("gitdir: ../.git/modules/submodule_checkout\n")
        (target / "README.md").write_text("content\n")  # non-empty

        worker = self._make_worker()
        invoked = []
        monkeypatch.setattr(worker, "_run", lambda cmd, **kw: invoked.append(cmd))
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "symbolic-ref": _cp("main"),  # on a branch, so pull --ff-only applies
                    "pull": _cp(""),
                }
            ),
        )
        worker._clone_or_update("http://example.invalid/x.git", target)

        # The point of the test: a .git FILE is recognised as a repo, so we
        # update rather than clone.
        assert invoked, "_clone_or_update should have issued git commands"
        assert not any("clone" in c for c in invoked)
        assert any("fetch" in c for c in invoked)


# ---------------------------------------------------------------------------
# Uninstaller._cleanup_edps — restore-from-backup vs full removal
# ---------------------------------------------------------------------------


class TestUninstallEdpsCleanup:
    def _make_worker(self):
        from metis_test_runner.installer import Uninstaller

        return Uninstaller()

    def _seed(self, tmp_path, content, *, backup=None):
        edps = tmp_path / ".edps"
        edps.mkdir()
        props = edps / "application.properties"
        props.write_text(content)
        if backup is not None:
            props.with_name("application.properties_backup").write_text(backup)
        return props

    def test_restores_original_from_backup(self, tmp_path, monkeypatch):
        # A backup means the install displaced a pre-existing config — restore
        # it and leave the rest of ~/.edps intact.
        monkeypatch.setenv("HOME", str(tmp_path))
        props = self._seed(tmp_path, "port=4444\n", backup="port=9999\n")
        self._make_worker()._cleanup_edps()
        assert props.read_text() == "port=9999\n"
        assert not props.with_name("application.properties_backup").exists()
        assert (tmp_path / ".edps").exists()

    def test_removes_edps_and_bookkeeping_when_no_backup(self, tmp_path, monkeypatch):
        # No backup → the install created everything; remove ~/.edps AND the
        # base_dir bookkeeping directory named in the config.
        monkeypatch.setenv("HOME", str(tmp_path))
        book = tmp_path / "EDPS_store"
        book.mkdir()
        (book / "db.json").write_text("{}")
        self._seed(tmp_path, f"port=4444\nbase_dir={book}\n")
        self._make_worker()._cleanup_edps()
        assert not (tmp_path / ".edps").exists()
        assert not book.exists()

    def test_no_backup_falls_back_to_default_bookkeeping(self, tmp_path, monkeypatch):
        # When the config has no base_dir line, the default ~/EDPS_data is used.
        monkeypatch.setenv("HOME", str(tmp_path))
        default_book = tmp_path / "EDPS_data"
        default_book.mkdir()
        self._seed(tmp_path, "port=4444\n")
        self._make_worker()._cleanup_edps()
        assert not (tmp_path / ".edps").exists()
        assert not default_book.exists()

    def test_edps_base_dir_parses_config_value(self, tmp_path):
        from metis_test_runner.installer import Uninstaller

        props = tmp_path / "application.properties"
        props.write_text("port=4444\nbase_dir=/data/edps\nmode=link\n")
        assert Uninstaller._edps_base_dir(props) == Path("/data/edps")

    def test_edps_base_dir_defaults_when_absent(self, tmp_path, monkeypatch):
        from metis_test_runner.installer import Uninstaller

        monkeypatch.setenv("HOME", str(tmp_path))
        props = tmp_path / "application.properties"
        props.write_text("port=4444\n")
        assert Uninstaller._edps_base_dir(props) == tmp_path / "EDPS_data"


# ---------------------------------------------------------------------------
# Uninstaller.PIPELINE_PACKAGES — distribution names
# ---------------------------------------------------------------------------


class TestUninstallPackages:
    def test_removes_both_pymetis_names(self):
        # The clone registers as ``pymetis`` since 2026-07-13, ``eso-pymetis`` before.
        from metis_test_runner.installer import Uninstaller

        assert {"pymetis", "eso-pymetis"} <= set(Uninstaller.PIPELINE_PACKAGES)


# ---------------------------------------------------------------------------
# Uninstaller._remove_data_dir — whole-tree removal
# ---------------------------------------------------------------------------


class TestUninstallRemoveDataDir:
    def _make_worker(self):
        from metis_test_runner.installer import Uninstaller

        return Uninstaller()

    def test_removes_entire_data_tree(self, tmp_path, monkeypatch):
        from metis_test_runner import installer

        data = tmp_path / "data"
        sims = data / "METIS_Simulations"  # inside the data dir
        sims.mkdir(parents=True)
        (data / ".env").write_text("X=1\n")
        (data / "inst_pkgs").mkdir()
        monkeypatch.setattr(installer, "REPO_ROOT", data)
        monkeypatch.setattr(installer, "TARGET_B", sims)
        self._make_worker()._remove_data_dir()
        assert not data.exists()

    def test_also_removes_externally_relocated_simulations(self, tmp_path, monkeypatch):
        # METIS_SIMULATIONS_DIR can point outside the data dir; removing the
        # data dir alone would leave that clone behind.
        from metis_test_runner import installer

        data = tmp_path / "data"
        data.mkdir()
        external_sims = tmp_path / "elsewhere" / "METIS_Simulations"
        external_sims.mkdir(parents=True)
        monkeypatch.setattr(installer, "REPO_ROOT", data)
        monkeypatch.setattr(installer, "TARGET_B", external_sims)
        self._make_worker()._remove_data_dir()
        assert not data.exists()
        assert not external_sims.exists()

    def test_noop_when_nothing_to_remove(self, tmp_path, monkeypatch):
        from metis_test_runner import installer

        data = tmp_path / "missing"
        monkeypatch.setattr(installer, "REPO_ROOT", data)
        monkeypatch.setattr(installer, "TARGET_B", data / "METIS_Simulations")
        # Should not raise even though nothing exists.
        self._make_worker()._remove_data_dir()
        assert not data.exists()

    def test_refuses_to_remove_home(self, tmp_path, monkeypatch):
        """METIS_DATA_DIR=$HOME must not turn Uninstall into `rm -rf ~`."""
        from metis_test_runner import installer

        home = tmp_path / "home"
        (home / "precious").mkdir(parents=True)
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setattr(installer, "REPO_ROOT", home)
        monkeypatch.setattr(installer, "TARGET_B", home / "METIS_Simulations")
        self._make_worker()._remove_data_dir()
        assert (home / "precious").exists()

    def test_refuses_to_remove_root(self, monkeypatch):
        from metis_test_runner import installer

        monkeypatch.setattr(installer, "REPO_ROOT", Path("/"))
        monkeypatch.setattr(installer, "TARGET_B", Path("/"))
        self._make_worker()._remove_data_dir()
        assert Path("/").exists()

    def test_refuses_shallow_paths(self, monkeypatch):
        from metis_test_runner import installer

        monkeypatch.setattr(installer, "REPO_ROOT", Path("/tmp"))
        monkeypatch.setattr(installer, "TARGET_B", Path("/tmp"))
        self._make_worker()._remove_data_dir()
        assert Path("/tmp").exists()

    def test_refuses_a_symlinked_data_dir(self, tmp_path, monkeypatch):
        from metis_test_runner import installer

        real = tmp_path / "real"
        (real / "keep").mkdir(parents=True)
        link = tmp_path / "link"
        link.symlink_to(real, target_is_directory=True)
        monkeypatch.setattr(installer, "REPO_ROOT", link)
        monkeypatch.setattr(installer, "TARGET_B", link)
        self._make_worker()._remove_data_dir()
        assert (real / "keep").exists()


# ---------------------------------------------------------------------------
# _validate_ref
# ---------------------------------------------------------------------------


class TestValidateRef:
    @pytest.mark.parametrize("raw", ["", "   ", None])
    def test_blank_means_default_branch(self, raw):
        assert installer._validate_ref(raw) == ""

    def test_strips_pasted_whitespace(self):
        # Copying a ref out of a terminal drags a newline along.
        assert installer._validate_ref("  main\n") == "main"

    @pytest.mark.parametrize(
        "ref",
        [
            "main",
            "feature/my-branch",
            "be/master_associations",
            "v0.4.2",
            "2024-06-01",
            "release+1",
            "user@host",
            "8a50c60d4a5417a17d784ab0588d2d85212543db",
            "8a50c60",
        ],
    )
    def test_accepts_plausible_refs(self, ref):
        assert installer._validate_ref(ref) == ref

    @pytest.mark.parametrize(
        "ref",
        [
            "-x",
            "--upload-pack=echo",
            "a b",
            "a..b",
            "HEAD^",
            "x~1",
            "a:b",
            "refs/heads/x.lock",
            "x/",
            "x.",
            "a\nb",
            "a?b",
            "a*b",
            "a[b",
            "a\\b",
            "a{b",
            "x" * 300,
        ],
    )
    def test_rejects_bad_refs(self, ref):
        with pytest.raises(ValueError):
            installer._validate_ref(ref)


class TestLooksLikeAbbrevSha:
    @pytest.mark.parametrize("ref", ["8a50c60", "abcd", "0" * 39])
    def test_true_for_short_hex(self, ref):
        assert installer._looks_like_abbrev_sha(ref)

    @pytest.mark.parametrize("ref", ["0" * 40, "main", "v1.0", "abc", "deadbeefz"])
    def test_false_otherwise(self, ref):
        assert not installer._looks_like_abbrev_sha(ref)


class TestSameRemote:
    def test_ignores_dot_git_and_trailing_slash(self):
        assert installer._same_remote("https://h/o/r.git", "https://h/o/r/")

    def test_distinguishes_forks(self):
        assert not installer._same_remote("https://h/me/r.git", "https://h/them/r.git")


# ---------------------------------------------------------------------------
# _parse_ls_remote
# ---------------------------------------------------------------------------


class TestParseLsRemote:
    SAMPLE = (
        "aaa\trefs/heads/zebra\n"
        "bbb\trefs/heads/main\n"
        "ccc\trefs/heads/AIT_Templates\n"
        "ddd\trefs/tags/v2025.05.15\n"
        "eee\trefs/tags/v2025.05.15^{}\n"
        "fff\trefs/tags/v2024.11.15\n"
        "ggg\trefs/pull/12/head\n"
        "hhh\tHEAD\n"
    )

    def test_strips_prefixes(self):
        out = installer._parse_ls_remote(self.SAMPLE)
        assert "main" in out and "AIT_Templates" in out and "v2025.05.15" in out
        assert not any(r.startswith("refs/") for r in out)

    def test_drops_peeled_tag_duplicates(self):
        assert installer._parse_ls_remote(self.SAMPLE).count("v2025.05.15") == 1

    def test_ignores_non_branch_non_tag_refs(self):
        out = installer._parse_ls_remote(self.SAMPLE)
        assert "HEAD" not in out and not any("pull" in r for r in out)

    def test_default_branch_hoisted_above_alphabetical_branches(self):
        out = installer._parse_ls_remote(self.SAMPLE)
        assert out[0] == "main"
        assert out.index("main") < out.index("AIT_Templates")

    def test_branches_sort_before_tags(self):
        out = installer._parse_ls_remote(self.SAMPLE)
        assert out.index("zebra") < out.index("v2025.05.15")

    def test_name_that_is_both_branch_and_tag_appears_once(self):
        out = installer._parse_ls_remote("aaa\trefs/heads/v1.0\nbbb\trefs/tags/v1.0\n")
        assert out.count("v1.0") == 1

    def test_empty_input(self):
        assert installer._parse_ls_remote("") == []


# ---------------------------------------------------------------------------
# _describe_head
# ---------------------------------------------------------------------------


class TestDescribeHead:
    def test_absent_clone_never_shells_out(self, tmp_path, monkeypatch):
        called = []
        monkeypatch.setattr(installer, "_git", _fake_git({}, recorder=called))
        assert installer._describe_head(tmp_path / "nope") == "not cloned"
        assert called == []

    def _repo(self, tmp_path):
        (tmp_path / ".git").mkdir()
        return tmp_path

    def test_on_a_branch(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "rev-parse --short=8": _cp("d2d257c5\n"),
                    "symbolic-ref": _cp("main\n"),
                    "status": _cp(""),
                    "is-shallow-repository": _cp("false\n"),
                }
            ),
        )
        assert installer._describe_head(self._repo(tmp_path)) == "main @ d2d257c5"

    def test_detached_at_a_tag(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "rev-parse --short=8": _cp("8a50c604\n"),
                    "symbolic-ref": _cp("", 1),
                    "describe": _cp("v0.4.2\n"),
                    "status": _cp(""),
                    "is-shallow-repository": _cp("false\n"),
                }
            ),
        )
        assert installer._describe_head(self._repo(tmp_path)) == "v0.4.2 (tag) @ 8a50c604"

    def test_detached_without_a_tag(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "rev-parse --short=8": _cp("8a50c604\n"),
                    "symbolic-ref": _cp("", 1),
                    "describe": _cp("", 128),
                    "status": _cp(""),
                    "is-shallow-repository": _cp("false\n"),
                }
            ),
        )
        assert installer._describe_head(self._repo(tmp_path)) == "detached @ 8a50c604"

    def test_dirty_and_shallow_markers(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "rev-parse --short=8": _cp("d2d257c5\n"),
                    "symbolic-ref": _cp("main\n"),
                    "status": _cp(" M x.py\n"),
                    "is-shallow-repository": _cp("true\n"),
                }
            ),
        )
        out = installer._describe_head(self._repo(tmp_path))
        assert out == "main @ d2d257c5 · modified · shallow"

    def test_broken_repo(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "rev-parse --short=8": _cp("", 128),
                }
            ),
        )
        assert installer._describe_head(self._repo(tmp_path)) == "not a git repository"


# ---------------------------------------------------------------------------
# _dirty_files
# ---------------------------------------------------------------------------


class TestDirtyFiles:
    def test_absent_clone_is_not_dirty(self, tmp_path):
        assert installer._dirty_files(tmp_path / "nope") == []

    def test_clean_tree(self, tmp_path, monkeypatch):
        (tmp_path / ".git").mkdir()
        monkeypatch.setattr(installer, "_git", _fake_git({"status": _cp("")}))
        assert installer._dirty_files(tmp_path) == []

    def test_dirty_tree_returns_lines(self, tmp_path, monkeypatch):
        (tmp_path / ".git").mkdir()
        monkeypatch.setattr(installer, "_git", _fake_git({"status": _cp(" M a.py\n?? b.py\n")}))
        assert installer._dirty_files(tmp_path) == [" M a.py", "?? b.py"]

    def test_git_failure_raises_rather_than_assuming_clean(self, tmp_path, monkeypatch):
        # Assuming "clean" here would silently destroy work.
        (tmp_path / ".git").mkdir()
        monkeypatch.setattr(installer, "_git", _fake_git({"status": _cp("", 128, "index corrupt")}))
        with pytest.raises(RuntimeError, match="index corrupt"):
            installer._dirty_files(tmp_path)


# ---------------------------------------------------------------------------
# Installer._clone_or_update — pinned refs
# ---------------------------------------------------------------------------

URL = "http://example.invalid/x.git"
SHA = "8a50c60d4a5417a17d784ab0588d2d85212543db"


def _worker(**kw):
    from metis_test_runner.installer import Installer

    return Installer(**kw)


def _spy(worker, monkeypatch):
    """Record every argv passed to the fatal/streaming _run."""
    invoked = []
    monkeypatch.setattr(worker, "_run", lambda cmd, **kw: invoked.append([str(c) for c in cmd]))
    return invoked


def _repo(tmp_path, name="clone"):
    target = tmp_path / name
    (target / ".git").mkdir(parents=True)
    return target


def _flat(invoked):
    return [_argv_text(c) for c in invoked]


class TestCloneOrUpdateRef:
    def test_absent_target_with_branch_inits_fetches_and_checks_out(self, tmp_path, monkeypatch):
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "fetch": _cp(""),  # _try_fetch succeeds
                    "rev-parse FETCH_HEAD": _cp(SHA),
                    "rev-parse HEAD": _cp("other"),
                    "refs/remotes/origin/main": _cp(SHA),  # it is a branch
                }
            ),
        )
        w._clone_or_update(URL, tmp_path / "new", "main")

        flat = _flat(invoked)
        assert any(c.startswith("git init") for c in flat)
        assert any("remote add origin" in c for c in flat)
        assert any("checkout -f -B main FETCH_HEAD" in c for c in flat)
        assert not any("clone" in c for c in flat)

    def test_branch_fetch_is_shallow_and_option_safe(self, tmp_path, monkeypatch):
        seen = []
        w = _worker()
        _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "rev-parse FETCH_HEAD": _cp(SHA),
                    "rev-parse HEAD": _cp("other"),
                    "refs/remotes/origin/main": _cp(SHA),
                },
                recorder=seen,
            ),
        )
        w._clone_or_update(URL, tmp_path / "new", "main")

        fetches = [_argv_text(c) for c in seen if "fetch" in c]
        assert fetches, "expected a fetch"
        # --end-of-options must precede the remote so a dash-prefixed ref can
        # never be read as an option.
        assert "--depth 1 --end-of-options origin main" in fetches[0]

    def test_tag_or_sha_checks_out_detached_not_a_branch(self, tmp_path, monkeypatch):
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "rev-parse FETCH_HEAD": _cp(SHA),
                    "rev-parse HEAD": _cp("other"),
                    "refs/remotes/origin/": _cp("", 1),  # not a branch
                }
            ),
        )
        w._clone_or_update(URL, _repo(tmp_path), "v0.4.2")

        flat = _flat(invoked)
        assert any("checkout -f --detach FETCH_HEAD" in c for c in flat)
        assert not any(" -B " in c for c in flat)

    def test_full_sha_needs_only_one_fetch_and_no_unshallow(self, tmp_path, monkeypatch):
        seen = []
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "rev-parse FETCH_HEAD": _cp(SHA),
                    "rev-parse HEAD": _cp("other"),
                    "refs/remotes/origin/": _cp("", 1),
                },
                recorder=seen,
            ),
        )
        w._clone_or_update(URL, _repo(tmp_path), SHA)

        assert len([c for c in seen if "fetch" in c]) == 1
        assert not any("unshallow" in c for c in _flat(invoked))

    def test_abbreviated_sha_skips_the_doomed_fetch_and_unshallows(self, tmp_path, monkeypatch):
        # GitHub cannot serve `fetch origin <short-sha>`, so the shallow fetch
        # is skipped outright rather than attempted and failed.
        seen = []
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "is-shallow-repository": _cp("true\n"),
                    "8a50c60^{commit}": _cp(SHA),
                    "rev-parse HEAD": _cp("other"),
                    "refs/remotes/origin/": _cp("", 1),
                },
                recorder=seen,
            ),
        )
        w._clone_or_update(URL, _repo(tmp_path), "8a50c60")

        assert not any("fetch" in c and "8a50c60" in _argv_text(c) for c in seen)
        flat = _flat(invoked)
        assert any("fetch --unshallow" in c for c in flat)
        assert any(f"checkout -f --detach {SHA}" in c for c in flat)

    def test_unshallow_is_skipped_on_a_complete_repo(self, tmp_path, monkeypatch):
        # `fetch --unshallow` errors out on a repo that is not shallow.
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "is-shallow-repository": _cp("false\n"),
                    "8a50c60^{commit}": _cp(SHA),
                    "rev-parse HEAD": _cp("other"),
                    "refs/remotes/origin/": _cp("", 1),
                }
            ),
        )
        w._clone_or_update(URL, _repo(tmp_path), "8a50c60")
        assert not any("unshallow" in c for c in _flat(invoked))

    def test_unresolvable_ref_raises_and_never_touches_fetch_head(self, tmp_path, monkeypatch):
        # A failed fetch truncates FETCH_HEAD, so checking it out would pick up
        # a stale commit from an earlier fetch.
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "fetch": _cp("", 128, "couldn't find remote ref"),
                    "^{commit}": _cp("", 128),
                    "is-shallow-repository": _cp("false\n"),
                }
            ),
        )
        with pytest.raises(RuntimeError, match="not a branch, tag or commit"):
            w._clone_or_update(URL, _repo(tmp_path), SHA)
        assert not any("checkout" in c for c in _flat(invoked))

    def test_mistyped_branch_fails_fast_without_a_full_history_fetch(self, tmp_path, monkeypatch):
        # Only a hex commit id can need the un-shallow fallback; a bad branch
        # name must not cost a full clone before erroring.
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "fetch": _cp("", 128, "couldn't find remote ref"),
                }
            ),
        )
        with pytest.raises(RuntimeError, match="not a branch or tag"):
            w._clone_or_update(URL, _repo(tmp_path), "no-such-branch")
        assert not any("unshallow" in c for c in _flat(invoked))

    def test_already_at_target_leaves_the_tree_untouched(self, tmp_path, monkeypatch):
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "rev-parse FETCH_HEAD": _cp(SHA),
                    "rev-parse HEAD": _cp(SHA),
                    "refs/remotes/origin/main": _cp(SHA),
                    "symbolic-ref": _cp("main\n"),
                }
            ),
        )
        w._clone_or_update(URL, _repo(tmp_path), "main")
        flat = _flat(invoked)
        assert not any("checkout" in c for c in flat)
        assert not any("clean" in c for c in flat)

    def test_non_empty_non_repo_dir_refuses_before_git_init(self, tmp_path, monkeypatch):
        # The guard must sit ABOVE the ref dispatch: `git init` would happily
        # initialise over a non-empty foreign directory.
        target = tmp_path / "foreign"
        target.mkdir()
        (target / "important.txt").write_text("data\n")
        w = _worker()
        invoked = _spy(w, monkeypatch)
        with pytest.raises(RuntimeError, match="not a git repo and is not empty"):
            w._clone_or_update(URL, target, "main")
        assert invoked == []

    def test_invalid_ref_surfaces_as_a_worker_error(self, tmp_path, monkeypatch):
        w = _worker()
        invoked = _spy(w, monkeypatch)
        with pytest.raises(RuntimeError, match="not a valid branch"):
            w._clone_or_update(URL, _repo(tmp_path), "--upload-pack=echo")
        assert invoked == []


class TestCloneOrUpdateBlankRef:
    def test_absent_target_still_takes_the_shallow_clone_fast_path(self, tmp_path, monkeypatch):
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(installer, "_git", _fake_git({}))
        w._clone_or_update(URL, tmp_path / "new")
        flat = _flat(invoked)
        assert any("clone --depth 1" in c for c in flat)
        assert not any("git init" in c for c in flat)

    def test_branch_checkout_fast_forwards_without_resetting(self, tmp_path, monkeypatch):
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "symbolic-ref": _cp("main\n"),
                    "pull": _cp("Already up to date.\n"),
                }
            ),
        )
        w._clone_or_update(URL, _repo(tmp_path))
        flat = _flat(invoked)
        assert any("fetch --all --prune" in c for c in flat)
        assert not any("reset" in c for c in flat)
        assert not any("clean" in c for c in flat)

    def test_failed_pull_is_fatal_instead_of_silently_swallowed(self, tmp_path, monkeypatch):
        # Pre-existing bug: the old bare subprocess.run never checked the
        # return code, so a diverged branch left the install running on the
        # wrong commit.
        w = _worker()
        _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "symbolic-ref": _cp("main\n"),
                    "pull": _cp("", 1, "Not possible to fast-forward"),
                }
            ),
        )
        with pytest.raises(RuntimeError, match="ff-only"):
            w._clone_or_update(URL, _repo(tmp_path))

    def test_failed_pull_with_force_resets_then_retries(self, tmp_path, monkeypatch):
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "symbolic-ref": _cp("main\n"),
                    "pull": _cp("", 1, "diverged"),
                    "status": _cp(" M x.py\n"),
                }
            ),
        )
        w._clone_or_update(URL, _repo(tmp_path), force=True)
        flat = _flat(invoked)
        assert any("reset --hard" in c for c in flat)
        assert any("clean -fd" in c for c in flat)
        assert not any("-fdx" in c for c in flat)
        assert any("pull --ff-only" in c for c in flat)

    def test_detached_head_returns_to_the_default_branch(self, tmp_path, monkeypatch):
        # Leftover from an earlier pinned install: `pull --ff-only` cannot work
        # on a detached HEAD, and blank means "go back to normal".
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "symbolic-ref": _cp("", 1),
                    "ls-remote --symref": _cp("ref: refs/heads/main\tHEAD\nabc\tHEAD\n"),
                    "rev-parse FETCH_HEAD": _cp(SHA),
                    "rev-parse HEAD": _cp("other"),
                    "refs/remotes/origin/main": _cp(SHA),
                }
            ),
        )
        w._clone_or_update(URL, _repo(tmp_path))
        flat = _flat(invoked)
        assert any("checkout -f -B main FETCH_HEAD" in c for c in flat)
        assert not any("pull --ff-only" in c for c in flat)

    def test_symref_failure_falls_back_to_head_without_raising(self, tmp_path, monkeypatch):
        w = _worker()
        _spy(w, monkeypatch)
        monkeypatch.setattr(
            installer,
            "_git",
            _fake_git(
                {
                    "symbolic-ref": _cp("", 1),
                    "ls-remote --symref": _cp("", 128),
                    "rev-parse FETCH_HEAD": _cp(SHA),
                    "rev-parse HEAD": _cp("other"),
                    "refs/remotes/origin/": _cp("", 1),
                }
            ),
        )
        w._clone_or_update(URL, _repo(tmp_path))  # must not raise


class TestMakeRoom:
    def test_clean_tree_is_never_reset_even_with_force(self, tmp_path, monkeypatch):
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(installer, "_git", _fake_git({"status": _cp("")}))
        w._make_room(_repo(tmp_path), force=True)
        assert invoked == []

    def test_dirty_tree_without_confirmation_refuses(self, tmp_path, monkeypatch):
        # The worker re-checks rather than trusting the GUI's flag.
        w = _worker()
        _spy(w, monkeypatch)
        monkeypatch.setattr(installer, "_git", _fake_git({"status": _cp(" M x.py\n")}))
        with pytest.raises(RuntimeError, match="not confirmed"):
            w._make_room(_repo(tmp_path), force=False)

    def test_dirty_tree_with_confirmation_resets_and_cleans(self, tmp_path, monkeypatch):
        w = _worker()
        invoked = _spy(w, monkeypatch)
        monkeypatch.setattr(installer, "_git", _fake_git({"status": _cp("?? x.py\n")}))
        w._make_room(_repo(tmp_path), force=True)
        flat = _flat(invoked)
        assert any("reset --hard" in c for c in flat)
        assert any(c.endswith("clean -fd") for c in flat)
        # -x would eat gitignored build output, *.fits products and inst_pkgs/.
        assert not any("-fdx" in c for c in flat)


# ---------------------------------------------------------------------------
# stream_subprocess — real subprocesses, real deadlines
# ---------------------------------------------------------------------------


class TestStreamSubprocess:
    """These run actual processes: the bug being guarded is that the old
    implementation drained stdout to EOF *before* calling wait(timeout=...),
    so the timeout could never fire and a hung child hung the worker forever.
    """

    def _lines(self):
        out = []
        return out, lambda text, colour="": out.append(text)

    def test_streams_stdout(self):
        out, on_line = self._lines()
        installer.stream_subprocess(
            [sys.executable, "-c", "print('hello')"],
            on_line=on_line,
        )
        assert any("hello" in line for line in out)

    def test_echoes_the_command_first(self):
        out, on_line = self._lines()
        installer.stream_subprocess([sys.executable, "-c", "pass"], on_line=on_line)
        assert out[0].startswith("$ ")

    def test_nonzero_exit_raises(self):
        _, on_line = self._lines()
        with pytest.raises(RuntimeError, match="exited 3"):
            installer.stream_subprocess(
                [sys.executable, "-c", "raise SystemExit(3)"],
                on_line=on_line,
            )

    def test_stdin_text_is_delivered(self):
        out, on_line = self._lines()
        installer.stream_subprocess(
            [sys.executable, "-c", "import sys; print(sys.stdin.read().strip())"],
            on_line=on_line,
            stdin_text="from-stdin",
        )
        assert any("from-stdin" in line for line in out)

    def test_cwd_is_honoured(self, tmp_path):
        out, on_line = self._lines()
        installer.stream_subprocess(
            [sys.executable, "-c", "import os; print(os.getcwd())"],
            on_line=on_line,
            cwd=tmp_path,
        )
        assert any(str(tmp_path) in line for line in out)

    def test_ansi_escapes_are_stripped(self):
        out, on_line = self._lines()
        installer.stream_subprocess(
            [sys.executable, "-c", r"print('\x1b[31mred\x1b[0m')"],
            on_line=on_line,
        )
        assert any("red" in line and "\x1b" not in line for line in out)

    def test_timeout_actually_fires_on_a_hung_child(self):
        """The regression test: a child that never exits must not hang us."""
        _, on_line = self._lines()
        start = time.monotonic()
        with pytest.raises(TimeoutError, match="timed out"):
            installer.stream_subprocess(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                on_line=on_line,
                timeout=1,
            )
        assert time.monotonic() - start < 20, "watchdog did not interrupt the wait"

    def test_timeout_kills_grandchildren_too(self):
        """pip and git spawn children; killing only the direct child orphans them."""
        _, on_line = self._lines()
        code = (
            "import subprocess, sys, time; "
            "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
            "time.sleep(60)"
        )
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            installer.stream_subprocess(
                [sys.executable, "-c", code],
                on_line=on_line,
                timeout=1,
            )
        assert time.monotonic() - start < 20

    def test_fast_command_is_not_killed_by_the_watchdog(self):
        out, on_line = self._lines()
        installer.stream_subprocess(
            [sys.executable, "-c", "print('quick')"],
            on_line=on_line,
            timeout=30,
        )
        assert any("quick" in line for line in out)


# ---------------------------------------------------------------------------
# Headless guarantee — no PyQt6 on the mtr-install / mtr-uninstall path
# ---------------------------------------------------------------------------


class TestNoQt:
    def test_install_path_imports_no_pyqt(self):
        # A fresh interpreter, because this suite's own conftest/qapp may have
        # imported PyQt6 already. archive/credentials are what Uninstaller.run
        # imports lazily.
        code = (
            "import sys, metis_test_runner.installer, metis_test_runner.archive, "
            "metis_test_runner.credentials; "
            "print([m for m in sys.modules if m.startswith('PyQt6')])"
        )
        cp = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env={"PYTHONPATH": str(Path(installer.__file__).parents[1]), "PATH": "/usr/bin:/bin"},
        )
        assert cp.returncode == 0, cp.stderr
        assert cp.stdout.strip() == "[]"


# ---------------------------------------------------------------------------
# mtr-install / mtr-uninstall command line
# ---------------------------------------------------------------------------


class _StubRunner:
    """Stands in for Installer / Uninstaller; records how it was built."""

    built: list = []
    result = True

    def __init__(self, **kw):
        self.kw = kw
        type(self).built.append(self)

    def run(self):
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


@pytest.fixture
def stub_installer(monkeypatch):
    class Stub(_StubRunner):
        built = []

    monkeypatch.setattr(installer, "Installer", Stub)
    monkeypatch.setattr(installer, "_dirty_files", lambda t: [])
    monkeypatch.setattr(installer, "_interactive", lambda: False)
    return Stub


@pytest.fixture
def stub_uninstaller(monkeypatch):
    class Stub(_StubRunner):
        built = []

    monkeypatch.setattr(installer, "Uninstaller", Stub)
    monkeypatch.setattr(installer, "_interactive", lambda: False)
    return Stub


def _answer(monkeypatch, reply):
    """Make the process look interactive and feed *reply* to input()."""
    monkeypatch.setattr(installer, "_interactive", lambda: True)

    def fake_input(_prompt=""):
        if reply is EOFError:
            raise EOFError
        return reply

    monkeypatch.setattr("builtins.input", fake_input)


class TestInstallCli:
    def test_refs_are_passed_per_target(self, stub_installer):
        rc = installer.install_main(["--pipeline-ref", "develop", "--simulations-ref", SHA])
        assert rc == 0
        (run,) = stub_installer.built
        assert run.kw["refs"] == {installer.TARGET_A: "develop", installer.TARGET_B: SHA}
        assert run.kw["force"] == set()
        assert run.kw["log"] is installer._console_log

    def test_omitted_refs_mean_default_branch(self, stub_installer):
        assert installer.install_main([]) == 0
        assert stub_installer.built[0].kw["refs"] == {installer.TARGET_A: "", installer.TARGET_B: ""}

    def test_failed_install_exits_1(self, stub_installer):
        stub_installer.result = False
        assert installer.install_main([]) == 1

    def test_ctrl_c_exits_130(self, stub_installer):
        stub_installer.result = KeyboardInterrupt()
        assert installer.install_main([]) == 130

    def test_invalid_ref_is_a_usage_error(self, stub_installer, capsys):
        with pytest.raises(SystemExit) as exc:
            installer.install_main(["--pipeline-ref", "my branch"])
        assert exc.value.code == 2
        assert "not a valid branch" in capsys.readouterr().err
        assert stub_installer.built == []

    def test_dirty_clone_off_a_terminal_aborts(self, stub_installer, monkeypatch, capsys):
        monkeypatch.setattr(installer, "_dirty_files", lambda t: [" M x.py"])
        assert installer.install_main([]) == 2
        assert "--discard-changes" in capsys.readouterr().err
        assert stub_installer.built == []

    def test_discard_changes_forces_only_the_dirty_clone(self, stub_installer, monkeypatch):
        monkeypatch.setattr(
            installer, "_dirty_files", lambda t: [" M x.py"] if t == installer.TARGET_A else []
        )
        assert installer.install_main(["--discard-changes"]) == 0
        assert stub_installer.built[0].kw["force"] == {installer.TARGET_A}

    @pytest.mark.parametrize("reply", ["y", "YES"])
    def test_prompt_yes_forces(self, stub_installer, monkeypatch, reply):
        monkeypatch.setattr(installer, "_dirty_files", lambda t: [" M x.py"])
        _answer(monkeypatch, reply)
        assert installer.install_main([]) == 0
        assert stub_installer.built[0].kw["force"] == {installer.TARGET_A, installer.TARGET_B}

    @pytest.mark.parametrize("reply", ["", "n", EOFError])
    def test_prompt_no_or_eof_aborts(self, stub_installer, monkeypatch, reply):
        monkeypatch.setattr(installer, "_dirty_files", lambda t: [" M x.py"])
        _answer(monkeypatch, reply)
        assert installer.install_main([]) == 2
        assert stub_installer.built == []

    def test_unknown_dirty_state_off_a_terminal_aborts(self, stub_installer, monkeypatch):
        def boom(_t):
            raise RuntimeError("index corrupt")

        monkeypatch.setattr(installer, "_dirty_files", boom)
        # Even --discard-changes: it authorises discarding known changes, not
        # skipping a check that could not run.
        assert installer.install_main(["--discard-changes"]) == 2
        assert stub_installer.built == []

    def test_unknown_dirty_state_can_be_confirmed_interactively(self, stub_installer, monkeypatch):
        def boom(_t):
            raise RuntimeError("index corrupt")

        monkeypatch.setattr(installer, "_dirty_files", boom)
        _answer(monkeypatch, "y")
        assert installer.install_main([]) == 0
        assert stub_installer.built[0].kw["force"] == set()


class TestGitMissing:
    @pytest.mark.parametrize("argv", [[], ["--list-refs"]])
    def test_says_so_instead_of_a_traceback(self, stub_installer, monkeypatch, capsys, argv):
        monkeypatch.setattr(installer.shutil, "which", lambda name: None)
        assert installer.install_main(argv) == 1
        assert "git is not installed" in capsys.readouterr().err
        assert stub_installer.built == []


class TestListRefs:
    def test_prints_current_checkout_and_remote_refs(self, stub_installer, monkeypatch, capsys):
        monkeypatch.setattr(installer, "_describe_head", lambda t: "main @ abc12345")
        monkeypatch.setattr(
            installer, "_git", _fake_git({"ls-remote": _cp("a\trefs/heads/main\nb\trefs/tags/v1.0\n")})
        )
        assert installer.install_main(["--list-refs"]) == 0
        out = capsys.readouterr().out
        assert "METIS_Pipeline" in out and "METIS_Simulations" in out
        assert "currently: main @ abc12345" in out
        assert "    main\n    v1.0\n" in out
        assert stub_installer.built == []

    def test_offline_is_reported_not_fatal(self, stub_installer, monkeypatch, capsys):
        monkeypatch.setattr(installer, "_describe_head", lambda t: "not cloned")
        monkeypatch.setattr(
            installer, "_git", _fake_git({"ls-remote": _cp("", 128, "fatal: unable to access")})
        )
        assert installer.install_main(["--list-refs"]) == 0
        assert "offline? (fatal: unable to access)" in capsys.readouterr().out


class TestUninstallCli:
    def test_off_a_terminal_requires_yes(self, stub_uninstaller, capsys):
        assert installer.uninstall_main([]) == 2
        assert "--yes" in capsys.readouterr().err
        assert stub_uninstaller.built == []

    @pytest.mark.parametrize("result, rc", [(True, 0), (False, 1)])
    def test_yes_runs_the_uninstaller(self, stub_uninstaller, result, rc):
        stub_uninstaller.result = result
        assert installer.uninstall_main(["--yes"]) == rc
        assert stub_uninstaller.built[0].kw["log"] is installer._console_log

    def test_prompt_yes_runs(self, stub_uninstaller, monkeypatch):
        _answer(monkeypatch, "y")
        assert installer.uninstall_main([]) == 0
        assert len(stub_uninstaller.built) == 1

    def test_prompt_no_aborts(self, stub_uninstaller, monkeypatch):
        _answer(monkeypatch, "n")
        assert installer.uninstall_main([]) == 2
        assert stub_uninstaller.built == []


class TestConsoleLog:
    def test_plain_when_not_a_terminal(self, capsys):
        installer._console_log("hello\n", "green")
        assert capsys.readouterr().out == "hello\n"

    def test_coloured_on_a_terminal(self, capsys, monkeypatch):
        monkeypatch.delenv("NO_COLOR", raising=False)
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
        installer._console_log("ok\n", "green")
        assert capsys.readouterr().out == "\x1b[32mok\n\x1b[0m"

    def test_no_color_wins(self, capsys, monkeypatch):
        monkeypatch.setenv("NO_COLOR", "1")
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
        installer._console_log("ok\n", "green")
        assert capsys.readouterr().out == "ok\n"
