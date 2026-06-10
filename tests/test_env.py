"""Unit tests for the shared runtime-environment resolution (env.py)."""

import os
import sys
from pathlib import Path

import pytest

from metis_test_runner import paths
from metis_test_runner.env import resolve_runtime_env, runner_prefix


def _patch_paths(monkeypatch, tmp_path):
    pipe = tmp_path / "METIS_Pipeline"
    sims = tmp_path / "METIS_Simulations"
    inst = tmp_path / "inst_pkgs"
    monkeypatch.setattr(paths, "pipeline_dir", lambda: pipe)
    monkeypatch.setattr(paths, "simulations_dir", lambda: sims)
    monkeypatch.setattr(paths, "inst_pkgs_dir", lambda: inst)
    monkeypatch.setattr(paths, "env_file", lambda: tmp_path / ".env")  # absent
    return pipe, sims, inst


class TestResolveRuntimeEnv:
    def test_default_derives_pipeline_vars(self, monkeypatch, tmp_path):
        pipe, sims, inst = _patch_paths(monkeypatch, tmp_path)
        env = resolve_runtime_env("default")
        assert env["PYTHONPATH"] == f"{sims}:{pipe}/metisp/pymetis/src/"
        assert env["PYCPL_RECIPE_DIR"] == f"{pipe}/metisp/pyrecipes/"
        assert env["PYESOREX_PLUGIN_DIR"] == f"{pipe}/metisp/pyrecipes/"
        assert env["PYESOREX_MSG_LEVEL"] == "debug"
        assert env["PYESOREX_LOG_LEVEL"] == "debug"
        assert env["METIS_INST_PKGS"] == str(inst)
        assert env["PYTHONUNBUFFERED"] == "1"

    def test_default_prepends_venv_bin_to_path(self, monkeypatch, tmp_path):
        _patch_paths(monkeypatch, tmp_path)
        env = resolve_runtime_env("default")
        assert env["PATH"].split(os.pathsep)[0] == str(Path(sys.executable).parent)

    def test_existing_dotenv_overrides_derived_values(self, monkeypatch, tmp_path):
        _patch_paths(monkeypatch, tmp_path)
        env_file = tmp_path / ".env"
        env_file.write_text("PYESOREX_MSG_LEVEL=info\nEXTRA_VAR=hello\n")
        monkeypatch.setattr(paths, "env_file", lambda: env_file)
        env = resolve_runtime_env("default")
        assert env["PYESOREX_MSG_LEVEL"] == "info"   # override wins over derived
        assert env["EXTRA_VAR"] == "hello"

    def test_native_injects_nothing_clone_specific(self, monkeypatch, tmp_path):
        _patch_paths(monkeypatch, tmp_path)
        monkeypatch.delenv("PYCPL_RECIPE_DIR", raising=False)
        env = resolve_runtime_env("native")
        assert "PYCPL_RECIPE_DIR" not in env
        assert env["PYTHONUNBUFFERED"] == "1"


class TestRunnerPrefix:
    def test_default_and_native_have_no_prefix(self):
        assert runner_prefix("default", None) == []
        assert runner_prefix("native", "ignored") == []

    def test_docker_non_interactive(self):
        assert runner_prefix("docker", "c1") == ["docker", "exec", "-i", "c1"]

    def test_podman_interactive(self):
        assert runner_prefix("podman", "c1", interactive=True) == \
            ["podman", "exec", "-it", "c1"]

    def test_container_runner_requires_container(self):
        with pytest.raises(ValueError, match="requires --container"):
            runner_prefix("docker", None)
