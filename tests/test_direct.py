"""Unit tests for the direct-access entry points (mtr-exec / mtr-shell)."""

from metis_test_runner import direct


class _FakeProc:
    def __init__(self, rc=0):
        self.returncode = rc


def _recorder(store, rc=0):
    def run(cmd, env=None, **kwargs):
        store["cmd"] = cmd
        store["env"] = env
        return _FakeProc(rc)
    return run


class TestExecMain:
    def test_no_command_is_usage_error(self, capsys):
        assert direct.exec_main([]) == 2
        assert "no command" in capsys.readouterr().err

    def test_default_runs_command_with_resolved_env(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(direct.subprocess, "run", _recorder(captured))
        monkeypatch.setattr(direct, "resolve_runtime_env", lambda r: {"X": "1"})
        assert direct.exec_main(["--", "edps", "-lw"]) == 0
        assert captured["cmd"] == ["edps", "-lw"]
        assert captured["env"] == {"X": "1"}

    def test_separator_is_optional(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(direct.subprocess, "run", _recorder(captured))
        monkeypatch.setattr(direct, "resolve_runtime_env", lambda r: {})
        direct.exec_main(["pyesorex", "--recipes"])
        assert captured["cmd"] == ["pyesorex", "--recipes"]

    def test_command_flags_are_not_consumed_by_mtr(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(direct.subprocess, "run", _recorder(captured))
        monkeypatch.setattr(direct, "resolve_runtime_env", lambda r: {})
        # The -o after `--` belongs to edps, not mtr-exec.
        direct.exec_main(["--", "edps", "-w", "metis.metis_wkf", "-o", "/tmp/out"])
        assert captured["cmd"] == ["edps", "-w", "metis.metis_wkf", "-o", "/tmp/out"]

    def test_docker_wraps_command_in_exec_prefix(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(direct.subprocess, "run", _recorder(captured))
        direct.exec_main(["--runner", "docker", "--container", "c1",
                          "--", "edps", "-lw"])
        cmd = captured["cmd"]
        assert cmd[:2] == ["docker", "exec"]
        assert cmd[2] in ("-i", "-it")
        assert cmd[3] == "c1"
        assert cmd[-2:] == ["edps", "-lw"]

    def test_docker_without_container_is_error(self, capsys):
        assert direct.exec_main(["--runner", "docker", "--", "edps"]) == 2
        assert "requires --container" in capsys.readouterr().err

    def test_propagates_child_exit_code(self, monkeypatch):
        monkeypatch.setattr(direct.subprocess, "run", _recorder({}, rc=3))
        monkeypatch.setattr(direct, "resolve_runtime_env", lambda r: {})
        assert direct.exec_main(["--", "false"]) == 3

    def test_missing_executable_returns_127(self, monkeypatch):
        def boom(cmd, env=None, **kwargs):
            raise FileNotFoundError(cmd[0])
        monkeypatch.setattr(direct.subprocess, "run", boom)
        monkeypatch.setattr(direct, "resolve_runtime_env", lambda r: {})
        assert direct.exec_main(["--", "no_such_tool"]) == 127


class TestShellMain:
    def test_default_execs_user_shell_with_env(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(direct.os, "execvpe",
                            lambda f, a, e: captured.update(file=f, argv=a, env=e))
        monkeypatch.setattr(direct, "resolve_runtime_env", lambda r: {"X": "1"})
        monkeypatch.setattr(direct, "_print_banner", lambda *a, **k: None)
        monkeypatch.setenv("SHELL", "/bin/zsh")
        direct.shell_main([])
        assert captured["file"] == "/bin/zsh"
        assert captured["argv"] == ["/bin/zsh"]
        assert captured["env"] == {"X": "1"}

    def test_docker_runs_bash_in_container(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(direct.subprocess, "run", _recorder(captured))
        direct.shell_main(["--runner", "podman", "--container", "c1"])
        assert captured["cmd"] == ["podman", "exec", "-it", "c1", "bash"]

    def test_docker_without_container_is_error(self, capsys):
        assert direct.shell_main(["--runner", "docker"]) == 2
        assert "requires --container" in capsys.readouterr().err
