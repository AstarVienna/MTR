"""Direct, manual access to the pipeline environment.

These two console scripts bypass MTR's orchestration (simulation, workflow
detection, EDPS server lifecycle) and instead hand a developer the fully
resolved environment so they can drive ``edps`` / ``pyesorex`` / ``scopesim`` /
``python`` themselves — the modern, env-file-free replacement for the old
``uv run --env-file .env …`` workflow.

  mtr-exec [--runner R] [--container C] -- <command> [args...]
      Run a single command in MTR's resolved environment.

  mtr-shell [--runner R] [--container C]
      Open an interactive shell with that environment pre-applied.

Both are runner-aware: ``default`` derives the environment from
:mod:`metis_test_runner.env`; ``native`` inherits the parent environment;
``docker`` / ``podman`` run the command/shell inside the named container.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

from . import paths
from .env import resolve_runtime_env, runner_prefix

_RUNNERS = ["default", "native", "docker", "podman"]


def _add_runner_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--runner", choices=_RUNNERS,
        default=os.environ.get("METIS_RUNNER", "default"),
        help="Execution mode (env: METIS_RUNNER) [default: default]",
    )
    p.add_argument(
        "--container", metavar="NAME",
        default=os.environ.get("METIS_CONTAINER"),
        help="Container name/ID for --runner=docker/podman (env: METIS_CONTAINER)",
    )


def exec_main(argv=None) -> int:
    """Entry point for the ``mtr-exec`` console script."""
    p = argparse.ArgumentParser(
        prog="mtr-exec",
        description="Run a command in MTR's resolved pipeline environment.",
        epilog="Example: mtr-exec -- edps -w metis.metis_wkf -t metis_ifu_dark "
               "-i ./sim -o ./out",
    )
    _add_runner_args(p)
    p.add_argument(
        "command", nargs=argparse.REMAINDER,
        help="The command to run, e.g. `-- edps -lw`. A leading -- separating "
             "it from mtr-exec's own options is optional but recommended.",
    )
    args = p.parse_args(sys.argv[1:] if argv is None else argv)

    command = args.command
    if command and command[0] == "--":      # strip the optional separator
        command = command[1:]
    if not command:
        print("mtr-exec: no command given (e.g. `mtr-exec -- edps -lw`).",
              file=sys.stderr)
        return 2

    try:
        prefix = runner_prefix(args.runner, args.container,
                               interactive=sys.stdin.isatty())
    except ValueError as exc:
        print(f"mtr-exec: {exc}", file=sys.stderr)
        return 2

    if args.runner in ("docker", "podman"):
        return subprocess.run(prefix + command).returncode

    try:
        return subprocess.run(command, env=resolve_runtime_env(args.runner)).returncode
    except FileNotFoundError:
        print(f"mtr-exec: command not found: {command[0]}", file=sys.stderr)
        return 127


def shell_main(argv=None) -> int:
    """Entry point for the ``mtr-shell`` console script."""
    p = argparse.ArgumentParser(
        prog="mtr-shell",
        description="Open an interactive shell with MTR's pipeline environment "
                    "pre-applied (edps / pyesorex / python+scopesim on PATH).",
    )
    _add_runner_args(p)
    args = p.parse_args(sys.argv[1:] if argv is None else argv)

    try:
        prefix = runner_prefix(args.runner, args.container, interactive=True)
    except ValueError as exc:
        print(f"mtr-shell: {exc}", file=sys.stderr)
        return 2

    if args.runner in ("docker", "podman"):
        return subprocess.run(prefix + ["bash"]).returncode

    env = resolve_runtime_env(args.runner)
    _print_banner(args.runner, env)
    shell = os.environ.get("SHELL", "/bin/bash")
    # Replace this process with the shell so exit semantics are the shell's.
    os.execvpe(shell, [shell], env)


def _print_banner(runner: str, env: dict) -> None:
    from .run_metis import read_edps_port
    print("── MTR environment ready ───────────────────────────────")
    print(f"  runner          : {runner}")
    print(f"  EDPS port       : {read_edps_port()}")
    print(f"  recipe dir      : {env.get('PYCPL_RECIPE_DIR', '-')}")
    print(f"  instrument pkgs : {env.get('METIS_INST_PKGS', '-')}")
    print(f"  simulations     : {paths.simulations_dir()}")
    print("  on PATH         : edps, pyesorex, python (scopesim)")
    print("  type 'exit' to leave.")
    print("────────────────────────────────────────────────────────")


if __name__ == "__main__":
    sys.exit(exec_main())
