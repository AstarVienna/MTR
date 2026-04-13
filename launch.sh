#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

_run_gui() {
    cd "$SCRIPT_DIR"
    if [[ -n "${SMOKE_TEST:-}" ]]; then
        exec uv run gui.py --smoke-test
    fi
    exec uv run gui.py
}

# Check PATH
if command -v uv &>/dev/null; then _run_gui; fi

# Check common install location (uv not yet on PATH in current shell)
if [[ -x "$HOME/.local/bin/uv" ]]; then
    export PATH="$HOME/.local/bin:$PATH"
    _run_gui
fi

# uv is not installed — ask before running curl | sh
echo "uv is not installed. It is required to manage Python dependencies."
read -rp "Install uv now? [y/N] " answer
case "$answer" in
    [yY]|[yY][eE][sS]) ;;
    *) echo "Aborted."; exit 1 ;;
esac

curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
_run_gui
