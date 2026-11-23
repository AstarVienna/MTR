"""Read and patch EDPS's configuration, ``~/.edps/application.properties``.

The one parser for that file. It is an INI file (``[server]``, ``[executor]``,
…); keys match case-insensitively, ``key = value`` and ``key: value`` are
accepted, and every untouched line is written back unchanged. A key outside any
section (only test fixtures write those) is found too.
"""

from __future__ import annotations

import re
import stat
from pathlib import Path

from . import paths

_SECTION_RE = re.compile(r"^\s*\[([^\]]*)\]\s*$")
_ENTRY_RE = re.compile(r"^(\s*([^\s=:#;\[][^=:]*?)\s*[=:]\s*)(.*?)\s*$")

#: Section of each key MTR reads or writes, as EDPS lays the file out.
SECTIONS = {
    "port": "server",
    "workflow_dir": "application",
    "esorex_path": "executor",
    "base_dir": "executor",
    "association_preference": "generator",
    "truncate": "repository",
    "categories": "packager",
    "pattern": "packager",
    "mode": "packager",
}


def path() -> Path:
    return Path.home() / ".edps" / "application.properties"


def _find(lines: list[str], key: str, section: str | None) -> tuple[int, re.Match] | None:
    """Index and match of *key*'s line: in *section* if given, else anywhere; a sectionless key also counts."""
    key, section = key.lower(), (section or "").lower() or None
    current: str | None = None
    fallback = None
    for i, line in enumerate(lines):
        line = line.rstrip("\r\n")
        if line.lstrip().startswith(("#", ";")):
            continue
        if m := _SECTION_RE.match(line):
            current = m.group(1).strip().lower()
            continue
        m = _ENTRY_RE.match(line)
        if not m or m.group(2).lower() != key:
            continue
        if section is None or current == section:
            return i, m
        if current is None and fallback is None:
            fallback = (i, m)
    return fallback


def read_text(props: Path | None = None) -> str | None:
    """The file's text, or None if it is missing or unreadable."""
    try:
        return (props or path()).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def get(
    key: str, default: str | None = None, *, props: Path | None = None, text: str | None = None
) -> str | None:
    """Value of *key* (in its usual section, see SECTIONS), or *default*."""
    if text is None:
        text = read_text(props)
    if text is None:
        return default
    found = _find(text.splitlines(), key, SECTIONS.get(key))
    return found[1].group(3) if found else default


def patch_text(text: str, updates: dict[str, str]) -> tuple[str, dict[str, str | None]]:
    """*text* with each key set to its value; also returns the old values (None: key absent, left out)."""
    lines = text.splitlines(keepends=True)
    old: dict[str, str | None] = {}
    for key, value in updates.items():
        found = _find(lines, key, SECTIONS.get(key))
        if found is None:
            old[key] = None
            continue
        i, m = found
        old[key] = m.group(3)
        ending = lines[i][len(lines[i].rstrip("\r\n")) :]
        lines[i] = f"{m.group(1)}{value}{ending}"
    return "".join(lines), old


def update(updates: dict[str, str], props: Path | None = None) -> dict[str, str | None]:
    """Set keys in the file in place; returns the old values (None: key absent, not added).

    Atomic, so an interrupted run never leaves the config empty; resolved and
    chmod-ed so a symlinked file and its permissions survive.
    """
    target = (props or path()).resolve()
    text = target.read_text(encoding="utf-8")
    new_text, old = patch_text(text, updates)
    if new_text != text:
        paths.write_text_atomic(target, new_text, mode=stat.S_IMODE(target.stat().st_mode))
    return old


def base_dir(pipe_out: Path, *, text: str | None = None) -> Path | None:
    """EDPS's job directory (``base_dir``), or None if the config is unreadable.

    EDPS resolves a relative one against its working directory, *pipe_out*.
    """
    if text is None:
        text = read_text()
    if text is None:
        return None
    value = Path((get("base_dir", ".", text=text) or ".").strip()).expanduser()
    return value if value.is_absolute() else pipe_out / value
