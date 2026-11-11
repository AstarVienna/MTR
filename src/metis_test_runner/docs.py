"""Links from messages to the README sections that explain them.

Links go to the README on main, so users read the newest docs. Each anchor is an
explicit ``<a id>`` in README.md that is never removed (a retired section's id
moves to its replacement); tests/test_docs.py checks they all exist.
"""

from __future__ import annotations

README_PAGE = "https://github.com/AstarVienna/MTR/blob/main/README.md"

#: key → (README anchor id, section title shown in the message)
ANCHORS = {
    "own-checkout": ("own-checkout", "Using your own checkout"),
    "install-tab": ("install-tab", "Install tab"),
    "system-dependencies": ("system-dependencies", "System dependencies"),
    "uninstall-refusing": ("uninstall-refusing", 'Uninstall: "Refusing to remove …"'),
    "edps-hardlinks": ("edps-hardlinks", "EDPS hardlinks"),
}


def url(key: str) -> str:
    return f"{README_PAGE}#{ANCHORS[key][0]}"


def see(key: str) -> str:
    """'See README → <section>: <url>' for appending to a message."""
    return f"See README → {ANCHORS[key][1]}: {url(key)}"
