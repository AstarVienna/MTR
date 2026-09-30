"""Entry point for ``mtr`` / ``mtr-gui``: start the GUI, or say why it cannot.

Free of Qt imports, so a missing PyQt6 (not installed on ARM Linux, where the
pinned version has no wheel) or a missing Qt system library (a server without
libGL/libEGL) gives a readable message instead of a traceback.
"""

import sys

_HEADLESS = "The headless commands still work: mtr-install, mtr-cli, mtr-uninstall."

# How a missing shared library surfaces on Linux, macOS and Windows.
_MISSING_LIBRARY = ("cannot open shared object file", "Library not loaded", "DLL load failed")


def _import_gui():
    from .gui import main

    return main


def main() -> None:
    try:
        gui_main = _import_gui()
    except ModuleNotFoundError as exc:
        if (exc.name or "").partition(".")[0] != "PyQt6":
            raise
        sys.exit(
            "mtr: the GUI needs PyQt6, which is not installed here. MTR installs it on "
            f"every platform except ARM Linux, which has no build of the pinned version. {_HEADLESS}"
        )
    except ImportError as exc:
        if not any(s in str(exc) for s in _MISSING_LIBRARY):
            raise
        sys.exit(
            f"mtr: the GUI could not load Qt: {exc}\n"
            f"Install Qt's system libraries (README → System dependencies). {_HEADLESS}"
        )
    gui_main()
