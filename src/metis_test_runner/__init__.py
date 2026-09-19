"""metis_test_runner — GUI and CLI wrapper for end-to-end ESO METIS pipeline testing."""

from importlib.metadata import PackageNotFoundError, version

try:
    # Single source of truth: pyproject.toml. Hardcoding this here drifted to
    # four releases stale before it was noticed.
    __version__ = version("metis-test-runner")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = "0.0.0+unknown"
