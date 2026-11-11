"""The README anchors that messages link to (docs.ANCHORS) must exist."""

import re
from pathlib import Path

import pytest

from metis_test_runner import docs

README = (Path(__file__).parents[1] / "README.md").read_text()
README_IDS = set(re.findall(r'<a id="([^"]+)"></a>', README))


@pytest.mark.parametrize("key", docs.ANCHORS)
def test_anchor_exists_in_readme(key):
    # Anchors are never removed: an MTR release out in the wild links to them.
    assert docs.ANCHORS[key][0] in README_IDS


def test_see_names_section_and_url():
    assert docs.see("own-checkout") == (
        "See README → Using your own checkout: "
        "https://github.com/AstarVienna/MTR/blob/main/README.md#own-checkout"
    )
