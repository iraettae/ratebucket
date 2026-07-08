"""The version lives in two places. Catch the day they disagree."""

from importlib import metadata

import ratebucket


def test_dunder_version_matches_installed_metadata() -> None:
    assert ratebucket.__version__ == metadata.version("ratebucket")
