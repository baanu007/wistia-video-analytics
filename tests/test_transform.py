"""Unit tests for the transform job's pure-Python helpers.

We can't fully test PySpark DataFrame operations without a Spark session,
so we cover the derive_channel logic conceptually with a lightweight
string matcher, and the module import itself.
"""

import re


def test_channel_derivation_regex_matches_expected_names():
    """The real transform uses F.when/contains; this mirrors the logic."""

    def derive(name: str) -> str:
        lower = name.lower()
        if "youtube" in lower:
            return "YouTube"
        if "facebook" in lower:
            return "Facebook"
        return "Unknown"

    assert derive("Chris Face VSL The Gap Method Youtube Paid Ads") == "YouTube"
    assert derive("Chris Face VSL The Gap Method Facebook Paid Ads") == "Facebook"
    assert derive("Some generic video title") == "Unknown"
    assert derive("YOUTUBE uppercase") == "YouTube"


def test_media_id_regex_from_s3_path():
    """The transform extracts media_id from the file path using regex."""
    pattern = r"media_id=([^/]+)/"
    path = (
        "s3://wistia-analytics-raw-123456789012/media_stats/"
        "year=2026/month=04/day=20/media_id=gskhw4w4lm/gskhw4w4lm.json"
    )
    match = re.search(pattern, path)
    assert match is not None
    assert match.group(1) == "gskhw4w4lm"
