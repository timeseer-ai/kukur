"""Test the Inspect functions for AWS S3."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import urlparse

import pytest

from kukur.inspect import InspectedPath, ResourceType
from kukur.inspect.s3 import _get_blob_path, _remove_bucket_from_path

BLOB_URI = urlparse("s3://poc/bulk/data")


@pytest.mark.usefixtures("windows_paths")
def test_blob_path_uses_forward_slashes() -> None:
    assert str(_get_blob_path(BLOB_URI)) == "poc/bulk/data"


@pytest.mark.usefixtures("windows_paths")
def test_remove_bucket_uses_forward_slashes() -> None:
    paths = _remove_bucket_from_path(
        BLOB_URI,
        [InspectedPath(ResourceType.PARQUET, "poc/bulk/data/test.parquet")],
    )
    assert paths == [InspectedPath(ResourceType.PARQUET, "bulk/data/test.parquet")]
