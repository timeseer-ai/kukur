"""Test the Inspect functions for Azure Data Lake Storage."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import urlparse

import pytest

from kukur.inspect import InspectedPath, ResourceType
from kukur.inspect.adls import (
    HAS_AZURE_FS,
    _get_filesystem_path,
    _remove_container_from_path,
)

BLOB_URI = urlparse("abfss://poc@account.dfs.core.windows.net/bulk/data")


@pytest.mark.skipif(not HAS_AZURE_FS, reason="AzureFileSystem not available")
@pytest.mark.usefixtures("windows_paths")
def test_filesystem_path_uses_forward_slashes() -> None:
    _, path = _get_filesystem_path(BLOB_URI)
    assert str(path) == "poc/bulk/data"


@pytest.mark.usefixtures("windows_paths")
def test_remove_container_uses_forward_slashes() -> None:
    paths = _remove_container_from_path(
        BLOB_URI,
        [InspectedPath(ResourceType.PARQUET, "poc/bulk/data/test.parquet")],
    )
    assert paths == [InspectedPath(ResourceType.PARQUET, "bulk/data/test.parquet")]
