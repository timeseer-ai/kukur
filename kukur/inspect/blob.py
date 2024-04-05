"""Inspect blob stores."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import List
from urllib.parse import urlparse

from kukur.inspect import InspectedPath, UnsupportedBlobException, adls


def inspect_blob(blob_uri: str) -> List[InspectedPath]:
    """Inspect a blob store.

    Uses the URI scheme to determine the type of blob store.

    s3:// will list contents of S3 buckets
    abfss:// will list contents of Azure Blob Storage Containers.
    """
    parsed_url = urlparse(blob_uri)
    if parsed_url.scheme == "abfss":
        return adls.inspect(parsed_url)

    raise UnsupportedBlobException(parsed_url.scheme)
