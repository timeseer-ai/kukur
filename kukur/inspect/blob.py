"""Inspect blob stores."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Generator, List, Optional
from urllib.parse import urlparse

import pyarrow as pa

from kukur.inspect import InspectedPath, UnsupportedBlobException, adls, s3


def inspect_blob(blob_uri: str) -> List[InspectedPath]:
    """Inspect a blob store.

    Uses the URI scheme to determine the type of blob store.

    s3:// will list contents of S3 buckets
    abfss:// will list contents of Azure Blob Storage Containers.
    """
    parsed_url = urlparse(blob_uri)
    if parsed_url.scheme == "abfss":
        return adls.inspect(parsed_url)
    if parsed_url.scheme == "s3":
        return s3.inspect(parsed_url)

    raise UnsupportedBlobException(parsed_url.scheme)


def preview_blob(blob_uri: str, num_rows: int = 5000) -> Optional[pa.Table]:
    """Preview the contents of a blob."""
    parsed_url = urlparse(blob_uri)
    if parsed_url.scheme == "abfss":
        return adls.preview(parsed_url, num_rows)
    if parsed_url.scheme == "s3":
        return s3.preview(parsed_url, num_rows)

    raise UnsupportedBlobException(parsed_url.scheme)


def read_blob(
    blob_uri: str, column_names: Optional[List[str]] = None
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over the RecordBatches at the given URI."""
    parsed_url = urlparse(blob_uri)
    if parsed_url.scheme == "abfss":
        return adls.read(parsed_url, column_names)
    if parsed_url.scheme == "s3":
        return s3.read(parsed_url, column_names)

    raise UnsupportedBlobException(parsed_url.scheme)
