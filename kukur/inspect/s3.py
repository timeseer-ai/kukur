"""Inspect contents of a AWS S3 bucket."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from pathlib import PurePath
from urllib.parse import ParseResult

import pyarrow as pa
from pyarrow.fs import S3FileSystem, resolve_s3_region

from kukur.inspect import DataOptions, FileOptions, InspectedPath, InvalidInspectURI
from kukur.inspect.arrow import BlobResource
from kukur.inspect.arrow import inspect as inspect_s3


def inspect(blob_uri: ParseResult, options: FileOptions) -> list[InspectedPath]:
    """Inspect a path in an AWS S3 bucket.

    Recurses into subdirectories when recursive is True.
    """
    blob_path = _get_blob_path(blob_uri)
    return _remove_bucket_from_path(
        blob_uri,
        inspect_s3(
            S3FileSystem(region=resolve_s3_region(blob_path.parts[0])),
            blob_path,
            options,
        ),
    )


def preview(
    blob_uri: ParseResult, num_rows: int, options: DataOptions | None
) -> pa.Table | None:
    """Return the first nuw_rows of the blob."""
    resource = _get_resource(blob_uri)
    data_set = resource.get_data_set(options)
    return data_set.head(num_rows, batch_size=num_rows, batch_readahead=1)


def read(
    blob_uri: ParseResult, options: DataOptions | None
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over all RecordBatches at the given URI."""
    resource = _get_resource(blob_uri)
    yield from resource.read_batches(options)


def _get_resource(blob_uri: ParseResult) -> BlobResource:
    blob_path = _get_blob_path(blob_uri)
    filesystem = S3FileSystem(region=resolve_s3_region(blob_path.parts[0]))
    return BlobResource(blob_uri.geturl(), filesystem, blob_path)


def _get_blob_path(blob_uri: ParseResult) -> PurePath:
    bucket_name = blob_uri.netloc
    if bucket_name is None:
        raise InvalidInspectURI("missing bucket name")
    return PurePath(bucket_name) / PurePath(blob_uri.path.lstrip("/"))


def _remove_bucket_from_path(
    blob_uri: ParseResult, paths: list[InspectedPath]
) -> list[InspectedPath]:
    """Remove the bucket name.

    It is already the hostname.
    """
    bucket_name = blob_uri.hostname
    if bucket_name is None:
        raise InvalidInspectURI("missing bucket name")
    return [
        InspectedPath(
            path.resource_type,
            str(PurePath(path.path).relative_to(PurePath(bucket_name))),
        )
        for path in paths
    ]
