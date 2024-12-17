"""Inspect contents of a Azure Data Lake Storage Gen 2."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import PurePath
from typing import Generator, List, Optional, Tuple
from urllib.parse import ParseResult

import pyarrow as pa
from pyarrow import fs

from kukur.exceptions import MissingModuleException
from kukur.inspect import DataOptions, InspectedPath, InvalidInspectURI
from kukur.inspect.arrow import BlobResource
from kukur.inspect.arrow import inspect as inspect_blob

try:
    from pyarrow.fs import AzureFileSystem

    HAS_AZURE_FS = True
except ImportError:
    HAS_AZURE_FS = False


def inspect(blob_uri: ParseResult, *, recursive: bool = False) -> List[InspectedPath]:
    """Inspect a path on ADLS Gen 2 storage.

    Recurses into subdirectories when recursive is True.
    """
    filesystem, blob_path = _get_filesystem_path(blob_uri)
    return _remove_container_from_path(
        blob_uri, inspect_blob(filesystem, blob_path, recursive=recursive)
    )


def preview(
    blob_uri: ParseResult, num_rows: int, options: Optional[DataOptions]
) -> Optional[pa.Table]:
    """Return the first nuw_rows of the blob."""
    resource = _get_resource(blob_uri)
    data_set = resource.get_data_set(options)
    return data_set.head(num_rows, batch_size=num_rows, batch_readahead=1)


def read(
    blob_uri: ParseResult, options: Optional[DataOptions]
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over all RecordBatches at the given URI."""
    resource = _get_resource(blob_uri)
    yield from resource.read_batches(options)


def _get_resource(blob_uri: ParseResult) -> BlobResource:
    filesystem, blob_path = _get_filesystem_path(blob_uri)
    return BlobResource(blob_uri.geturl(), filesystem, blob_path)


def _get_filesystem_path(blob_uri: ParseResult) -> Tuple[fs.FileSystem, PurePath]:
    """Add the container name as the first path component."""
    if not HAS_AZURE_FS:
        raise MissingModuleException("azurefilesystem")
    if blob_uri.hostname is None:
        raise InvalidInspectURI("missing storage account name")
    account_name = blob_uri.hostname.split(".")[0]

    container_name = blob_uri.username
    if container_name is None:
        raise InvalidInspectURI("missing container name")

    path = PurePath(container_name) / PurePath(blob_uri.path.lstrip("/"))
    return AzureFileSystem(account_name), path


def _remove_container_from_path(
    blob_uri: ParseResult, paths: list[InspectedPath]
) -> list[InspectedPath]:
    """Remove the container name.

    It is already part of the URL before the @.
    """
    container_name = blob_uri.username
    if container_name is None:
        raise InvalidInspectURI("missing container name")
    return [
        InspectedPath(
            path.resource_type,
            str(PurePath(path.path).relative_to(PurePath(container_name))),
        )
        for path in paths
    ]
