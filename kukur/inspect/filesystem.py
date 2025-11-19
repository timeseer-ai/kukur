"""Inspect resources on a local filesystem."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
from pyarrow import fs

from kukur.inspect import DataOptions, FileOptions, InspectedPath
from kukur.inspect.arrow import BlobResource, inspect


def inspect_filesystem(
    path: Path, *, options: FileOptions | None = None
) -> list[InspectedPath]:
    """Inspect a filesystem path.

    Lists all files in the path.
    Tries to determine which files are supported by Kukur and returns them.
    """
    if options is None:
        options = FileOptions()
    local = fs.LocalFileSystem()
    return inspect(local, path, options)


def preview_filesystem(
    path: Path, num_rows: int = 5000, options: DataOptions | None = None
) -> pa.Table | None:
    """Preview a data file at the specified filesystem location."""
    local = fs.LocalFileSystem()
    resource = BlobResource(str(path), local, path)
    data_set = resource.get_data_set(options)
    return data_set.head(num_rows, batch_size=num_rows, batch_readahead=1)


def read_filesystem(
    path: Path, options: DataOptions | None = None
) -> Generator[pa.RecordBatch, None, None]:
    """Read path as a series of record batches.

    Optionally filters the columns returned.
    """
    local = fs.LocalFileSystem()
    resource = BlobResource(str(path), local, path)
    yield from resource.read_batches(options)
