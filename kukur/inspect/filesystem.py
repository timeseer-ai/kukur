"""Inspect resources on a local filesystem."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Generator, List, Optional

import pyarrow as pa
from pyarrow import fs

from kukur.inspect import DataOptions, InspectedPath
from kukur.inspect.arrow import BlobResource, inspect


def inspect_filesystem(path: Path, *, recursive: bool = False) -> List[InspectedPath]:
    """Inspect a filesystem path.

    Lists all files in the path.
    Tries to determine which files are supported by Kukur and returns them.

    Recurses into subdirectories when recursive is True.
    """
    local = fs.LocalFileSystem()
    return inspect(local, path, recursive=recursive)


def preview_filesystem(
    path: Path, num_rows: int = 5000, options: Optional[DataOptions] = None
) -> Optional[pa.Table]:
    """Preview a data file at the specified filesystem location."""
    local = fs.LocalFileSystem()
    resource = BlobResource(str(path), local, path)
    data_set = resource.get_data_set(options)
    return data_set.head(num_rows, batch_size=num_rows, batch_readahead=1)


def read_filesystem(
    path: Path, options: Optional[DataOptions] = None
) -> Generator[pa.RecordBatch, None, None]:
    """Read path as a series of record batches.

    Optionally filters the columns returned.
    """
    local = fs.LocalFileSystem()
    resource = BlobResource(str(path), local, path)
    yield from resource.read_batches(options)
