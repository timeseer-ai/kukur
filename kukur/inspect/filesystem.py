"""Inspect resources on a local filesystem."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Generator, List, Optional

import pyarrow as pa
from pyarrow import fs
from pyarrow.dataset import Dataset

from kukur.exceptions import MissingModuleException
from kukur.inspect import InspectedPath
from kukur.inspect.arrow import get_data_set, inspect

try:
    from deltalake import DeltaTable

    HAS_DELTA_LAKE = True
except ImportError:
    HAS_DELTA_LAKE = False


def inspect_filesystem(path: Path) -> List[InspectedPath]:
    """Inspect a filesystem path.

    Lists all files in the path.
    Tries to determine which files are supported by Kukur and returns them.
    """
    local = fs.LocalFileSystem()
    return inspect(local, path)


def preview_filesystem(path: Path, num_rows: int = 5000) -> Optional[pa.Table]:
    """Preview a data file at the specified filesystem location."""
    data_set = _get_data_set(path)
    return data_set.head(num_rows)


def read_filesystem(
    path: Path, column_names: Optional[List[str]] = None
) -> Generator[pa.RecordBatch, None, None]:
    """Read path as a series of record batches.

    Optionally filters the columns returned.
    """
    data_set = _get_data_set(path)
    for record_batch in data_set.to_batches(columns=column_names):
        yield record_batch


def _get_data_set(path: Path) -> Dataset:
    local = fs.LocalFileSystem()
    data_set = get_data_set(local, path)
    if data_set is None:
        if not HAS_DELTA_LAKE:
            raise MissingModuleException("deltalake")
        data_set = DeltaTable(path).to_pyarrow_dataset()
    return data_set
