"""Inspect resources on a local filesystem."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import List, Optional

import pyarrow as pa
from pyarrow import fs

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
    local = fs.LocalFileSystem()
    data_set = get_data_set(local, path)
    if data_set is None:
        if not HAS_DELTA_LAKE:
            raise MissingModuleException("deltalake")
        data_set = DeltaTable(path).to_pyarrow_dataset()
    if data_set is None:
        return None

    return data_set.head(num_rows)
