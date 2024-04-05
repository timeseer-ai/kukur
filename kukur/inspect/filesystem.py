"""Inspect resources on a local filesystem."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import List, Optional

import pyarrow as pa
from pyarrow.dataset import dataset

from kukur.exceptions import MissingModuleException
from kukur.inspect import InspectedPath, ResourceType

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
    paths = []
    for sub_path in path.iterdir():
        resource_type = _get_resource_type(sub_path)
        if resource_type is not None:
            paths.append(InspectedPath(resource_type, str(sub_path)))
    return paths


def preview_filesystem(path: Path, num_rows: int = 5000) -> Optional[pa.Table]:
    """Preview a data file at the specified filesystem location."""
    resource_type = _get_resource_type(path)
    if resource_type in [ResourceType.ARROW, ResourceType.PARQUET, ResourceType.CSV]:
        data_set = dataset(path, format=resource_type.value)
        return data_set.head(num_rows)
    if resource_type == ResourceType.DELTA:
        if not HAS_DELTA_LAKE:
            raise MissingModuleException("deltalake")
        data_set = DeltaTable(path).to_pyarrow_dataset()
        return data_set.head(num_rows)

    return None


def _get_resource_type(path: Path) -> Optional[ResourceType]:  # noqa: PLR0911
    if path.is_dir():
        if (path / "_delta_log").is_dir():
            return ResourceType.DELTA
        return ResourceType.DIRECTORY
    if path.suffix.lower() == ".parquet":
        return ResourceType.PARQUET
    if path.suffix.lower() in [".arrow", ".feather"]:
        return ResourceType.ARROW
    if path.suffix.lower() in [".arrows"]:
        return ResourceType.ARROWS
    if path.suffix.lower() in [".csv", ".txt"]:
        return ResourceType.CSV
    return None
