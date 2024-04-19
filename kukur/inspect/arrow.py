"""Inspect and preview PyArrow filesystems."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import PurePath
from typing import List, Optional

from pyarrow import csv, fs
from pyarrow.dataset import CsvFileFormat, Dataset, dataset

from kukur.inspect import InspectedPath, InspectOptions, ResourceType


def inspect(filesystem: fs.FileSystem, path: PurePath) -> List[InspectedPath]:
    """Return the resource type of a path within a filesystem."""
    paths = []
    for sub_path in filesystem.get_file_info(fs.FileSelector(str(path))):
        resource_type = _get_resource_type(filesystem, sub_path)
        if resource_type is not None:
            paths.append(InspectedPath(resource_type, sub_path.path))
    return paths


def get_data_set(
    filesystem: fs.FileSystem, path: PurePath, options: Optional[InspectOptions]
) -> Optional[Dataset]:
    """Return a PyArrow dataset for the resources at the given path."""
    resource_type = _get_resource_type_from_extension(path.suffix.lstrip("."))
    if resource_type in [ResourceType.ARROW, ResourceType.PARQUET, ResourceType.CSV]:
        format = resource_type.value
        if resource_type == ResourceType.CSV and options is not None:
            format = CsvFileFormat(
                parse_options=csv.ParseOptions(delimiter=options.csv_delimiter)
            )
        return dataset(str(path), format=format, filesystem=filesystem)
    return None


def _get_resource_type(
    filesystem: fs.FileSystem, file_info: fs.FileInfo
) -> Optional[ResourceType]:
    if file_info.type == fs.FileType.Directory:
        for file_inside in filesystem.get_file_info(fs.FileSelector(file_info.path)):
            if file_inside.base_name == "_delta_log":
                return ResourceType.DELTA
        return ResourceType.DIRECTORY
    return _get_resource_type_from_extension(file_info.extension)


def _get_resource_type_from_extension(extension: str) -> Optional[ResourceType]:
    if extension == "parquet":
        return ResourceType.PARQUET
    if extension in ["arrow", "feather"]:
        return ResourceType.ARROW
    if extension in ["arrows"]:
        return ResourceType.ARROWS
    if extension in ["csv", "txt"]:
        return ResourceType.CSV
    return None
