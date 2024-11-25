"""Inspect and preview PyArrow filesystems."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import PurePath
from typing import Generator, List, Optional

from pyarrow import RecordBatch, csv, fs, parquet
from pyarrow.dataset import CsvFileFormat, Dataset, dataset

from kukur.exceptions import MissingModuleException
from kukur.inspect import InspectedPath, InspectOptions, ResourceType
from kukur.source.gpx import parse_gpx

try:
    from deltalake import DeltaTable

    HAS_DELTA_LAKE = True
except ImportError:
    HAS_DELTA_LAKE = False


def inspect(
    filesystem: fs.FileSystem, path: PurePath, *, recursive: bool
) -> List[InspectedPath]:
    """Return the resource type of a path within a filesystem.

    Set recursive to True to recurse into subdirectories.
    """
    paths = []
    for sub_path in filesystem.get_file_info(
        fs.FileSelector(str(path), recursive=recursive)
    ):
        resource_type = _get_resource_type(filesystem, sub_path)
        if resource_type is not None:
            paths.append(InspectedPath(resource_type, sub_path.path))
    return paths


class BlobResource:
    """A blob resource backed by PyArrow."""

    def __init__(self, uri: str, filesystem: fs.FileSystem, path: PurePath):
        self.__uri = uri
        self.__fs = filesystem
        self.__path = path

    def get_data_set(self, options: Optional[InspectOptions]) -> Dataset:
        """Return a DataSet for the resource."""
        data_set = get_data_set(self.__fs, self.__path, options)
        if data_set is None:
            if not HAS_DELTA_LAKE:
                raise MissingModuleException("deltalake")
            data_set = DeltaTable(self.__uri).to_pyarrow_dataset()
        return data_set

    def read_batches(
        self, options: Optional[InspectOptions]
    ) -> Generator[RecordBatch, None, None]:
        """Iterate over all record batches in a memory-efficient way."""
        if (
            get_resource_type_from_extension(self.__path.suffix.lstrip("."))
            == ResourceType.PARQUET
        ):
            stream = self.__fs.open_input_file(str(self.__path))
            rdr = parquet.ParquetFile(stream)
            column_names = _get_column_names(options)
            yield from rdr.iter_batches(columns=column_names)
        else:
            data_set = self.get_data_set(options)
            column_names = _get_column_names(options)
            yield from data_set.to_batches(columns=column_names, batch_readahead=1)


def _get_column_names(options: Optional[InspectOptions]) -> Optional[List[str]]:
    column_names = None
    if options is not None and options.column_names is not None:
        column_names = options.column_names
    return column_names


def get_data_set(
    filesystem: fs.FileSystem, path: PurePath, options: Optional[InspectOptions]
) -> Optional[Dataset]:
    """Return a PyArrow dataset for the resources at the given path."""
    resource_type = get_resource_type_from_extension(path.suffix.lstrip("."))
    if resource_type in [
        ResourceType.ARROW,
        ResourceType.PARQUET,
        ResourceType.CSV,
        ResourceType.NDJSON,
        ResourceType.ORC,
    ]:
        format = resource_type.value
        if resource_type == ResourceType.CSV and options is not None:
            format = CsvFileFormat(
                read_options=csv.ReadOptions(
                    autogenerate_column_names=not options.csv_header_row
                ),
                parse_options=csv.ParseOptions(delimiter=options.csv_delimiter),
            )
        return dataset(str(path), format=format, filesystem=filesystem)
    if resource_type == ResourceType.GPX:
        return dataset(parse_gpx(filesystem.open_input_file(str(path))))
    return None


def _get_resource_type(
    filesystem: fs.FileSystem, file_info: fs.FileInfo
) -> Optional[ResourceType]:
    if file_info.type == fs.FileType.Directory:
        for file_inside in filesystem.get_file_info(fs.FileSelector(file_info.path)):
            if file_inside.base_name == "_delta_log":
                return ResourceType.DELTA
        return ResourceType.DIRECTORY
    return get_resource_type_from_extension(file_info.extension)


def get_resource_type_from_extension(  # noqa: PLR0911
    extension: str,
) -> Optional[ResourceType]:
    """Return the resource type based on a file extension."""
    if extension == "parquet":
        return ResourceType.PARQUET
    if extension in ["arrow", "feather"]:
        return ResourceType.ARROW
    if extension in ["arrows"]:
        return ResourceType.ARROWS
    if extension in ["csv", "txt"]:
        return ResourceType.CSV
    if extension in ["gpx"]:
        return ResourceType.GPX
    if extension in ["ndjson", "json"]:
        return ResourceType.NDJSON
    if extension in ["orc"]:
        return ResourceType.ORC
    return None
