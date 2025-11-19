"""Inspect and preview PyArrow filesystems."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import os
from collections.abc import Generator
from pathlib import PurePath

from pyarrow import RecordBatch, csv, fs, parquet
from pyarrow.dataset import CsvFileFormat, Dataset, dataset

from kukur.exceptions import MissingModuleException
from kukur.inspect import DataOptions, FileOptions, InspectedPath, ResourceType
from kukur.source.excel import list_sheets, parse_excel
from kukur.source.gpx import parse_gpx

try:
    from deltalake import DeltaTable

    HAS_DELTA_LAKE = True
except ImportError:
    HAS_DELTA_LAKE = False


def inspect(
    filesystem: fs.FileSystem, path: PurePath, options: FileOptions
) -> list[InspectedPath]:
    """Return the resource type of a path within a filesystem."""
    info = filesystem.get_file_info(str(path))
    if info.type == fs.FileType.File:
        return _inspect_excel_workbook(filesystem, path)
    return _inspect_folder(filesystem, path, options)


class BlobResource:
    """A blob resource backed by PyArrow."""

    def __init__(self, uri: str, filesystem: fs.FileSystem, path: PurePath):
        self.__uri = uri
        self.__fs = filesystem
        self.__path = path

    def get_data_set(self, options: DataOptions | None) -> Dataset:
        """Return a DataSet for the resource."""
        data_set = get_data_set(self.__fs, self.__path, options)
        if data_set is None:
            if not HAS_DELTA_LAKE:
                raise MissingModuleException("deltalake")
            data_set = DeltaTable(self.__uri).to_pyarrow_dataset()
        return data_set

    def read_batches(
        self, options: DataOptions | None
    ) -> Generator[RecordBatch, None, None]:
        """Iterate over all record batches in a memory-efficient way."""
        default_resource_type = None
        if options is not None:
            default_resource_type = options.default_resource_type
        if (
            get_resource_type_from_extension(
                self.__path.suffix.lstrip("."), default_resource_type
            )
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


def _get_column_names(options: DataOptions | None) -> list[str] | None:
    column_names = None
    if options is not None and options.column_names is not None:
        column_names = options.column_names
    return column_names


def get_data_set(
    filesystem: fs.FileSystem, path: PurePath, options: DataOptions | None
) -> Dataset | None:
    """Return a PyArrow dataset for the resources at the given path."""
    default_resource_type = None
    if options is not None:
        default_resource_type = options.default_resource_type
    file_extension = _get_file_extension(str(path))
    resource_type = get_resource_type_from_extension(
        file_extension, default_resource_type
    )
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
    if resource_type in [ResourceType.EXCEL_WORKBOOK, ResourceType.EXCEL_WORKSHEET]:
        file_path, sheet_name = str(path).rsplit("@", 1)
        return dataset(
            parse_excel(filesystem.open_input_file(file_path), sheet_name, options)
        )
    return None


def _get_resource_type(
    filesystem: fs.FileSystem, file_info: fs.FileInfo, options: FileOptions
) -> ResourceType | None:
    if file_info.type == fs.FileType.Directory:
        if options.detect_delta:
            for file_inside in filesystem.get_file_info(
                fs.FileSelector(file_info.path)
            ):
                if file_inside.base_name == "_delta_log":
                    return ResourceType.DELTA
        return ResourceType.DIRECTORY
    return get_resource_type_from_extension(
        file_info.extension, options.default_resource_type
    )


def get_resource_type_from_extension(  # noqa: PLR0911
    extension: str, default_type: ResourceType | None
) -> ResourceType | None:
    """Return the resource type based on a file extension.

    Returns None when the extension is unknown.
    Returns default_type for files without an extensions.
    """
    if extension == "" and default_type is not None:
        return default_type
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
    if extension in ["xls", "xlsx"]:
        return ResourceType.EXCEL_WORKBOOK
    if extension in ["ndjson", "json"]:
        return ResourceType.NDJSON
    if extension in ["orc"]:
        return ResourceType.ORC
    return None


def _inspect_excel_workbook(
    filesystem: fs.FileSystem, path: PurePath
) -> list[InspectedPath]:
    return [
        InspectedPath(ResourceType.EXCEL_WORKSHEET, sheet)
        for sheet in list_sheets(filesystem.open_input_file(str(path)))
    ]


def _inspect_folder(
    filesystem: fs.FileSystem, path: PurePath, options: FileOptions
) -> list[InspectedPath]:
    paths = []
    for sub_path in filesystem.get_file_info(
        fs.FileSelector(str(path), recursive=options.recursive)
    ):
        resource_type = _get_resource_type(filesystem, sub_path, options)
        if resource_type is not None:
            paths.append(InspectedPath(resource_type, sub_path.path))
    return paths


def _get_file_extension(path: str) -> str:
    file_path = path.rsplit("@", 1)[0] if "@" in path else path
    return os.path.splitext(file_path)[1].lstrip(".")
