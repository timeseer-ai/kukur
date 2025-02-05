"""Find data files and schemas of resources.

Kukur will detect files based on their extension:

- Parquet in `.parquet`
- Arrow IPC in `.arrow` or `.feather`
- Arrow streaming IPC in `.arrows`
- CSV in `.csv` or `.txt`
- Delta in directories with a `_delta_log` directory
- Subdirectories

The `kukur.inspect.filesystem` module supports inspecting filesystems.
The `kukur.inspect.blob` module supports inspecting blob stores.
"""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from kukur.exceptions import KukurException


class UnsupportedBlobException(KukurException):
    """Raised when a blob store is not supported."""

    def __init__(self, uri_scheme: str):
        super().__init__(f"Unsupported blob URI scheme {uri_scheme}")


class InvalidInspectURI(KukurException):
    """Raised when the URI for inspecting a blob store is invalid."""


class ResourceType(Enum):
    """A type of resource that supports inspection."""

    ARROW = "arrow"
    ARROWS = "arrow"
    CSV = "csv"
    DELTA = "delta"
    DIRECTORY = "directory"
    GPX = "gpx"
    NDJSON = "json"
    ORC = "orc"
    PARQUET = "parquet"
    TABLE = "table"
    EXCEL_WORKBOOK = "excel-workbook"  # acts as a folder
    EXCEL_WORKSHEET = "excel-worksheet"


@dataclass
class InspectedPath:
    """A path to a resource that can be inspected."""

    resource_type: ResourceType
    path: str


@dataclass
class DataOptions:
    """Options for inspect data fetching operations.

    `column_names` restricts preview and data fetching operations to the specified columns only.
    `csv_delimiter` defines the delimiter used to separate columns in CSV files.
    `csv_header_row` indicates that the first row of a CSV file is a header row.
    `default_resource_type` assumes files without extension are of this type.
    """

    column_names: Optional[List[str]] = None
    csv_delimiter: Optional[str] = None
    csv_header_row: bool = True
    excel_header_row: bool = True
    default_resource_type: Optional[ResourceType] = None


@dataclass
class FileOptions:
    """Options for file based inspect operations.

    `detect_delta`: enable to try to detect Delta tables
    `default_resource_type`: assume files without extension are of this type.
    `recursive`: recurse into subdirectories when inspecting directories.
    """

    detect_delta: bool = False
    default_resource_type: Optional[ResourceType] = None
    recursive: bool = False

    @classmethod
    def from_data(cls, data: Dict) -> "FileOptions":
        """Read FileOptions from a data dictionary."""
        options = cls()
        if "detect_delta" in data:
            options.detect_delta = data["detect_delta"]
        if "default_resource_type" in data:
            options.default_resource_type = ResourceType(data["default_resource_type"])
        if "recursive" in data:
            options.recursive = data["recursive"]
        return options


@dataclass
class Connection:
    """Defines the connection to a database."""

    connection_type: str
    catalog: Optional[str]
    connection_string: Optional[str]
    connection_options: Optional[dict]
    limit_specification: Optional[str]
