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
    PARQUET = "parquet"


@dataclass
class InspectedPath:
    """A path to a resource that can be inspected."""

    resource_type: ResourceType
    path: str
