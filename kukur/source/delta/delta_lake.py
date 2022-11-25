"""
Delta Lake source for Kukur

Two formats are supported:
- row based, with many series in one file containing one row per data point
- pivot, with many series as columns in one file
"""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0


try:
    from deltalake import DeltaTable

    HAS_DELTA_LAKE = True
except ImportError:
    HAS_DELTA_LAKE = False

from pyarrow import Table

from kukur.exceptions import InvalidSourceException, MissingModuleException
from kukur.source.arrow import BaseArrowSource, BaseArrowSourceOptions
from kukur.source.quality import QualityMapper


class DeltaLakeLoader:
    """Fakes a loader for Delta Lake tables.

    It does not really load files, as the other loaders do."""

    def __init__(self, config: dict) -> None:
        self.__uri = config["uri"]

    def open(self):
        """Return the URI to connect to."""
        return self.__uri

    def has_child(self, subpath: str) -> bool:
        """Not supported for Delta Lake"""
        raise NotImplementedError()

    def open_child(self, subpath: str):
        """Not supported for Delta Lake"""
        raise NotImplementedError()


class DeltaLakeSource(BaseArrowSource):
    """Connect to a Delta Lake"""

    def read_file(self, file_like) -> Table:
        """Return a PyArrow Table for the Delta Table at the given URI."""
        return DeltaTable(file_like).to_pyarrow_table()

    def get_file_extension(self) -> str:
        """Delta lakes do not support row-based formats."""
        raise NotImplementedError()


def from_config(config: dict, quality_mapper: QualityMapper) -> DeltaLakeSource:
    """Create a new delta lake data source from the given configuration dictionary."""
    if not HAS_DELTA_LAKE:
        raise MissingModuleException("deltalake", "delta")

    data_format = config.get("format", "row")
    options = BaseArrowSourceOptions(data_format, config.get("column_mapping", {}))
    if data_format not in ["row", "pivot"]:
        raise InvalidSourceException(
            'Delta lake sources support only the "row" and "pivot" format.'
        )
    if "uri" not in config:
        raise InvalidSourceException('Delta lake sources require an "uri" entry')
    return DeltaLakeSource(
        options,
        DeltaLakeLoader(config),
        quality_mapper,
        sort_by_timestamp=config.get("sort_by_timestamp", False),
    )
