"""Delta Lake source for Kukur.

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
from kukur.loader import Loader
from kukur.source.arrow import BaseArrowSource, BaseArrowSourceOptions, SourcePartition
from kukur.source.quality import QualityMapper


class DeltaLakeLoader:
    """Fakes a loader for Delta Lake tables.

    It does not really load files, as the other loaders do.
    """

    def __init__(self, config: dict) -> None:
        self.__uri = config["uri"]

    def open(self):
        """Return the URI to connect to."""
        return self.__uri

    def has_child(self, subpath: str) -> bool:
        """Not supported for Delta Lake."""
        raise NotImplementedError()

    def open_child(self, subpath: str):
        """Not supported for Delta Lake."""
        raise NotImplementedError()


class DeltaLakeSource(BaseArrowSource):
    """Connect to a Delta Lake."""

    __options: BaseArrowSourceOptions

    def __init__(
        self,
        options: BaseArrowSourceOptions,
        loader: Loader,
        quality_mapper: QualityMapper,
        *,
        sort_by_timestamp: bool = False
    ):
        self.__options = options
        super().__init__(
            options, loader, quality_mapper, sort_by_timestamp=sort_by_timestamp
        )

    def read_file(self, file_like, selector=None) -> Table:
        """Return a PyArrow Table for the Delta Table at the given URI."""
        partitions = []
        if self.__options.partitions is not None:
            for partition in self.__options.partitions:
                if partition.origin == "tag":
                    column_name = self.__options.column_mapping.get(
                        partition.key, partition.key
                    )
                partitions.append((column_name, "=", selector.tags[partition.key]))

        return DeltaTable(file_like).to_pyarrow_table(partitions)

    def get_file_extension(self) -> str:
        """Delta lakes do not support row-based formats."""
        raise NotImplementedError()


def from_config(config: dict, quality_mapper: QualityMapper) -> DeltaLakeSource:
    """Create a new delta lake data source from the given configuration dictionary."""
    if not HAS_DELTA_LAKE:
        raise MissingModuleException("deltalake", "delta")

    data_format = config.get("format", "row")
    options = BaseArrowSourceOptions(
        data_format,
        config.get("column_mapping", {}),
        config.get("tag_columns", ["series name"]),
        config.get("field_columns", ["value"]),
    )
    options.partitions = [
        SourcePartition.from_data(partition)
        for partition in config.get("partitions", [])
    ]
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
