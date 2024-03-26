"""Kukur source for Apache Arrow DataFusion."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Generator, List, Optional, Union

import pyarrow as pa

from kukur import Metadata
from kukur.base import SeriesSearch, SeriesSelector
from kukur.exceptions import InvalidSourceException, MissingModuleException
from kukur.source.metadata import MetadataValueMapper

try:
    from datafusion import SessionConfig, SessionContext

    HAS_DATA_FUSION = True
except ImportError:
    HAS_DATA_FUSION = False


try:
    from deltalake import DeltaTable

    HAS_DELTA_LAKE = True
except ImportError:
    HAS_DELTA_LAKE = False


class DataFusionTableType(Enum):
    """The type of a DataFusion table."""

    CSV = "csv"
    DELTA = "delta"
    JSON = "json"
    PARQUET = "parquet"


@dataclass
class DataFusionTable:
    """A table registered in a DataFusion source."""

    type: DataFusionTableType
    name: str
    location: str

    @classmethod
    def from_data(cls, data: Dict) -> "DataFusionTable":
        """Create a table definition from a JSON dictionary."""
        return cls(DataFusionTableType(data["type"]), data["name"], data["location"])


@dataclass
class DataFusionSourceOptions:
    """Configuration for DataFusion sources."""

    tables: List[DataFusionTable]
    tag_columns: List[str]
    field_columns: List[str]
    list_query: Optional[str]

    @classmethod
    def from_data(cls, data: Dict) -> "DataFusionSourceOptions":
        """Create from a JSON dictionary."""
        return DataFusionSourceOptions(
            [DataFusionTable.from_data(table) for table in data.get("table", [])],
            data.get("tag_columns", ["series name"]),
            data.get("field_columns", ["value"]),
            data.get("list_query"),
        )


class DataFusionSource:
    """Query metadata from multiple Delta, CSV or JSON sources together using SQL."""

    def __init__(
        self,
        options: DataFusionSourceOptions,
        metadata_value_mapper: MetadataValueMapper,
    ):
        self.__options = options
        self.__metadata_value_mapper = metadata_value_mapper

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[SeriesSelector, Metadata], None, None]:
        """Return all time series or even the metadata of them in this source matching the selector."""
        if self.__options.list_query is None:
            raise InvalidSourceException("Missing list_query")

        context = self._get_context()
        table = context.sql(self.__options.list_query)
        for row in table.to_pylist():

            tags = {tag_name: row[tag_name] for tag_name in self.__options.tag_columns}
            for field_column in self.__options.field_columns:
                series = SeriesSelector(selector.source, tags, field_column)
                if len(row) == len(tags):
                    yield series
                else:
                    metadata = Metadata(series)
                    for k, v in row.items():
                        if k in self.__options.tag_columns:
                            continue
                        if v is None:
                            continue
                        metadata.coerce_field(
                            k, self.__metadata_value_mapper.from_source(k, v)
                        )
                    yield metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return metadata for the given time series."""
        raise NotImplementedError()

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        raise NotImplementedError()

    def _get_context(self):
        config = SessionConfig().set(
            "datafusion.sql_parser.enable_ident_normalization", "false"
        )
        context = SessionContext(config)
        for table in self.__options.tables:
            if table.type == DataFusionTableType.CSV:
                context.register_csv(table.name, table.location)
            if table.type == DataFusionTableType.JSON:
                context.register_json(table.name, table.location)
            if table.type == DataFusionTableType.PARQUET:
                context.register_parquet(table.name, table.location)
            if table.type == DataFusionTableType.DELTA:
                delta_table = DeltaTable(table.location)
                context.register_dataset(table.name, delta_table.to_pyarrow_dataset())
        return context


def from_config(
    config: Dict, metadata_value_mapper: MetadataValueMapper
) -> DataFusionSource:
    """Create a new DataFusion data source from the given configuration dictionary."""
    if not HAS_DATA_FUSION:
        raise MissingModuleException("datafusion", "datafusion")

    options = DataFusionSourceOptions.from_data(config)

    if not HAS_DELTA_LAKE:
        if any(table.type == DataFusionTableType.DELTA for table in options.tables):
            raise MissingModuleException("deltalake", "delta")

    return DataFusionSource(
        options,
        metadata_value_mapper,
    )
