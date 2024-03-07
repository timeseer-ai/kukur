"""Data sources for Kukur."""

import functools
import inspect
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.types

from kukur import (
    Metadata,
    PlotSource,
    SeriesSearch,
    SeriesSelector,
    SourceStructure,
    TagSource,
)
from kukur import Source as SourceProtocol
from kukur.exceptions import InvalidSourceException
from kukur.source import (
    adodb,
    arrows,
    azure_data_explorer,
    cratedb,
    csv,
    delta,
    feather,
    influxdb,
    integration_test,
    odbc,
    parquet,
    piwebapi_da,
    plugin,
    postgresql,
    redshift,
    simulator,
    sqlite,
)
from kukur.source import json as json_source
from kukur.source import kukur as kukur_source
from kukur.source.quality import QualityMapper

from .metadata import MetadataMapper, MetadataValueMapper

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

logger = logging.getLogger(__name__)

_FACTORY = {
    "adodb": adodb.from_config,
    "arrows": arrows.from_config,
    "cratedb": cratedb.from_config,
    "csv": csv.from_config,
    "azure-data-explorer": azure_data_explorer.from_config,
    "delta": delta.from_config,
    "feather": feather.from_config,
    "influxdb": influxdb.from_config,
    "integration-test": integration_test.from_config,
    "json": json_source.from_config,
    "kukur": kukur_source.from_config,
    "odbc": odbc.from_config,
    "parquet": parquet.from_config,
    "piwebapi-da": piwebapi_da.from_config,
    "plugin": plugin.from_config,
    "postgresql": postgresql.from_config,
    "redshift": redshift.from_config,
    "simulator": simulator.from_config,
    "sqlite": sqlite.from_config,
}


@dataclass
class MetadataSource:
    """A metadata source provides at least some metadata fields."""

    source: SourceProtocol
    fields: List[str] = field(default_factory=list)


@dataclass
class Source:
    """A Kukur source can contain different metadata and data sources.

    Source keeps them together.
    """

    metadata: Union[SourceProtocol, TagSource]
    data: SourceProtocol


def _retry(retry_count: int, retry_delay: float, data_fn, log_message: str):
    while True:
        try:
            return data_fn()
        except Exception as err:  # pylint: disable=broad-except
            retry_count = retry_count - 1
            if retry_count < 0:
                raise err
            logger.info(
                "%s, %d retries left",
                log_message,
                retry_count + 1,
                exc_info=True,
            )
            time.sleep(retry_delay)


class SourceWrapper:
    """Handles query policy for a data source.

    It ensures requests do not overload a source.

    Metadata for a source can be fetched from multiple external sources and will be merged here.
    """

    __source: Source
    __metadata: List[MetadataSource]
    __query_retry_count: int
    __query_retry_delay: float
    __data_query_interval: Optional[timedelta] = None

    def __init__(
        self,
        source: Source,
        metadata_sources: List[MetadataSource],
        common_options,
    ):
        self.__source = source
        self.__metadata = metadata_sources
        self.__query_retry_count = common_options.get("query_retry_count", 0)
        self.__query_retry_delay = common_options.get("query_retry_delay", 1.0)
        if "data_query_interval_seconds" in common_options:
            self.__data_query_interval = timedelta(
                seconds=common_options["data_query_interval_seconds"]
            )

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[SeriesSelector, Metadata], None, None]:
        """Search for all time series matching the given selector.

        The result is either a sequence of selectors for each time series in the source or a sequence of metadata
        entries for all series in the source if fetching the metadata can be done in the same request.

        If metadata sources are configured, query them as well and merge the results. This means that sources that
        are fast to search because they return metadata now also result in one additional query to each metadata
        source for each series.
        """
        query_fn = functools.partial(self.__source.metadata.search, selector)
        results = _retry(
            self.__query_retry_count,
            self.__query_retry_delay,
            query_fn,
            f"Search query for {selector.source} failed",
        )
        if results is None:
            return
        for result in results:
            if len(self.__metadata) == 0 or isinstance(result, SeriesSelector):
                yield result
            else:
                series_selector = SeriesSelector.from_tags(
                    result.series.source, result.series.tags, result.series.field
                )
                try:
                    extra_metadata = self.get_metadata(series_selector)
                    for k, v in result.iter_names():
                        if v is not None and v != "":
                            extra_metadata.set_field_by_name(k, v)
                    yield extra_metadata
                except Exception:  # pylint: disable=broad-except
                    logger.error(
                        """Metadata query for "%s" (%s) failed all attempts.""",
                        series_selector.name,
                        series_selector.source,
                    )
                    yield result

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return the metadata for the given series.

        The resulting metadata is the combination of the metadata in the source itself and any additional
        metadata sources. Metadata sources earlier in the list of sources take precendence over later ones.
        """
        metadata = Metadata(selector)
        for metadata_source in list(reversed(self.__metadata)) + [
            MetadataSource(self.__source.metadata)
        ]:
            query_fn = functools.partial(metadata_source.source.get_metadata, selector)

            received_metadata = _retry(
                self.__query_retry_count,
                self.__query_retry_delay,
                query_fn,
                f'Metadata query for "{selector.name}" ({selector.source}) failed',
            )
            if len(metadata_source.fields) == 0:
                for k, v in received_metadata.iter_names():
                    if v is not None and v != "":
                        metadata.set_field_by_name(k, v)
            else:
                for field_name in metadata_source.fields:
                    field_value = received_metadata.get_field_by_name(field_name)
                    if field_value is not None and field_value != "":
                        metadata.set_field_by_name(field_name, field_value)
        return metadata

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return the data for the given series in the given time frame, taking into account the request policy."""
        if start_date == end_date:
            return pa.Table.from_pydict({"ts": [], "value": [], "quality": []})
        tables = [
            self._get_data_chunk(selector, start, end)
            for start, end in self.__to_intervals(start_date, end_date)
        ]
        return concat_tables(tables)

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ) -> pa.Table:
        """Return the plotting data for the given series in the given time frame.

        Plot data calls are not subject to request splitting, but do obsever other settings.

        Returns normal data when plot data is not supported.
        """
        if start_date == end_date:
            return pa.Table.from_pydict({"ts": [], "value": [], "quality": []})
        if not isinstance(self.__source.data, PlotSource):
            return self.get_data(selector, start_date, end_date)
        query_fn = functools.partial(
            self.__source.data.get_plot_data,
            selector,
            start_date,
            end_date,
            interval_count,
        )
        return _retry(
            self.__query_retry_count,
            self.__query_retry_delay,
            query_fn,
            f'Plot data query for "{selector.name}" ({selector.source}) ({start_date} to {end_date}) failed',
        )

    def get_source_structure(
        self, selector: SeriesSelector
    ) -> Optional[SourceStructure]:
        """Return the structure of the source for the given series."""
        if not isinstance(self.__source.metadata, TagSource):
            return None
        query_fn = functools.partial(
            self.__source.metadata.get_source_structure, selector
        )
        return _retry(
            self.__query_retry_count,
            self.__query_retry_delay,
            query_fn,
            f"Source structure query for {selector.source} failed",
        )

    def _get_data_chunk(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ):
        query_fn = functools.partial(
            self.__source.data.get_data, selector, start_date, end_date
        )
        return _retry(
            self.__query_retry_count,
            self.__query_retry_delay,
            query_fn,
            f'Data query for "{selector.name}" ({selector.source}) ({start_date} to {end_date}) failed',
        )

    def __to_intervals(
        self, start_date: datetime, end_date: datetime
    ) -> Generator[Tuple[datetime, datetime], None, None]:
        if self.__data_query_interval is None:
            yield (start_date, end_date)
            return
        current_date = start_date
        while current_date < end_date:
            next_date = current_date + self.__data_query_interval
            next_date = min(next_date, end_date)
            yield (current_date, next_date)
            current_date = next_date


class SourceFactory:
    """Source factory to create Source objects."""

    __config: Dict[str, Any]
    __factory: Dict[str, Callable]

    def __init__(self, config):
        self.__config = config
        self.__factory = _FACTORY.copy()

    def register_source(
        self, source_type_name: str, source_factory: Callable[..., SourceProtocol]
    ):
        """Register a new source type with the factory."""
        self.__factory[source_type_name] = source_factory

    def get_source_names(self) -> List[str]:
        """Get the data sources names as configured in the Kukur configuration."""
        sources = []
        for name, _ in self.__config.get("source", {}).items():
            sources.append(name)
        return sources

    def get_source(self, source_name: str) -> Optional[SourceWrapper]:
        """Get the data source and type as configured in the Kukur configuration."""
        metadata_sources = self._get_extra_metadata_sources()

        for name, options in self.__config.get("source", {}).items():
            if source_name == name:
                if "type" not in options:
                    raise InvalidSourceException(f'"{name}" has no type')
                source_type = options["type"]
                if source_type not in self.__factory:
                    raise InvalidSourceException(
                        f'Source "{name}" has unknown type "{source_type}"'
                    )
                data_source = self._make_source(source_type, options)
                metadata_source = data_source
                metadata_source_type = options.get("metadata_type", source_type)
                if metadata_source_type != source_type:
                    if metadata_source_type not in self.__factory:
                        raise InvalidSourceException(
                            f'"Source {name}" has unknown metadata type "{metadata_source_type}"'
                        )
                    metadata_source = self._make_source(metadata_source_type, options)

                extra_metadata = []
                for metadata_source_name in options.get("metadata_sources", []):
                    if metadata_source_name not in metadata_sources:
                        raise InvalidSourceException(
                            f'Metadata source "{metadata_source_name}" for source "{name}" not found'
                        )
                    extra_metadata.append(metadata_sources[metadata_source_name])

                return SourceWrapper(
                    Source(metadata_source, data_source), extra_metadata, options
                )
        return None

    def _get_extra_metadata_sources(self) -> Dict[str, MetadataSource]:
        metadata_sources = {}
        for name, options in self.__config.get("metadata", {}).items():
            if "type" not in options:
                raise InvalidSourceException(f'Metadata source "{name}" has no type.')
            source_type = options["type"]
            if source_type not in self.__factory:
                raise InvalidSourceException(
                    f'Metadata source "{name}" has unknown type "{source_type}"'
                )
            metadata_sources[name] = MetadataSource(
                self._make_source(source_type, options), options.get("fields", [])
            )
        return metadata_sources

    def _make_source(
        self, source_type: str, source_config: Dict[str, Any]
    ) -> Union[SourceProtocol, TagSource]:
        metadata_mapper = self._get_metadata_mapper(
            source_config.get("metadata_mapping")
        )
        metadata_value_mapper = self._get_metadata_value_mapper(
            source_config.get("metadata_value_mapping")
        )
        quality_mapper = self._get_quality_mapper(source_config.get("quality_mapping"))

        factory_function: Any = self.__factory[source_type]

        arguments: List[Any] = []
        for parameter in inspect.signature(factory_function).parameters.values():
            if parameter.annotation == MetadataMapper:
                arguments.append(metadata_mapper)
            elif parameter.annotation == MetadataValueMapper:
                arguments.append(metadata_value_mapper)
            elif parameter.annotation == QualityMapper:
                arguments.append(quality_mapper)
            else:
                arguments.append(source_config)

        return factory_function(*arguments)

    def _get_metadata_mapper(self, name: Optional[str]) -> MetadataMapper:
        if name is None or name not in self.__config.get("metadata_mapping", {}):
            return MetadataMapper()

        return MetadataMapper.from_config(self.__config["metadata_mapping"][name])

    def _get_metadata_value_mapper(self, name: Optional[str]) -> MetadataValueMapper:
        if name is None or name not in self.__config.get("metadata_value_mapping", {}):
            return MetadataValueMapper()

        return MetadataValueMapper.from_config(
            self.__config["metadata_value_mapping"][name]
        )

    def _get_quality_mapper(self, name: Optional[str]) -> QualityMapper:
        if name is None or name not in self.__config.get("quality_mapping", {}):
            return QualityMapper()

        return QualityMapper.from_config(self.__config["quality_mapping"][name])


def concat_tables(tables: List[pa.Table]) -> pa.Table:
    """Safely concatenate multiple pyarrow.Table's.

    If any of the given tables contains strings, the result will contain a
    string value. If any of the given tables contains a floating point number,
    the value will be a double.
    """
    tables = [table for table in tables if len(table) > 0]
    if len(tables) == 0:
        return pa.Table.from_pydict({"ts": [], "value": [], "quality": []})

    schema = pa.schema(
        [
            ("ts", pa.timestamp("us", "UTC")),
            ("value", pa.float64()),
        ]
    )

    if _has_any_string(tables):
        schema = pa.schema(
            [
                ("ts", pa.timestamp("us", "UTC")),
                ("value", pa.string()),
            ]
        )

    if _is_all_integer(tables):
        schema = pa.schema(
            [
                ("ts", pa.timestamp("us", "UTC")),
                ("value", pa.int64()),
            ]
        )

    if _has_quality_data_flag(tables):
        schema = schema.append(pa.field("quality", pa.int8()))

    return pa.concat_tables([table.cast(schema) for table in tables])


def _has_any_string(tables: List[pa.Table]) -> bool:
    string_tables = [
        table
        for table in tables
        if pyarrow.types.is_string(table.schema.field("value").type)
    ]
    return len(string_tables) > 0


def _is_all_integer(tables: List[pa.Table]) -> bool:
    integer_tables = [
        table
        for table in tables
        if pyarrow.types.is_integer(table.schema.field("value").type)
    ]
    return len(integer_tables) == len(tables)


def _has_quality_data_flag(tables: List[pa.Table]) -> bool:
    quality_table = [table for table in tables if "quality" in table.column_names]
    return len(quality_table) > 0
