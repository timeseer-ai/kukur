"""Test connections to Timeseer data sources.

This takes care to not persistently store metadata."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging

from datetime import datetime, timezone
from typing import Any, Generator, List

from dateutil.tz import tzlocal
from pyarrow import Table

from kukur import Metadata, PlotSource, SeriesSearch, SeriesSelector, Source


logger = logging.getLogger(__name__)


def search(source: Source, source_name: str) -> Generator[List[Any], None, None]:
    """Test listing all series (or metadata) in a source."""
    header_printed = False
    logger.info('Searching for time series in "%s"', source_name)
    for result in source.search(SeriesSearch(source_name)):
        if isinstance(result, SeriesSelector):
            if not header_printed:
                yield ["series name"]
                header_printed = True
            yield [result.name]
        else:
            if not header_printed:
                yield _get_metadata_header(result)
                header_printed = True
            yield _get_metadata(result)


def metadata(
    source: Source, source_name: str, series_name: str
) -> Generator[List[Any], None, None]:
    """Test fetching metadata from a source.

    This does not store the metadata."""
    logger.info('Requesting metadata for "%s (%s)"', series_name, source_name)
    result = source.get_metadata(SeriesSelector(source_name, series_name))
    yield _get_metadata_header(result)
    yield _get_metadata(result)


def data(
    source: Source,
    source_name: str,
    series_name: str,
    start_date: datetime,
    end_date: datetime,
) -> Generator[List[Any], None, None]:
    """Test fetching data for a time series."""
    start_date = _make_aware(start_date)
    end_date = _make_aware(end_date)
    logger.info(
        'Requesting data for "%s (%s)" from %s to %s',
        series_name,
        source_name,
        start_date,
        end_date,
    )

    table = source.get_data(
        SeriesSelector(source_name, series_name),
        start_date,
        end_date,
    )
    return _yield_table(table)


def plot(  # pylint: disable=too-many-arguments
    source: Source,
    source_name: str,
    series_name: str,
    start_date: datetime,
    end_date: datetime,
    interval_count: int,
) -> Generator[List[Any], None, None]:
    """Test fetching plot data for a time series."""
    if not isinstance(source, PlotSource):
        logger.warning("Plot data not supported by source %s", source_name)
        return
    start_date = _make_aware(start_date)
    end_date = _make_aware(end_date)
    logger.info(
        'Requesting plot data for "%s (%s)" from %s to %s',
        series_name,
        source_name,
        start_date,
        end_date,
    )

    table = source.get_plot_data(
        SeriesSelector(source_name, series_name), start_date, end_date, interval_count
    )
    yield from _yield_table(table)


def _get_metadata_header(result: Metadata) -> List[str]:
    return ["series name"] + [k for k, _ in result.iter_serialized()]


def _get_metadata(result: Metadata) -> List[Any]:
    return [result.series.tags["series name"]] + [
        v for _, v in result.iter_serialized()
    ]


def _make_aware(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=tzlocal()).astimezone(timezone.utc)
    return timestamp


def _yield_table(table: Table) -> Generator[List[Any], None, None]:
    if "quality" in table.column_names:
        for ts, value, quality in zip(table["ts"], table["value"], table["quality"]):
            yield [ts.as_py().isoformat(), value.as_py(), quality.as_py()]
    else:
        for ts, value in zip(table["ts"], table["value"]):
            yield [ts.as_py().isoformat(), value.as_py()]
