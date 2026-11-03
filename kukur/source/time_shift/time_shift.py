"""Turn a source covering a fixed time range in history into a live source.

The time-shift source wraps another source. It either shifts all timestamps by a
fixed amount or repeats a period of the other source.
"""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.compute

from kukur import Metadata, PlotSource, SeriesSearch, SeriesSelector, SourceProvider
from kukur.arrow import concat_tables
from kukur.exceptions import InvalidSourceException
from kukur.timestamp import parse_timestamp


@dataclass
class ShiftedRange:
    """A time range in the wrapped source that maps to a part of a request.

    The offset is added to timestamps in the wrapped source.
    """

    start_date: datetime
    end_date: datetime
    offset: timedelta


class TimeMapping(Protocol):
    """Maps requested time ranges to time ranges in the wrapped source."""

    def ranges(
        self, start_date: datetime, end_date: datetime
    ) -> Generator[ShiftedRange, None, None]:
        """Return the time ranges in the wrapped source that cover the request."""
        ...


class Shift:
    """Shift all timestamps by a fixed offset."""

    def __init__(self, offset: timedelta):
        self.__offset = offset

    def ranges(
        self, start_date: datetime, end_date: datetime
    ) -> Generator[ShiftedRange, None, None]:
        """Return the shifted request."""
        yield ShiftedRange(
            start_date - self.__offset, end_date - self.__offset, self.__offset
        )


class Repeat:
    """Repeat the data between source_start and source_end.

    When data_start is given, the repetitions start at data_start and there is no
    data before it. Otherwise, the period repeats forever in both directions.
    """

    def __init__(
        self,
        source_start: datetime,
        source_end: datetime,
        data_start: datetime | None,
    ):
        self.__source_start = source_start
        self.__period = source_end - source_start
        self.__data_start = data_start

    def ranges(
        self, start_date: datetime, end_date: datetime
    ) -> Generator[ShiftedRange, None, None]:
        """Return one time range for each repetition the request overlaps."""
        anchor = self.__source_start
        if self.__data_start is not None:
            anchor = self.__data_start
            start_date = max(start_date, self.__data_start)
        if start_date >= end_date:
            return
        repetition = (start_date - anchor) // self.__period
        repetition_start = anchor + repetition * self.__period
        while repetition_start < end_date:
            offset = repetition_start - self.__source_start
            range_start = max(start_date, repetition_start)
            range_end = min(end_date, repetition_start + self.__period)
            yield ShiftedRange(range_start - offset, range_end - offset, offset)
            repetition_start = repetition_start + self.__period


class TimeShiftSource:
    """Moves the data of another source in time."""

    def __init__(self, source: PlotSource, mapping: TimeMapping):
        self.__source = source
        self.__mapping = mapping

    def search(
        self, selector: SeriesSearch
    ) -> Generator[SeriesSelector | Metadata, None, None]:
        """Search for series in the wrapped source."""
        yield from self.__source.search(selector)

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return the metadata of the series in the wrapped source."""
        return self.__source.get_metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return the data of the wrapped source, moved in time."""
        return concat_tables(
            [
                _shift(
                    self.__source.get_data(
                        selector, time_range.start_date, time_range.end_date
                    ),
                    time_range.offset,
                )
                for time_range in self.__mapping.ranges(start_date, end_date)
            ]
        )

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ) -> pa.Table:
        """Return the plot data of the wrapped source, moved in time.

        The intervals are divided over the time ranges according to their duration.
        """
        duration = end_date - start_date
        tables = []
        for time_range in self.__mapping.ranges(start_date, end_date):
            range_interval_count = max(
                1,
                round(
                    interval_count
                    * (time_range.end_date - time_range.start_date)
                    / duration
                ),
            )
            table = self.__source.get_plot_data(
                selector,
                time_range.start_date,
                time_range.end_date,
                range_interval_count,
            )
            tables.append(_shift(table, time_range.offset))
        return concat_tables(tables)


def from_config(
    config: dict[str, Any], source_provider: SourceProvider
) -> TimeShiftSource:
    """Create a new time-shift source."""
    if "source" not in config:
        raise InvalidSourceException('time-shift source requires a "source"')
    source = source_provider.get_source(config["source"])
    if source is None:
        raise InvalidSourceException(f'Source "{config["source"]}" not found')
    return TimeShiftSource(source, _get_mapping(config))


def _get_mapping(config: dict[str, Any]) -> TimeMapping:
    repeat_keys = ["source_start", "source_end", "data_start"]
    has_shift = "time_shift_seconds" in config
    has_repeat = any(key in config for key in repeat_keys)
    if has_shift == has_repeat:
        raise InvalidSourceException(
            'time-shift source requires either "time_shift_seconds" or "source_start" and "source_end"'
        )
    if has_shift:
        return Shift(timedelta(seconds=config["time_shift_seconds"]))
    if "source_start" not in config or "source_end" not in config:
        raise InvalidSourceException(
            'time-shift source requires both "source_start" and "source_end"'
        )
    source_start = _parse_date(config["source_start"])
    source_end = _parse_date(config["source_end"])
    if source_end <= source_start:
        raise InvalidSourceException(
            'time-shift source requires "source_end" to be after "source_start"'
        )
    data_start = None
    if "data_start" in config:
        data_start = _parse_date(config["data_start"])
    return Repeat(source_start, source_end, data_start)


def _parse_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        timestamp = value
    elif isinstance(value, date):
        timestamp = datetime(value.year, value.month, value.day)  # noqa: DTZ001
    else:
        timestamp = parse_timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp


def _shift(table: pa.Table, offset: timedelta) -> pa.Table:
    if offset == timedelta(0) or len(table) == 0:
        return table
    index = table.schema.get_field_index("ts")
    return table.set_column(
        index, "ts", pyarrow.compute.add(table["ts"], pa.scalar(offset))
    )
