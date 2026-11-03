"""Test the time-shift source."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from kukur import SeriesSearch, SeriesSelector
from kukur.exceptions import InvalidSourceException
from kukur.source import SourceFactory, SourceWrapper


def get_source(options: dict[str, Any]) -> SourceWrapper:
    config = {
        "source": {
            "row": {
                "type": "csv",
                "path": "tests/test_data/csv/row.csv",
            },
            "time-shift": {"type": "time-shift", "source": "row"} | options,
        }
    }
    source = SourceFactory(config).get_source("time-shift")
    assert source is not None
    return source


def test_search() -> None:
    source = get_source({"time_shift_seconds": 3600})
    names = [series.name for series in source.search(SeriesSearch("time-shift"))]
    assert "test-tag-1" in names


def test_time_shift_seconds() -> None:
    source = get_source({"time_shift_seconds": 86400})
    table = source.get_data(
        SeriesSelector("time-shift", "test-tag-1"),
        datetime(2020, 1, 2, tzinfo=UTC),
        datetime(2020, 3, 2, tzinfo=UTC),
    )
    assert table["ts"].to_pylist() == [
        datetime(2020, 1, 2, tzinfo=UTC),
        datetime(2020, 2, 2, tzinfo=UTC),
    ]
    assert table["value"].to_pylist() == [1, 2]


def test_time_shift_seconds_plot_data() -> None:
    source = get_source({"time_shift_seconds": -86400})
    table = source.get_plot_data(
        SeriesSelector("time-shift", "test-tag-1"),
        datetime(2019, 12, 31, tzinfo=UTC),
        datetime(2020, 2, 29, tzinfo=UTC),
        10,
    )
    assert table["ts"].to_pylist() == [
        datetime(2019, 12, 31, tzinfo=UTC),
        datetime(2020, 1, 31, tzinfo=UTC),
    ]


def test_repeat() -> None:
    source = get_source(
        {
            "source_start": datetime(2020, 1, 1, tzinfo=UTC),
            "source_end": datetime(2020, 3, 1, tzinfo=UTC),
            "data_start": "2026-01-01T00:00:00Z",
        }
    )
    table = source.get_data(
        SeriesSelector("time-shift", "test-tag-1"),
        datetime(2025, 1, 1, tzinfo=UTC),
        datetime(2026, 5, 1, tzinfo=UTC),
    )
    period = timedelta(days=60)
    assert table["ts"].to_pylist() == [
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 2, 1, tzinfo=UTC),
        datetime(2026, 1, 1, tzinfo=UTC) + period,
        datetime(2026, 2, 1, tzinfo=UTC) + period,
    ]
    assert table["value"].to_pylist() == [1, 2, 1, 2]


def test_repeat_partial() -> None:
    source = get_source(
        {
            "source_start": "2020-01-01T00:00:00Z",
            "source_end": "2020-03-01T00:00:00Z",
            "data_start": "2026-01-01T00:00:00Z",
        }
    )
    table = source.get_data(
        SeriesSelector("time-shift", "test-tag-1"),
        datetime(2026, 1, 15, tzinfo=UTC),
        datetime(2026, 3, 15, tzinfo=UTC),
    )
    assert table["ts"].to_pylist() == [
        datetime(2026, 2, 1, tzinfo=UTC),
        datetime(2026, 3, 2, tzinfo=UTC),
    ]
    assert table["value"].to_pylist() == [2, 1]


def test_repeat_before_data_start() -> None:
    source = get_source(
        {
            "source_start": "2020-01-01T00:00:00Z",
            "source_end": "2020-03-01T00:00:00Z",
            "data_start": "2026-01-01T00:00:00Z",
        }
    )
    table = source.get_data(
        SeriesSelector("time-shift", "test-tag-1"),
        datetime(2025, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert len(table) == 0


def test_repeat_without_data_start() -> None:
    source = get_source(
        {
            "source_start": "2020-01-01T00:00:00Z",
            "source_end": "2020-03-01T00:00:00Z",
        }
    )
    period = timedelta(days=60)
    selector = SeriesSelector("time-shift", "test-tag-1")
    # 2026-01-01 is 37 periods after 2020-01-01, 2019-11-02 is one period before it.
    for repetition in [-1, 37]:
        repetition_start = datetime(2020, 1, 1, tzinfo=UTC) + repetition * period
        table = source.get_data(selector, repetition_start, repetition_start + period)
        assert table["ts"].to_pylist() == [
            repetition_start,
            repetition_start + timedelta(days=31),
        ]
        assert table["value"].to_pylist() == [1, 2]


def test_repeat_without_data_start_partial() -> None:
    source = get_source(
        {
            "source_start": "2020-01-01T00:00:00Z",
            "source_end": "2020-03-01T00:00:00Z",
        }
    )
    table = source.get_data(
        SeriesSelector("time-shift", "test-tag-1"),
        datetime(2019, 12, 1, tzinfo=UTC),
        datetime(2020, 1, 15, tzinfo=UTC),
    )
    # The previous repetition starts on 2019-11-02, so February 1 is on 2019-12-03.
    assert table["ts"].to_pylist() == [
        datetime(2019, 12, 3, tzinfo=UTC),
        datetime(2020, 1, 1, tzinfo=UTC),
    ]
    assert table["value"].to_pylist() == [2, 1]


def test_repeat_plot_data() -> None:
    source = get_source(
        {
            "source_start": "2020-01-01T00:00:00Z",
            "source_end": "2020-03-01T00:00:00Z",
            "data_start": "2026-01-01T00:00:00Z",
        }
    )
    table = source.get_plot_data(
        SeriesSelector("time-shift", "test-tag-1"),
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 5, 1, tzinfo=UTC),
        10,
    )
    assert table["value"].to_pylist() == [1, 2, 1, 2]


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"data_start": "2026-01-01T00:00:00Z"},
        {"source_start": "2020-01-01T00:00:00Z", "data_start": "2026-01-01"},
        {
            "time_shift_seconds": 3600,
            "source_start": "2020-01-01T00:00:00Z",
            "source_end": "2020-03-01T00:00:00Z",
            "data_start": "2026-01-01T00:00:00Z",
        },
        {
            "source_start": "2020-03-01T00:00:00Z",
            "source_end": "2020-01-01T00:00:00Z",
            "data_start": "2026-01-01T00:00:00Z",
        },
        {"time_shift_seconds": 3600, "source": "unknown"},
        {"time_shift_seconds": 3600, "source": "time-shift"},
    ],
)
def test_invalid_config(options: dict[str, Any]) -> None:
    with pytest.raises(InvalidSourceException):
        get_source(options)
