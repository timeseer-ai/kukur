# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta

import pyarrow as pa
import pytest

from kukur.source import Source, SourceWrapper, SeriesSelector, Metadata


SELECTOR = SeriesSelector("fake", "test-tag-1")
START_DATE = datetime.fromisoformat("2020-01-01T00:00:00+00:00")
END_DATE = datetime.fromisoformat("2020-02-01T00:00:00+00:00")


class FakeSource:
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        return Metadata(selector)

    def get_data(
        self, _: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        # end_date should not be part of the returned interval, but is here for easy comparison
        return pa.Table.from_pydict({"ts": [start_date, end_date], "value": [42, 24]})


class EmptyOddHoursSource:
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        return Metadata(selector)

    def get_data(
        self, _: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        if start_date.hour % 2 == 0:
            return pa.Table.from_pydict(
                {"ts": [start_date, end_date], "value": [42, 24]}
            )
        return pa.Table.from_pydict({"ts": [], "value": []})


class EmptySource:
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        return Metadata(selector)

    def get_data(
        self, _: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        return pa.Table.from_pydict({"ts": [], "value": []})


class DifferentNumericalTypesSource:
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        return Metadata(selector)

    def get_data(
        self, _: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        if start_date.hour % 2 == 0:
            return pa.Table.from_pydict({"ts": [start_date], "value": [1]})
        return pa.Table.from_pydict({"ts": [start_date], "value": [2.5]})


class OnlyIntegersSource:
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        return Metadata(selector)

    def get_data(
        self, _: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        if start_date.hour % 2 == 0:
            return pa.Table.from_pydict({"ts": [start_date], "value": [1]})
        return pa.Table.from_pydict({"ts": [start_date], "value": [2]})


class SomeStringTypesSource:
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        return Metadata(selector)

    def get_data(
        self, _: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        if start_date.hour % 2 == 0:
            return pa.Table.from_pydict({"ts": [start_date], "value": ["ok"]})
        return pa.Table.from_pydict({"ts": [start_date], "value": [2.5]})


class FailureSource:

    __failure_count: int

    def __init__(self, *, failure_count=1) -> None:
        self.__failure_count = failure_count

    def search(self, selector: SeriesSelector):
        if self.__failure_count == 0:
            yield Metadata(selector)
        else:
            self.__failure_count = self.__failure_count - 1
            raise Exception("Search failure")

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        if self.__failure_count == 0:
            return Metadata(selector)
        self.__failure_count = self.__failure_count - 1
        raise Exception("Metadata failure")

    def get_data(
        self, _: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        if self.__failure_count == 0:
            return pa.Table.from_pydict({"ts": [start_date], "value": [2.5]})

        self.__failure_count = self.__failure_count - 1
        raise Exception("Data failure")


def test_split_empty():
    source = EmptySource()
    wrapper = SourceWrapper(
        Source(source, source), [], {"data_query_interval_seconds": 60 * 60}
    )
    result = wrapper.get_data(SELECTOR, START_DATE, END_DATE)
    assert len(result) == 0
    assert "ts" in result.column_names
    assert "value" in result.column_names


def test_split_none():
    wrapper = SourceWrapper(_make_source(), [], {})
    result = wrapper.get_data(SELECTOR, START_DATE, END_DATE)
    assert len(result) == 2
    data = result.to_pydict()
    assert data["ts"][0] == START_DATE
    assert data["value"][0] == 42
    assert data["ts"][1] == END_DATE
    assert data["value"][1] == 24


def test_split_equal_start_end():
    wrapper = SourceWrapper(
        _make_source(), [], {"data_query_interval_seconds": 24 * 60 * 60}
    )
    result = wrapper.get_data(SELECTOR, START_DATE, START_DATE)
    assert len(result) == 0
    assert "ts" in result.column_names
    assert "value" in result.column_names


def test_split_one_day():
    wrapper = SourceWrapper(
        _make_source(), [], {"data_query_interval_seconds": 24 * 60 * 60}
    )
    result = wrapper.get_data(SELECTOR, START_DATE, END_DATE)
    assert len(result) == 62
    data = result.to_pydict()
    assert data["ts"][0] == START_DATE
    assert data["value"][0] == 42
    assert data["ts"][1] == START_DATE + timedelta(seconds=24 * 60 * 60)
    assert data["value"][1] == 24
    assert data["ts"][61] == END_DATE
    assert data["value"][61] == 24


def test_split_partial_end_interval():
    end_date = datetime.fromisoformat("2020-01-31T12:00:00+00:00")

    wrapper = SourceWrapper(
        _make_source(), [], {"data_query_interval_seconds": 24 * 60 * 60}
    )
    result = wrapper.get_data(SELECTOR, START_DATE, end_date)
    assert len(result) == 62
    data = result.to_pydict()
    assert data["ts"][0] == START_DATE
    assert data["value"][0] == 42
    assert data["ts"][1] == START_DATE + timedelta(seconds=24 * 60 * 60)
    assert data["value"][1] == 24
    assert data["ts"][61] == end_date
    assert data["value"][61] == 24


def test_split_partial_interval():
    end_date = START_DATE + timedelta(seconds=60)

    wrapper = SourceWrapper(
        _make_source(), [], {"data_query_interval_seconds": 24 * 60 * 60}
    )
    result = wrapper.get_data(SELECTOR, START_DATE, end_date)
    assert len(result) == 2
    data = result.to_pydict()
    assert data["ts"][0] == START_DATE
    assert data["value"][0] == 42
    assert data["ts"][1] == end_date
    assert data["value"][1] == 24


def test_empty_interval():
    source = EmptyOddHoursSource()
    wrapper = SourceWrapper(
        Source(source, source), [], {"data_query_interval_seconds": 60 * 60}
    )

    end_date = datetime.fromisoformat("2020-01-02T00:00:00+00:00")
    result = wrapper.get_data(SELECTOR, START_DATE, end_date)
    assert len(result) == 24
    data = result.to_pydict()
    assert data["ts"][0] == START_DATE
    assert data["ts"][1] == START_DATE + timedelta(hours=1)
    assert data["ts"][2] == START_DATE + timedelta(hours=2)


def test_merge_numerical_types():
    source = DifferentNumericalTypesSource()

    wrapper = SourceWrapper(
        Source(source, source), [], {"data_query_interval_seconds": 60 * 60}
    )

    end_date = datetime.fromisoformat("2020-01-02T00:00:00+00:00")
    result = wrapper.get_data(SELECTOR, START_DATE, end_date)
    assert result.field("value").type == pa.float64()


def test_keep_integers():
    source = OnlyIntegersSource()

    wrapper = SourceWrapper(
        Source(source, source), [], {"data_query_interval_seconds": 60 * 60}
    )

    end_date = datetime.fromisoformat("2020-01-02T00:00:00+00:00")
    result = wrapper.get_data(SELECTOR, START_DATE, end_date)
    assert result.field("value").type == pa.int64()


def test_string_when_any():
    source = SomeStringTypesSource()

    wrapper = SourceWrapper(
        Source(source, source), [], {"data_query_interval_seconds": 60 * 60}
    )

    end_date = datetime.fromisoformat("2020-01-02T00:00:00+00:00")
    result = wrapper.get_data(SELECTOR, START_DATE, end_date)
    assert result.field("value").type == pa.string()


def test_failure_propagated():
    source = FailureSource()
    wrapper = SourceWrapper(Source(source, source), [], {})
    with pytest.raises(Exception):
        wrapper.get_data(SELECTOR, START_DATE, END_DATE)


def test_retry_on_search_failure():
    source = FailureSource()
    wrapper = SourceWrapper(
        Source(source, source), [], {"query_retry_count": 1, "query_retry_delay": 0.05}
    )
    wrapper.search(SELECTOR)


def test_retry_on_metadata_failure():
    source = FailureSource()
    wrapper = SourceWrapper(
        Source(source, source), [], {"query_retry_count": 1, "query_retry_delay": 0.05}
    )
    wrapper.get_metadata(SELECTOR)


def test_retry_on_data_failure():
    source = FailureSource()
    wrapper = SourceWrapper(
        Source(source, source), [], {"query_retry_count": 1, "query_retry_delay": 0.05}
    )
    wrapper.get_data(SELECTOR, START_DATE, END_DATE)


def test_exception_after_too_many_retries():
    source = FailureSource(failure_count=2)
    wrapper = SourceWrapper(
        Source(source, source), [], {"query_retry_count": 1, "query_retry_delay": 0.05}
    )
    with pytest.raises(Exception):
        wrapper.get_data(SELECTOR, START_DATE, END_DATE)


def _make_source():
    source = FakeSource()
    return Source(source, source)
