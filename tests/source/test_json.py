"""Test the JSON source."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

import pytest

import kukur.config

from kukur import DataType, Metadata, SeriesSearch, SeriesSelector, Source
from kukur.exceptions import InvalidSourceException
from kukur.metadata import fields
from kukur.source import SourceFactory


def get_source(source_name: str) -> Source:
    source = SourceFactory(
        kukur.config.from_toml("tests/test_data/Kukur.toml")
    ).get_source(source_name)
    assert source is not None
    return source


def test_search_metadata() -> None:
    all_series = list(get_source("json").search(SeriesSearch("json")))
    assert len(all_series) == 2
    series_1 = [
        series
        for series in all_series
        if series.series.tags["series name"] == "test-tag-1"
    ][0]
    assert isinstance(series_1, Metadata)
    assert series_1.get_field(fields.Description) == "hello"
    assert series_1.get_field(fields.LimitLowFunctional) == 42
    series_2 = [
        series
        for series in all_series
        if series.series.tags["series name"] == "test-tag-2"
    ][0]
    assert isinstance(series_2, Metadata)
    assert series_2.get_field(fields.Description) == "world"
    assert series_2.get_field(fields.DataType) == DataType.FLOAT64


def test_metadata() -> None:
    selector = SeriesSelector("json", "test-tag-2")
    metadata = get_source("json").get_metadata(selector)
    assert metadata.series == selector
    assert metadata.get_field(fields.Description) == "world"
    assert metadata.get_field(fields.DataType) == DataType.FLOAT64


def test_metadata_unknown() -> None:
    selector = SeriesSelector("json", "test-tag-0")
    metadata = get_source("json").get_metadata(selector)
    assert metadata.series == selector
    for _, v in metadata.iter_names():
        assert not v


def test_metadata_directory_traversal() -> None:
    selector = SeriesSelector("json", "../../../test-tag-1")
    with pytest.raises(ValueError):
        get_source("json").get_metadata(selector)


def test_data_unsupported() -> None:
    selector = SeriesSelector("json", "../../../test-tag-1")
    with pytest.raises(InvalidSourceException):
        get_source("json").get_data(selector, datetime.now(), datetime.now())
