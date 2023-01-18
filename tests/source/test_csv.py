"""Test the CSV time series source."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Dict

from dateutil.parser import parse as parse_date
import pytz

import kukur.config

from kukur import (
    DataType,
    Dictionary,
    InterpolationType,
    SeriesSearch,
    SeriesSelector,
    Source,
)
from kukur.metadata import fields
from kukur.source import SourceFactory


def get_source(source_name: str) -> Source:
    source = SourceFactory(
        kukur.config.from_toml("tests/test_data/Kukur.toml")
    ).get_source(source_name)
    assert source is not None
    return source


def make_series(
    source: str, tags: Dict[str, str] = {"series name": "test-tag-1"}
) -> SeriesSelector:
    return SeriesSelector.from_tags(source, tags)


START_DATE = parse_date("2020-01-01T00:00:00Z")
END_DATE = parse_date("2020-11-01T00:00:00Z")


def test_dir() -> None:
    table = get_source("dir").get_data(make_series("dir"), START_DATE, END_DATE)
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_quality() -> None:
    table = get_source("dir-quality").get_data(
        make_series("dir-quality"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
    assert table["quality"][0].as_py() == 1
    assert table["quality"][2].as_py() == 0
    assert table["quality"][3].as_py() == 1


def test_search_row() -> None:
    series = list(get_source("row_no_metadata").search(SeriesSearch("row_no_metadata")))
    assert len(series) == 5
    assert SeriesSelector("row_no_metadata", "test-tag-1") in series


def test_row() -> None:
    table = get_source("row").get_data(make_series("row"), START_DATE, END_DATE)
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_quality() -> None:
    table = get_source("row_quality").get_data(
        make_series("row_quality"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
    assert table["quality"][0].as_py() == 1
    assert table["quality"][2].as_py() == 0


def test_search_pivot() -> None:
    series = list(
        get_source("pivot_no_metadata").search(SeriesSearch("pivot_no_metadata"))
    )
    assert len(series) == 2
    assert SeriesSelector("pivot_no_metadata", "test-tag-1") in series


def test_pivot() -> None:
    table = get_source("pivot").get_data(make_series("pivot"), START_DATE, END_DATE)
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_metadata() -> None:
    series = make_series("row")
    metadata = get_source("row").get_metadata(series)
    assert metadata.series == SeriesSelector(series.source, series.tags, series.field)
    assert isinstance(metadata.get_field(fields.Description), str)
    assert isinstance(metadata.get_field(fields.Unit), str)
    assert isinstance(metadata.get_field(fields.LimitLowFunctional), float)
    assert isinstance(metadata.get_field(fields.LimitHighFunctional), float)
    assert isinstance(metadata.get_field(fields.Accuracy), float)


def test_row_metadata_dictionary() -> None:
    metadata = get_source("row").get_metadata(
        SeriesSelector.from_tags("row", {"series name": "test-tag-6"})
    )
    assert metadata.series == SeriesSelector.from_tags(
        "row", {"series name": "test-tag-6"}
    )
    assert metadata.get_field(fields.DataType) == DataType.DICTIONARY
    assert metadata.get_field(fields.DictionaryName) == "Active"
    assert isinstance(metadata.get_field(fields.Dictionary), Dictionary)


def test_metadata_mapping() -> None:
    metadata = get_source("mapping").get_metadata(make_series("mapping"))
    assert metadata.series == SeriesSelector.from_tags(
        "mapping", {"series name": "test-tag-1"}
    )
    assert metadata.get_field(fields.Unit) == "kg"
    assert metadata.get_field(fields.LimitLowFunctional) == 1
    assert metadata.get_field(fields.InterpolationType) == InterpolationType.LINEAR


def test_metadata_mapping_multiple() -> None:
    metadata = get_source("mapping").get_metadata(make_series("mapping"))
    assert metadata.get_field(fields.DataType) == DataType.FLOAT64
    metadata = get_source("mapping").get_metadata(
        make_series("mapping", {"series name": "test-tag-4"})
    )
    assert metadata.get_field(fields.DataType) == DataType.FLOAT64


def test_custom_fields_search() -> None:
    all_metadata = list(
        get_source("custom-fields-simple").search(SeriesSelector("custom-fields"))
    )
    assert len(all_metadata) == 1
    metadata = all_metadata[0]
    assert isinstance(metadata, kukur.Metadata)
    assert metadata.get_field(fields.Description) == "Test for custom metadata fields"
    assert metadata.get_field_by_name("location") == "Antwerp"
    assert "plant" not in [name for name, _ in metadata.iter_names()]


def test_custom_fields_metadata() -> None:
    metadata = get_source("custom-fields-simple").get_metadata(
        SeriesSelector.from_tags("custom-fields", {"series name": "test-tag-custom"})
    )
    assert isinstance(metadata, kukur.Metadata)
    assert metadata.get_field(fields.Description) == "Test for custom metadata fields"
    assert metadata.get_field_by_name("location") == "Antwerp"
    assert "plant" not in [name for name, _ in metadata.iter_names()]


def test_custom_fields_extra_metadata() -> None:
    metadata = get_source("custom-fields").get_metadata(
        make_series("custom-fields", {"series name": "test-tag-custom"})
    )
    assert metadata.get_field(fields.Description) == "Test for custom metadata fields"
    assert metadata.get_field_by_name("process type") == "BATCH"
    assert metadata.get_field_by_name("location") == "Antwerp"
    assert "plant" not in [name for name, _ in metadata.iter_names()]


def test_custom_fields_search_extra_metadata() -> None:
    all_metadata = list(
        get_source("custom-fields").search(SeriesSelector("custom-fields"))
    )
    assert len(all_metadata) == 1
    metadata = all_metadata[0]
    assert isinstance(metadata, kukur.Metadata)
    assert metadata.get_field(fields.Description) == "Test for custom metadata fields"
    assert metadata.get_field_by_name("process type") == "BATCH"
    assert metadata.get_field_by_name("location") == "Antwerp"
    assert "plant" not in [name for name, _ in metadata.iter_names()]


def test_metadata_accuracy_percentage() -> None:
    metadata = get_source("row").get_metadata(
        SeriesSelector.from_tags("row", {"series name": "test-tag-1"})
    )
    assert metadata.get_field(fields.AccuracyPercentage) == 2
    assert metadata.get_field(fields.LimitLowPhysical) == 0
    assert metadata.get_field(fields.LimitHighPhysical) == 10
    assert metadata.get_field(fields.Accuracy) == 0.2


def test_search_metadata_accuracy_percentage() -> None:
    all_metadata = list(get_source("row").search(SeriesSelector("row")))
    metadata = all_metadata[0]
    assert isinstance(metadata, kukur.Metadata)
    assert metadata.get_field(fields.AccuracyPercentage) == 2
    assert metadata.get_field(fields.LimitLowPhysical) == 0
    assert metadata.get_field(fields.LimitHighPhysical) == 10
    assert metadata.get_field(fields.Accuracy) == 0.2


def test_metadata_with_different_encoding() -> None:
    metadata = get_source("cp1252-encoding").get_metadata(
        SeriesSelector.from_tags("cp1252-encoding", {"series name": "test-tag-3"})
    )
    assert metadata.get_field(fields.Unit) == "°C"
    assert metadata.get_field(fields.LimitLowFunctional) == 0
    assert metadata.get_field(fields.InterpolationType) == InterpolationType.LINEAR


def test_row_format_with_header() -> None:
    table = get_source("row_header").get_data(
        make_series("row_header"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_column_mapping() -> None:
    table = get_source("row_column_mapping").get_data(
        make_series("row_column_mapping"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_header() -> None:
    table = get_source("dir-header").get_data(
        make_series("dir-header"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_mapping() -> None:
    table = get_source("dir-mapping").get_data(
        make_series("dir-mapping"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_data_datetime_format() -> None:
    table = get_source("row_timestamp").get_data(
        make_series("row_timestamp"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC
    assert table["value"][0].as_py() == 1.0


def test_row_data_timezone() -> None:
    table = get_source("row_timezone").get_data(
        make_series("row_timezone"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert table["value"][0].as_py() == 1.0
