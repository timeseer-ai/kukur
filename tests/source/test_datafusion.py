# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0


from datetime import datetime

import kukur.config
from kukur import DataType, Metadata, SeriesSearch, SeriesSelector, Source
from kukur.metadata import fields
from kukur.source import SourceFactory


def get_source(source_name: str) -> Source:
    source = SourceFactory(
        kukur.config.from_toml("tests/test_data/Kukur.toml")
    ).get_source(source_name)
    assert source is not None
    return source


def test_datafusion() -> None:
    all_metadata = list(get_source("datafusion").search(SeriesSearch("datafusion")))
    assert len(all_metadata) == 3
    test_1 = _get_metadata(all_metadata, "test-tag-1")
    assert test_1.get_field_by_name("unit") == "m"
    assert test_1.get_field_by_name("description") == "test series 1"
    assert test_1.get_field(fields.DataType) == DataType.FLOAT64


def test_datafusion_tags() -> None:
    all_metadata = list(
        get_source("datafusion_tags_fields").search(
            SeriesSearch("datafusion_tags_fields")
        )
    )
    assert len(all_metadata) == 8


def _get_metadata(
    all_metadata: list[Metadata | SeriesSelector], series_name: str
) -> Metadata:
    matching_series = None
    for metadata in all_metadata:
        assert isinstance(metadata, Metadata)
        if metadata.series.tags["series name"] == series_name:
            matching_series = metadata
    assert matching_series is not None
    return matching_series


def test_datafusion_data() -> None:
    data = get_source("datafusion").get_data(
        SeriesSelector("datafusion", "test-tag-1"),
        datetime.fromisoformat("2020-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2020-01-05T00:00:00+00:00"),
    )
    assert len(data) == 4
