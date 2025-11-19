# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0


import kukur.config
from kukur import Metadata, Source
from kukur.base import DataType, SeriesSearch
from kukur.metadata import fields
from kukur.source import SourceFactory


def get_source(source_name: str) -> Source:
    source = SourceFactory(
        kukur.config.from_toml("tests/test_data/Kukur.toml")
    ).get_source(source_name)
    assert source is not None
    return source


def test_data_fusion():
    all_metadata = list(get_source("datafusion").search(SeriesSearch("datafusion")))
    assert len(all_metadata) == 3
    test_1 = _get_metadata(all_metadata, "test-tag-1")
    assert test_1.get_field_by_name("unit") == "m"
    assert test_1.get_field_by_name("description") == "test series 1"
    assert test_1.get_field(fields.DataType) == DataType.FLOAT64


def test_data_fusion_tags():
    all_metadata = list(
        get_source("datafusion_tags_fields").search(
            SeriesSearch("datafusion_tags_fields")
        )
    )
    assert len(all_metadata) == 8


def _get_metadata(all_metadata: list[Metadata], series_name: str) -> Metadata:
    matching_series = None
    for metadata in all_metadata:
        if metadata.series.tags["series name"] == series_name:
            matching_series = metadata
    assert matching_series is not None
    return matching_series
