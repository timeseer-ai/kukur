"""Integration tests require a running Kukur instance.

They use the client to request data.
"""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import os
from datetime import datetime

import pytest

from kukur import Client, Metadata, SeriesSelector
from kukur.base import SeriesSearch
from kukur.metadata import fields

pytestmark = pytest.mark.elasticsearch


def suffix_source(source_name: str) -> str:
    if "KUKUR_INTEGRATION_TARGET" in os.environ:
        target = os.environ["KUKUR_INTEGRATION_TARGET"]
        return f"{source_name}-{target}"
    return source_name  # works in docker container


@pytest.fixture
def client() -> Client:
    kukur_client = Client()
    kukur_client._get_client().wait_for_available(timeout=10)
    return kukur_client


def test_search(client: Client):
    many_series = list(client.search(SeriesSearch(suffix_source("noaa-es"))))
    assert len(many_series) == 3
    series = [
        series
        for series in many_series
        if series.series.tags["series name"] == "h2o"
        and series.series.tags["location"] == "coyote_creek"
        and series.series.field == "water_level"
    ][0]
    assert isinstance(series, Metadata)
    assert series.get_field(fields.Description) == "between 6 and 9 feet"
    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_metadata(client: Client):
    tags = {
        "series name": "h2o",
        "location": "coyote_creek",
    }
    series = client.get_metadata(
        SeriesSelector.from_tags(suffix_source("noaa-es"), tags, "water_level")
    )

    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_data(client: Client):
    tags = {
        "series name": "h2o",
        "location": "coyote_creek",
    }
    start_date = datetime.fromisoformat("2024-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2024-01-02T00:00:00+00:00")
    data = client.get_data(
        SeriesSelector.from_tags(suffix_source("noaa-es"), tags, "water_level"),
        start_date,
        end_date,
    )
    assert len(data) == 241
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 9.982635
    assert data["ts"][240].as_py() == end_date
    assert data["value"][240].as_py() == 6.944541


def test_search_minimal(client: Client):
    many_series = list(client.search(SeriesSearch(suffix_source("noaa-es-minimal"))))
    assert len(many_series) == 3
    series = [
        series
        for series in many_series
        if series.series.tags["series name"] == "h2o"
        and series.series.tags["location"] == "coyote_creek"
        and series.series.field == "water_level"
    ][0]
    assert isinstance(series, Metadata)
    assert (
        len([(k, v) for k, v in series.iter_names() if v is not None and v != ""]) == 1
    )
    assert series.get_field(fields.Description) == "between 6 and 9 feet"
    assert series.get_field(fields.LimitLowFunctional) is None
    assert series.get_field(fields.LimitHighFunctional) is None


def test_data_minimal(client: Client):
    tags = {
        "series name": "h2o",
        "location": "coyote_creek",
    }
    start_date = datetime.fromisoformat("2024-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2024-01-02T00:00:00+00:00")
    data = client.get_data(
        SeriesSelector.from_tags(suffix_source("noaa-es-minimal"), tags, "water_level"),
        start_date,
        end_date,
    )
    assert len(data) == 241
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 9.982635
    assert data["ts"][240].as_py() == end_date
    assert data["value"][240].as_py() == 6.944541


def test_search_sql(client: Client):
    many_series = list(client.search(SeriesSearch(suffix_source("noaa-es-sql"))))
    assert len(many_series) == 3
    series = [
        series
        for series in many_series
        if series.series.tags["series name"] == "h2o"
        and series.series.tags["location"] == "coyote_creek"
        and series.series.field == "water_level"
    ][0]
    assert isinstance(series, Metadata)
    assert series.get_field(fields.Description) == "between 6 and 9 feet"
    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_metadata_sql(client: Client):
    tags = {
        "series name": "h2o",
        "location": "coyote_creek",
    }
    series = client.get_metadata(
        SeriesSelector.from_tags(suffix_source("noaa-es-sql"), tags, "water_level")
    )

    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_data_sql(client: Client):
    tags = {
        "series name": "h2o",
        "location": "coyote_creek",
    }
    start_date = datetime.fromisoformat("2024-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2024-01-02T00:00:00+00:00")
    data = client.get_data(
        SeriesSelector.from_tags(suffix_source("noaa-es-sql"), tags, "water_level"),
        start_date,
        end_date,
    )
    assert len(data) == 241
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 9.982635
    assert data["ts"][240].as_py() == end_date
    assert data["value"][240].as_py() == 6.944541
