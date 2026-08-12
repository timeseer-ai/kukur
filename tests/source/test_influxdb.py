# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from unittest.mock import patch

import pytest
from dateutil.parser import parse as parse_date

from kukur import SeriesSearch, SeriesSelector
from kukur.source.influxdb import from_config
from kukur.source.influxdb.client import InfluxDBClientError

SHOW_SERIES = {
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "columns": ["key"],
                    "values": [
                        ["h2o_feet,location=coyote_creek"],
                        ["h2o_feet,location=santa_monica"],
                        ["h2o_temperature,location=coyote_creek"],
                    ],
                }
            ],
        }
    ]
}

SHOW_SERIES_FILTERED = {
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "columns": ["key"],
                    "values": [["h2o_feet,location=coyote_creek"]],
                }
            ],
        }
    ]
}

SHOW_FIELD_KEYS = {
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "name": "h2o_feet",
                    "columns": ["fieldKey", "fieldType"],
                    "values": [
                        ["level description", "string"],
                        ["water_level", "float"],
                    ],
                },
                {
                    "name": "h2o_temperature",
                    "columns": ["fieldKey", "fieldType"],
                    "values": [["degrees", "float"]],
                },
            ],
        }
    ]
}

DATA = {
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "name": "h2o_feet",
                    "columns": ["time", "water_level"],
                    "values": [
                        ["2019-09-17T00:00:00Z", 8.412],
                        ["2019-09-17T00:06:00Z", 8.419],
                    ],
                }
            ],
        }
    ]
}


class MockResponse:
    def __init__(self, json_data, status_code=200, text=""):
        self.json_data = json_data
        self.status_code = status_code
        self.text = text

    def json(self):
        if self.json_data is None:
            raise ValueError("no json")
        return self.json_data


REQUESTS: list[dict] = []


def mocked_requests_get(*args, **kwargs):
    assert args[0] == "http://localhost:8086/query"
    params = kwargs["params"]
    REQUESTS.append(params)
    assert params["db"] == "NOAA_water_database"
    query = params["q"]

    if query.startswith("SHOW SERIES"):
        if "WHERE" in query:
            return MockResponse(SHOW_SERIES_FILTERED)
        return MockResponse(SHOW_SERIES)
    if query == "SHOW FIELD KEYS":
        return MockResponse(SHOW_FIELD_KEYS)
    if query.strip().startswith("SELECT"):
        return MockResponse(DATA)

    raise Exception(query)


@pytest.fixture(autouse=True)
def _clear_requests():
    REQUESTS.clear()


def _source():
    return from_config({"database": "NOAA_water_database"})


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_search(_) -> None:
    many_series = list(_source().search(SeriesSearch("influx", None, "value")))
    assert REQUESTS[0]["q"] == "SHOW SERIES"
    assert len(many_series) == 5
    assert many_series[0].series.tags == {
        "series name": "h2o_feet",
        "location": "coyote_creek",
    }
    assert many_series[0].series.field == "level description"
    assert many_series[-1].series.field == "degrees"


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_search_with_tags(_) -> None:
    many_series = list(
        _source().search(
            SeriesSearch(
                "influx",
                {"series name": "h2o_feet", "location": "coyote_creek"},
                "value",
            )
        )
    )
    assert (
        REQUESTS[0]["q"]
        == 'SHOW SERIES FROM "h2o_feet" WHERE "location" = \'coyote_creek\''
    )
    assert len(many_series) == 2


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_search_with_field(_) -> None:
    many_series = list(_source().search(SeriesSearch("influx", None, "water_level")))
    assert len(many_series) == 2
    assert {series.series.field for series in many_series} == {"water_level"}


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data(_) -> None:
    tags = {"series name": "h2o_feet", "location": "coyote_creek"}
    table = _source().get_data(
        SeriesSelector.from_tags("influx", tags, "water_level"),
        parse_date("2019-09-17T00:00:00Z"),
        parse_date("2019-09-18T00:00:00Z"),
    )
    query = REQUESTS[0]["q"]
    assert 'SELECT time, "water_level"' in query
    assert 'FROM "h2o_feet"' in query
    assert "time >= $start_date and time <= $end_date" in query
    assert '"location" = $1' in query
    assert REQUESTS[0]["params"] == (
        '{"start_date": "2019-09-17T00:00:00Z",'
        ' "end_date": "2019-09-18T00:00:00Z", "1": "coyote_creek"}'
    )
    assert len(table) == 2
    assert table["ts"][0].as_py() == datetime.fromisoformat("2019-09-17T00:00:00+00:00")
    assert table["value"][0].as_py() == 8.412


def _error_response(*args, **kwargs):
    return MockResponse({"error": 'database not found: "foo"'}, 404)


def _error_in_result(*args, **kwargs):
    return MockResponse({"results": [{"statement_id": 0, "error": "invalid query"}]})


@patch("requests.Session.get", side_effect=_error_response)
def test_error_response(_) -> None:
    with pytest.raises(InfluxDBClientError, match="database not found"):
        list(_source().search(SeriesSearch("influx", None, "value")))


@patch("requests.Session.get", side_effect=_error_in_result)
def test_error_in_result(_) -> None:
    with pytest.raises(InfluxDBClientError, match="invalid query"):
        list(_source().search(SeriesSearch("influx", None, "value")))
