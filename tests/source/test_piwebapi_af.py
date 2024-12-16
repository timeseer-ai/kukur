# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from unittest.mock import patch

from dateutil.parser import parse as parse_date

from kukur import SeriesSelector
from kukur.base import SeriesSearch
from kukur.source.piwebapi_af import from_config

_BASE_URL = "https://test_pi.net"
SAMPLE_DATABASE = {
    "Links": {
        "Elements": f"{_BASE_URL}/db/elements",
    },
}

SAMPLE_SITES = {
    "Links": {},
    "Items": [
        {
            "Name": "Antwerp",
            "TemplateName": "Site",
            "HasChildren": True,
            "Links": {
                "Attributes": f"{_BASE_URL}/empty-attributes",
                "Elements": f"{_BASE_URL}/elements/1/elements",
            },
        },
    ],
}

SAMPLE_CHILD_ELEMENTS = {
    "Links": {},
    "Items": [
        {
            "WebId": "1",
            "Id": "776a75c2-b946-11ef-889f-6045bd94f59b",
            "Name": "Reactor02",
            "Description": "",
            "Path": "\\\\path\\sample\\Antwerp\\Active",
            "TemplateName": "Reactor",
            "HasChildren": False,
            "CategoryNames": [],
            "ExtendedProperties": {},
            "Links": {
                "Attributes": f"{_BASE_URL}/elements/1/attributes",
            },
        }
    ],
}
SAMPLE_EMPTY_ATTRIBUTES = {"Links": {}, "Items": []}
SAMPLE_CHILD_ATTRIBUTES = {
    "Links": {},
    "Items": [
        {
            "WebId": "2",
            "Name": "Active",
            "Description": "Sample",
            "HasChildren": False,
            "Step": False,
            "Type": "Double",
            "Path": "\\\\path\\sample\\Antwerp\\Active",
            "DataReferencePlugIn": "PI Point",
            "DefaultUnitsNameAbbreviation": "",
            "Span": 200.0,
            "Zero": 0.0,
            "Links": {
                "PlotData": f"{_BASE_URL}/streams/2/plot",
                "Recorded": f"{_BASE_URL}/streams/2/plot",
                "Attributes": f"{_BASE_URL}/empty-attributes",
            },
        },
        {
            "WebId": "3",
            "Name": "Concentration",
            "Description": "Sample description",
            "Path": "\\\\path\\sample\\Antwerp\\Concentration",
            "Type": "Double",
            "DataReferencePlugIn": "PI Point",
            "DefaultUnitsNameAbbreviation": "",
            "HasChildren": False,
            "Step": False,
            "Span": 200.0,
            "Zero": 0.0,
            "Links": {
                "PlotData": f"{_BASE_URL}/streams/3/plot",
                "Recorded": f"{_BASE_URL}/streams/3/plot",
                "Attributes": f"{_BASE_URL}/empty-attributes",
            },
        },
        {
            "WebId": "4",
            "Name": "Lookup Values",
            "Description": "Sample description",
            "Path": "\\\\path\\sample\\Antwerp\\Lookup",
            "Type": "Double",
            "DataReferencePlugIn": "Table Lookup",
            "DefaultUnitsNameAbbreviation": "",
            "HasChildren": False,
            "Step": False,
            "Span": 200.0,
            "Zero": 0.0,
            "Links": {
                "PlotData": f"{_BASE_URL}/streams/4/plot",
                "Recorded": f"{_BASE_URL}/streams/4/plot",
                "Attributes": f"{_BASE_URL}/empty-attributes",
            },
        },
    ],
}

SAMPLE_DATA_POINTS = [
    {"Timestamp": "2020-01-01T00:00:00Z", "Value": 81.83204, "Good": True},
    {"Timestamp": "2020-01-01T07:33:25Z", "Value": 13.6064939, "Good": True},
    {"Timestamp": "2020-01-01T08:37:25Z", "Value": 0.9678813, "Good": True},
    {"Timestamp": "2020-01-01T09:42:25Z", "Value": 3.38636, "Good": True},
    {"Timestamp": "2020-01-01T09:42:25Z", "Value": 23.6581783, "Good": True},
    {"Timestamp": "2020-01-01T09:42:25Z", "Value": 23.6581783, "Good": True},
    {"Timestamp": "2020-01-01T09:44:25Z", "Value": 23.6581783, "Good": True},
    {"Timestamp": "2020-01-02T00:00:00Z", "Value": 81.83204, "Good": True},
    {"Timestamp": "2020-01-02T07:33:25Z", "Value": 13.6064939, "Good": True},
    {"Timestamp": "2020-01-02T08:37:25Z", "Value": 0.9678813, "Good": True},
    {"Timestamp": "2020-01-02T09:42:25Z", "Value": 3.38636, "Good": True},
    {"Timestamp": "2020-01-02T10:56:25Z", "Value": 23.6581783, "Good": True},
    {"Timestamp": "2020-01-03T00:00:00Z", "Value": 81.83204, "Good": True},
    {"Timestamp": "2020-01-03T07:33:25Z", "Value": 13.6064939, "Good": True},
    {"Timestamp": "2020-01-03T08:37:25Z", "Value": 0.9678813, "Good": True},
    {"Timestamp": "2020-01-03T09:42:25Z", "Value": 3.38636, "Good": True},
    {"Timestamp": "2020-01-03T10:56:25Z", "Value": 23.6581783, "Good": True},
]


def _get_data(start_date: datetime, end_date: datetime, limit: int):
    def between_dates(item):
        date = parse_date(item["Timestamp"])
        return date >= start_date and date <= end_date

    return list(filter(between_dates, SAMPLE_DATA_POINTS))[:limit]


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def raise_for_status(self):
            return

        def json(self):
            return self.json_data

    if args[0] == _BASE_URL:
        response = SAMPLE_DATABASE
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/db/elements":
        response = SAMPLE_SITES
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/elements/1/elements":
        response = SAMPLE_CHILD_ELEMENTS
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/elements/1/attributes":
        response = SAMPLE_CHILD_ATTRIBUTES
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/empty-attributes":
        response = SAMPLE_EMPTY_ATTRIBUTES
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/streams/2/plot":
        params = kwargs.get("params", {})
        start_index = int(params.get("startIndex", 0))

        response = {"Items": SAMPLE_DATA_POINTS}
        if start_index > 0:
            response["Items"] = []
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/streams/2/recorded":
        response = SAMPLE_DATA_POINTS
        params = kwargs.get("params", {})
        start_date = parse_date(params["startTime"])
        end_date = parse_date(params["endTime"])
        max_count = int(params["maxCount"])

        response = {"Items": _get_data(start_date, end_date, max_count)}
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/streams/3/plot":
        params = kwargs.get("params", {})
        start_index = int(params.get("startIndex", 0))

        response = {"Items": SAMPLE_DATA_POINTS}
        if start_index > 0:
            response["Items"] = []
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/streams/3/recorded":
        response = SAMPLE_DATA_POINTS
        params = kwargs.get("params", {})
        start_date = parse_date(params["startTime"])
        end_date = parse_date(params["endTime"])
        max_count = int(params["maxCount"])

        response = {"Items": _get_data(start_date, end_date, max_count)}
        return MockResponse(response, 200)

    raise Exception(args[0])


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_search(_) -> None:
    source = from_config(
        {
            "database_uri": "https://test_pi.net",
            "max_returned_items_per_call": 5,
            "username": "test",
            "password": "test",
            "verify_ssl": "false",
        }
    )
    series = list(source.search(SeriesSearch("Test")))
    assert len(series) == 2
    assert series[0].series.tags.get("series name") == "Reactor02"
    assert series[1].series.tags.get("series name") == "Reactor02"
    assert series[0].series.field == "Active"
    assert series[1].series.field == "Concentration"


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data_without_limits(_) -> None:
    source = from_config(
        {
            "database_uri": "https://test_pi.net",
            "max_returned_items_per_call": 10,
            "username": "test",
            "password": "test",
            "verify_ssl": "false",
        }
    )
    start_date = parse_date("2020-01-01T00:00:00Z")
    end_date = parse_date("2020-01-02T00:00:00Z")
    data = source.get_data(
        SeriesSelector("Test", {"__id__": "2"}), start_date, end_date
    )
    assert len(data) == 8


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data_multiple_requests(_) -> None:
    source = from_config(
        {
            "database_uri": "https://test_pi.net",
            "max_returned_items_per_call": 4,
            "username": "test",
            "password": "test",
            "verify_ssl": "false",
        }
    )
    start_date = parse_date("2020-01-01T00:00:00Z")
    end_date = parse_date("2020-01-02T10:56:25Z")

    data = source.get_data(
        SeriesSelector("Test", {"__id__": "2"}), start_date, end_date
    )
    assert len(data) == 12
    assert data["ts"][0].as_py() == parse_date("2020-01-01T00:00:00Z")
    assert data["ts"][-1].as_py() == parse_date("2020-01-02T10:56:25Z")


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data_dates_outside_limits(_) -> None:
    source = from_config(
        {
            "database_uri": "https://test_pi.net",
            "max_returned_items_per_call": 4,
            "username": "test",
            "password": "test",
            "verify_ssl": "false",
        }
    )
    start_date = parse_date("2019-10-01T00:00:00Z")
    end_date = parse_date("2020-02-01T10:56:25Z")

    data = source.get_data(
        SeriesSelector("Test", {"__id__": "2"}), start_date, end_date
    )
    assert len(data) == 17
    assert data["ts"][0].as_py() == parse_date("2020-01-01T00:00:00Z")
    assert data["ts"][-1].as_py() == parse_date("2020-01-03T10:56:25Z")


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_search_with_table_lookup_enabled(_) -> None:
    source = from_config(
        {
            "database_uri": "https://test_pi.net",
            "max_returned_items_per_call": 4,
            "username": "test",
            "password": "test",
            "use_table_lookup": "true",
            "verify_ssl": "false",
        }
    )
    series = list(source.search(SeriesSearch("Test")))
    assert len(series) == 3
    assert series[0].series.tags.get("series name") == "Reactor02"
    assert series[1].series.tags.get("series name") == "Reactor02"
    assert series[2].series.tags.get("series name") == "Reactor02"
    assert series[0].series.field == "Active"
    assert series[1].series.field == "Concentration"
    assert series[2].series.field == "Lookup Values"
