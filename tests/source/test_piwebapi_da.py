# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from unittest.mock import patch

from dateutil.parser import parse as parse_date

from kukur import SeriesSelector
from kukur.base import SeriesSearch
from kukur.source.piwebapi_da import from_config

SAMPLE_POINTS = [
    {
        "WebId": "1",
        "Id": 8,
        "Name": "CDT158",
        "Path": "\\\\vm-ts-pi\\CDT158",
        "Descriptor": "Atmospheric Tower OH Vapor",
        "PointClass": "classic",
        "PointType": "Float32",
        "DigitalSetName": "",
        "EngineeringUnits": "DEG. C",
        "Span": 200.0,
        "Zero": 50.0,
        "Step": False,
        "Future": False,
        "DisplayDigits": -5,
        "Links": {
            "Self": "https://test_pi.net/points/1",
            "DataServer": "https://test_pi.net/dataservers/F1DSBd9Ab83Z90SNSjy4JtD5fQVk0tVFMtUEk",
            "Attributes": "https://test_pi.net/points/1/attributes",
            "InterpolatedData": "https://test_pi.net/streams/1/interpolated",
            "RecordedData": "https://test_pi.net/streams/1/recorded",
            "PlotData": "https://test_pi.net/streams/1/plot",
            "SummaryData": "https://test_pi.net/streams/1/summary",
            "Value": "https://test_pi.net/streams/1/value",
            "EndValue": "https://test_pi.net/streams/1/end",
        },
    },
    {
        "WebId": "2",
        "Id": 13,
        "Name": "CDT159",
        "Path": "\\\\vm-ts-pi\\CDT159",
        "Descriptor": "PICampaign storage point. PI Batch Database generated, do not delete or edit.",
        "PointClass": "base",
        "PointType": "String",
        "DigitalSetName": "",
        "EngineeringUnits": "",
        "Span": 100.0,
        "Zero": 0.0,
        "Step": True,
        "Future": False,
        "DisplayDigits": -5,
        "Links": {
            "Self": "https://test_pi.net/points/2",
            "DataServer": "https://test_pi.net/dataservers/F1DSBd9Ab83Z90SNSjy4JtD5fQVk0tVFMtUEk",
            "Attributes": "https://test_pi.net/points/2/attributes",
            "InterpolatedData": "https://test_pi.net/streams/2/interpolated",
            "RecordedData": "https://test_pi.net/streams/2/recorded",
            "PlotData": "https://test_pi.net/streams/2/plot",
            "SummaryData": "https://test_pi.net/streams/2/summary",
            "Value": "https://test_pi.net/streams/2/value",
            "EndValue": "https://test_pi.net/streams/2/end",
        },
    },
]
DATA_POINTS = [
    {"Timestamp": "2020-01-01T00:00:00Z", "Value": 81.83204, "Good": True},
    {"Timestamp": "2020-01-01T07:33:25Z", "Value": 13.6064939, "Good": True},
    {"Timestamp": "2020-01-01T08:37:25Z", "Value": 0.9678813, "Good": True},
    {"Timestamp": "2020-01-01T09:42:25Z", "Value": 3.38636, "Good": True},
    {"Timestamp": "2020-01-01T09:42:25Z", "Value": 23.6581783, "Good": True},
    {"Timestamp": "2020-01-01T09:42:25Z", "Value": 23.6581783, "Good": True},
    {"Timestamp": "2020-01-01T09:44:25Z", "Value": 23.6581783, "Good": True},
    {
        "Timestamp": "2020-01-01T17:24:18Z",
        "Value": {"Name": "Shutdown", "Value": 254, "IsSystem": True},
        "Good": False,
    },
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

    return list(filter(between_dates, DATA_POINTS))[:limit]


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def raise_for_status(self):
            return

        def json(self):
            return self.json_data

    if args[0] == "https://test_pi.net":
        response = {"Links": {"Points": "https://test_pi.net/points"}}
        return MockResponse(response, 200)

    if args[0] == "https://test_pi.net/points":
        params = kwargs.get("params", {})
        start_index = int(params.get("startIndex", 0))

        response = {"Items": SAMPLE_POINTS}
        if start_index > 0:
            response["Items"] = []
        return MockResponse(response, 200)

    if args[0] == "https://test_pi.net/streams/1/recorded":
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
            "data_archive_uri": "https://test_pi.net",
            "max_returned_items_per_call": 5,
            "username": "test",
            "password": "test",
            "verify_ssl": "false",
        }
    )
    series = list(source.search(SeriesSearch("Test")))
    assert len(series) == 2
    assert series[0].series.name == "CDT158"
    assert series[1].series.name == "CDT159"


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data_without_limits(_) -> None:
    source = from_config(
        {
            "data_archive_uri": "https://test_pi.net",
            "max_returned_items_per_call": 10,
            "username": "test",
            "password": "test",
            "verify_ssl": "false",
        }
    )
    start_date = parse_date("2020-01-01T00:00:00Z")
    end_date = parse_date("2020-01-02T00:00:00Z")
    data = source.get_data(SeriesSelector("Test", "CDT158"), start_date, end_date)
    assert len(data) == 8


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data_multiple_requests(_) -> None:
    source = from_config(
        {
            "data_archive_uri": "https://test_pi.net",
            "max_returned_items_per_call": 4,
            "username": "test",
            "password": "test",
            "verify_ssl": "false",
        }
    )
    start_date = parse_date("2020-01-01T00:00:00Z")
    end_date = parse_date("2020-01-02T10:56:25Z")

    data = source.get_data(SeriesSelector("Test", "CDT158"), start_date, end_date)
    assert len(data) == 12
    assert data["ts"][0].as_py() == parse_date("2020-01-01T00:00:00Z")
    assert data["ts"][-1].as_py() == parse_date("2020-01-02T10:56:25Z")


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data_dates_outside_limits(_) -> None:
    source = from_config(
        {
            "data_archive_uri": "https://test_pi.net",
            "max_returned_items_per_call": 4,
            "username": "test",
            "password": "test",
            "verify_ssl": "false",
        }
    )
    start_date = parse_date("2019-10-01T00:00:00Z")
    end_date = parse_date("2020-02-01T10:56:25Z")

    data = source.get_data(SeriesSelector("Test", "CDT158"), start_date, end_date)
    assert len(data) == 17
    assert data["ts"][0].as_py() == parse_date("2020-01-01T00:00:00Z")
    assert data["ts"][-1].as_py() == parse_date("2020-01-03T10:56:25Z")
