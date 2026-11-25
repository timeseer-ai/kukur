# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from unittest.mock import patch
from urllib.parse import parse_qsl

import pyarrow as pa
from dateutil.parser import parse as parse_date

from kukur import DataType, Quality, SeriesSelector, quality
from kukur.metadata import fields
from kukur.source.piwebapi_af import from_config
from kukur.source.piwebapi_af.pi_asset_framework import _get_metadata

_BASE_URL = "https://test_pi.net"
SAMPLE_DATABASE = {
    "Links": {
        "Elements": f"{_BASE_URL}/db/elements",
    },
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


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def raise_for_status(self):
        return

    def json(self):
        return self.json_data


def batch_through(mocked_get):
    """Answer each request of a batch request as `mocked_get` answers it."""

    def mocked_post(*args, **kwargs):
        assert args[0] == f"{_BASE_URL}/batch"
        result = {}
        for name, request in kwargs["json"].items():
            url, _, query = request["Resource"].partition("?")
            response = mocked_get(url, params=dict(parse_qsl(query)))
            result[name] = {"Status": response.status_code, "Content": response.json()}
        return MockResponse(result, 200)

    return mocked_post


def mocked_requests_get(*args, **kwargs):
    if args[0].startswith(f"{_BASE_URL}/attributes/"):
        return MockResponse({"Type": "Double"}, 200)

    if args[0] == _BASE_URL:
        response = SAMPLE_DATABASE
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/streams/A2/plot":
        params = kwargs.get("params", {})
        start_index = int(params.get("startIndex", 0))

        response = {"Items": SAMPLE_DATA_POINTS}
        if start_index > 0:
            response["Items"] = []
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/streams/A2/recorded":
        response = SAMPLE_DATA_POINTS
        params = kwargs.get("params", {})
        start_date = parse_date(params["startTime"])
        end_date = parse_date(params["endTime"])
        max_count = int(params["maxCount"])

        response = {"Items": _get_data(start_date, end_date, max_count)}
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/streams/A3/plot":
        params = kwargs.get("params", {})
        start_index = int(params.get("startIndex", 0))

        response = {"Items": SAMPLE_DATA_POINTS}
        if start_index > 0:
            response["Items"] = []
        return MockResponse(response, 200)

    if args[0] == f"{_BASE_URL}/streams/A3/recorded":
        response = SAMPLE_DATA_POINTS
        params = kwargs.get("params", {})
        start_date = parse_date(params["startTime"])
        end_date = parse_date(params["endTime"])
        max_count = int(params["maxCount"])

        response = {"Items": _get_data(start_date, end_date, max_count)}
        return MockResponse(response, 200)

    raise Exception(args[0])


def mocked_requests_get_system_points(*args, **kwargs):
    """Return one page full of system points."""
    if args[0].startswith(f"{_BASE_URL}/attributes/"):
        return MockResponse({"Type": "Double"}, 200)
    params = kwargs.get("params", {})
    start_time = parse_date(params["startTime"])

    if start_time == parse_date("2020-01-01T17:24:21Z"):
        response = {
            "Items": [
                {
                    "Timestamp": "2020-01-01T17:24:21Z",
                    "Value": {"Name": "Shutdown", "Value": 254, "IsSystem": True},
                    "Good": False,
                },
                {
                    "Timestamp": "2020-01-02T00:00:00Z",
                    "Value": 81.83204,
                    "Good": True,
                },
            ]
        }
    else:
        response = {
            "Items": [
                {
                    "Timestamp": "2020-01-01T17:24:18Z",
                    "Value": {"Name": "Shutdown", "Value": 254, "IsSystem": True},
                    "Good": False,
                },
                {
                    "Timestamp": "2020-01-01T17:24:19Z",
                    "Value": {"Name": "Shutdown", "Value": 254, "IsSystem": True},
                    "Good": False,
                },
                {
                    "Timestamp": "2020-01-01T17:24:20Z",
                    "Value": {"Name": "Shutdown", "Value": 254, "IsSystem": True},
                    "Good": False,
                },
                {
                    "Timestamp": "2020-01-01T17:24:21Z",
                    "Value": {"Name": "Shutdown", "Value": 254, "IsSystem": True},
                    "Good": False,
                },
            ]
        }
    return MockResponse(response, 200)


@patch("requests.Session.post", side_effect=batch_through(mocked_requests_get))
@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data_without_limits(_, __) -> None:
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
        SeriesSelector("Test", {"__id__": "A2"}), start_date, end_date
    )
    assert len(data) == 8


@patch("requests.Session.post", side_effect=batch_through(mocked_requests_get))
@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data_multiple_requests(_, __) -> None:
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
        SeriesSelector("Test", {"__id__": "A2"}), start_date, end_date
    )
    assert len(data) == 12
    assert data["ts"][0].as_py() == parse_date("2020-01-01T00:00:00Z")
    assert data["ts"][-1].as_py() == parse_date("2020-01-02T10:56:25Z")


@patch("requests.Session.post", side_effect=batch_through(mocked_requests_get))
@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_data_dates_outside_limits(_, __) -> None:
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
        SeriesSelector("Test", {"__id__": "A2"}), start_date, end_date
    )
    assert len(data) == 17
    assert data["ts"][0].as_py() == parse_date("2020-01-01T00:00:00Z")
    assert data["ts"][-1].as_py() == parse_date("2020-01-03T10:56:25Z")


@patch(
    "requests.Session.post",
    side_effect=batch_through(mocked_requests_get_system_points),
)
@patch("requests.Session.get", side_effect=mocked_requests_get_system_points)
def test_get_data_system_points(_, __) -> None:
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
        SeriesSelector("Test", {"__id__": "A9"}), start_date, end_date
    )
    assert len(data) == 5
    assert data["ts"][0].as_py() == parse_date("2020-01-01T17:24:18Z")
    assert data["quality"][0].as_py() == 254
    assert data["ts"][-1].as_py() == parse_date("2020-01-02T00:00:00Z")
    assert data["quality"][-1].as_py() == Quality.GOOD.value
    assert data.schema.field("quality").type == pa.int16()
    assert quality.get_mapping(data) == {"GOOD": [0]}
    simplified = quality.simplify(data)["quality"]
    assert simplified[0].as_py() == Quality.BAD.value
    assert simplified[-1].as_py() == Quality.GOOD.value


@patch(
    "requests.Session.post",
    side_effect=batch_through(mocked_requests_get_system_points),
)
@patch("requests.Session.get", side_effect=mocked_requests_get_system_points)
def test_get_data_system_points_value_is_null(_, __) -> None:
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
        SeriesSelector("Test", {"__id__": "A9"}), start_date, end_date
    )
    assert data["value"][0].as_py() is None
    assert data["value"][-1].as_py() == 81.83204


_DATETIME_DATA_POINTS = [
    {
        "Timestamp": "2020-01-01T00:00:00Z",
        "Value": "2019-06-01T08:30:00Z",
        "Good": True,
    },
    {
        "Timestamp": "2020-01-02T00:00:00Z",
        "Value": "2019-12-01T08:30:00Z",
        "Good": True,
    },
]


def mocked_requests_get_string_attributes(*args, **kwargs):
    attribute_types = {"A4": "DateTime", "A5": "String"}
    for attribute_id, attribute_type in attribute_types.items():
        if args[0] == f"{_BASE_URL}/attributes/{attribute_id}":
            assert kwargs["params"] == {"selectedFields": "Type"}
            return MockResponse({"Type": attribute_type}, 200)
        if args[0] in [
            f"{_BASE_URL}/streams/{attribute_id}/recorded",
            f"{_BASE_URL}/streams/{attribute_id}/plot",
        ]:
            return MockResponse({"Items": _DATETIME_DATA_POINTS}, 200)
    raise Exception(args[0])


def _string_attributes_source():
    return from_config(
        {
            "database_uri": "https://test_pi.net",
            "username": "test",
            "password": "test",
            "verify_ssl": "false",
        }
    )


@patch(
    "requests.Session.post",
    side_effect=batch_through(mocked_requests_get_string_attributes),
)
@patch("requests.Session.get", side_effect=mocked_requests_get_string_attributes)
def test_get_data_datetime_attribute(get, post) -> None:
    data = _string_attributes_source().get_data(
        SeriesSelector("Test", {"__id__": "A4"}),
        parse_date("2020-01-01T00:00:00Z"),
        parse_date("2020-01-03T00:00:00Z"),
    )
    assert data["value"].type == pa.timestamp("us", "UTC")
    assert data["value"].to_pylist() == [
        parse_date("2019-06-01T08:30:00Z"),
        parse_date("2019-12-01T08:30:00Z"),
    ]
    assert quality.get_mapping(data) == quality.DEFAULT_MAPPING
    assert post.call_count == 1, "the type and the data are read in one batch request"
    get.assert_not_called()


@patch(
    "requests.Session.post",
    side_effect=batch_through(mocked_requests_get_string_attributes),
)
@patch("requests.Session.get", side_effect=mocked_requests_get_string_attributes)
def test_get_plot_data_datetime_attribute(_, __) -> None:
    data = _string_attributes_source().get_plot_data(
        SeriesSelector("Test", {"__id__": "A4"}),
        parse_date("2020-01-01T00:00:00Z"),
        parse_date("2020-01-03T00:00:00Z"),
        interval_count=200,
    )
    assert data["value"].type == pa.timestamp("us", "UTC")


@patch(
    "requests.Session.post",
    side_effect=batch_through(mocked_requests_get_string_attributes),
)
@patch("requests.Session.get", side_effect=mocked_requests_get_string_attributes)
def test_get_data_string_attribute(_, __) -> None:
    data = _string_attributes_source().get_data(
        SeriesSelector("Test", {"__id__": "A5"}),
        parse_date("2020-01-01T00:00:00Z"),
        parse_date("2020-01-03T00:00:00Z"),
    )
    assert data["value"].to_pylist() == [
        "2019-06-01T08:30:00Z",
        "2019-12-01T08:30:00Z",
    ]


def test_datetime_attribute_metadata() -> None:
    attribute = {
        "Description": "Last calibration",
        "DefaultUnitsNameAbbreviation": "",
        "Step": True,
        "Zero": None,
        "Span": None,
        "Type": "DateTime",
        "CategoryNames": [],
        "Path": "\\\\server\\db\\element|Last calibration",
    }
    metadata = _get_metadata(SeriesSelector("Test", {"__id__": "A4"}), attribute, {})
    assert metadata is not None
    assert metadata.get_field(fields.DataType) == DataType.DATETIME
