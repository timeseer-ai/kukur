# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pytest

from kukur import DataType
from kukur.base import SeriesSearch
from kukur.exceptions import KukurException
from kukur.source.piwebapi_af_template import from_config
from kukur.source.piwebapi_af_template.piwebapi_af_template import (
    ElementInOtherDatabaseException,
)

WEB_API_URI = "https://pi.example.org/piwebapi/"
DATABASE_URI = f"{WEB_API_URI}/assetdatabases/F1RDMyvy4jYfVEyvgGiLVLmYvAjR9OmSafhkGfF09iWIcaIwVk0tVFMtUElcVElNRVNFRVI"
ROOT_URI = f"{WEB_API_URI}/elements/F1EmMyvy4jYfVEyvgGiLVLmYvAe-IYOLTf7xGIoGBFvZT1mwVk0tVFMtUElcVElNRVNFRVJcUkVBQ1RPUlM"

BATCH_RESPONSE = {
    "GetAttributes": {
        "Status": 207,
        "Headers": {},
        "Content": {
            "Total": 2,
            "Items": [
                {
                    "Status": 200,
                    "Headers": {"Content-Type": "application/json; charset=utf-8"},
                    "Content": {
                        "Items": [
                            {
                                "WebId": "A1_1",
                                "Name": "Active",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Houston\\Reactor01|Status|Active",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Status"],
                                "Step": True,
                                "Span": 1.0,
                                "Zero": 0.0,
                            },
                            {
                                "WebId": "A1_2",
                                "Name": "Concentration",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Houston\\Reactor01|Concentration",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Measurement"],
                                "Step": False,
                                "Span": 200.0,
                                "Zero": 0.0,
                            },
                            {
                                "WebId": "A1_3",
                                "Name": "Level",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Houston\\Reactor01|Level",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Measurement"],
                                "Step": False,
                                "Span": 100.0,
                                "Zero": 0.0,
                            },
                            {
                                "WebId": "A1_4",
                                "Name": "Phase",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Houston\\Reactor01|Status|Phase",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Status"],
                                "Step": True,
                                "Span": 7.0,
                                "Zero": 0.0,
                            },
                            {
                                "WebId": "A1_5",
                                "Name": "Status",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Houston\\Reactor01|Status",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "",
                                "CategoryNames": [],
                                "Step": True,
                                "Span": None,
                                "Zero": None,
                            },
                            {
                                "WebId": "A1_6",
                                "Name": "Temperature",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Houston\\Reactor01|Temperature",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Measurement"],
                                "Step": False,
                                "Span": 100.0,
                                "Zero": 0.0,
                            },
                        ]
                    },
                },
                {
                    "Status": 200,
                    "Headers": {"Content-Type": "application/json; charset=utf-8"},
                    "Content": {
                        "Items": [
                            {
                                "WebId": "A2_1",
                                "Name": "Active",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Antwerp\\Reactor02|Status|Active",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Status"],
                                "Step": True,
                                "Span": 1.0,
                                "Zero": 0.0,
                            },
                            {
                                "WebId": "A2_2",
                                "Name": "Concentration",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Antwerp\\Reactor02|Concentration",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Measurement"],
                                "Step": False,
                                "Span": 200.0,
                                "Zero": 0.0,
                            },
                            {
                                "WebId": "A2_3",
                                "Name": "Level",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Antwerp\\Reactor02|Level",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Measurement"],
                                "Step": False,
                                "Span": 100.0,
                                "Zero": 0.0,
                            },
                            {
                                "WebId": "A2_4",
                                "Name": "Phase",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Antwerp\\Reactor02|Status|Phase",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Status"],
                                "Step": True,
                                "Span": 7.0,
                                "Zero": 0.0,
                            },
                            {
                                "WebId": "A2_5",
                                "Name": "Status",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Antwerp\\Reactor02|Status",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "",
                                "CategoryNames": [],
                                "Step": True,
                                "Span": None,
                                "Zero": None,
                            },
                            {
                                "WebId": "A2_6",
                                "Name": "Temperature",
                                "Description": "",
                                "Path": "\\\\vm-ts-pi\\Timeseer\\TSAI Antwerp\\Reactor02|Temperature",
                                "Type": "Double",
                                "DefaultUnitsNameAbbreviation": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Measurement"],
                                "Step": False,
                                "Span": 100.0,
                                "Zero": 0.0,
                            },
                        ]
                    },
                },
            ],
        },
    },
    "GetElements": {
        "Status": 200,
        "Headers": {"Content-Type": "application/json; charset=utf-8"},
        "Content": {
            "Items": [
                {
                    "WebId": "R1",
                    "Name": "Reactor01",
                    "Description": "Reactor Houston",
                    "CategoryNames": ["Production"],
                    "Links": {
                        "Attributes": "https://pi.example.org/piwebapi/elements/R1/attributes"
                    },
                },
                {
                    "WebId": "R2",
                    "Name": "Reactor02",
                    "Description": "Reactor Antwerp",
                    "CategoryNames": ["Test"],
                    "Links": {
                        "Attributes": "https://pi.example.org/piwebapi/elements/R2/attributes"
                    },
                },
            ]
        },
    },
}

BATCH_ERROR = {
    "GetAttributes": {
        "Status": 409,
        "Headers": {},
        "Content": "The following ParentIds did not complete successfully: GetElements.",
    },
    "GetElements": {
        "Status": 400,
        "Headers": {"Content-Type": "application/json; charset=utf-8"},
        "Content": {
            "Errors": [
                "The specified element category was not found in the specified Asset Database."
            ]
        },
    },
}


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def raise_for_status(self):
        return

    def json(self):
        return self.json_data


def mocked_requests_post(*args, **kwargs):
    if args[0] == f"{WEB_API_URI}batch":
        assert "X-Requested-With" in kwargs["headers"]

        if "categoryName=Invalid" in kwargs["json"]["GetElements"]["Resource"]:
            return MockResponse(BATCH_ERROR, 200)

        if "templateName=Reactor" in kwargs["json"]["GetElements"]["Resource"]:
            response = BATCH_RESPONSE
            return MockResponse(response, 200)

    raise Exception(args[0])


def mocked_requests_get(*args, **kwargs):
    if args[0] == f"{ROOT_URI}":
        return MockResponse({"Links": {"Database": DATABASE_URI}}, 200)

    raise Exception(args[0])


def mocked_requests_get_invalid_database(*args, **kwargs):
    if args[0] == f"{ROOT_URI}":
        return MockResponse(
            {"Links": {"Database": "https://pi.example.org/piwebapi/hacker"}}, 200
        )

    raise Exception(args[0])


@patch("requests.Session.post", side_effect=mocked_requests_post)
def test_search(_post) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "element_template": "Reactor",
        }
    )
    series_metadata = list(source.search(SeriesSearch("Test")))
    assert len(series_metadata) == 10

    concentration = [
        metadata
        for metadata in series_metadata
        if metadata.series.field == "Concentration"
    ]
    assert len(concentration) == 2

    templates = [metadata.get_field_by_name("Reactor") for metadata in concentration]
    assert "Reactor01" in templates
    assert "Reactor02" in templates

    metadata = [
        metadata
        for metadata in concentration
        if metadata.get_field_by_name("Reactor") == "Reactor01"
    ][0]

    assert metadata.series.tags["series name"] == "Reactor01"
    assert metadata.series.tags["__id__"] == "A1_2"
    assert metadata.get_field_by_name("description") == "Reactor Houston"
    assert metadata.get_field_by_name("data type") == DataType.FLOAT64
    assert metadata.get_field_by_name("Element category") == "Production"
    assert metadata.get_field_by_name("Attribute category") == "Measurement"


@patch("requests.Session.post", side_effect=mocked_requests_post)
def test_search_attribute_filter(_post) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "element_template": "Reactor",
            "attribute_names": ["Level", "Status|Active"],
        }
    )
    series_metadata = list(source.search(SeriesSearch("Test")))
    assert len(series_metadata) == 4

    active = [
        metadata for metadata in series_metadata if metadata.series.field == "Active"
    ]
    assert len(active) == 2


@patch("requests.Session.post", side_effect=mocked_requests_post)
def test_search_invalid_category(_post) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "element_template": "Reactor",
            "element_category": "Invalid",
        }
    )
    with pytest.raises(KukurException):
        list(source.search(SeriesSearch("Test")))


@patch("requests.Session.post", side_effect=mocked_requests_post)
@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_search_root_uri(_post, _get) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "root_uri": ROOT_URI,
            "element_template": "Reactor",
        }
    )
    series_metadata = list(source.search(SeriesSearch("Test")))
    assert len(series_metadata) == 10


@patch("requests.Session.post", side_effect=mocked_requests_post)
@patch("requests.Session.get", side_effect=mocked_requests_get_invalid_database)
def test_search_invalid_root_uri(_post, _get) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "root_uri": ROOT_URI,
            "element_template": "Reactor",
        }
    )
    with pytest.raises(ElementInOtherDatabaseException):
        list(source.search(SeriesSearch("Test")))
