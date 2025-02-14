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
    ElementTemplateQueryFailedException,
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

BATCH_ELEMENT_TEMPLATES_RESPONSE = {
    "GetAttributeTemplates": {
        "Status": 207,
        "Content": {
            "Total": 2,
            "Items": [
                {
                    "Status": 200,
                    "Headers": {"Content-Type": "application/json; charset=utf-8"},
                    "Content": {
                        "Items": [
                            {
                                "Name": "Reactor",
                                "Description": "",
                            },
                            {
                                "Name": "Sites",
                                "Description": "",
                            },
                        ]
                    },
                },
                {
                    "Status": 200,
                    "Headers": {"Content-Type": "application/json; charset=utf-8"},
                    "Content": {"Items": []},
                },
            ],
        },
    },
    "GetElementTemplates": {
        "Status": 200,
        "Headers": {},
        "Content": {
            "Total": 2,
            "Items": [
                {
                    "Name": "A1",
                    "Description": "",
                    "Links": {
                        "AttributeTemplates": "https://pi.example.org/piwebapi/elements/A1/attributetemplates"
                    },
                },
                {
                    "Name": "A2",
                    "Description": "",
                    "Links": {
                        "AttributeTemplates": "https://pi.example.org/piwebapi/elements/A2/attributetemplates"
                    },
                },
            ],
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

BATCH_ERROR_TEMPLATES = {
    "GetAttributeTemplates": {
        "Status": 409,
        "Headers": {},
        "Content": "The following ParentIds did not complete successfully: GetElements.",
    },
    "GetElementTemplates": {
        "Status": 400,
        "Headers": {"Content-Type": "application/json; charset=utf-8"},
        "Content": {
            "Errors": [
                "The specified element category was not found in the specified Asset Database."
            ]
        },
    },
}


MAIN_ELEMENTS_RESPONSE = {
    "Items": [
        {
            "WebId": "A1_1",
            "Name": "Reactors",
            "Description": "Reactors",
            "HasChildren": True,
        },
        {
            "WebId": "A1_2",
            "Name": "Sites",
            "Description": "Sites",
            "HasChildren": False,
        },
        {
            "WebId": "A1_3",
            "Name": "Test",
            "Description": "",
            "HasChildren": True,
        },
    ]
}

ELEMENTS_RESPONSE = {
    "Items": [
        {
            "WebId": "A2_1",
            "Name": "Reactor 1",
            "Description": "First reactor",
            "HasChildren": True,
        },
        {
            "WebId": "A2_2",
            "Name": "Reactor 2",
            "Description": "Second reactor",
            "HasChildren": False,
        },
    ]
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

        if "GetElementTemplates" in kwargs["json"]:
            response = BATCH_ELEMENT_TEMPLATES_RESPONSE
            return MockResponse(response, 200)

        if "categoryName=Invalid" in kwargs["json"]["GetElements"]["Resource"]:
            return MockResponse(BATCH_ERROR, 200)

        if "templateName=Reactor" in kwargs["json"]["GetElements"]["Resource"]:
            response = BATCH_RESPONSE
            return MockResponse(response, 200)

    raise Exception(args[0])


def mocked_requests_batch_error_templates(*args, **kwargs):
    if args[0] == f"{WEB_API_URI}batch":
        assert "X-Requested-With" in kwargs["headers"]
        return MockResponse(BATCH_ERROR_TEMPLATES, 200)
    raise Exception(args[0])


def mocked_requests_get(*args, **kwargs):
    if args[0] == f"{ROOT_URI}":
        return MockResponse({"Links": {"Database": DATABASE_URI}}, 200)

    if args[0] == f"{DATABASE_URI}/elements":
        response = MAIN_ELEMENTS_RESPONSE
        return MockResponse(response, 200)

    if args[0] == f"{WEB_API_URI}elements/A1_1":
        response = {
            "WebId": "A1_1",
            "Name": "Reactors",
            "HasChildren": True,
            "Links": {
                "Database": DATABASE_URI,
            },
        }
        return MockResponse(response, 200)

    if args[0] == f"{WEB_API_URI}elements/B1_1":
        response = {
            "WebId": "A1_1",
            "Name": "Reactors",
            "HasChildren": True,
            "Links": {
                "Database": "https://pi.example.org/piwebapi/hacker",
            },
        }
        return MockResponse(response, 200)

    if args[0] == f"{WEB_API_URI}elements/A1_1/elements":
        response = ELEMENTS_RESPONSE
        return MockResponse(response, 200)

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


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_elements(_) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "element_template": "Reactor",
        }
    )
    elements = list(source.list_elements(None))
    assert len(elements) == 3
    assert elements[0].name == "Reactors"


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_elements_for_element(_) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "element_template": "Reactor",
        }
    )
    element_web_id = "A1_1"
    elements = list(source.list_elements(element_web_id))
    assert len(elements) == 2
    assert elements[0].name == "Reactor 1"
    assert elements[1].name == "Reactor 2"


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_elements_for_ivalid_element(_) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "element_template": "Reactor",
        }
    )
    element_web_id = "B1_1"
    with pytest.raises(ElementInOtherDatabaseException):
        list(source.list_elements(element_web_id))


@patch("requests.Session.post", side_effect=mocked_requests_post)
def test_get_element_templates(_) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "element_template": "Reactor",
        }
    )
    element_templates = source.list_element_templates()
    assert len(element_templates) == 2
    assert len(element_templates[0].attribute_templates) == 2
    assert len(element_templates[1].attribute_templates) == 0


@patch("requests.Session.post", side_effect=mocked_requests_batch_error_templates)
def test_get_element_template_request_error(_) -> None:
    source = from_config(
        {
            "database_uri": DATABASE_URI,
            "element_template": "Reactor",
        }
    )
    with pytest.raises(ElementTemplateQueryFailedException):
        source.list_element_templates()
