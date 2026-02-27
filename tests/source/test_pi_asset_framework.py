"""Tests for non-source PI Asset Framework methods"""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pytest

from kukur.source.piwebapi_af.pi_asset_framework import (
    BatchRequestFailedException,
    ElementInOtherDatabaseException,
    PIAssetFramework,
    PIWebAPIConnection,
)

WEB_API_URI = "https://pi.example.org/piwebapi/"
DATABASE_URI = f"{WEB_API_URI}assetdatabases/F1RDMyvy4jYfVEyvgGiLVLmYvAjR9OmSafhkGfF09iWIcaIwVk0tVFMtUElcVElNRVNFRVI"
ROOT_ID = "F1EmMyvy4jYfVEyvgGiLVLmYvAe-IYOLTf7xGIoGBFvZT1mwVk0tVFMtUElcVElNRVNFRVJcUkVBQ1RPUlM"
ROOT_URI = f"{WEB_API_URI}elements/{ROOT_ID}"

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
                                "Path": "\\\\vm-ts-pi\\Timeseer\\ElementTemplates[Reactor]|Temperature",
                                "Description": "",
                                "DataReferencePlugIn": "PI Point",
                                "CategoryNames": ["Measurement"],
                            },
                            {
                                "Path": "\\\\vm-ts-pi\\Timeseer\\ElementTemplates[Reactor]|TemperatureKelvin",
                                "Description": "",
                                "DataReferencePlugIn": "Formula",
                                "CategoryNames": ["Measurement"],
                            },
                            {
                                "Path": "\\\\vm-ts-pi\\Timeseer\\ElementTemplates[Reactor]|Status",
                                "DataReferencePlugIn": "",
                                "Description": "",
                                "CategoryNames": [],
                            },
                            {
                                "Path": "\\\\vm-ts-pi\\Timeseer\\ElementTemplates[Reactor]|Status|Active",
                                "DataReferencePlugIn": "PI Point",
                                "Description": "",
                                "CategoryNames": [],
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
                    "Name": "Reactor",
                    "Description": "",
                    "Links": {
                        "AttributeTemplates": "https://pi.example.org/piwebapi/elements/Reactor/attributetemplates"
                    },
                },
                {
                    "Name": "Site",
                    "Description": "",
                    "Links": {
                        "AttributeTemplates": "https://pi.example.org/piwebapi/elements/Sites/attributetemplates"
                    },
                },
            ],
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

ELEMENT_CATEGORIES_RESPONSE = {
    "Links": {},
    "Items": [
        {
            "Name": "Production",
            "Description": "",
        },
        {
            "Name": "Test",
            "Description": "",
        },
    ],
}

ATTRIBUTE_CATEGORIES_RESPONSE = {
    "Links": {},
    "Items": [
        {
            "Name": "Measurement",
            "Description": "",
        },
        {
            "Name": "Status",
            "Description": "",
        },
    ],
}

BATCH_ATTRIBUTE_CATEGORY_RESPONSE = {
    "GetElement": {
        "Status": 207,
        "Headers": {},
        "Content": {
            "Total": 2,
            "Items": [
                {
                    "Status": 200,
                    "Content": {
                        "WebId": "R1",
                        "Name": "Reactor01",
                        "Description": "",
                        "TemplateName": "Reactor",
                        "CategoryNames": [],
                    },
                },
                {
                    "Status": 200,
                    "Content": {
                        "WebId": "R2",
                        "Name": "Reactor02",
                        "Description": "",
                        "TemplateName": "Reactor",
                        "CategoryNames": [],
                    },
                },
            ],
        },
    },
    "GetAttributes": {
        "Status": 200,
        "Content": {
            "Items": [
                {
                    "WebId": "A1",
                    "Name": "Level",
                    "Description": "",
                    "Path": "\\\\vm-ts-pi\\WriteBack\\Reactors\\Reactor01|Level",
                    "Type": "Double",
                    "TypeQualifier": "",
                    "DefaultUnitsNameAbbreviation": "",
                    "DataReferencePlugIn": "PI Point",
                    "CategoryNames": ["Validation"],
                    "Step": False,
                    "Span": 100.0,
                    "Zero": 0.0,
                    "Links": {"Element": "https://pi.timeseer.ai/piwebapi/elements/A1"},
                },
                {
                    "WebId": "A2",
                    "Name": "Level",
                    "Description": "",
                    "Path": "\\\\vm-ts-pi\\WriteBack\\Reactors\\Reactor02|Level",
                    "Type": "Double",
                    "TypeQualifier": "",
                    "DefaultUnitsNameAbbreviation": "",
                    "DataReferencePlugIn": "PI Point",
                    "CategoryNames": ["Validation"],
                    "Step": False,
                    "Span": 100.0,
                    "Zero": 0.0,
                    "Links": {"Element": "https://pi.timeseer.ai/piwebapi/elements/A2"},
                },
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
        if "GetElementTemplates" in kwargs["json"]:
            assert (
                "showDescendants=true"
                in kwargs["json"]["GetAttributeTemplates"]["RequestTemplate"][
                    "Resource"
                ]
            )
            assert (
                "showInherited=true"
                in kwargs["json"]["GetAttributeTemplates"]["RequestTemplate"][
                    "Resource"
                ]
            )

            response = BATCH_ELEMENT_TEMPLATES_RESPONSE
            return MockResponse(response, 200)
        if "GetElement" in kwargs["json"]:
            return MockResponse(BATCH_ATTRIBUTE_CATEGORY_RESPONSE, 200)

    raise Exception(args[0])


def mocked_requests_batch_error_templates(*args, **kwargs):
    if args[0] == f"{WEB_API_URI}batch":
        return MockResponse(BATCH_ERROR_TEMPLATES, 200)
    raise Exception(args[0])


def mocked_requests_batch_error_unknown_response(*args, **kwargs):
    if args[0] == f"{WEB_API_URI}batch":
        return MockResponse(
            {"GetElementTemplates": {"Status": 400, "Content": {"error": "message"}}},
            200,
        )
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

    if args[0] == f"{DATABASE_URI}/elementcategories":
        response = ELEMENT_CATEGORIES_RESPONSE
        return MockResponse(response, 200)

    if args[0] == f"{DATABASE_URI}/attributecategories":
        response = ATTRIBUTE_CATEGORIES_RESPONSE
        return MockResponse(response, 200)

    raise Exception(args[0])


@pytest.fixture
def af(request):
    marker = request.node.get_closest_marker("config")
    if marker is None:
        config = {
            "database_uri": DATABASE_URI,
            "element_template": "Reactor",
        }
    else:
        config = marker.args[0]
    with PIWebAPIConnection({}) as connection:
        af = PIAssetFramework(connection, config)
        yield af


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_elements(_, af: PIAssetFramework) -> None:
    elements = list(af.list_elements(None))
    assert len(elements) == 3
    assert elements[0].name == "Reactors"


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_elements_for_element(_, af: PIAssetFramework) -> None:
    element_web_id = "A1_1"
    elements = list(af.list_elements(element_web_id))
    assert len(elements) == 2
    assert elements[0].name == "Reactor 1"
    assert elements[1].name == "Reactor 2"


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_elements_for_invalid_element(_, af: PIAssetFramework) -> None:
    element_web_id = "B1_1"
    with pytest.raises(ElementInOtherDatabaseException):
        list(af.list_elements(element_web_id))


@patch("requests.Session.post", side_effect=mocked_requests_post)
@pytest.mark.config({"database_uri": DATABASE_URI})
def test_get_element_templates(_, af: PIAssetFramework) -> None:
    element_templates = af.list_element_templates()
    assert len(element_templates) == 2
    assert "Reactor" in [template.name for template in element_templates]
    reactor_template = [
        template for template in element_templates if template.name == "Reactor"
    ][0]
    assert len(reactor_template.attribute_templates) == 2
    assert "Temperature" in [
        attribute.name for attribute in reactor_template.attribute_templates
    ]
    temperature_template = [
        attribute
        for attribute in reactor_template.attribute_templates
        if attribute.name == "Temperature"
    ][0]
    assert temperature_template.categories == ["Measurement"]
    status_template = [
        attribute
        for attribute in reactor_template.attribute_templates
        if attribute.name == "Status|Active"
    ][0]
    assert len(status_template.categories) == 0


@patch("requests.Session.post", side_effect=mocked_requests_batch_error_templates)
def test_get_element_template_request_error(_, af: PIAssetFramework) -> None:
    with pytest.raises(BatchRequestFailedException):
        af.list_element_templates()


@patch(
    "requests.Session.post", side_effect=mocked_requests_batch_error_unknown_response
)
def test_get_element_template_request_unknown_error(_, af: PIAssetFramework) -> None:
    with pytest.raises(BatchRequestFailedException) as excinfo:
        af.list_element_templates()
    assert '"error": "message"' in str(excinfo.value)


@patch("requests.Session.post", side_effect=mocked_requests_post)
@pytest.mark.config(
    {"database_uri": DATABASE_URI, "allowed_data_references": ["Formula"]}
)
def test_get_element_templates_formula(_, af: PIAssetFramework) -> None:
    element_templates = af.list_element_templates()
    assert len(element_templates) == 2
    assert "Reactor" in [template.name for template in element_templates]
    reactor_template = [
        template for template in element_templates if template.name == "Reactor"
    ][0]
    assert len(reactor_template.attribute_templates) == 1
    assert "TemperatureKelvin" in [
        attribute.name for attribute in reactor_template.attribute_templates
    ]


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_element_categories(_, af: PIAssetFramework) -> None:
    element_categories = af.list_element_categories()
    assert len(element_categories) == 2
    assert element_categories[0].name == "Production"
    assert element_categories[1].name == "Test"


@patch("requests.Session.get", side_effect=mocked_requests_get)
def test_get_attribute_categories(_, af: PIAssetFramework) -> None:
    attribute_categories = af.list_attribute_categories()
    assert len(attribute_categories) == 2
    assert attribute_categories[0].name == "Measurement"
    assert attribute_categories[1].name == "Status"
