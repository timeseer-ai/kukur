"""Kukur source for a PI AF Template using PI Web API."""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import urllib.parse
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath

import pyarrow as pa

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.auth import AuthenticationProperties
from kukur.base import DataType, Dictionary, InterpolationType
from kukur.exceptions import (
    InvalidSourceException,
    KukurException,
    MissingModuleException,
)
from kukur.metadata import fields
from kukur.source.piwebapi_af.piwebapi_af import (
    DataRequest,
    RequestProperties,
    read_data,
)

try:
    import urllib3
    from requests import Session

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logger = logging.getLogger(__name__)

HTTP_OK = 200
HTTP_MULTI_STATUS = 207
HTTP_NOT_FOUND = 404


class MetadataSearchFailedException(KukurException):
    """Raised when the metadata search failed."""


class ElementTemplateQueryFailedException(KukurException):
    """Raised when element template query failed."""


class AttributeTemplateQueryFailedException(KukurException):
    """Raised when attribute template query failed."""


class ElementInOtherDatabaseException(KukurException):
    """Raised when an element is in another AF database."""


@dataclass
class AFTemplateSourceConfiguration:
    """Configuration to find PI AF attributes of a template."""

    database_uri: str
    root_id: str | None
    element_template: str | None
    element_category: str | None
    attribute_names: list[str] | None
    attribute_category: str | None
    allowed_data_references: list[str]

    @classmethod
    def from_data(cls, config: dict) -> "AFTemplateSourceConfiguration":
        """Create an object from a configuration dict."""
        return cls(
            config["database_uri"],
            config.get("root_id"),
            config.get("element_template"),
            config.get("element_category"),
            config.get("attribute_names"),
            config.get("attribute_category"),
            config.get("allowed_data_references", ["PI Point"]),
        )


@dataclass
class Element:
    """One element in a PI AF structure."""

    web_id: str
    name: str
    description: str
    has_children: bool


@dataclass
class AttributeTemplate:
    """One attribute template of an element template."""

    name: str
    description: str
    categories: list[str]


@dataclass
class ElementTemplate:
    """One element template in a PI AF structure."""

    name: str
    description: str
    attribute_templates: list[AttributeTemplate]


@dataclass
class ElementCategory:
    """An element category definition in a PI AF structure."""

    name: str
    description: str


@dataclass
class AttributeCategory:
    """An attribute category definition in a PI AF structure."""

    name: str
    description: str


class PIWebAPIConnection:
    """Stateful connection to PI Asset Framework using PI Web API.

    Should be used as a context manager.
    """

    def __init__(self, config: dict):
        self._auth = AuthenticationProperties.from_data(config)
        self._verify_ssl = config.get("verify_ssl", True)

        if not self._verify_ssl:
            urllib3.disable_warnings()

    def __enter__(self) -> "PIWebAPIConnection":
        self.session = Session()
        self.session.headers["X-Requested-With"] = "Kukur"
        self.session.verify = self._verify_ssl
        self._auth.apply(self.session)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.session is not None:
            self.session.close()


class PIWebAPIAssetFrameworkTemplateSource:
    """Connect to PI AF using the PI Web API."""

    def __init__(self, config: dict):
        self._config = config

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all attributes of the selected elements in the Asset Framework."""
        with PIWebAPIConnection(self._config) as connection:
            af = PIAssetFramework(connection, self._config)
            yield from af.search(selector)

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return metadata for one tag."""
        raise NotImplementedError()

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        with PIWebAPIConnection(self._config) as connection:
            af = PIAssetFramework(connection, self._config)
            yield from af.get_data(selector, start_date, end_date)

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ):
        """Return plot data for the given time series in the given time period."""
        with PIWebAPIConnection(self._config) as connection:
            af = PIAssetFramework(connection, self._config)
            yield from af.get_plot_data(selector, start_date, end_date, interval_count)


class PIAssetFramework:
    """Query PI Asset Framework using PI Web API."""

    def __init__(self, connection: PIWebAPIConnection, config: dict):
        self._request_properties = RequestProperties(
            verify_ssl=config.get("verify_ssl", True),
            timeout_seconds=config.get("timeout_seconds", 60),
            metadata_request_timeout_seconds=config.get(
                "metadata_request_timeout_seconds", 10
            ),
            max_returned_items_per_call=config.get(
                "max_returned_items_per_call", 150000
            ),
            max_returned_metadata_items_per_call=config.get(
                "max_returned_metadata_items_per_call", 150
            ),
        )

        self.__config = AFTemplateSourceConfiguration.from_data(config)
        self._session = connection.session

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all attributes in the Asset Framework."""
        if (
            self.__config.element_template is None
            or self.__config.element_template.strip() == ""
        ):
            logger.info("Cannot search in element template source without template")
            return

        element_params = {
            "templateName": self.__config.element_template,
            "searchFullHierarchy": "true",
            "selectedFields": ";".join(
                [
                    "Items.Name",
                    "Items.WebId",
                    "Items.Description",
                    "Items.CategoryNames",
                    "Items.Links.Attributes",
                ]
            ),
            "maxCount": self._request_properties.max_returned_items_per_call,
        }
        if self.__config.element_category is not None:
            element_params["categoryName"] = self.__config.element_category
        attribute_params = {
            "searchFullHierarchy": "true",
            "selectedFields": ";".join(
                [
                    "Items.WebId",
                    "Items.Name",
                    "Items.Description",
                    "Items.Path",
                    "Items.CategoryNames",
                    "Items.DataReferencePlugin",
                    "Items.Type",
                    "Items.TypeQualifier",
                    "Items.DefaultUnitsNameAbbreviation",
                    "Items.Step",
                    "Items.Span",
                    "Items.Zero",
                    "Items.Links.EnumerationValues",
                ]
            ),
            "maxCount": self._request_properties.max_returned_items_per_call,
        }
        if self.__config.attribute_category is not None:
            attribute_params["categoryName"] = self.__config.attribute_category
        batch_query = {
            "GetElements": {
                "Method": "GET",
                "Resource": add_query_params(
                    self._get_element_search_url(),
                    element_params,
                ),
            },
            "GetAttributes": {
                "Method": "GET",
                "RequestTemplate": {
                    "Resource": "{0}?"
                    + urllib.parse.urlencode(
                        attribute_params,
                        doseq=True,
                    ),
                },
                "Parameters": ["$.GetElements.Content.Items[*].Links.Attributes"],
                "ParentIds": ["GetElements"],
            },
        }

        response = self._session.post(
            self._get_batch_url(),
            timeout=self._request_properties.metadata_request_timeout_seconds,
            json=batch_query,
        )
        response.raise_for_status()
        result = response.json()
        _validate_batch_response_status(result)

        dictionary_lookup = _DictionaryLookup(self._request_properties, self._session)
        for i, element in enumerate(result["GetElements"]["Content"].get("Items")):
            element_metadata = {self.__config.element_template: element["Name"]}
            if len(element["CategoryNames"]) > 0:
                element_metadata["Element category"] = ";".join(
                    element["CategoryNames"]
                )

            attributes = result["GetAttributes"]["Content"]["Items"][i]
            _validate_attribute_batch_item_status(element["Name"], attributes)
            for attribute in attributes["Content"].get("Items", []):
                if self.__config.attribute_names is not None:
                    attribute_path = attribute["Path"].split("|", maxsplit=1)[1]
                    if attribute_path not in self.__config.attribute_names:
                        continue

                tags = {
                    "series name": element["Name"],
                    "__id__": attribute["WebId"],
                }

                if (
                    "DataReferencePlugIn" in attribute
                    and attribute["DataReferencePlugIn"]
                    in self.__config.allowed_data_references
                ):
                    metadata = _get_metadata(
                        SeriesSelector(selector.source, tags, attribute["Name"]),
                        attribute,
                        element_metadata,
                    )
                    if metadata is not None:
                        if metadata.get_field(fields.Description) == "":
                            metadata.set_field(
                                fields.Description, element["Description"]
                            )
                        dictionary_lookup.lookup_dictionary(metadata, attribute)
                        yield metadata

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        data_url = self._get_data_url(selector)
        return read_data(
            self._session,
            self._request_properties,
            DataRequest(data_url, start_date, end_date, None),
        )

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ):
        """Return plot data for the given time series in the given time period."""
        plot_url = self._get_plot_url(selector)
        return read_data(
            self._session,
            self._request_properties,
            DataRequest(plot_url, start_date, end_date, interval_count),
        )

    def list_elements(self, element_id: str | None) -> list[Element]:
        """Return all direct child elements."""
        if element_id is None:
            url = f"{self.__config.database_uri}/elements"
        else:
            self._verify_element_in_database(f"{self._get_elements_url()}/{element_id}")

            url = f"{self._get_elements_url()}/{element_id}/elements"
        response = self._session.get(
            url,
            timeout=self._request_properties.metadata_request_timeout_seconds,
            params={
                "selectedFields": ";".join(
                    [
                        "Items.WebId",
                        "Items.Name",
                        "Items.Description",
                        "Items.HasChildren",
                    ]
                ),
            },
        )
        response.raise_for_status()
        data = response.json()

        return [
            Element(
                item["WebId"],
                item["Name"],
                item["Description"],
                item["HasChildren"],
            )
            for item in data["Items"]
        ]

    def list_element_templates(self) -> list[ElementTemplate]:
        """Return all element templates in the database."""
        element_template_params = {
            "selectedFields": ";".join(
                [
                    "Items.Name",
                    "Items.Description",
                    "Items.Links.AttributeTemplates",
                ]
            ),
        }
        attribute_template_params = {
            "selectedFields": ";".join(
                [
                    "Items.Path",
                    "Items.Description",
                    "Items.DataReferencePlugIn",
                    "Items.CategoryNames",
                ]
            ),
            "showDescendants": "true",
            "showInherited": "true",
        }

        batch_query = {
            "GetElementTemplates": {
                "Method": "GET",
                "Resource": add_query_params(
                    f"{self.__config.database_uri}/elementtemplates",
                    element_template_params,
                ),
            },
            "GetAttributeTemplates": {
                "Method": "GET",
                "RequestTemplate": {
                    "Resource": "{0}?"
                    + urllib.parse.urlencode(
                        attribute_template_params,
                        doseq=True,
                    ),
                },
                "Parameters": [
                    "$.GetElementTemplates.Content.Items[*].Links.AttributeTemplates"
                ],
                "ParentIds": ["GetElementTemplates"],
            },
        }

        response = self._session.post(
            self._get_batch_url(),
            timeout=self._request_properties.metadata_request_timeout_seconds,
            json=batch_query,
        )
        response.raise_for_status()
        result = response.json()

        if result["GetElementTemplates"]["Status"] != HTTP_OK:
            raise ElementTemplateQueryFailedException(
                ";".join(
                    result["GetElementTemplates"]["Content"].get(
                        "Errors", [json.dumps(result)]
                    )
                )
            )

        element_templates = []
        for i, element_template in enumerate(
            result["GetElementTemplates"]["Content"].get("Items", [])
        ):
            attributes = result["GetAttributeTemplates"]["Content"]["Items"][i]
            element_templates.append(
                ElementTemplate(
                    element_template["Name"],
                    element_template["Description"],
                    [
                        AttributeTemplate(
                            item["Path"].split("|", maxsplit=1)[1],
                            item["Description"],
                            item["CategoryNames"],
                        )
                        for item in attributes["Content"].get("Items", [])
                        if item["DataReferencePlugIn"]
                        in self.__config.allowed_data_references
                    ],
                )
            )

        return element_templates

    def list_element_categories(self) -> list[ElementCategory]:
        """Return all element categories in the database."""
        url = f"{self.__config.database_uri}/elementcategories"

        response = self._session.get(
            url,
            timeout=self._request_properties.metadata_request_timeout_seconds,
            params={
                "selectedFields": ";".join(
                    [
                        "Items.Name",
                        "Items.Description",
                    ]
                ),
            },
        )
        response.raise_for_status()
        data = response.json()

        return [
            ElementCategory(
                item["Name"],
                item["Description"],
            )
            for item in data["Items"]
        ]

    def list_attribute_categories(self) -> list[AttributeCategory]:
        """Return all attribute categories in the database."""
        url = f"{self.__config.database_uri}/attributecategories"

        response = self._session.get(
            url,
            verify=self._request_properties.verify_ssl,
            timeout=self._request_properties.metadata_request_timeout_seconds,
            params={
                "selectedFields": ";".join(
                    [
                        "Items.Name",
                        "Items.Description",
                    ]
                ),
            },
        )
        response.raise_for_status()
        data = response.json()

        return [
            AttributeCategory(
                item["Name"],
                item["Description"],
            )
            for item in data["Items"]
        ]

    def _get_batch_url(self) -> str:
        database_uri = urllib.parse.urlparse(self.__config.database_uri)
        batch_path = PurePath(database_uri.path).parent.parent / "batch"
        return urllib.parse.urlunparse(database_uri._replace(path=str(batch_path)))

    def _get_database_elements_url(self) -> str:
        database_uri = urllib.parse.urlparse(self.__config.database_uri)
        elements_path = PurePath(database_uri.path) / "elements"
        return urllib.parse.urlunparse(database_uri._replace(path=str(elements_path)))

    def _get_elements_url(self) -> str:
        database_uri = urllib.parse.urlparse(self.__config.database_uri)
        elements_path = PurePath(database_uri.path).parent.parent / "elements"
        return urllib.parse.urlunparse(database_uri._replace(path=str(elements_path)))

    def _get_data_url(self, selector: SeriesSelector) -> str:
        database_uri = urllib.parse.urlparse(self.__config.database_uri)
        recorded_path = (
            PurePath(database_uri.path).parent.parent
            / "streams"
            / selector.tags["__id__"]
            / "recorded"
        )
        return urllib.parse.urlunparse(database_uri._replace(path=str(recorded_path)))

    def _get_plot_url(self, selector: SeriesSelector) -> str:
        database_uri = urllib.parse.urlparse(self.__config.database_uri)
        plot_path = (
            PurePath(database_uri.path).parent.parent
            / "streams"
            / selector.tags["__id__"]
            / "plot"
        )
        return urllib.parse.urlunparse(database_uri._replace(path=str(plot_path)))

    def _get_element_search_url(self) -> str:
        elements_uri = self._get_database_elements_url()
        if self.__config.root_id is not None:
            element_id = self.__config.root_id
            self._verify_element_in_database(f"{self._get_elements_url()}/{element_id}")

            elements_uri = f"{self._get_elements_url()}/{element_id}/elements"
        return elements_uri

    def _verify_element_in_database(self, url: str):
        """Raise an exception when an element is not in the configured database."""
        response = self._session.get(
            url,
            timeout=self._request_properties.metadata_request_timeout_seconds,
            params={
                "selectedFields": ";".join(
                    [
                        "Links.Database",
                    ]
                ),
            },
        )
        response.raise_for_status()
        data = response.json()
        if data["Links"]["Database"] != self.__config.database_uri:
            raise ElementInOtherDatabaseException(
                f"element {url} is not in configured database"
            )


def _validate_batch_response_status(result: dict):
    error_message = None
    elements_content = result["GetElements"]["Content"]
    if result["GetElements"]["Status"] not in [HTTP_OK, HTTP_MULTI_STATUS]:
        error_message = json.dumps(result)
        if isinstance(elements_content, dict):
            error_message = ";".join(elements_content.get("Errors", [error_message]))
        if isinstance(elements_content, str):
            error_message = elements_content

    if error_message is not None:
        raise MetadataSearchFailedException(error_message)

    if result["GetAttributes"]["Status"] not in [HTTP_OK, HTTP_MULTI_STATUS]:
        error_message = json.dumps(result)
        attributes_content = result["GetAttributes"]["Content"]
        if isinstance(attributes_content, dict):
            error_message = ";".join(attributes_content.get("Errors", [error_message]))
        if isinstance(attributes_content, str):
            error_message = attributes_content
    if error_message is not None:
        if isinstance(elements_content, dict):
            elements = [
                f"{element['Name']}" for element in elements_content.get("Items", [])
            ]
            element_names = ", ".join(elements[:5])
            raise MetadataSearchFailedException(
                f"Failed to get attributes. Five first elements: {element_names}. {error_message}"
            )
        raise MetadataSearchFailedException(
            f"Failed to get attributes: {error_message}"
        )


def _validate_attribute_batch_item_status(element_name: str, item_data: dict):
    error_message = None
    if "Status" in item_data and item_data["Status"] != HTTP_OK:
        error_message = json.dumps(item_data)
        if "Content" in item_data:
            if isinstance(item_data["Content"], str):
                error_message = item_data["Content"]
            elif isinstance(item_data["Content"], dict):
                error_message = item_data["Content"].get("Message", item_data)
    if error_message is not None:
        raise AttributeTemplateQueryFailedException(
            f"Failed to get attributes for element '{element_name}': {error_message}"
        )


def _get_metadata(
    selector: SeriesSelector, attribute: dict, extra_metadata: dict
) -> Metadata | None:
    metadata = Metadata(selector)
    metadata.set_field(fields.Description, attribute["Description"])
    metadata.set_field(fields.Unit, attribute["DefaultUnitsNameAbbreviation"])

    if attribute["Step"]:
        metadata.set_field(fields.InterpolationType, InterpolationType.STEPPED)
    else:
        metadata.set_field(fields.InterpolationType, InterpolationType.LINEAR)

    if attribute["Zero"] is not None:
        metadata.set_field(fields.LimitLowFunctional, attribute["Zero"])
        if attribute["Span"] is not None:
            metadata.set_field(
                fields.LimitHighFunctional, attribute["Zero"] + attribute["Span"]
            )

    # From https://docs.aveva.com/bundle/pi-web-api-reference/page/help/topics/supported-attribute-data-types.html
    attribute_type = attribute["Type"]
    attribute_types = {
        "Boolean": DataType.CATEGORICAL,
        "Single": DataType.FLOAT32,
        "Double": DataType.FLOAT64,
        "Int16": DataType.FLOAT32,
        "Int32": DataType.FLOAT64,
        "Int64": DataType.FLOAT64,
        "String": DataType.STRING,
        "EnumerationValue": DataType.DICTIONARY,
    }
    if attribute_type not in attribute_types:
        return None
    metadata.set_field(fields.DataType, attribute_types[attribute_type])

    if attribute["Type"] == "EnumerationValue":
        metadata.set_field(fields.DictionaryName, attribute.get("TypeQualifier"))

    if len(attribute["CategoryNames"]) > 0:
        metadata.set_field_by_name(
            "Attribute category", ";".join(attribute["CategoryNames"])
        )

    metadata.set_field_by_name("Path", attribute["Path"])
    for k, v in extra_metadata.items():
        metadata.set_field_by_name(k, v)
    return metadata


class _DictionaryLookup:
    def __init__(self, request_properties: RequestProperties, session):
        self._request_properties = request_properties
        self._session = session
        self._lookup: dict[str, Dictionary] = {}

    def lookup_dictionary(self, metadata: Metadata, attribute: dict):
        """Add a dictionary to the series for enumeration sets."""
        dictionary_name = metadata.get_field(fields.DictionaryName)
        if dictionary_name is not None:
            if (
                dictionary_name not in self._lookup
                and "EnumerationValues" in attribute.get("Links", {})
            ):
                response = self._session.get(
                    attribute["Links"]["EnumerationValues"],
                    verify=self._request_properties.verify_ssl,
                    timeout=self._request_properties.metadata_request_timeout_seconds,
                    headers={"X-Requested-With": "Kukur"},
                    params={
                        "selectedFields": ";".join(
                            [
                                "Items.Name",
                                "Items.Value",
                            ]
                        ),
                    },
                )
                response.raise_for_status()
                result = response.json()

                mapping = {}
                for item in result.get("Items", []):
                    mapping[item["Value"]] = item["Name"]
                self._lookup[dictionary_name] = Dictionary(mapping)

            metadata.set_field(fields.Dictionary, self._lookup.get(dictionary_name))


def from_config(config: dict) -> PIWebAPIAssetFrameworkTemplateSource:
    """Create a new PIWebAPIAssetFrameworkSource."""
    if "database_uri" not in config:
        raise InvalidSourceException(
            'piwebapi-af-template sources require a "database_uri" entry'
        )
    if not HAS_REQUESTS:
        raise MissingModuleException("requests", "piwebapi-af")
    return PIWebAPIAssetFrameworkTemplateSource(config)


def add_query_params(url: str, params: dict) -> str:
    """Add additional query parameters to a URL."""
    parts = urllib.parse.urlsplit(url)
    query_params = urllib.parse.parse_qs(parts.query)
    query_params.update(params)
    parts = parts._replace(query=urllib.parse.urlencode(query_params, doseq=True))
    return urllib.parse.urlunsplit(parts)
