"""Kukur source for a PI AF Template using PI Web API."""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import urllib.parse
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath

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
HTTP_BAD_REQUEST = 400
HTTP_NOT_FOUND = 404


class ElementInOtherDatabaseException(KukurException):
    """Raised when an element is in another AF database."""


class BatchRequestFailedException(KukurException):
    """Raised when a batch query fails."""


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


class DatabaseURLBuilder:
    """Build Web API URLs for Asset Databases.

    Should be used for building the starting point for AF navigation.
    Use the Links to navigate.
    """

    def __init__(self, database_uri: str):
        self.database_uri = urllib.parse.urlparse(database_uri)

    def root(self, path: list[str] | str) -> str:
        """Generate a URL relative to the root of the Web API."""
        if isinstance(path, str):
            path = [path]
        result_path = PurePosixPath(
            self.database_uri.path
        ).parent.parent / PurePosixPath(*path)
        return urllib.parse.urlunparse(
            self.database_uri._replace(path=str(result_path))
        )

    def database(self, path: list[str] | str) -> str:
        """Generate a URL relative to the database."""
        if isinstance(path, str):
            path = [path]
        result_path = PurePosixPath(self.database_uri.path) / PurePosixPath(*path)
        return urllib.parse.urlunparse(
            self.database_uri._replace(path=str(result_path))
        )

    def database_id(self) -> str:
        """Return the WebID of the Asset Database."""
        return PurePosixPath(self.database_uri.path).stem


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

        self._config = AFTemplateSourceConfiguration.from_data(config)
        self._session = connection.session
        self._url = DatabaseURLBuilder(self._config.database_uri)

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all attributes in the Asset Framework."""
        if (
            self._config.element_template is not None
            and self._config.element_template.strip() != ""
        ):
            yield from self._search_template(selector)
        elif (
            self._config.attribute_category is not None
            and self._config.attribute_category.strip() != ""
        ):
            yield from self._search_attribute_category(selector)
        else:
            logger.info(
                "Cannot search in AF source without template or attribute category"
            )

    def _search_template(
        self, selector: SeriesSearch
    ) -> Generator[Metadata, None, None]:
        element_params = {
            "templateName": self._config.element_template,
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
        if self._config.element_category is not None:
            element_params["categoryName"] = self._config.element_category
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
        if self._config.attribute_category is not None:
            attribute_params["categoryName"] = self._config.attribute_category
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
        validate_batch_response(result)

        dictionary_lookup = _DictionaryLookup(self._request_properties, self._session)
        for i, element in enumerate(result["GetElements"]["Content"].get("Items")):
            element_metadata = {self._config.element_template: element["Name"]}
            if len(element["CategoryNames"]) > 0:
                element_metadata["Element category"] = ";".join(
                    element["CategoryNames"]
                )

            attributes = result["GetAttributes"]["Content"]["Items"][i]
            for attribute in attributes["Content"].get("Items", []):
                if self._config.attribute_names is not None:
                    attribute_path = attribute["Path"].split("|", maxsplit=1)[1]
                    if attribute_path not in self._config.attribute_names:
                        continue

                tags = {
                    "series name": element["Name"],
                    "__id__": attribute["WebId"],
                }

                if (
                    "DataReferencePlugIn" in attribute
                    and attribute["DataReferencePlugIn"]
                    in self._config.allowed_data_references
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

    def _search_attribute_category(
        self, selector: SeriesSearch
    ) -> Generator[Metadata, None, None]:
        if self._config.root_id is not None:
            raise InvalidSourceException("Cannot search attributes with element root")

        attribute_params = {
            "databaseWebId": self._url.database_id(),
            "query": f"Element:{{ Name:=* }} category:{self._config.attribute_category}",
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
                    "Items.Links.Element",
                ]
            ),
            "maxCount": self._request_properties.max_returned_items_per_call,
        }

        element_params = {
            "selectedFields": ";".join(
                [
                    "Name",
                    "WebId",
                    "Description",
                    "TemplateName",
                    "CategoryNames",
                ]
            ),
            "maxCount": self._request_properties.max_returned_items_per_call,
        }
        if self._config.element_category is not None:
            element_params["categoryName"] = self._config.element_category

        batch_query = {
            "GetAttributes": {
                "Method": "GET",
                "Resource": add_query_params(
                    self._url.root(["attributes", "search"]),
                    attribute_params,
                ),
            },
            "GetElement": {
                "Method": "GET",
                "RequestTemplate": {
                    "Resource": "{0}?"
                    + urllib.parse.urlencode(
                        element_params,
                        doseq=True,
                    ),
                },
                "Parameters": ["$.GetAttributes.Content.Items[*].Links.Element"],
                "ParentIds": ["GetAttributes"],
            },
        }

        response = self._session.post(
            self._get_batch_url(),
            timeout=self._request_properties.metadata_request_timeout_seconds,
            json=batch_query,
        )
        response.raise_for_status()
        result = response.json()
        validate_batch_response(result)

        dictionary_lookup = _DictionaryLookup(self._request_properties, self._session)
        for i, element_request in enumerate(
            result["GetElement"]["Content"].get("Items")
        ):
            element = element_request["Content"]
            element_metadata = {}
            if (
                "TemplateName" in element
                and element["TemplateName"] is not None
                and len(element["TemplateName"]) > 0
            ):
                element_metadata[element["TemplateName"]] = element["Name"]

            if len(element["CategoryNames"]) > 0:
                element_metadata["Element category"] = ";".join(
                    element["CategoryNames"]
                )

            attribute = result["GetAttributes"]["Content"]["Items"][i]
            if self._config.attribute_names is not None:
                attribute_path = attribute["Path"].split("|", maxsplit=1)[1]
                if attribute_path not in self._config.attribute_names:
                    continue

            tags = {
                "series name": element["Name"],
                "__id__": attribute["WebId"],
            }

            if (
                "DataReferencePlugIn" in attribute
                and attribute["DataReferencePlugIn"]
                in self._config.allowed_data_references
            ):
                metadata = _get_metadata(
                    SeriesSelector(selector.source, tags, attribute["Name"]),
                    attribute,
                    element_metadata,
                )
                if metadata is not None:
                    if metadata.get_field(fields.Description) == "":
                        metadata.set_field(fields.Description, element["Description"])
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
            url = self._url.database("elements")
        else:
            self._verify_element_in_database(self._url.root(["elements", element_id]))
            url = self._url.root(["elements", element_id, "elements"])

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
                    f"{self._config.database_uri}/elementtemplates",
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
        validate_batch_response(result)

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
                        in self._config.allowed_data_references
                    ],
                )
            )

        return element_templates

    def list_element_categories(self) -> list[ElementCategory]:
        """Return all element categories in the database."""
        url = f"{self._config.database_uri}/elementcategories"

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
        url = f"{self._config.database_uri}/attributecategories"

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
        return self._url.root("batch")

    def _get_data_url(self, selector: SeriesSelector) -> str:
        return self._url.root(["streams", selector.tags["__id__"], "recorded"])

    def _get_plot_url(self, selector: SeriesSelector) -> str:
        return self._url.root(["streams", selector.tags["__id__"], "plot"])

    def _get_element_search_url(self) -> str:
        if self._config.root_id is not None:
            element_id = self._config.root_id
            self._verify_element_in_database(self._url.root(["elements", element_id]))
            return self._url.root(["elements", element_id, "elements"])
        return self._url.database("elements")

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
        if data["Links"]["Database"] != self._config.database_uri:
            raise ElementInOtherDatabaseException(
                f"element {url} is not in configured database"
            )


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
            return af.get_data(selector, start_date, end_date)

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ) -> pa.Table:
        """Return plot data for the given time series in the given time period."""
        with PIWebAPIConnection(self._config) as connection:
            af = PIAssetFramework(connection, self._config)
            return af.get_plot_data(selector, start_date, end_date, interval_count)


def validate_batch_response(result: dict):
    """Validate a batch controller result.

    Successful queries return 200 or 207 (for templated queries).
    Queries fail with 400.
    Queries that were not executed because their parent failed return 409.
    """
    errors = []

    for batch_id, batch_response in result.items():
        batch_status = batch_response["Status"]
        if batch_status == HTTP_MULTI_STATUS:
            for item in batch_response["Content"]["Items"]:
                if item["Status"] >= HTTP_BAD_REQUEST:
                    errors.append((batch_id, _extract_error(item)))
        elif batch_status >= HTTP_BAD_REQUEST:
            errors.append((batch_id, _extract_error(batch_response)))

    if len(errors) > 0:
        raise BatchRequestFailedException(
            ";".join([f"{batch_id}: {error}" for batch_id, error in errors])
        )


def _extract_error(item: dict) -> str:
    content = item["Content"]
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        if "Errors" in content:
            return ";".join(content["Errors"])
    return json.dumps(content)


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
