"""Query PI Asset Framework."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import itertools
import json
import logging
import time
import urllib.parse
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, TypeVar

import pyarrow as pa

from kukur import Metadata, SeriesSearch, SeriesSelector, quality
from kukur.auth import AuthenticationProperties
from kukur.base import DataType, Dictionary, InterpolationType
from kukur.exceptions import (
    DataNotFoundException,
    InvalidSourceException,
    KukurException,
)
from kukur.metadata import fields
from kukur.quality import Quality
from kukur.timestamp import parse_timestamp

try:
    import urllib3
    from requests import RequestException, Session
    from requests.exceptions import ConnectionError as RequestsConnectionError
    from requests.exceptions import HTTPError, Timeout

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logger = logging.getLogger(__name__)

HTTP_OK = 200
HTTP_MULTI_STATUS = 207
HTTP_BAD_REQUEST = 400
HTTP_NOT_FOUND = 404
HTTP_TOO_MANY_REQUESTS = 429
HTTP_SERVICE_UNAVAILABLE = 503
TRANSIENT_STATUS_CODES = (HTTP_TOO_MANY_REQUESTS, HTTP_SERVICE_UNAVAILABLE)

T = TypeVar("T")

# An element path starts with "\\server\database": 4 components when split on "\".
ELEMENT_PATH_PREFIX_COMPONENTS = 4


class ElementInOtherDatabaseException(KukurException):
    """Raised when an element is in another AF database."""


class BatchRequestFailedException(KukurException):
    """Raised when a batch query fails."""


class TransientBatchRequestFailedException(BatchRequestFailedException):
    """Raised when a batch query fails because PI Web API is rate limited or unavailable."""


@dataclass
class MetadataAttribute:
    """An attribute that sets a metadata field of all series of an element.

    The attribute belongs to the element itself,
    to the nearest ancestor with the element template `template_name`
    or to the ancestor `level` levels up.
    """

    attribute: str
    template_name: str | None = None
    level: int | None = None

    @classmethod
    def from_data(cls, data: str | dict) -> "MetadataAttribute":
        """Create from an attribute path or a configuration dict."""
        if isinstance(data, str):
            return cls(data)
        if not isinstance(data, dict):
            raise InvalidSourceException(f"invalid metadata attribute: {data}")
        unknown_keys = set(data.keys()) - {"attribute", "template_name", "level"}
        if len(unknown_keys) > 0:
            raise InvalidSourceException(
                f"unknown metadata attribute keys: {', '.join(sorted(unknown_keys))}"
            )
        attribute = data.get("attribute")
        if not isinstance(attribute, str) or attribute.strip() == "":
            raise InvalidSourceException('metadata attributes require an "attribute"')
        template_name = data.get("template_name")
        if template_name is not None and (
            not isinstance(template_name, str) or template_name.strip() == ""
        ):
            raise InvalidSourceException(
                f'invalid "template_name" for metadata attribute {attribute}'
            )
        level = data.get("level")
        if level is not None and (
            not isinstance(level, int) or isinstance(level, bool) or level < 1
        ):
            raise InvalidSourceException(
                f'invalid "level" for metadata attribute {attribute}'
            )
        if template_name is not None and level is not None:
            raise InvalidSourceException(
                f'metadata attribute {attribute} has both "template_name" and "level"'
            )
        return cls(attribute, template_name, level)

    @classmethod
    def list_from_data(cls, data: str | dict | list) -> list["MetadataAttribute"]:
        """Create the metadata attributes to try in order for one metadata field."""
        if not isinstance(data, list):
            return [cls.from_data(data)]
        if len(data) == 0:
            raise InvalidSourceException("empty list of metadata attributes")
        return [cls.from_data(item) for item in data]

    def is_of_ancestor(self) -> bool:
        """Return True when the attribute belongs to an ancestor of the element."""
        return self.template_name is not None or self.level is not None


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
    attributes_as_fields: bool
    use_attribute_path: bool
    metadata_attributes: dict[str, list[MetadataAttribute]]
    element_paths: list[str] | None

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
            config.get("attributes_as_fields", True),
            config.get("use_attribute_path", False),
            {
                field_name: MetadataAttribute.list_from_data(metadata_attributes)
                for field_name, metadata_attributes in config.get(
                    "metadata_attributes", {}
                ).items()
            },
            config.get("element_paths"),
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
    is_data: bool


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


@dataclass
class RequestProperties:
    """Request properties for PI Web API connection."""

    verify_ssl: bool
    timeout_seconds: float
    metadata_request_timeout_seconds: float
    max_returned_items_per_call: int
    max_returned_metadata_items_per_call: int
    web_id_type: str
    retry_count: int
    retry_delay: float


@dataclass
class DataRequest:
    """A request for AF data."""

    url: str
    start_date: datetime
    end_date: datetime
    interval_count: int | None


class PIWebAPIConnection:
    """Stateful connection to PI Asset Framework using PI Web API.

    Should be used as a context manager.
    """

    def __init__(self, config: dict):
        self._auth = AuthenticationProperties.from_data(config)
        self._verify_ssl = config.get("verify_ssl", True)
        self._enable_trace_logging = config.get("enable_trace_logging", False)

        if not self._verify_ssl:
            urllib3.disable_warnings()

    def __enter__(self) -> "PIWebAPIConnection":
        session = Session()
        session.headers["X-Requested-With"] = "Kukur"
        session.verify = self._verify_ssl
        self._auth.apply(session)
        self.session = _TracingSession(session, self._enable_trace_logging)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.session is not None:
            self.session.close()


class _TracingSession:
    """Wrap a requests Session to optionally log all HTTP traffic.

    When trace logging is enabled, every request logs its method, URL and the
    JSON or query parameters sent, together with the JSON received.
    """

    def __init__(self, session: "Session", enable_trace_logging: bool):
        self._session = session
        self._enable_trace_logging = enable_trace_logging

    def get(self, url: str, **kwargs):
        """Send a GET request, tracing it when enabled."""
        response = self._session.get(url, **kwargs)
        self._trace("GET", url, kwargs, response)
        return response

    def post(self, url: str, **kwargs):
        """Send a POST request, tracing it when enabled."""
        response = self._session.post(url, **kwargs)
        self._trace("POST", url, kwargs, response)
        return response

    def close(self) -> None:
        """Close the wrapped session."""
        self._session.close()

    def _trace(self, method: str, url: str, kwargs: dict, response) -> None:
        if not self._enable_trace_logging:
            return
        sent = kwargs.get("json")
        if sent is None:
            sent = kwargs.get("params")
        logger.info(
            "PI Web API %s %s\nrequest:\n%s\nresponse:\n%s",
            method,
            url,
            json.dumps(sent, indent=2, default=str),
            _format_response_body(response),
        )


def _format_response_body(response) -> str:
    try:
        return json.dumps(response.json(), indent=2, default=str)
    except ValueError:
        return response.text


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
            web_id_type=config.get("web_id_type", "Full"),
            retry_count=config.get("query_retry_count", 0),
            retry_delay=config.get("query_retry_delay", 1.0),
        )

        self._config = AFTemplateSourceConfiguration.from_data(config)
        self._session = connection.session
        self._url = DatabaseURLBuilder(self._config.database_uri)

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all attributes in the Asset Framework."""
        if self._config.element_paths is not None:
            yield from self._search_element_paths(selector)
        elif (
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
        dictionary_lookup = _DictionaryLookup(self._request_properties, self._session)
        metadata_attribute_lookup = self._create_metadata_attribute_lookup()
        start_index = 0
        while True:
            element_params = {
                "templateName": self._config.element_template,
                "searchFullHierarchy": "true",
                "selectedFields": ";".join(
                    [
                        "Items.Name",
                        "Items.WebId",
                        "Items.Path",
                        "Items.Description",
                        "Items.CategoryNames",
                        "Items.Links.Attributes",
                    ]
                ),
                "maxCount": self._request_properties.max_returned_metadata_items_per_call,
                "startIndex": start_index,
                "webIdType": self._request_properties.web_id_type,
            }
            if self._config.element_category is not None:
                element_params["categoryName"] = self._config.element_category
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
                            self._get_element_attribute_params(),
                            doseq=True,
                        ),
                    },
                    "Parameters": ["$.GetElements.Content.Items[*].Links.Attributes"],
                    "ParentIds": ["GetElements"],
                },
            }

            result = validate_batch_response(self._send_batch(batch_query))
            if result is None:
                break

            elements = result["GetElements"]["Content"].get("Items", [])
            attributes = result["GetAttributes"]["Content"]["Items"]
            yield from self._create_template_metadata(
                selector,
                elements,
                attributes,
                dictionary_lookup,
                metadata_attribute_lookup,
            )

            element_count = len(elements)
            if (
                element_count
                != self._request_properties.max_returned_metadata_items_per_call
            ):
                break

            start_index = start_index + element_count

    def _create_template_metadata(
        self,
        selector: SeriesSearch,
        elements: list,
        attributes: list,
        dictionary_lookup: "_DictionaryLookup",
        metadata_attribute_lookup: "_MetadataAttributeLookup",
    ) -> Generator[Metadata, None, None]:
        metadata_attribute_values = metadata_attribute_lookup.lookup(elements)
        for i, element in enumerate(elements):
            element_metadata = {}
            template_name = element.get("TemplateName", self._config.element_template)
            if template_name:
                element_metadata[template_name] = element["Name"]
            if len(element["CategoryNames"]) > 0:
                element_metadata["Element category"] = ";".join(
                    element["CategoryNames"]
                )

            for attribute in attributes[i]["Content"].get("Items", []):
                if self._config.attribute_names is not None:
                    attribute_path = attribute["Path"].split("|", maxsplit=1)[1]
                    if attribute_path not in self._config.attribute_names:
                        continue

                field_name = self._name_attribute(attribute)
                tags = {
                    "series name": element["Name"],
                    "__id__": attribute["WebId"],
                }
                if not self._config.attributes_as_fields:
                    tags["series name"] = self._name_attribute(attribute)
                    tags["element"] = element["Name"]
                    field_name = "value"

                if (
                    "DataReferencePlugIn" in attribute
                    and attribute["DataReferencePlugIn"]
                    in self._config.allowed_data_references
                ):
                    metadata = _get_metadata(
                        SeriesSelector(selector.source, tags, field_name),
                        attribute,
                        element_metadata,
                    )
                    if metadata is not None:
                        if metadata.get_field(fields.Description) == "":
                            metadata.set_field(
                                fields.Description, element["Description"]
                            )
                        dictionary_lookup.lookup_dictionary(metadata, attribute)
                        _set_metadata_attribute_values(
                            metadata,
                            element,
                            metadata_attribute_values.get(element["WebId"], {}),
                        )
                        yield metadata

    def _search_element_paths(
        self, selector: SeriesSearch
    ) -> Generator[Metadata, None, None]:
        assert self._config.element_paths is not None
        dictionary_lookup = _DictionaryLookup(self._request_properties, self._session)
        metadata_attribute_lookup = self._create_metadata_attribute_lookup()
        page_size = self._request_properties.max_returned_metadata_items_per_call
        for start_index in range(0, len(self._config.element_paths), page_size):
            elements, attributes = self._get_elements_by_path(
                self._config.element_paths[start_index : start_index + page_size]
            )
            yield from self._create_template_metadata(
                selector,
                elements,
                attributes,
                dictionary_lookup,
                metadata_attribute_lookup,
            )

    def _get_elements_by_path(self, element_paths: list[str]) -> tuple[list, list]:
        """Return the elements at the given paths and their attributes.

        Each element is requested separately, as paths do not fit in one URL.
        """
        element_params = {
            "selectedFields": ";".join(
                [
                    "Name",
                    "WebId",
                    "Path",
                    "Description",
                    "TemplateName",
                    "CategoryNames",
                    "Links.Attributes",
                    "Links.Database",
                ]
            ),
        }
        attribute_resource = "{0}?" + urllib.parse.urlencode(
            self._get_element_attribute_params(),
            doseq=True,
        )
        batch_query: dict[str, dict] = {}
        for i, element_path in enumerate(element_paths):
            batch_query[f"GetElement{i}"] = {
                "Method": "GET",
                "Resource": add_query_params(
                    self._url.root("elements"),
                    {**element_params, "path": element_path},
                ),
            }
            batch_query[f"GetAttributes{i}"] = {
                "Method": "GET",
                "Resource": attribute_resource,
                "Parameters": [f"$.GetElement{i}.Content.Links.Attributes"],
                "ParentIds": [f"GetElement{i}"],
            }

        result = self._send_batch(batch_query)

        elements = []
        attributes = []
        for i, element_path in enumerate(element_paths):
            element_response = result[f"GetElement{i}"]
            if element_response["Status"] != HTTP_OK:
                logger.warning(
                    "Failed to find element %s: %s",
                    element_path,
                    _extract_error(element_response),
                )
                continue
            element = element_response["Content"]
            if element["Links"]["Database"] != self._config.database_uri:
                raise ElementInOtherDatabaseException(
                    f"element {element_path} is not in configured database"
                )
            attribute_response = result[f"GetAttributes{i}"]
            if attribute_response["Status"] >= HTTP_BAD_REQUEST:
                raise BatchRequestFailedException(
                    f"GetAttributes{i}: {_extract_error(attribute_response)}"
                )
            elements.append(element)
            attributes.append(attribute_response)
        return elements, attributes

    def _search_attribute_category(
        self, selector: SeriesSearch
    ) -> Generator[Metadata, None, None]:
        if self._config.root_id is not None:
            raise InvalidSourceException("Cannot search attributes with element root")

        metadata_attribute_lookup = self._create_metadata_attribute_lookup()
        start_index = 0
        while True:
            attribute_params = {
                "databaseWebId": self._url.database_id(),
                "query": f'Element:{{ Name:=* }} category:"{self._config.attribute_category}"',
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
                "maxCount": self._request_properties.max_returned_metadata_items_per_call,
                "startIndex": start_index,
                "webIdType": self._request_properties.web_id_type,
            }

            element_params = {
                "selectedFields": ";".join(
                    [
                        "Name",
                        "WebId",
                        "Path",
                        "Description",
                        "TemplateName",
                        "CategoryNames",
                    ]
                ),
                "webIdType": self._request_properties.web_id_type,
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

            result = validate_batch_response(self._send_batch(batch_query))
            if result is None:
                break

            yield from self._create_metadata(
                selector.source, result, metadata_attribute_lookup
            )

            attribute_count = len(result["GetAttributes"]["Content"].get("Items", []))
            if (
                attribute_count
                != self._request_properties.max_returned_metadata_items_per_call
            ):
                break

            start_index = start_index + attribute_count

    def _create_metadata(
        self,
        source_name: str,
        result: dict,
        metadata_attribute_lookup: "_MetadataAttributeLookup",
    ) -> Generator[Metadata, None, None]:
        dictionary_lookup = _DictionaryLookup(self._request_properties, self._session)
        elements = [
            element_request["Content"]
            for element_request in result["GetElement"]["Content"].get("Items")
        ]
        metadata_attribute_values = metadata_attribute_lookup.lookup(elements)
        for i, element in enumerate(elements):
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

            field_name = self._name_attribute(attribute)
            tags = {
                "series name": element["Name"],
                "__id__": attribute["WebId"],
            }
            if not self._config.attributes_as_fields:
                tags["series name"] = self._name_attribute(attribute)
                tags["element"] = element["Name"]
                field_name = "value"

            if (
                "DataReferencePlugIn" in attribute
                and attribute["DataReferencePlugIn"]
                in self._config.allowed_data_references
            ):
                metadata = _get_metadata(
                    SeriesSelector(source_name, tags, field_name),
                    attribute,
                    element_metadata,
                )
                if metadata is not None:
                    if metadata.get_field(fields.Description) == "":
                        metadata.set_field(fields.Description, element["Description"])
                    dictionary_lookup.lookup_dictionary(metadata, attribute)
                    _set_metadata_attribute_values(
                        metadata,
                        element,
                        metadata_attribute_values.get(element["WebId"], {}),
                    )
                    yield metadata

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        data_url = self._get_data_url(selector)
        return _read_data(
            self._session,
            self._request_properties,
            DataRequest(
                data_url,
                start_date,
                end_date,
                None,
            ),
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
        return _read_data(
            self._session,
            self._request_properties,
            DataRequest(
                plot_url,
                start_date,
                end_date,
                interval_count,
            ),
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
                "webIdType": self._request_properties.web_id_type,
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
                            item["DataReferencePlugIn"]
                            in self._config.allowed_data_references,
                        )
                        for item in attributes["Content"].get("Items", [])
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

    def _send_batch(self, batch_query: dict) -> dict:
        def send() -> dict:
            response = self._session.post(
                self._get_batch_url(),
                timeout=self._request_properties.metadata_request_timeout_seconds,
                json=batch_query,
            )
            response.raise_for_status()
            result = response.json()
            _raise_for_transient_failure(result)
            return result

        return _retry_transient(self._request_properties, send)

    def _create_metadata_attribute_lookup(self) -> "_MetadataAttributeLookup":
        return _MetadataAttributeLookup(
            self._config.metadata_attributes,
            self._request_properties,
            self._session,
            self._url,
        )

    def _get_element_attribute_params(self) -> dict:
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
            "maxCount": self._request_properties.max_returned_metadata_items_per_call,
            "webIdType": self._request_properties.web_id_type,
        }
        if self._config.attribute_category is not None:
            attribute_params["categoryName"] = self._config.attribute_category
        return attribute_params

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
            database_link = data["Links"]["Database"]
            raise ElementInOtherDatabaseException(
                f"element {url} (database: {database_link}) is not in configured database"
            )

    def _name_attribute(self, attribute: dict) -> str:
        if self._config.use_attribute_path:
            return attribute["Path"].split("|", maxsplit=1)[1]
        return attribute["Name"]


def validate_batch_response(result: dict) -> dict | None:
    """Validate a batch controller result.

    Successful queries return 200 or 207 (for templated queries).
    Queries fail with 400.
    Queries that were not executed because their parent failed return 409.

    Sometimes queries return a 400 when the parent list is empty. `None` is returned in this case.
    """
    errors = []

    for batch_id, batch_response in result.items():
        batch_status = batch_response["Status"]
        if batch_status == HTTP_MULTI_STATUS:
            for item in batch_response["Content"]["Items"]:
                if item["Status"] >= HTTP_BAD_REQUEST:
                    errors.append((batch_id, _extract_error(item)))
        elif batch_status >= HTTP_BAD_REQUEST:
            error_message = _extract_error(batch_response)
            if "Some JSON paths did not select any tokens" in error_message:
                return None
            errors.append((batch_id, error_message))

    if len(errors) > 0:
        raise BatchRequestFailedException(
            ";".join([f"{batch_id}: {error}" for batch_id, error in errors])
        )

    return result


def _extract_error(item: dict) -> str:
    content = item["Content"]
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        if "Errors" in content:
            return ";".join(content["Errors"])
    return json.dumps(content)


def _raise_for_transient_failure(result: dict) -> None:
    for batch_id, batch_response in result.items():
        responses = [batch_response]
        if batch_response["Status"] == HTTP_MULTI_STATUS:
            responses.extend(batch_response["Content"]["Items"])
        for response in responses:
            if response["Status"] in TRANSIENT_STATUS_CODES:
                raise TransientBatchRequestFailedException(
                    f"{batch_id}: {_extract_error(response)}"
                )


def _is_transient(err: Exception) -> bool:
    if isinstance(
        err, (Timeout, RequestsConnectionError, TransientBatchRequestFailedException)
    ):
        return True
    return (
        isinstance(err, HTTPError)
        and err.response is not None
        and err.response.status_code in TRANSIENT_STATUS_CODES
    )


def _retry_transient(request_properties: RequestProperties, fn: Callable[[], T]) -> T:
    """Call fn, retrying when PI Web API is rate limited, unavailable or times out.

    Search results are generated while paging through PI Web API,
    so the query retries of the source cannot retry them.
    """
    retries_left = request_properties.retry_count
    while True:
        try:
            return fn()
        except Exception as err:  # pylint: disable=broad-except
            if retries_left == 0 or not _is_transient(err):
                raise
            logger.info(
                "PI Web API request failed, %d retries left: %s", retries_left, err
            )
            retries_left = retries_left - 1
            time.sleep(request_properties.retry_delay)


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
                result = _retry_transient(
                    self._request_properties,
                    lambda: self._get_enumeration_values(
                        attribute["Links"]["EnumerationValues"]
                    ),
                )

                mapping = {}
                for item in result.get("Items", []):
                    mapping[item["Value"]] = item["Name"]
                self._lookup[dictionary_name] = Dictionary(mapping)

            metadata.set_field(fields.Dictionary, self._lookup.get(dictionary_name))

    def _get_enumeration_values(self, url: str) -> dict:
        response = self._session.get(
            url,
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
        return response.json()


class _MetadataAttributeLookup:
    """Look up the values of element attributes that provide metadata.

    The values are requested in a separate batch request.
    Value lookups can be slow or fail,
    which should not prevent the series of an element from being found.

    Attributes of ancestors require the ancestor elements,
    which are requested in a batch request before the values.
    Ancestors are shared by many elements and are kept for the whole search.
    """

    def __init__(
        self,
        metadata_attributes: dict[str, list[MetadataAttribute]],
        request_properties: RequestProperties,
        session,
        url: DatabaseURLBuilder,
    ):
        self._metadata_attributes = metadata_attributes
        self._request_properties = request_properties
        self._session = session
        self._url = url
        self._ancestors: dict[str, dict] = {}

    def lookup(self, elements: list[dict]) -> dict[str, dict[str, Any]]:
        """Return the metadata values of the given elements by element WebId."""
        if len(self._metadata_attributes) == 0 or len(elements) == 0:
            return {}

        unique_elements = {element["WebId"]: element for element in elements}
        self._find_ancestors(list(unique_elements.values()))

        batch_query = {}
        lookups: dict[str, str] = {}
        field_lookups = []
        for web_id, element in unique_elements.items():
            for field_name, metadata_attributes in self._metadata_attributes.items():
                paths = []
                for metadata_attribute in metadata_attributes:
                    try:
                        target = self._find_target(element, metadata_attribute)
                    except _AncestorUnavailableException:
                        break
                    if target is None:
                        continue
                    attribute_path = metadata_attribute.attribute
                    path = f"{target['Path']}|{attribute_path}"
                    if path not in lookups:
                        batch_id = str(len(lookups))
                        batch_query[batch_id] = self._value_request(
                            target["WebId"], attribute_path
                        )
                        lookups[path] = batch_id
                    paths.append(path)
                field_lookups.append((web_id, field_name, paths))

        if len(batch_query) == 0:
            return {}

        result = self._send_batch(batch_query)
        if result is None:
            return {}

        attribute_values = _read_attribute_values(lookups, result)
        values: dict[str, dict[str, Any]] = {}
        for web_id, field_name, paths in field_lookups:
            value = _select_attribute_value(paths, attribute_values)
            if value is not None:
                values.setdefault(web_id, {})[field_name] = value
        return values

    def _value_request(self, element_web_id: str, attribute_path: str) -> dict:
        params = {
            "nameFilter": attribute_path.rsplit("|", maxsplit=1)[-1],
            "searchFullHierarchy": "true" if "|" in attribute_path else "false",
            "selectedFields": ";".join(
                [
                    "Items.Path",
                    "Items.Value.Value",
                    "Items.Value.Good",
                ]
            ),
        }
        return {
            "Method": "GET",
            "Resource": add_query_params(
                self._url.root(["streamsets", element_web_id, "value"]),
                params,
            ),
        }

    def _find_target(
        self, element: dict, metadata_attribute: MetadataAttribute
    ) -> dict | None:
        """Return the element that has the metadata attribute of the given element.

        Returns None when the element has no such ancestor.
        Ancestors always exist, so an ancestor that is not known failed to load.
        """
        if not metadata_attribute.is_of_ancestor():
            return element
        for level, path in enumerate(_ancestor_paths(element["Path"]), start=1):
            if (
                metadata_attribute.level is not None
                and level != metadata_attribute.level
            ):
                continue
            ancestor = self._ancestors.get(path)
            if ancestor is None:
                raise _AncestorUnavailableException(path)
            if metadata_attribute.level is not None:
                return ancestor
            assert metadata_attribute.template_name is not None
            template_name = ancestor.get("TemplateName") or ""
            if template_name.casefold() == metadata_attribute.template_name.casefold():
                return ancestor
        return None

    def _find_ancestors(self, elements: list[dict]):
        """Request the ancestors of the elements that metadata attributes need."""
        paths = set()
        for element in elements:
            ancestor_paths = _ancestor_paths(element["Path"])
            for metadata_attribute in itertools.chain.from_iterable(
                self._metadata_attributes.values()
            ):
                if metadata_attribute.template_name is not None:
                    paths.update(ancestor_paths)
                elif (
                    metadata_attribute.level is not None
                    and metadata_attribute.level <= len(ancestor_paths)
                ):
                    paths.add(ancestor_paths[metadata_attribute.level - 1])

        unknown_paths = sorted(paths - self._ancestors.keys())
        if len(unknown_paths) == 0:
            return

        params = {
            "selectedFields": ";".join(["WebId", "Path", "TemplateName"]),
            "webIdType": self._request_properties.web_id_type,
        }
        batch_query = {
            str(i): {
                "Method": "GET",
                "Resource": add_query_params(
                    self._url.root("elements"), {**params, "path": path}
                ),
            }
            for i, path in enumerate(unknown_paths)
        }
        result = self._send_batch(batch_query)
        if result is None:
            return

        for i, path in enumerate(unknown_paths):
            batch_response = result.get(str(i))
            if batch_response is None:
                continue
            if batch_response["Status"] != HTTP_OK:
                logger.warning(
                    "Failed to look up element %s: %s",
                    path,
                    _extract_error(batch_response),
                )
                continue
            self._ancestors[path] = batch_response["Content"]

    def _send_batch(self, batch_query: dict) -> dict | None:
        try:
            response = self._session.post(
                self._url.root("batch"),
                timeout=self._request_properties.metadata_request_timeout_seconds,
                json=batch_query,
            )
            response.raise_for_status()
            return response.json()
        except (RequestException, ValueError) as err:
            logger.warning("Failed to look up metadata attribute values: %s", err)
            return None


class _AncestorUnavailableException(KukurException):
    """Raised when an ancestor of an element failed to load."""


def _read_attribute_values(lookups: dict[str, str], result: dict) -> dict[str, Any]:
    """Return the attribute values in a batch response by attribute path.

    Attributes that do not exist are left out.
    Attributes that failed to load have a None value,
    to prevent another metadata attribute from being used instead.
    """
    attribute_values: dict[str, Any] = {}
    for path, batch_id in lookups.items():
        batch_response = result.get(batch_id)
        if batch_response is None:
            attribute_values[path] = None
            continue
        if batch_response["Status"] != HTTP_OK:
            logger.warning(
                "Failed to look up metadata attribute %s: %s",
                path,
                _extract_error(batch_response),
            )
            attribute_values[path] = None
            continue
        for item in batch_response["Content"].get("Items", []):
            if item["Path"] == path:
                attribute_values[path] = _get_metadata_attribute_value(item["Value"])
    return attribute_values


def _select_attribute_value(paths: list[str], attribute_values: dict[str, Any]) -> Any:
    """Return the value of the first attribute that exists."""
    for path in paths:
        if path in attribute_values:
            return attribute_values[path]
    return None


def _ancestor_paths(element_path: str) -> list[str]:
    """Return the paths of the ancestors of the element at the given path, nearest first."""
    components = element_path.split("\\")
    return [
        "\\".join(components[:end])
        for end in range(len(components) - 1, ELEMENT_PATH_PREFIX_COMPONENTS, -1)
    ]


def _get_metadata_attribute_value(value: dict) -> Any | None:
    if not value.get("Good", True):
        return None
    attribute_value = value.get("Value")
    if isinstance(attribute_value, dict):
        if attribute_value.get("IsSystem", False):
            return None
        return attribute_value.get("Name")
    return attribute_value


def _set_metadata_attribute_values(
    metadata: Metadata, element: dict, values: dict[str, Any]
):
    """Set the metadata attribute values of an element on the metadata of a series.

    Invalid values are removed from the values of the element,
    to warn only once for all series of the element.
    """
    for field_name, value in list(values.items()):
        try:
            metadata.coerce_field(field_name, value)
        except ValueError:
            logger.warning(
                'Invalid value "%s" for metadata field "%s" of element %s',
                value,
                field_name,
                element["Name"],
            )
            del values[field_name]


def _read_data(
    session, request_properties: RequestProperties, data_request: DataRequest
):
    """Read AF data.

    This handles pagination.
    """
    timestamps = []
    values: list[float | int | str | None] = []
    quality_flags = []

    start_date = data_request.start_date

    while True:
        params: dict[str, str | int] = {
            "maxCount": str(request_properties.max_returned_items_per_call),
            "startTime": start_date.isoformat(),
            "endTime": data_request.end_date.isoformat(),
            "selectedFields": "Items.Value;Items.Timestamp;Items.Good",
        }
        if data_request.interval_count is not None:
            params.update({"intervals": data_request.interval_count})
        response = session.get(
            data_request.url,
            verify=request_properties.verify_ssl,
            timeout=request_properties.timeout_seconds,
            params=params,
        )
        if response.status_code == HTTP_NOT_FOUND:
            raise DataNotFoundException(f"Data not found for {data_request.url}")

        response.raise_for_status()

        data_points = response.json()["Items"]

        last_timestamp: datetime | None = None
        for data_point in data_points:
            timestamp = parse_timestamp(data_point["Timestamp"])
            last_timestamp = timestamp
            timestamps.append(timestamp)
            value = data_point["Value"]
            if isinstance(value, dict):
                if value.get("IsSystem", False):
                    values.append(None)
                    quality_flags.append(value["Value"])
                    continue
                values.append(value["Value"])
            else:
                values.append(value)

            if data_point["Good"]:
                quality_flags.append(Quality.GOOD.value)
            else:
                quality_flags.append(Quality.BAD.value)

        if (
            len(data_points) != request_properties.max_returned_items_per_call
            or last_timestamp is None
        ):
            break

        start_date = last_timestamp
        while len(timestamps) > 0 and timestamps[-1] == start_date:
            timestamps.pop()
            values.pop()
            quality_flags.pop()

    table = pa.Table.from_pydict(
        {
            "ts": timestamps,
            "value": values,
            "quality": pa.array(quality_flags, pa.int16()),
        }
    )
    return quality.set_mapping(table, quality.DEFAULT_MAPPING)


def add_query_params(url: str, params: dict) -> str:
    """Add additional query parameters to a URL."""
    parts = urllib.parse.urlsplit(url)
    query_params = urllib.parse.parse_qs(parts.query)
    query_params.update(params)
    parts = parts._replace(query=urllib.parse.urlencode(query_params, doseq=True))
    return urllib.parse.urlunsplit(parts)
