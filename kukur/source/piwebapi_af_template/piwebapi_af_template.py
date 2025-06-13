"""Kukur source for a PI AF Template using PI Web API."""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath
from typing import Dict, Generator, List, Optional

import pyarrow as pa

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.auth import get_oidc_token
from kukur.base import DataType, InterpolationType
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

try:
    from requests_kerberos import HTTPKerberosAuth

    HAS_REQUESTS_KERBEROS = True
except ImportError:
    HAS_REQUESTS_KERBEROS = False


logger = logging.getLogger(__name__)

HTTP_OK = 200
HTTP_NOT_FOUND = 404


class MetadataSearchFailedException(KukurException):
    """Raised when the metadata search failed."""


class ElementTemplateQueryFailedException(KukurException):
    """Raised when element template query failed."""


class ElementInOtherDatabaseException(KukurException):
    """Raised when an element is in another AF database."""


@dataclass
class AFTemplateSourceConfiguration:
    """Configuration to find PI AF attributes of a template."""

    database_uri: str
    root_id: Optional[str]
    element_template: Optional[str]
    element_category: Optional[str]
    attribute_names: Optional[List[str]]
    attribute_category: Optional[str]
    allowed_data_references: List[str]

    @classmethod
    def from_data(cls, config: Dict) -> "AFTemplateSourceConfiguration":
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
    categories: List[str]


@dataclass
class ElementTemplate:
    """One element template in a PI AF structure."""

    name: str
    description: str
    attribute_templates: List[AttributeTemplate]


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


class PIWebAPIAssetFrameworkTemplateSource:
    """Connect to PI AF using the PI Web API."""

    def __init__(self, config: Dict):
        self.__request_properties = RequestProperties(
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

        self.__basic_auth = None
        if "username" in config and "password" in config:
            self.__basic_auth = (config["username"], config["password"])

        self.__bearer_token = None
        if (
            (client_id := config.get("client_id"))
            and (client_secret := config.get("client_secret"))
            and (oidc_token_url := config.get("oidc_token_url"))
        ):
            self.__bearer_token = get_oidc_token(
                client_id, client_secret, oidc_token_url
            )

        if not self.__request_properties.verify_ssl:
            urllib3.disable_warnings()

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all attributes in the Asset Framework."""
        if (
            self.__config.element_template is None
            or self.__config.element_template.strip() == ""
        ):
            logger.info("Cannot search in element template source without template")
            return

        session = self._get_session()

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
            "maxCount": self.__request_properties.max_returned_items_per_call,
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
                    "Items.DefaultUnitsNameAbbreviation",
                    "Items.Step",
                    "Items.Span",
                    "Items.Zero",
                ]
            ),
            "maxCount": self.__request_properties.max_returned_items_per_call,
        }
        if self.__config.attribute_category is not None:
            attribute_params["categoryName"] = self.__config.attribute_category
        batch_query = {
            "GetElements": {
                "Method": "GET",
                "Resource": add_query_params(
                    self._get_element_search_url(session),
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

        response = session.post(
            self._get_batch_url(),
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.metadata_request_timeout_seconds,
            headers={"X-Requested-With": "Kukur"},
            json=batch_query,
        )
        response.raise_for_status()
        result = response.json()

        if result["GetElements"]["Status"] != HTTP_OK:
            raise MetadataSearchFailedException(
                ";".join(
                    result["GetElements"]["Content"].get("Errors", ["unknown error"])
                )
            )

        for i, element in enumerate(result["GetElements"]["Content"].get("Items")):
            element_metadata = {self.__config.element_template: element["Name"]}
            if len(element["CategoryNames"]) > 0:
                element_metadata["Element category"] = ";".join(
                    element["CategoryNames"]
                )

            attributes = result["GetAttributes"]["Content"]["Items"][i]
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
                        yield metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return metadata for one tag."""
        raise NotImplementedError()

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        data_url = self._get_data_url(selector)
        return read_data(
            self._get_session(),
            self.__request_properties,
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
            self._get_session(),
            self.__request_properties,
            DataRequest(plot_url, start_date, end_date, interval_count),
        )

    def list_elements(self, element_id: Optional[str]) -> List[Element]:
        """Return all direct child elements."""
        session = self._get_session()
        if element_id is None:
            url = f"{self.__config.database_uri}/elements"
        else:
            self._verify_element_in_database(
                session, f"{self._get_elements_url()}/{element_id}"
            )

            url = f"{self._get_elements_url()}/{element_id}/elements"
        response = session.get(
            url,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.metadata_request_timeout_seconds,
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

    def list_element_templates(self) -> List[ElementTemplate]:
        """Return all element templates in the database."""
        session = self._get_session()

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

        response = session.post(
            self._get_batch_url(),
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.metadata_request_timeout_seconds,
            headers={"X-Requested-With": "Kukur"},
            json=batch_query,
        )
        response.raise_for_status()
        result = response.json()

        if result["GetElementTemplates"]["Status"] != HTTP_OK:
            raise ElementTemplateQueryFailedException(
                ";".join(
                    result["GetElementTemplates"]["Content"].get(
                        "Errors", ["unknown error"]
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

    def list_element_categories(self) -> List[ElementCategory]:
        """Return all element categories in the database."""
        session = self._get_session()
        url = f"{self.__config.database_uri}/elementcategories"

        response = session.get(
            url,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.metadata_request_timeout_seconds,
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

    def list_attribute_categories(self) -> List[AttributeCategory]:
        """Return all attribute categories in the database."""
        session = self._get_session()
        url = f"{self.__config.database_uri}/attributecategories"

        response = session.get(
            url,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.metadata_request_timeout_seconds,
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

    def _get_session(self):
        session = Session()
        if self.__bearer_token is not None:
            session.headers.update({"Authorization": f"Bearer {self.__bearer_token}"})
        elif self.__basic_auth is None and HAS_REQUESTS_KERBEROS:
            session.auth = HTTPKerberosAuth(
                mutual_authentication="REQUIRED", sanitize_mutual_error_response=False
            )
        elif self.__basic_auth is not None:
            session.auth = self.__basic_auth
        return session

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

    def _get_element_search_url(self, session) -> str:
        elements_uri = self._get_database_elements_url()
        if self.__config.root_id is not None:
            element_id = self.__config.root_id
            self._verify_element_in_database(
                session, f"{self._get_elements_url()}/{element_id}"
            )

            elements_uri = f"{self._get_elements_url()}/{element_id}/elements"
        return elements_uri

    def _verify_element_in_database(self, session, url: str):
        """Raise an exception when an element is not in the configured database."""
        response = session.get(
            url,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.metadata_request_timeout_seconds,
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


def _get_metadata(
    selector: SeriesSelector, attribute: Dict, extra_metadata: Dict
) -> Optional[Metadata]:
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
    }
    if attribute_type not in attribute_types:
        return None
    metadata.set_field(fields.DataType, attribute_types[attribute_type])

    if len(attribute["CategoryNames"]) > 0:
        metadata.set_field_by_name(
            "Attribute category", ";".join(attribute["CategoryNames"])
        )

    metadata.set_field_by_name("Path", attribute["Path"])
    for k, v in extra_metadata.items():
        metadata.set_field_by_name(k, v)
    return metadata


def from_config(config: Dict) -> PIWebAPIAssetFrameworkTemplateSource:
    """Create a new PIWebAPIAssetFrameworkSource."""
    if "database_uri" not in config:
        raise InvalidSourceException(
            'piwebapi-af-template sources require a "database_uri" entry'
        )
    if not HAS_REQUESTS:
        raise MissingModuleException("requests", "piwebapi-af")
    return PIWebAPIAssetFrameworkTemplateSource(config)


def add_query_params(url: str, params: Dict) -> str:
    """Add additional query parameters to a URL."""
    parts = urllib.parse.urlsplit(url)
    query_params = urllib.parse.parse_qs(parts.query)
    query_params.update(params)
    parts = parts._replace(query=urllib.parse.urlencode(query_params, doseq=True))
    return urllib.parse.urlunsplit(parts)
