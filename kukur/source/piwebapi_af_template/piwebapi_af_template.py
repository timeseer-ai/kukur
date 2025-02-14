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
from kukur.base import DataType, InterpolationType
from kukur.exceptions import (
    InvalidSourceException,
    KukurException,
    MissingModuleException,
)
from kukur.metadata import fields

try:
    import urllib3
    from requests import Session

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from dateutil.parser import isoparse as parse_date

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


@dataclass
class _RequestProperties:
    verify_ssl: bool
    timeout_seconds: float
    metadata_request_timeout_seconds: float
    max_returned_items_per_call: int
    max_returned_metadata_items_per_call: int


@dataclass
class AFTemplateSourceConfiguration:
    """Configuration to find PI AF attributes of a template."""

    web_api_uri: str
    root_uri: str
    element_template: str
    element_category: Optional[str]
    attribute_names: Optional[List[str]]
    attribute_category: Optional[str]

    @classmethod
    def from_data(cls, config: Dict) -> "AFTemplateSourceConfiguration":
        """Create an object from a configuration dict."""
        return cls(
            config["web_api_uri"],
            config["root_uri"],
            config["element_template"],
            config.get("element_category"),
            config.get("attribute_names"),
            config.get("attribute_category"),
        )


class PIWebAPIAssetFrameworkSource:
    """Connect to PI AF using the PI Web API."""

    def __init__(self, config: Dict):
        self.__request_properties = _RequestProperties(
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

        if not self.__request_properties.verify_ssl:
            urllib3.disable_warnings()

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all attributes in the Asset Framework."""
        if self.__config.element_template.strip() == "":
            raise InvalidSourceException("element template required")

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
                    urllib.parse.urljoin(self.__config.root_uri, "elements"),
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
            urllib.parse.urljoin(self.__config.web_api_uri, "batch"),
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
            for attribute in attributes["Content"].get("Items"):
                if self.__config.attribute_names is not None:
                    attribute_path = attribute["Path"].split("|", maxsplit=1)[1]
                    if attribute_path not in self.__config.attribute_names:
                        continue

                tags = {
                    "series name": element["Name"],
                    "__id__": attribute["WebId"],
                }

                if "DataReferencePlugIn" in attribute:
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
        return self._read_data(data_url, start_date, end_date, None)

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ):
        """Return plot data for the given time series in the given time period."""
        plot_url = self._get_plot_url(selector)
        return self._read_data(
            plot_url, start_date, end_date, {"intervals": interval_count}
        )

    def _read_data(
        self,
        data_url: str,
        start_date: datetime,
        end_date: datetime,
        extra_params: Optional[Dict],
    ):
        session = self._get_session()
        timestamps = []
        values = []
        quality_flags = []

        while True:
            params = {
                "maxCount": str(self.__request_properties.max_returned_items_per_call),
                "startTime": start_date.isoformat(),
                "endTime": end_date.isoformat(),
                "selectedFields": "Items.Value;Items.Timestamp;Items.Good",
            }
            if extra_params is not None:
                params.update(extra_params)
            response = session.get(
                data_url,
                verify=self.__request_properties.verify_ssl,
                timeout=self.__request_properties.timeout_seconds,
                params=params,
            )
            if response.status_code == HTTP_NOT_FOUND:
                logger.warning("No data found for %s", data_url)
                return pa.Table.from_pydict({"ts": [], "value": [], "quality": []})

            response.raise_for_status()

            data_points = response.json()["Items"]

            for data_point in data_points:
                timestamp = parse_date(data_point["Timestamp"])
                value = data_point["Value"]
                if isinstance(value, dict):
                    if value.get("IsSystem", False):
                        continue
                    values.append(value["Value"])
                else:
                    values.append(value)
                timestamps.append(timestamp)
                if data_point["Good"]:
                    quality_flags.append(1)
                else:
                    quality_flags.append(0)

            if (
                len(data_points)
                != self.__request_properties.max_returned_items_per_call
            ):
                break

            start_date = timestamps[-1]
            while timestamps[-1] == start_date:
                timestamps.pop()
                values.pop()
                quality_flags.pop()

        return pa.Table.from_pydict(
            {"ts": timestamps, "value": values, "quality": quality_flags}
        )

    def _get_session(self):
        session = Session()
        if self.__basic_auth is None and HAS_REQUESTS_KERBEROS:
            session.auth = HTTPKerberosAuth(
                mutual_authentication="REQUIRED", sanitize_mutual_error_response=False
            )
        elif self.__basic_auth is not None:
            session.auth = self.__basic_auth
        return session

    def _get_data_url(self, selector: SeriesSelector) -> str:
        web_api_uri = urllib.parse.urlparse(self.__config.web_api_uri)
        recorded_path = (
            PurePath(web_api_uri.path)
            / "streams"
            / selector.tags["__id__"]
            / "recorded"
        )
        return urllib.parse.urlunparse(web_api_uri._replace(path=str(recorded_path)))

    def _get_plot_url(self, selector: SeriesSelector) -> str:
        web_api_uri = urllib.parse.urlparse(self.__config.web_api_uri)
        recorded_path = (
            PurePath(web_api_uri.path) / "streams" / selector.tags["__id__"] / "plot"
        )
        return urllib.parse.urlunparse(web_api_uri._replace(path=str(recorded_path)))


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


def from_config(config: Dict) -> PIWebAPIAssetFrameworkSource:
    """Create a new PIWebAPIAssetFrameworkSource."""
    if "web_api_uri" not in config:
        raise InvalidSourceException(
            'piwebapi-af sources require a "web_api_uri" entry'
        )
    if not HAS_REQUESTS:
        raise MissingModuleException("requests", "piwebapi-af")
    return PIWebAPIAssetFrameworkSource(config)


def add_query_params(url: str, params: Dict) -> str:
    """Add additional query parameters to a URL."""
    parts = urllib.parse.urlsplit(url)
    query_params = urllib.parse.parse_qs(parts.query)
    query_params.update(params)
    parts = parts._replace(query=urllib.parse.urlencode(query_params, doseq=True))
    return urllib.parse.urlunsplit(parts)
