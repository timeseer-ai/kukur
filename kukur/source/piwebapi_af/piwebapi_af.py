"""Kukur source for PI AF using PI Web API."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath
from typing import Any, Dict, Generator, List, Optional, Tuple

import pyarrow as pa

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.base import DataType, InterpolationType
from kukur.exceptions import (
    InvalidSourceException,
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

NOT_FOUND = 404


@dataclass
class _RequestProperties:
    verify_ssl: bool
    timeout_seconds: float
    metadata_request_timeout_seconds: float
    max_returned_items_per_call: int


@dataclass
class Element:
    """A PI AF Element."""

    path: str
    name: str
    description: str
    template: Optional[str]
    metadata: Dict[str, str]
    paths: List[str]


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
        )
        self.__database_uri = config["database_uri"]

        self.__basic_auth = None
        if "username" in config and "password" in config:
            self.__basic_auth = (config["username"], config["password"])

        if not self.__request_properties.verify_ssl:
            urllib3.disable_warnings()

        self.use_table_lookup = config.get("use_table_lookup", False)

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all attributes in the Asset Framework."""
        session = self._get_session()

        response = session.get(
            self.__database_uri,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.metadata_request_timeout_seconds,
            params=dict(
                maxCount=self.__request_properties.max_returned_items_per_call,
            ),
        )
        response.raise_for_status()
        database = response.json()

        all_elements = self._get_all_elements(session, database["Links"]["Elements"])
        logger.info("Found %s elements", len(all_elements))
        self._add_extra_templates(all_elements)
        data_attributes, metadata_attributes = self._get_all_attributes(session)
        logger.info(
            "Found %s data attributes, %s metadata attributes",
            len(data_attributes),
            len(metadata_attributes),
        )
        self._assign_metadata_to_elements(session, metadata_attributes, all_elements)
        yield from self._build_metadata(selector.source, data_attributes, all_elements)

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
            if response.status_code == NOT_FOUND:
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
        database_uri = urllib.parse.urlparse(self.__database_uri)
        recorded_path = (
            PurePath(database_uri.path).parent.parent
            / "streams"
            / selector.tags["__id__"]
            / "recorded"
        )
        return urllib.parse.urlunparse(database_uri._replace(path=str(recorded_path)))

    def _get_plot_url(self, selector: SeriesSelector) -> str:
        database_uri = urllib.parse.urlparse(self.__database_uri)
        recorded_path = (
            PurePath(database_uri.path).parent.parent
            / "streams"
            / selector.tags["__id__"]
            / "plot"
        )
        return urllib.parse.urlunparse(database_uri._replace(path=str(recorded_path)))

    def _get_all_elements(self, session, uri: str) -> List[Element]:
        all_elements: List[Element] = []

        next_uri: Optional[str] = uri
        while next_uri is not None:
            response = session.get(
                next_uri,
                verify=self.__request_properties.verify_ssl,
                timeout=self.__request_properties.metadata_request_timeout_seconds,
                params={
                    "searchFullHierarchy": "true",
                    "associations": "paths",
                    "maxCount": self.__request_properties.max_returned_items_per_call,
                },
            )
            response.raise_for_status()

            elements = response.json()
            next_uri = elements["Links"].get("Next")

            for element_data in elements["Items"]:
                template_name: Optional[str] = None
                element_metadata = {}
                if (
                    "TemplateName" in element_data
                    and element_data["TemplateName"] != ""
                ):
                    template_name = element_data["TemplateName"]
                    element_metadata[element_data["TemplateName"]] = element_data[
                        "Name"
                    ]
                all_elements.append(
                    Element(
                        element_data["Path"],
                        element_data["Name"],
                        element_data["Description"],
                        template_name,
                        element_metadata,
                        element_data["Paths"],
                    )
                )

        return all_elements

    def _add_extra_templates(self, elements: List[Element]):
        element_lookup: Dict[str, Element] = {
            element.path: element
            for element in elements
            if element.template is not None
        }
        for element in elements:
            extra_metadata = self._get_parents_template(element.path, element_lookup)
            element.metadata.update(extra_metadata)

    def _get_parents_template(
        self, path: str, element_lookup: Dict[str, Element]
    ) -> dict[str, str]:
        extra_metadata = {}
        for parent_path in _get_parent_paths(path)[1:]:
            if parent_path in element_lookup:
                parent_element = element_lookup[parent_path]
                if parent_element.template is not None:
                    extra_metadata[parent_element.template] = parent_element.name
        return extra_metadata

    def _get_all_attributes(self, session) -> Tuple[List[Dict], List[Dict]]:
        data_attributes: List[Dict] = []
        metadata_attributes: List[Dict] = []

        next_uri: Optional[str] = self._get_element_attributes_url()
        while next_uri is not None:
            attributes_response = session.get(
                next_uri,
                verify=self.__request_properties.verify_ssl,
                timeout=self.__request_properties.metadata_request_timeout_seconds,
                params={
                    "searchFullHierarchy": "true",
                    "maxCount": self.__request_properties.max_returned_items_per_call,
                },
            )
            attributes_response.raise_for_status()
            attributes_data = attributes_response.json()

            next_uri = attributes_data["Links"].get("Next")
            attributes = attributes_data["Items"]

            new_data_attributes, new_metadata_attributes = self._classify_attributes(
                attributes
            )
            data_attributes.extend(new_data_attributes)
            metadata_attributes.extend(new_metadata_attributes)

        return data_attributes, metadata_attributes

    def _get_element_attributes_url(self) -> str:
        database_uri = urllib.parse.urlparse(self.__database_uri)
        attributes_path = PurePath(database_uri.path) / "elementattributes"
        return urllib.parse.urlunparse(database_uri._replace(path=str(attributes_path)))

    def _assign_metadata_to_elements(
        self, session, metadata_attributes: list[Dict], elements: List[Element]
    ):
        element_lookup = {element.path: element for element in elements}
        for attribute in sorted(
            metadata_attributes, key=lambda attribute: len(attribute["Path"])
        ):
            attribute_value = self._get_attribute_value(session, attribute)
            if attribute_value is None:
                continue
            element_path = attribute["Path"].split("|", 1)[0]
            if element_path in element_lookup:
                element_lookup[element_path].metadata[
                    attribute["Name"]
                ] = attribute_value

    def _get_attribute_value(self, session, attribute: Dict) -> Optional[Any]:
        try:
            attribute_value_response = session.get(
                attribute["Links"]["Value"],
                verify=self.__request_properties.verify_ssl,
                timeout=self.__request_properties.metadata_request_timeout_seconds,
            )
            attribute_value_response.raise_for_status()
        except Exception as exc:
            logger.warning("Failed to get value for attribute %s", attribute["Path"])
            logger.error(exc)
            return None

        return attribute_value_response.json()["Value"]

    def _build_metadata(
        self, source_name: str, data_attributes: List[Dict], elements: List[Element]
    ) -> Generator[Metadata, None, None]:
        element_lookup = {element.path: element for element in elements}
        for attribute in data_attributes:
            # Find matching element
            element_path = attribute["Path"].split("|")[0]
            element = element_lookup[element_path]

            element_metadata = element.metadata.copy()
            element_metadata["Paths"] = ";".join(element.paths)
            for parent_path in _get_parent_paths(element_path):
                if parent_path in element_lookup:
                    parent_element = element_lookup[parent_path]
                    for k, v in parent_element.metadata.items():
                        if k not in element_metadata:
                            element_metadata[k] = v

            tags = {
                "series name": element.name,
                "__id__": attribute["WebId"],
            }

            if "DataReferencePlugIn" in attribute:
                metadata = _get_metadata(
                    SeriesSelector(source_name, tags, attribute["Name"]),
                    attribute,
                    element_metadata,
                )
                if metadata is not None:
                    if metadata.get_field(fields.Description) == "":
                        metadata.set_field(fields.Description, element.description)
                    yield metadata

    def _classify_attributes(
        self, attributes: List[Dict]
    ) -> Tuple[List[Dict], List[Dict]]:
        data_attributes = []
        metadata_attributes = []
        data_reference_plugins = ["PI Point", "Formula"]
        if self.use_table_lookup:
            data_reference_plugins.append("Table Lookup")
        for attribute in attributes:
            if attribute.get("DataReferencePlugIn", "") in data_reference_plugins:
                data_attributes.append(attribute)
            elif attribute.get("DataReferencePlugIn", "") == "":
                metadata_attributes.append(attribute)

        return data_attributes, metadata_attributes


def _get_parent_paths(path: str) -> list[str]:
    paths = []

    parent_parts = path.split("\\")
    for i in range(0, len(parent_parts)):
        parts = parent_parts[0 : len(parent_parts) - i]
        parent_path = "\\".join(parts)
        paths.append(parent_path)

    return paths


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

    metadata.set_field_by_name("Path", attribute["Path"])
    for k, v in extra_metadata.items():
        metadata.set_field_by_name(k, v)

    return metadata


def from_config(config: Dict) -> PIWebAPIAssetFrameworkSource:
    """Create a new PIWebAPIAssetFrameworkSource."""
    if "database_uri" not in config:
        raise InvalidSourceException(
            'piwebapi-af sources require a "database_uri" entry'
        )
    if not HAS_REQUESTS:
        raise MissingModuleException("requests", "piwebapi-af")
    return PIWebAPIAssetFrameworkSource(config)
