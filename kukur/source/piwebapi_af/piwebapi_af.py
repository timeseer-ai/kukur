"""Kukur source for PI AF using PI Web API."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath
from typing import Dict, Generator, Optional

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


@dataclass
class _RequestProperties:
    verify_ssl: bool
    timeout_seconds: float
    max_returned_items_per_call: int


class PIWebAPIAssetFrameworkSource:
    """Connect to PI AF using the PI Web API."""

    def __init__(self, config: Dict):
        self.__request_properties = _RequestProperties(
            verify_ssl=config.get("verify_ssl", True),
            timeout_seconds=config.get("timeout_seconds", 60),
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

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all attributes in the Asset Framework."""
        session = self._get_session()

        response = session.get(
            self.__database_uri,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.timeout_seconds,
            params=dict(
                maxCount=self.__request_properties.max_returned_items_per_call,
            ),
        )
        response.raise_for_status()
        database = response.json()
        yield from self._get_elements(
            session, selector.source, database["Links"]["Elements"], {}
        )

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return metadata for one tag."""
        raise NotImplementedError()

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        session = self._get_session()
        data_url = self._get_data_url(selector)

        timestamps = []
        values = []
        quality_flags = []

        while True:
            response = session.get(
                data_url,
                verify=self.__request_properties.verify_ssl,
                timeout=self.__request_properties.timeout_seconds,
                params=dict(
                    maxCount=str(self.__request_properties.max_returned_items_per_call),
                    startTime=start_date.isoformat(),
                    endTime=end_date.isoformat(),
                    selectedFields="Items.Value;Items.Timestamp;Items.Good",
                ),
            )
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
        stream_uri = urllib.parse.urlunparse(
            database_uri._replace(path=str(recorded_path))
        )
        return stream_uri

    def _get_elements(
        self, session, source_name: str, uri: str, extra_metadata: Dict[str, str]
    ) -> Generator[Metadata, None, None]:
        response = session.get(
            uri,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.timeout_seconds,
            params=dict(
                maxCount=self.__request_properties.max_returned_items_per_call,
            ),
        )
        response.raise_for_status()
        elements = response.json()

        for element in elements["Items"]:
            attribute_response = session.get(
                element["Links"]["Attributes"],
                verify=self.__request_properties.verify_ssl,
                timeout=self.__request_properties.timeout_seconds,
                params=dict(
                    maxCount=self.__request_properties.max_returned_items_per_call,
                ),
            )
            attribute_response.raise_for_status()

            attributes = attribute_response.json()
            for attribute in attributes["Items"]:
                tags = {"series name": attribute["Name"], "__id__": attribute["WebId"]}

                metadata = _get_metadata(
                    SeriesSelector(source_name, tags),
                    element,
                    attribute,
                )
                if metadata is not None:
                    yield metadata

            if element.get("HasChildren", False):
                self._get_elements(
                    session, source_name, element["Links"]["Elements"], {}
                )


def _get_metadata(
    selector: SeriesSelector, asset: Dict, attribute: Dict
) -> Optional[Metadata]:
    metadata = Metadata(selector)
    metadata.set_field(fields.Description, attribute["Description"])
    metadata.set_field(fields.Unit, attribute["EngineeringUnits"])

    if attribute["Step"]:
        metadata.set_field(fields.InterpolationType, InterpolationType.STEPPED)
    else:
        metadata.set_field(fields.InterpolationType, InterpolationType.LINEAR)

    metadata.set_field(fields.LimitLowFunctional, attribute["Zero"])
    metadata.set_field(
        fields.LimitHighFunctional, attribute["Zero"] + attribute["Span"]
    )

    attribute_type = attribute["Type"]
    attribute_types = {
        "Digital": DataType.DICTIONARY,
        "Float16": DataType.FLOAT32,
        "Float32": DataType.FLOAT32,
        "Float64": DataType.FLOAT64,
        "Double": DataType.FLOAT64,
        "Int16": DataType.FLOAT32,
        "Int32": DataType.FLOAT64,
        "String": DataType.STRING,
    }
    if attribute_type not in attribute_types:
        return None
    metadata.set_field(fields.DataType, attribute_types[attribute_type])

    metadata.set_field_by_name("Path", attribute["Path"])
    metadata.set_field_by_name(asset["TemplateName"], asset["Name"])
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
