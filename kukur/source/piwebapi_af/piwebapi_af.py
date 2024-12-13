"""Kukur source for PI AF using PI Web API."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath
from typing import Dict, Generator

import pyarrow as pa
import urllib.parse

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.exceptions import InvalidSourceException, MissingModuleException

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
            self.__data_archive_uri,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.timeout_seconds,
            params=dict(selectedFields="Links.Points;Links.EnumerationSets"),
        )
        response.raise_for_status()

        data_archive = response.json()

        dictionary_lookup = _DictionaryLookup(
            session, self.__request_properties, data_archive
        )

        page = 0
        while True:
            response = session.get(
                data_archive["Links"]["Points"],
                verify=self.__request_properties.verify_ssl,
                timeout=self.__request_properties.timeout_seconds,
                params=dict(
                    maxCount=self.__request_properties.max_returned_items_per_call,
                    startIndex=page
                    * self.__request_properties.max_returned_items_per_call,
                ),
            )
            response.raise_for_status()
            points = response.json().get("Items", [])
            if len(points) == 0:
                break

            page = page + 1
            for point in points:
                metadata = _get_metadata(
                    SeriesSelector(selector.source, point["Name"]),
                    point,
                    dictionary_lookup,
                )
                if metadata is not None:
                    yield metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return metadata for one tag."""
        raise NotImplementedError()

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        session = self._get_session()
        data_url = self._get_data_url(session, selector)

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

    def _get_data_url(self, session, selector: SeriesSelector) -> str:
        database_uri = urllib.parse.urlparse(self.__database_uri)
        recorded_path= PurePath(database_uri.path).parent.parent / "streams" / selector.tags["__id__"] / "recorded"
        stream_uri = urllib.parse.urlunparse(database_uri._replace(path=str(recorded_path)))

        response = session.get(
            stream_uri,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.timeout_seconds,
            params=dict(selectedFields="Links.Points"),
        )
        response.raise_for_status()

        data_archive = response.json()
        response = session.get(
            data_archive["Links"]["Points"],
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.timeout_seconds,
            params=dict(
                maxCount=str(self.__request_properties.max_returned_items_per_call),
                nameFilter=selector.name,
                selectedFields="Items.Links.RecordedData",
            ),
        )
        response.raise_for_status()

        data_points = response.json()["Items"]
        if len(data_points) == 0:
            raise InvalidDataError("Series not found")

        return response.json()["Items"][0]["Links"]["RecordedData"]


def _get_metadata(
    selector: SeriesSelector, point: Dict, dictionary_lookup: _DictionaryLookup
) -> Optional[Metadata]:
    metadata = Metadata(SeriesSelector(selector.source, point["Name"]))
    metadata.set_field(fields.Description, point["Descriptor"])
    metadata.set_field(fields.Unit, point["EngineeringUnits"])

    if point["Step"]:
        metadata.set_field(fields.InterpolationType, InterpolationType.STEPPED)
    else:
        metadata.set_field(fields.InterpolationType, InterpolationType.LINEAR)

    metadata.set_field(fields.LimitLowFunctional, point["Zero"])
    metadata.set_field(fields.LimitHighFunctional, point["Zero"] + point["Span"])

    if len(point["DigitalSetName"]) > 0:
        dictionary_name = point["DigitalSetName"]
        metadata.set_field(fields.DictionaryName, dictionary_name)
        metadata.set_field(fields.Dictionary, dictionary_lookup.get(dictionary_name))

    point_type = point["PointType"]
    point_types = {
        "Digital": DataType.DICTIONARY,
        "Float16": DataType.FLOAT32,
        "Float32": DataType.FLOAT32,
        "Float64": DataType.FLOAT64,
        "Int16": DataType.FLOAT32,
        "Int32": DataType.FLOAT64,
        "String": DataType.STRING,
    }
    if point_type not in point_types:
        return None
    metadata.set_field(fields.DataType, point_types[point_type])
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
