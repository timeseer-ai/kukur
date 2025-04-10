"""Kukur sink for PI Data Archives using PI Web API."""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
from dataclasses import dataclass
from typing import Dict

import pyarrow as pa

from kukur.exceptions import (
    InvalidDataError,
    InvalidSinkException,
    MissingModuleException,
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

from kukur.base import SeriesSelector

logger = logging.getLogger(__name__)
HTTP_OK = 200


@dataclass
class _RequestProperties:
    verify_ssl: bool
    timeout_seconds: float


class PIWebAPIDataArchiveSink:
    """Connect to PI Data Archives using the PI Web API."""

    def __init__(self, config: Dict):
        self.__request_properties = _RequestProperties(
            verify_ssl=config.get("verify_ssl", True),
            timeout_seconds=config.get("timeout_seconds", 60),
        )
        self.__data_archive_uri = config["data_archive_uri"]

        self.__basic_auth = None
        if "username" in config and "password" in config:
            self.__basic_auth = (config["username"], config["password"])

        if not self.__request_properties.verify_ssl:
            urllib3.disable_warnings()

    def write_data(self, selector: SeriesSelector, data: pa.Table):
        """Write data for the given time series."""
        session = self._get_session()
        data_url = self._write_data_url(session, selector)

        new_names = {"ts": "Timestamp", "value": "Value"}
        data = data.rename_columns(new_names)

        response = session.post(
            data_url,
            verify=self.__request_properties.verify_ssl,
            timeout=self.__request_properties.timeout_seconds,
            headers={
                "X-Requested-With": "Kukur",
                "Content-Type": "application/json",
            },
            json=data.to_pylist(),
        )
        response.raise_for_status()
        if response.content is not None and len(response.content) != 0:
            response = response.json()
            if len(response["Items"]) > 0:
                for item in response["Items"]:
                    if item["Substatus"] != HTTP_OK:
                        logger.error(item["Message"])

    def _get_session(self):
        session = Session()
        if self.__basic_auth is None and HAS_REQUESTS_KERBEROS:
            session.auth = HTTPKerberosAuth(
                mutual_authentication="REQUIRED", sanitize_mutual_error_response=False
            )
        elif self.__basic_auth is not None:
            session.auth = self.__basic_auth
        return session

    def _write_data_url(self, session, selector: SeriesSelector) -> str:
        response = session.get(
            self.__data_archive_uri,
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
                nameFilter=selector.name,
                selectedFields="Items.Links.RecordedData",
            ),
        )
        response.raise_for_status()
        data_points = response.json()["Items"]

        if len(data_points) == 0:
            raise InvalidDataError("Series not found")

        return response.json()["Items"][0]["Links"]["RecordedData"]


def from_config(config: Dict) -> PIWebAPIDataArchiveSink:
    """Create a new PIWebAPIDataArchiveSink."""
    if "data_archive_uri" not in config:
        raise InvalidSinkException(
            'piwebapi-da sink require a "data_archive_uri" entry'
        )
    if not HAS_REQUESTS:
        raise MissingModuleException("requests", "piwebapi-da")
    return PIWebAPIDataArchiveSink(config)
