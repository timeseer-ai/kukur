"""Kukur sink for PI Asset Framework using PI Web API."""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
import urllib.parse
from dataclasses import dataclass
from pathlib import PurePath
from typing import Dict

import pyarrow as pa

from kukur.exceptions import (
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
class RequestProperties:
    """Request properties for PI Web API connection."""

    verify_ssl: bool
    timeout_seconds: float


class PIWebAPIAssetFrameworkSink:
    """Connect to PI AF using the PI Web API."""

    def __init__(self, config: Dict):
        self.__request_properties = RequestProperties(
            verify_ssl=config.get("verify_ssl", True),
            timeout_seconds=config.get("timeout_seconds", 60),
        )
        self.__database_uri = config["database_uri"]

        self.__basic_auth = None
        if "username" in config and "password" in config:
            self.__basic_auth = (config["username"], config["password"])

        if not self.__request_properties.verify_ssl:
            urllib3.disable_warnings()

    def write_data(self, selector: SeriesSelector, data: pa.Table):
        """Write data for the given time series."""
        data_url = self._write_data_url(selector)

        new_names = {"ts": "Timestamp", "value": "Value"}
        data = data.rename_columns(new_names)

        response = self._get_session().post(
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

    def _get_session(self):
        session = Session()
        if self.__basic_auth is None and HAS_REQUESTS_KERBEROS:
            session.auth = HTTPKerberosAuth(
                mutual_authentication="REQUIRED", sanitize_mutual_error_response=False
            )
        elif self.__basic_auth is not None:
            session.auth = self.__basic_auth
        return session

    def _write_data_url(self, selector: SeriesSelector) -> str:
        database_uri = urllib.parse.urlparse(self.__database_uri)
        recorded_path = (
            PurePath(database_uri.path).parent.parent
            / "streams"
            / selector.tags["__id__"]
            / "recorded"
        )
        return urllib.parse.urlunparse(database_uri._replace(path=str(recorded_path)))


def from_config(config: Dict) -> PIWebAPIAssetFrameworkSink:
    """Create a new PIWebAPIAssetFrameworkSource."""
    if "database_uri" not in config:
        raise InvalidSinkException('piwebapi-af sinks require a "database_uri" entry')
    if not HAS_REQUESTS:
        raise MissingModuleException("requests", "piwebapi-af")
    return PIWebAPIAssetFrameworkSink(config)
