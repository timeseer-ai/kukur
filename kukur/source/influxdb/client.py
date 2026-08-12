"""A minimal client for the InfluxDB v1 HTTP API."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

import json
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import Any

from kukur.exceptions import KukurException, MissingModuleException

try:
    from requests import RequestException, Response, Session

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

HTTP_OK = 200


class InvalidClientConnection(KukurException):
    """Raised when an error occured when making the connection."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"Connection error: {message}")


class InfluxDBClientError(KukurException):
    """Raised when InfluxDB returns an error response."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"InfluxDB error: {message}")


@dataclass
class ClientOptions:
    """Options for a connection to an InfluxDB database."""

    database: str
    host: str
    port: int
    ssl: bool
    verify_ssl: bool
    timeout_seconds: float
    username: str | None = None
    password: str | None = None


@dataclass
class QueryResult:
    """The series returned by an InfluxQL query."""

    series: list[dict[str, Any]] = field(default_factory=list)

    def get_points(
        self, measurement: str | None = None
    ) -> Generator[dict[str, Any], None, None]:
        """Return all points in the result.

        When a measurement is given, only the points of the series with that
        name are returned.
        """
        for one_series in self.series:
            if measurement is not None and one_series.get("name") != measurement:
                continue
            columns = one_series.get("columns", [])
            for values in one_series.get("values", []):
                yield dict(zip(columns, values, strict=True))


class InfluxDBClient:
    """A client for the InfluxDB v1 HTTP API.

    Only the read operations required by Kukur are supported.
    """

    def __init__(self, options: ClientOptions):
        if not HAS_REQUESTS:
            raise MissingModuleException("requests", "influxdb")
        scheme = "https" if options.ssl else "http"
        self._url = f"{scheme}://{options.host}:{options.port}/query"
        self._database = options.database
        self._timeout_seconds = options.timeout_seconds
        self._session = Session()
        self._session.verify = options.verify_ssl
        if options.username is not None and options.password is not None:
            self._session.auth = (options.username, options.password)

    def __enter__(self) -> "InfluxDBClient":
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.close()

    def close(self) -> None:
        """Close the connections to InfluxDB."""
        self._session.close()

    def query(
        self, query: str, bind_params: dict[str, Any] | None = None
    ) -> QueryResult:
        """Run an InfluxQL query.

        Any ``$name`` placeholder in the ``WHERE`` clause of the query is
        replaced by the value of ``name`` in ``bind_params``.
        """
        params = {"q": query, "db": self._database}
        if bind_params is not None:
            params["params"] = json.dumps(bind_params)

        try:
            response = self._session.get(
                self._url, params=params, timeout=self._timeout_seconds
            )
        except RequestException as err:
            raise InvalidClientConnection(str(err)) from err

        return _parse_response(response)

    def get_list_series(
        self,
        measurement: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> list[str]:
        """Return the series keys of all series in the database.

        Series can be limited to one measurement and to the series matching all
        given tags.
        """
        query = "SHOW SERIES"
        if measurement is not None:
            query = f"{query} FROM {_quote_identifier(measurement)}"
        if tags:
            conditions = " and ".join(
                f"{_quote_identifier(tag_key)} = {_quote_literal(tag_value)}"
                for tag_key, tag_value in tags.items()
            )
            query = f"{query} WHERE {conditions}"

        return [
            value
            for point in self.query(query).get_points()
            for value in point.values()
        ]


def _parse_response(response: "Response") -> QueryResult:
    try:
        body = response.json()
    except ValueError:
        body = {}

    if response.status_code != HTTP_OK:
        raise InfluxDBClientError(body.get("error", response.text))

    if "error" in body:
        raise InfluxDBClientError(body["error"])

    series = []
    for result in body.get("results", []):
        if "error" in result:
            raise InfluxDBClientError(result["error"])
        series.extend(result.get("series", []))

    return QueryResult(series)


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _quote_literal(literal: str) -> str:
    escaped = literal.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
