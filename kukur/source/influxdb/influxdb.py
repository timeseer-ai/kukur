"""Connections to InfluxDB data sources from Timeseer."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

import json
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import dateutil.parser
import pyarrow as pa

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError

    HAS_INFLUX = True
except ImportError:
    HAS_INFLUX = False

from kukur import Metadata, SeriesSearch, SeriesSelector, SourceStructure
from kukur.exceptions import InvalidDataError, KukurException, MissingModuleException


class InvalidClientConnection(KukurException):
    """Raised when an error occured when making the connection."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"Connection error: {message}")


@dataclass
class InfluxConfiguration:
    """Configuration for an Influx source."""

    host: str
    port: int
    ssl: bool
    database: str
    username: str | None
    password: str | None

    @classmethod
    def from_data(cls, data: dict) -> "InfluxConfiguration":
        """Create from a data dictionary."""
        return cls(
            host=data.get("host", "localhost"),
            port=data.get("port", 8086),
            ssl=data.get("ssl", False),
            database=data["database"],
            username=data.get("username"),
            password=data.get("password"),
        )


def from_config(config: dict[str, Any]):
    """Create a new Influx data source."""
    if not HAS_INFLUX:
        raise MissingModuleException("influxdb", "influxdb")
    return InfluxSource(InfluxConfiguration.from_data(config))


class InfluxSource:
    """An InfluxDB data source."""

    def __init__(self, config: InfluxConfiguration):
        if not HAS_INFLUX:
            raise MissingModuleException("influxdb", "influxdb")
        self._config = config

    def _get_client(self):
        if not HAS_INFLUX:
            raise MissingModuleException("influxdb", "influxdb")
        try:
            if self._config.username is not None and self._config.password is not None:
                client = InfluxDBClient(
                    host=self._config.host,
                    port=self._config.port,
                    ssl=self._config.ssl,
                    username=self._config.username,
                    password=self._config.password,
                )
            else:
                client = InfluxDBClient(
                    host=self._config.host, port=self._config.port, ssl=self._config.ssl
                )

            client.switch_database(self._config.database)
            return client
        except InfluxDBClientError as err:
            raise InvalidClientConnection(err) from err

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        measurement = None
        if "series name" in selector.tags:
            measurement = selector.tags["series name"]
            del selector.tags["series name"]
        with self._get_client() as client:
            many_series = client.get_list_series(
                measurement=measurement, tags=selector.tags
            )
            fields = client.query("SHOW FIELD KEYS")
        for series in many_series:
            measurement, tags = _parse_influx_series(series)
            for field in fields.get_points(measurement=measurement):
                if selector.field in ["value", field["fieldKey"]]:
                    yield Metadata(
                        SeriesSelector.from_tags(
                            selector.source,
                            tags,
                            field["fieldKey"],
                        )
                    )

    # pylint: disable=no-self-use
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Influx currently always returns empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        if "series name" not in selector.tags:
            raise InvalidDataError("No series name")

        query = f"""SELECT time, "{_escape(selector.field)}"
                    FROM "{_escape(selector.tags["series name"])}"
                    WHERE time >= $start_date and time <= $end_date"""

        bind_params = {
            "start_date": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for i, (tag_key, tag_value) in enumerate(selector.tags.items()):
            if tag_key == "series name":
                continue
            bind_params[str(i)] = tag_value
            query = query + f' and "{_escape(tag_key)}" = ${str(i)}'

        timestamps = []
        values = []

        with self._get_client() as client:
            for item in client.query(query=query, bind_params=bind_params).get_points():
                timestamps.append(dateutil.parser.parse(item["time"]))
                values.append(item[selector.field])

        return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def get_source_structure(self, _: SeriesSelector) -> SourceStructure | None:
        """Return the available tag keys, tag value and tag fields."""
        with self._get_client() as client:
            query_tag_keys = "SHOW TAG KEYS"
            tag_keys = []
            for results in client.query(query=query_tag_keys).get_points():
                tag_keys.extend(list(results.values()))
            tag_keys = list(set(tag_keys))

            query_fields = "SHOW FIELD KEYS"
            fields = []
            for results in client.query(query=query_fields).get_points():
                for key, value in results.items():
                    if key == "fieldKey":
                        fields.append(value)

            tag_key_placeholder = json.dumps(tag_keys)
            query_tag_values = f"SHOW TAG VALUES WITH KEY IN {tag_key_placeholder.replace('[', '(').replace(']', ')')}"
            tag_values = []
            for result in client.query(query=query_tag_values).get_points():
                if result not in tag_values:
                    tag_values.append(result)
            return SourceStructure(fields, tag_keys, tag_values)


def _parse_influx_series(series: str) -> tuple[str, dict[str, str]]:
    series_name = series.replace("\\", "")
    measurement = series_name.split(",")[0]
    tags = {}
    for tag in series_name.split(","):
        split_tag = tag.split("=")
        if len(split_tag) == 1:
            tags["series name"] = split_tag[0]
        else:
            tags[split_tag[0]] = split_tag[1]
    return measurement, tags


def _escape(context: str | None) -> str:
    if context is None:
        context = "value"
    if '"' in context:
        context = context.replace('"', "")
    if ";" in context:
        context = context.replace(";", "")
    return context
