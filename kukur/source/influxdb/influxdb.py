"""Connections to InfluxDB data sources from Timeseer."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Any, Generator, Tuple

import dateutil.parser
import pyarrow as pa

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError

    HAS_INFLUX = True
except ImportError:
    HAS_INFLUX = False

from kukur import ComplexSeriesSelector, Metadata
from kukur.exceptions import InvalidDataError, KukurException, MissingModuleException


class InvalidClientConnection(KukurException):
    """Raised when an error occured when making the connection."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"Connection error: {message}")


def from_config(config: dict[str, Any]):
    """Create a new Influx data source"""
    if not HAS_INFLUX:
        raise MissingModuleException("influxdb", "influxdb")
    host = config.get("host", "localhost")
    port = config.get("port", 8086)
    ssl = config.get("ssl", False)
    database = config["database"]
    username = config.get("username", "")
    password = config.get("password", "")
    return InfluxSource(host, port, ssl, database, username=username, password=password)


class InfluxSource:
    """An InfluxDB data source."""

    if HAS_INFLUX:
        __client: InfluxDBClient

    def __init__(
        self,
        host: str,
        port: int,
        ssl: bool,
        database: str,
        *,
        username: str,
        password: str,
    ):
        if not HAS_INFLUX:
            raise MissingModuleException("influxdb", "influxdb")
        try:
            if username != "" and password != "":
                self.__client = InfluxDBClient(
                    host=host, port=port, ssl=ssl, username=username, password=password
                )
            else:
                self.__client = InfluxDBClient(host=host, port=port, ssl=ssl)

            self.__client.switch_database(database)
        except InfluxDBClientError as err:
            raise InvalidClientConnection(err) from err

    def search(
        self, selector: ComplexSeriesSelector
    ) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        many_series = self.__client.get_list_series()
        fields = self.__client.query("SHOW FIELD KEYS")
        for series in many_series:
            measurement, tags = _parse_influx_series(series)
            for field in fields.get_points(measurement=measurement):
                yield Metadata(
                    ComplexSeriesSelector(
                        selector.source,
                        tags,
                        field["fieldKey"],
                    )
                )

    # pylint: disable=no-self-use
    def get_metadata(self, selector: ComplexSeriesSelector) -> Metadata:
        """Influx currently always returns empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: ComplexSeriesSelector, start_date: datetime, end_date: datetime
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

        for item in self.__client.query(
            query=query, bind_params=bind_params
        ).get_points():
            timestamps.append(dateutil.parser.parse(item["time"]))
            values.append(item[selector.field])

        return pa.Table.from_pydict({"ts": timestamps, "value": values})


def _parse_influx_series(series: str) -> Tuple[str, dict[str, str]]:
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


def _escape(context: str) -> str:
    if '"' in context:
        context = context.replace('"', "")
    if ";" in context:
        context = context.replace(";", "")
    return context
