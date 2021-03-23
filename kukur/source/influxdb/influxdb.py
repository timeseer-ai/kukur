"""Connections to InfluxDB data sources from Timeseer."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Any, Dict, List, Generator, Tuple

import dateutil.parser
import pyarrow as pa

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError

    HAS_INFLUX = True
except ImportError:
    HAS_INFLUX = False

from kukur import Metadata, SeriesSelector
from kukur.exceptions import InvalidDataError


class InfluxdbNotInstalledError(Exception):
    """Raised when the influxdb is not available."""

    def __init__(self):
        Exception.__init__(self, "the influxdb is not available. Install influxdb.")


class InvalidClientConnection(Exception):
    """Raised when an error occured when making the connection."""

    def __init__(self, message: str):
        Exception.__init__(self, f"Connection error: {message}")


def from_config(config: Dict[str, Any]):
    """Create a new Influx data source"""
    if not HAS_INFLUX:
        raise InfluxdbNotInstalledError()
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
            raise InfluxdbNotInstalledError()
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

    def search(self, selector: SeriesSelector) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        many_series = self.__client.get_list_series()
        fields = self.__client.query("SHOW FIELD KEYS")
        for series in many_series:
            series_name = series.replace("\\", "")
            measurement = series_name.split(",")[0]
            for field in fields.get_points(measurement=measurement):
                yield Metadata(
                    SeriesSelector(
                        selector.source, f'{series_name}::{field["fieldKey"]}'
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
        if selector.name is None:
            raise InvalidDataError("No series name")
        measurement, tags, field_key = _parse_influx_series(selector.name)

        query = f"""SELECT time, "{_escape(field_key)}"
                    FROM "{_escape(measurement)}"
                    WHERE time >= $start_date and time <= $end_date"""

        bind_params = {
            "start_date": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        for i, (tag_key, tag_value) in enumerate(tags):
            bind_params[str(i)] = tag_value
            query = query + f' and "{_escape(tag_key)}" = ${str(i)}'

        timestamps = []
        values = []
        for item in self.__client.query(
            query=query, bind_params=bind_params
        ).get_points():
            timestamps.append(dateutil.parser.parse(item["time"]))
            values.append(item[field_key])

        return pa.Table.from_pydict({"ts": timestamps, "value": values})


def _parse_influx_series(series: str) -> Tuple[str, List[List[str]], str]:
    field_split = series.rsplit("::", 1)
    field_key = field_split[1]

    measurement_split = field_split[0].split(",")
    measurement = measurement_split[0]
    tags = [tag.split("=") for tag in measurement_split[1:]]
    return measurement, tags, field_key


def _escape(context: str) -> str:
    if '"' in context:
        context = context.replace('"', "")
    if ";" in context:
        context = context.replace(";", "")
    return context
