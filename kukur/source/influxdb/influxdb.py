"""Connections to InfluxDB data sources from Timeseer."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from datetime import datetime
from typing import Any

import dateutil.parser
import pyarrow as pa

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.exceptions import InvalidDataError, MissingModuleException
from kukur.source.influxdb.client import HAS_REQUESTS, ClientOptions, InfluxDBClient


def from_config(config: dict[str, Any]):
    """Create a new Influx data source."""
    if not HAS_REQUESTS:
        raise MissingModuleException("requests", "influxdb")
    return InfluxSource(_get_client_options(config))


def _get_client_options(config: dict[str, Any]) -> ClientOptions:
    return ClientOptions(
        database=config["database"],
        host=config.get("host", "localhost"),
        port=config.get("port", 8086),
        ssl=config.get("ssl", False),
        verify_ssl=config.get("verify_ssl", True),
        timeout_seconds=config.get("timeout_seconds", 60.0),
        username=config.get("username"),
        password=config.get("password"),
    )


class InfluxSource:
    """An InfluxDB data source."""

    def __init__(self, options: ClientOptions):
        if not HAS_REQUESTS:
            raise MissingModuleException("requests", "influxdb")
        self._options = options

    def _get_client(self) -> InfluxDBClient:
        if not HAS_REQUESTS:
            raise MissingModuleException("requests", "influxdb")
        return InfluxDBClient(self._options)

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
