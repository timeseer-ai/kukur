"""Provide remote access to Kukur over Arrow Flight."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import json

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.flight as fl

from kukur import Metadata, SeriesSearch, SeriesSelector, SourceStructure


class Client:
    """Client connects to Kukur using Arrow Flight."""

    _client: fl.FlightClient = None

    def __init__(
        self,
        api_key: Tuple[str, str] = ("", ""),
        host: str = "localhost",
        port: int = 8081,
    ):
        """Create a new Client.

        Creating a client does not open a connection. The connection will be opened lazily.

        Args:
            host: the hostname where the Kukur instance is running. Defaults to ``localhost``.
            port: the port where the Kukur instance is running. Defaults to ``8081``.
            api_key: the api key to connect: this is a tuple of (key name, key).
        """
        self._location = (host, port)
        self._api_key = api_key

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Search Kukur for time series matching the given ``SeriesSelector``.

        Args:
            selector: return time series matching the given selector.
                      Use ``name = None`` (the default) to select all series in a source.

        Returns:
            A generator that returns either ``Metadata`` or ``SeriesSelector``s.
            The return value depends on the search that is supported by the source.
        """
        body = selector.to_data()
        results = list(
            self._get_client().do_action(("search", json.dumps(body).encode()))
        )
        for result in results:
            data = json.loads(result.body.to_pybytes())
            if "series" not in data:
                yield SeriesSelector.from_data(data)
            else:
                yield _read_metadata(data)

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read metadata for the time series selected by the ``SeriesSelector``.

        Args:
            selector: the selected time series

        Returns:
            The ``Metadata`` for the time series.
        """
        body = selector.to_data()
        results = list(
            self._get_client().do_action(("get_metadata", json.dumps(body).encode()))
        )
        result = results[0]
        data = json.loads(result.body.to_pybytes())
        return _read_metadata(data)

    def get_data(
        self,
        selector: SeriesSelector,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pa.Table:
        """Get raw data for the time series selected by the SeriesSelector.

        Args:
            selector: return data for the time series selected by this selector.
            start_date: the start date of the time range of data to return. Defaults to one year ago.
            end_date: the end date of the time range of data to return. Defaults to now.

        Returns:
            A pyarrow Table with two columns: 'ts' and 'value'.
        """
        start_date, end_date = _apply_default_range(start_date, end_date)
        query = {
            "query": "get_data",
            "selector": selector.to_data(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        ticket = fl.Ticket(json.dumps(query))
        return self._get_client().do_get(ticket).read_all()

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        interval_count: int = 200,
    ) -> pa.Table:
        """Get plot data for the time series selected by the SeriesSelector.

        Args:
            selector: return data for the time series selected by this selector.
            start_date: the start date of the time range of data to return. Defaults to one year ago.
            end_date: the end date of the time range of data to return. Defaults to now.
            interval_count: the number of intervals included in the plot. Defaults to 200.

        Returns:
            A pyarrow Table with two columns: 'ts' and 'value'.
        """
        start_date, end_date = _apply_default_range(start_date, end_date)
        query = {
            "query": "get_plot_data",
            "selector": selector.to_data(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "interval_count": interval_count,
        }
        ticket = fl.Ticket(json.dumps(query))
        return self._get_client().do_get(ticket).read_all()

    def list_sources(self) -> List[str]:
        """List all configured sources.

        Returns:
            A list of source names that are configured in Kukur.
        """
        results = list(self._get_client().do_action(("list_sources")))
        data = json.loads(results[0].body.to_pybytes())
        return data

    def get_source_structure(
        self, selector: SeriesSelector
    ) -> Optional[SourceStructure]:
        """List all tags and fields from a source.

        Returns:
            A list of tag keys, tag values and fields that are configured in the source.
        """
        body = selector.to_data()
        results = list(
            self._get_client().do_action(
                ("get_source_structure", json.dumps(body).encode())
            )
        )
        data = json.loads(results[0].body.to_pybytes())
        if data is None:
            return None
        return SourceStructure.from_data(data)

    def _get_client(self):
        if self._client is None:
            self._client = fl.FlightClient(self._location)
            if self._api_key != ("", ""):
                self._client.authenticate(ClientAuthenticationHandler(self._api_key))
        return self._client


def _read_metadata(data: Dict[str, Any]) -> Metadata:
    return Metadata.from_data(data)


def _apply_default_range(
    start_date: Optional[datetime], end_date: Optional[datetime]
) -> Tuple[datetime, datetime]:
    if start_date is None or end_date is None:
        now = datetime.utcnow().replace(tzinfo=timezone(timedelta(0)))
        if start_date is None:
            start_date = now.replace(year=now.year - 1)
        if end_date is None:
            end_date = now
    return start_date, end_date


class ClientAuthenticationHandler(fl.ClientAuthHandler):
    """Client authentication handler for api keys"""

    def __init__(self, api_key):
        super().__init__()
        self.basic_auth = fl.BasicAuth(*api_key)
        self.token = None

    def authenticate(self, outgoing, incoming):
        """Client - server handshake"""
        auth = self.basic_auth.serialize()
        outgoing.write(auth)
        self.token = incoming.read()

    def get_token(self):
        """Get the token"""
        return self.token
