"""Expose Kukur using Arrow Flight."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import json

from typing import Any, Callable, Dict, Generator, List

import pyarrow.flight as fl

from dateutil.parser import parse as parse_date

from kukur import PlotSource, SeriesSelector, Source, TagSource
from kukur.app import Kukur
from kukur.exceptions import InvalidSourceException

__all__ = ["JSONFlightServer"]


class JSONFlightServer(fl.FlightServerBase):
    """JSONFlightServer handles JSON Apache Arrow Flight tickets.

    Extra keyword arguments are passed to FlightServerBase.

    It supports registering custom actions and request handlers. Register a GET
    handler to return Arrow data. To return JSON, register an action handler."""

    __get_handlers: Dict[str, Callable]
    __action_handlers: Dict[str, Callable]

    def __init__(self, config: Dict[str, Any], **kwargs):
        host = "0.0.0.0"
        port = 8081
        if "flight" in config:
            host = config["flight"].get("host", host)
            port = config["flight"].get("port", port)
        super().__init__(location=(host, port), **kwargs)
        self.__get_handlers = {}
        self.__action_handlers = {}

    def register_get_handler(self, handler_name: str, func: Callable):
        """Register a handler for GET requests that returns Arrow data.

        The 'query' field in a request maps to the handler_name.

        The handler func will receive the Flight Context and the JSON-parsed ticket as arguments.
        """
        self.__get_handlers[handler_name] = func

    def register_action_handler(self, action_type: str, func: Callable):
        """Register a handler for action requests.

        The handler func will receive the Flight Context and the Flight Action.

        Note that there is no requirement to return JSON here."""
        self.__action_handlers[action_type] = func

    def do_get(self, context, ticket: fl.Ticket):
        """Respond with Arrow columnar data to the given ticket."""
        request = json.loads(ticket.ticket)

        return self.__get_handlers[request["query"]](context, request)

    def do_action(self, context, action: fl.Action):
        """Respond to a generic action request."""
        return self.__action_handlers[action.type](context, action)


class KukurFlightServer:
    """KukurFlightServer exposes the data sources provided by a SourceFactory over Arrow Flight."""

    def __init__(self, source: Source):
        self.__source = source

    def search(self, _, action: fl.Action) -> Generator[bytes, None, None]:
        """Search a data source for time series.

        This returns either a SeriesSelector or Metadata as JSON, depending on
        what is supported by the source."""
        request = json.loads(action.body.to_pybytes())
        selector = SeriesSelector.from_data(request)
        for result in self.__source.search(selector):
            yield json.dumps(result.to_data()).encode()

    def get_metadata(self, _, action: fl.Action) -> List[bytes]:
        """Return metadata for the given time series as JSON."""
        request = json.loads(action.body.to_pybytes())
        selector = SeriesSelector.from_data(request)
        metadata = self.__source.get_metadata(selector).to_data()
        return [json.dumps(metadata).encode()]

    def get_data(self, _, request) -> Any:
        """Return time series data as Arrow data."""
        selector = SeriesSelector.from_data(request["selector"])
        start_date = parse_date(request["start_date"])
        end_date = parse_date(request["end_date"])
        data = self.__source.get_data(selector, start_date, end_date)
        return fl.RecordBatchStream(data)

    def get_plot_data(self, _, request) -> Any:
        """Return plot data as Arrow."""
        selector = SeriesSelector.from_data(request["selector"])
        start_date = parse_date(request["start_date"])
        end_date = parse_date(request["end_date"])
        interval_count: int = request["interval_count"]
        if not isinstance(self.__source, PlotSource):
            raise InvalidSourceException("get_plot_data not supported by source")
        data = self.__source.get_plot_data(
            selector, start_date, end_date, interval_count
        )
        return fl.RecordBatchStream(data)

    def get_source_structure(self, _, action: fl.Action) -> List[bytes]:
        """Return the structure of a source for the given time series as JSON."""
        request = json.loads(action.body.to_pybytes())
        selector = SeriesSelector.from_data(request)
        if not isinstance(self.__source, TagSource):
            raise InvalidSourceException("get_source_structure not supported by source")
        source_structure = self.__source.get_source_structure(selector)
        if source_structure is None:
            return [json.dumps(source_structure).encode()]
        return [json.dumps(source_structure.to_data()).encode()]


class KukurServerAuthHandler(fl.ServerAuthHandler):
    """KukurServerAuthHandler handles the authentication"""

    _app: Kukur

    def __init__(self, app: Kukur):
        super().__init__()
        self._app = app

    def authenticate(self, outgoing, incoming):  # pylint: disable=no-self-use
        """Check the authentication."""
        buf = incoming.read()
        outgoing.write(buf)

    def is_valid(self, token: bytes):
        """Check if the supplied token is valid."""
        if token == "" or token is None:
            raise fl.FlightUnauthenticatedError("invalid token")

        auth = fl.BasicAuth.deserialize(token)
        if auth.username is None:
            raise fl.FlightUnauthenticatedError("invalid username")
        if auth.password is None:
            raise fl.FlightUnauthenticatedError("invalid password")

        if not self._app.get_api_keys().has_api_key(auth.username.decode("UTF-8")):
            raise fl.FlightUnauthenticatedError("invalid token")
        if not self._app.get_api_keys().is_valid(
            auth.username.decode("UTF-8"), auth.password.decode("UTF-8")
        ):
            raise fl.FlightUnauthenticatedError("invalid token")
        return token


class KukurServerNoAuthHandler(fl.ServerAuthHandler):
    """KukurServerNoAuthHandler handles the authentication when it is disabled"""

    def authenticate(self, outgoing, incoming):
        """Do nothing."""

    def is_valid(self, token: bytes):  # pylint: disable=no-self-use, unused-argument
        """Return empty string."""
        return ""
