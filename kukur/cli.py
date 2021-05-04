"""CLI for Kukur.

This allows launching Kukur as a service or testing data sources."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import argparse
import csv
import sys

from dateutil.parser import parse as parse_date

import kukur.logging
import kukur.source.test as test_source

from kukur.app import Kukur
from kukur.config import from_toml
from kukur.flight import (
    JSONFlightServer,
    KukurFlightServer,
    KukurServerAuthHandler,
    KukurServerNoAuthHandler,
)


def parse_args():
    """Parse the command line arguments given to Kukur."""
    parser = argparse.ArgumentParser(description="Start Kukur.")
    parser.add_argument(
        "--config-file", default="Kukur.toml", help="Path to the configuration file"
    )
    subparsers = parser.add_subparsers(dest="action", help="Select the CLI action")

    subparsers.add_parser(
        "flight", help="Enable the Arrow Flight interface (the default)"
    )
    test_parser = subparsers.add_parser("test", help="Test data source connectivity")
    api_key_parser = subparsers.add_parser(
        "api-key", help="Create an api key for the Arrow Flight interface"
    )

    test_subparsers = test_parser.add_subparsers(
        dest="test_action", help="Select the type of data to test"
    )
    search_parser = test_subparsers.add_parser(
        "search", help="List all time series in the source"
    )
    metadata_parser = test_subparsers.add_parser(
        "metadata", help="Display metadata for one time series"
    )
    data_parser = test_subparsers.add_parser(
        "data", help="Display data for one time series"
    )

    search_parser.add_argument(
        "--source",
        required=True,
        metavar="SOURCE NAME",
        help="The name of the data source to test",
    )
    metadata_parser.add_argument(
        "--source",
        required=True,
        metavar="SOURCE NAME",
        help="The name of the data source to test",
    )
    metadata_parser.add_argument(
        "--name",
        required=True,
        metavar="SERIES NAME",
        help="The name of the series to query",
    )
    data_parser.add_argument(
        "--source",
        required=True,
        metavar="SOURCE NAME",
        help="The name of the data source to test",
    )
    data_parser.add_argument(
        "--name",
        required=True,
        metavar="SERIES NAME",
        help="The name of the series to query",
    )
    data_parser.add_argument(
        "--start",
        required=True,
        metavar="START DATE",
        help="The start date of the time period to query",
    )
    data_parser.add_argument(
        "--end",
        required=True,
        metavar="END DATE",
        help="The end date of the time period to query",
    )

    api_key_subparser = api_key_parser.add_subparsers(
        dest="api_key_action", help="Select the action for api keys"
    )
    create_parser = api_key_subparser.add_parser("create", help="Create an api key")
    revoke_parser = api_key_subparser.add_parser("revoke", help="Revoke an api key")
    api_key_subparser.add_parser("list", help="List all api keys")

    create_parser.add_argument(
        "--name", required=True, metavar="API KEY NAME", help="The name of the api key"
    )
    revoke_parser.add_argument(
        "--name", required=True, metavar="API KEY NAME", help="The name of the api key"
    )

    return parser.parse_args()


def _serve(kukur_app: Kukur, server_config):
    service = KukurFlightServer(kukur_app)

    auth_handler = KukurServerAuthHandler(kukur_app)
    if "flight" in server_config and not server_config["flight"].get(
        "authentication", True
    ):
        auth_handler = KukurServerNoAuthHandler()

    server = JSONFlightServer(server_config, auth_handler=auth_handler)
    server.register_action_handler("search", service.search)
    server.register_action_handler("get_metadata", service.get_metadata)
    server.register_get_handler("get_data", service.get_data)
    server.register_action_handler("list_sources", kukur_app.list_sources)
    server.serve()


def _test_source(kukur_app: Kukur, args):
    if args.test_action not in ["search", "metadata", "data"]:
        return

    writer = csv.writer(sys.stdout)

    source_name: str = args.source

    series_name: str
    if args.test_action == "search":
        for row in test_source.search(kukur_app, source_name):
            writer.writerow(row)
    elif args.test_action == "metadata":
        series_name = args.name
        for row in test_source.metadata(kukur_app, source_name, series_name):
            writer.writerow(row)
    elif args.test_action == "data":
        series_name = args.name
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
        for row in test_source.data(
            kukur_app, source_name, series_name, start_date, end_date
        ):
            writer.writerow(row)


def _api_keys(kukur_app: Kukur, args):
    if args.api_key_action not in ["create", "revoke", "list"]:
        return

    writer = csv.writer(sys.stdout)

    if args.api_key_action == "create":
        api_key = kukur_app.get_api_keys().create(args.name)
        writer.writerow([api_key])
    elif args.api_key_action == "revoke":
        kukur_app.get_api_keys().revoke(args.name)
    elif args.api_key_action == "list":
        api_keys = kukur_app.get_api_keys().list()
        for key in api_keys:
            writer.writerow((key.name, key.creation_date.isoformat()))


def _run():
    args = parse_args()
    config = from_toml(args.config_file)
    kukur.logging.configure(config)
    app = Kukur(config)
    if args.action == "test":
        _test_source(app, args)
    elif args.action == "api-key":
        _api_keys(app, args)
    else:
        _serve(app, config)


if __name__ == "__main__":
    _run()
