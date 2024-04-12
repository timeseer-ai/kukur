"""CLI for Kukur.

This allows launching Kukur as a service or testing data sources.
"""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import argparse

import kukur.logging
import kukur.subcommands as subcommand
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
    inspect_parser = subparsers.add_parser(
        "inspect", help="List resources in a blob store and determine their schema"
    )
    test_parser = subparsers.add_parser("test", help="Test data source connectivity")
    api_key_parser = subparsers.add_parser(
        "api-key", help="Create an api key for the Arrow Flight interface"
    )

    subcommand.api_key.define_arguments(api_key_parser)
    subcommand.inspect.define_arguments(inspect_parser)
    subcommand.test_source.define_arguments(test_parser)

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
    server.register_get_handler("get_plot_data", service.get_plot_data)
    server.register_action_handler("list_sources", kukur_app.list_sources)
    server.register_action_handler("get_source_structure", service.get_source_structure)
    server.serve()


def _run() -> None:
    args = parse_args()
    config = from_toml(args.config_file)
    kukur.logging.configure(config)
    app = Kukur(config)
    if args.action == "test":
        subcommand.test_source.run(app, args)
    elif args.action == "api-key":
        subcommand.api_key.run(app, args)
    elif args.action == "inspect":
        subcommand.inspect.run(args)
    else:
        _serve(app, config)


if __name__ == "__main__":
    _run()
