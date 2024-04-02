"""The `api-key` subcommand."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import csv
import sys
from argparse import ArgumentParser, Namespace

from kukur.app import Kukur


def define_arguments(parser: ArgumentParser):
    """Create subcommand to create/revoke/list API keys."""
    api_key_subparser = parser.add_subparsers(
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


def run(kukur_app: Kukur, args: Namespace):
    """Run the selected API key subcommand."""
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
