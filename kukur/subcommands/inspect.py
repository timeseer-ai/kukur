"""The `inspect` CLI command."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from argparse import ArgumentParser


def define_arguments(parser: ArgumentParser):
    """Create a subcommand for each resource supported by inspect."""
    inspect_subparser = parser.add_subparsers(
        dest="inspect_action", help="Select the type of resource to inspect"
    )
    inspect_subparser.add_parser("blob", help="Inspect blob storage")
