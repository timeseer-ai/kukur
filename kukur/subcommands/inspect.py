"""The `inspect` CLI command."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import csv
import sys
from argparse import ArgumentParser, Namespace
from pathlib import Path

from kukur.inspect.blob import inspect_blob
from kukur.inspect.filesystem import inspect_filesystem


def define_arguments(parser: ArgumentParser):
    """Create a subcommand for each resource supported by inspect."""
    inspect_subparser = parser.add_subparsers(
        dest="inspect_action", help="Select the type of resource to inspect"
    )
    blob_parser = inspect_subparser.add_parser("blob", help="Inspect blob storage")
    blob_parser.add_argument(
        "--uri", required=True, help="URI of the blob (s3://, abfss://container@sa)."
    )

    fs_parser = inspect_subparser.add_parser("filesystem", help="Inspect a filesystem")
    fs_parser.add_argument(
        "--path",
        type=Path,
        required=True,
        help="The path of the filesystem item to inspect",
    )


def run(args: Namespace):
    """Inspect a data source."""
    paths = None
    if args.inspect_action == "blob":
        paths = inspect_blob(args.uri)
    if args.inspect_action == "filesystem":
        paths = inspect_filesystem(args.path)

    if paths is not None:
        writer = csv.writer(sys.stdout)
        for path in paths:
            writer.writerow([path.resource_type.value, path.path])
