"""The `inspect` CLI command."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import csv
import sys
import time
from argparse import ArgumentParser, Namespace
from pathlib import Path

from kukur.inspect.blob import inspect_blob, preview_blob
from kukur.inspect.filesystem import inspect_filesystem, preview_filesystem


def define_arguments(parser: ArgumentParser):
    """Create a subcommand for each resource supported by inspect."""
    inspect_subparser = parser.add_subparsers(
        dest="inspect_action", help="Select the type of resource to inspect"
    )
    blob_parser = inspect_subparser.add_parser("blob", help="Inspect blob storage")
    blob_parser.add_argument(
        "--uri", required=True, help="URI of the blob (s3://, abfss://container@sa)."
    )
    blob_parser.add_argument(
        "--preview",
        action="store_true",
        help="Display a preview of the file contents.",
    )

    fs_parser = inspect_subparser.add_parser("filesystem", help="Inspect a filesystem")
    fs_parser.add_argument(
        "--path",
        type=Path,
        required=True,
        help="The path of the filesystem item to inspect",
    )
    fs_parser.add_argument(
        "--preview",
        action="store_true",
        help="Display a preview of the file contents.",
    )


def run(args: Namespace):
    """Inspect a data source."""
    paths = None
    preview = None
    if args.inspect_action == "blob":
        if args.preview:
            preview = preview_blob(args.uri)
        else:
            paths = inspect_blob(args.uri)
    if args.inspect_action == "filesystem":
        if args.preview:
            preview = preview_filesystem(args.path)
        else:
            paths = inspect_filesystem(args.path)

    if paths is not None:
        writer = csv.writer(sys.stdout)
        for path in paths:
            writer.writerow([path.resource_type.value, path.path])
    if preview is not None:
        print(preview)  # noqa: T201
        time.sleep(1)  # otherwise pyarrow datasets segfault
