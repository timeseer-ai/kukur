"""The `test` CLI subcommand."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import csv
import sys
from argparse import ArgumentParser, Namespace

from dateutil.parser import parse as parse_date

import kukur.source.test as test_source
from kukur.app import Kukur


def define_arguments(parser: ArgumentParser):
    """Create subcommand to test data sources."""
    test_subparsers = parser.add_subparsers(
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
    plot_parser = test_subparsers.add_parser(
        "plot", help="Display plot data for one time series"
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
    plot_parser.add_argument(
        "--source",
        required=True,
        metavar="SOURCE NAME",
        help="The name of the data source to test",
    )
    plot_parser.add_argument(
        "--name",
        required=True,
        metavar="SERIES NAME",
        help="The name of the series to query",
    )
    plot_parser.add_argument(
        "--start",
        required=True,
        metavar="START DATE",
        help="The start date of the time period to query",
    )
    plot_parser.add_argument(
        "--end",
        required=True,
        metavar="END DATE",
        help="The end date of the time period to query",
    )
    plot_parser.add_argument(
        "--interval-count",
        type=int,
        default=200,
        metavar="INTERVAL COUNT",
        help="The number of intervals to divide the plot into.",
    )


def run(kukur_app: Kukur, args: Namespace):
    """Run the selected test subcommand."""
    if args.test_action not in ["search", "metadata", "data", "plot"]:
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
    elif args.test_action == "plot":
        series_name = args.name
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
        for row in test_source.plot(
            kukur_app,
            source_name,
            series_name,
            start_date,
            end_date,
            args.interval_count,
        ):
            writer.writerow(row)
