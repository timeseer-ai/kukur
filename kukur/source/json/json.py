"""Kukur source for JSON files."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import json

from datetime import datetime
from pathlib import Path
from typing import Dict, Generator

from pyarrow import Table

from kukur import SeriesSearch, SeriesSelector, Metadata
from kukur.exceptions import InvalidSourceException


class JSONSource:
    """Read Kukur exported JSON data."""

    __path: Path

    def __init__(self, path: Path):
        self.__path = path

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Read all Kukur JSON files with metadata."""
        for metadata_file in self.__path.glob("*.json"):
            series_name = metadata_file.stem
            yield _read_metadata(
                SeriesSelector(selector.source, series_name), metadata_file
            )

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read exactly one Kukur JSON file"""
        path = self.__path / (
            self.__path.joinpath(f"{selector.tags['series name']}.json")
            .resolve()
            .relative_to(self.__path.resolve())
        )
        metadata = Metadata(selector)
        if not path.is_file():
            return metadata
        return _read_metadata(selector, path)

    def get_data(  # pylint: disable=no-self-use
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> Table:
        """Get data from the Flight service."""
        raise InvalidSourceException("The 'json' source does not support reading data.")


def _read_metadata(selector: SeriesSelector, path: Path) -> Metadata:
    with path.open("rb") as f:
        data = json.load(f)
        return Metadata.from_data(data, selector)


def from_config(config: Dict) -> JSONSource:
    """Create a new JSON source."""
    if "metadata" not in config:
        raise InvalidSourceException("'metadata' is required for 'json' sources")
    path = Path(config["metadata"])
    if not path.is_dir():
        raise InvalidSourceException("'metadata' should be a directory")
    return JSONSource(path)
