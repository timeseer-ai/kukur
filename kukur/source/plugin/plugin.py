"""Pluggable data source."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import subprocess
from datetime import datetime
from typing import Generator, Union

from pyarrow import Table, ipc

from kukur.base import SeriesSearch, SeriesSelector
from kukur.exceptions import InvalidSourceException
from kukur.metadata import Metadata
from kukur.source.arrow import conform_to_schema
from kukur.source.quality import QualityMapper

logger = logging.getLogger(__name__)


class PluginSource:
    """PluginSource defines a json/binary interface for external programs."""

    def __init__(self, cmd: list[str], config: dict, quality_mapper: QualityMapper):
        self.__cmd = cmd
        self.__config = config
        self.__quality_mapper = quality_mapper

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[SeriesSelector, Metadata], None, None]:
        """Request series or metadata from the binary."""
        data = {
            "config": self.__config,
            "search": {
                "source": selector.source,
            },
        }
        result = json.loads(self._run(selector.source, "search", data))
        for item in result.get("metadata", []):
            yield Metadata.from_data(item)
        for item in result.get("series", []):
            yield SeriesSelector.from_data(item)

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read series metadata from the binary."""
        data = {
            "config": self.__config,
            "metadata": {"series": selector.to_data()},
        }
        result = json.loads(self._run(selector.source, "metadata", data))
        return Metadata.from_data(result, selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> Table:
        """Get data from the Flight service."""
        data = {
            "config": self.__config,
            "data": {
                "series": selector.to_data(),
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
            },
        }
        result = self._run(selector.source, "data", data)
        with ipc.open_stream(result) as reader:
            table = reader.read_all()
            return conform_to_schema(table, self.__quality_mapper)

    def _run(self, name: str, action: str, data: dict) -> bytes:
        try:
            output = subprocess.run(
                self.__cmd + [action],
                capture_output=True,
                input=json.dumps(data).encode("utf-8"),
                check=True,
            )
        except subprocess.CalledProcessError as err:
            logger.error('Source "%s" logged: %s', name, err.stderr)
            raise err
        if output.stderr != b"":
            logger.warning('Source "%s" logged: %s', name, output.stderr)
        return output.stdout


def from_config(data: dict, quality_mapper: QualityMapper) -> PluginSource:
    """Create a new pluggable data source with the given configuration."""
    if "cmd" not in data:
        raise InvalidSourceException('Plugin sources require a "cmd" entry')
    cmd = data["cmd"]
    if isinstance(cmd, str):
        cmd = [cmd]
    return PluginSource(cmd, data, quality_mapper)
