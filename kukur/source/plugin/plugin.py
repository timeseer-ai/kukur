"""Pluggable data source."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import subprocess
from collections.abc import Generator
from datetime import datetime

from pyarrow import Table, ipc

from kukur.base import SeriesSearch, SeriesSelector
from kukur.exceptions import InvalidSourceException, KukurException
from kukur.metadata import Metadata
from kukur.source.arrow import conform_to_schema

logger = logging.getLogger(__name__)


class PluginException(KukurException):
    """Raised when the plugin binary fails."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"plugin error: {message}")


class PluginSource:
    """PluginSource defines a json/binary interface for external programs."""

    def __init__(self, cmd: list[str], config: dict):
        self.__cmd = cmd
        self.__config = config
        # A timeout of 0 disables it.
        self.__timeout_seconds = config.get("timeout_seconds", 120) or None
        self.__metadata_request_timeout_seconds = (
            config.get("metadata_request_timeout_seconds", 600) or None
        )

    def search(
        self, selector: SeriesSearch
    ) -> Generator[SeriesSelector | Metadata, None, None]:
        """Request series or metadata from the binary."""
        data = {
            "config": self.__config,
            "search": {
                "source": selector.source,
            },
        }
        result = json.loads(
            self._run(
                selector.source,
                "search",
                data,
                self.__metadata_request_timeout_seconds,
            )
        )
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
        result = json.loads(
            self._run(
                selector.source,
                "metadata",
                data,
                self.__metadata_request_timeout_seconds,
            )
        )
        return Metadata.from_data(result, selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> Table:
        """Get data from the binary."""
        data = {
            "config": self.__config,
            "data": {
                "series": selector.to_data(),
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
            },
        }
        result = self._run(selector.source, "data", data, self.__timeout_seconds)
        with ipc.open_stream(result) as reader:
            table = reader.read_all()
            return conform_to_schema(table)

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ) -> Table:
        """Get plot data from the binary.

        Falls back to regular data when the binary does not support plot data.
        """
        data = {
            "config": self.__config,
            "data": {
                "series": selector.to_data(),
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "intervalCount": interval_count,
            },
        }
        if self.__config.get("features", {}).get("plot", False):
            result = self._run(selector.source, "plot", data, self.__timeout_seconds)
        else:
            result = self._run(selector.source, "data", data, self.__timeout_seconds)
        with ipc.open_stream(result) as reader:
            table = reader.read_all()
            return conform_to_schema(table)

    def _run(
        self, name: str, action: str, data: dict, timeout_seconds: float | None
    ) -> bytes:
        try:
            output = subprocess.run(
                self.__cmd + [action],
                capture_output=True,
                input=json.dumps(data).encode("utf-8"),
                check=True,
                timeout=timeout_seconds,
            )
        except subprocess.CalledProcessError as err:
            stderr = _decode(err.stderr)
            logger.error('Source "%s" logged: %s', name, stderr)
            message = f'source "{name}" exited with status {err.returncode}'
            if stderr != "":
                message = f"{message}: {stderr}"
            raise PluginException(message) from err
        except subprocess.TimeoutExpired as err:
            raise PluginException(
                f'source "{name}" timed out after {err.timeout} seconds'
            ) from err
        if stderr := _decode(output.stderr):
            logger.warning('Source "%s" logged: %s', name, stderr)
        return output.stdout


def _decode(stderr: bytes) -> str:
    return stderr.decode(errors="replace").strip()


def from_config(data: dict) -> PluginSource:
    """Create a new pluggable data source with the given configuration."""
    if "cmd" not in data:
        raise InvalidSourceException('Plugin sources require a "cmd" entry')
    cmd = data["cmd"]
    if isinstance(cmd, str):
        cmd = [cmd]
    return PluginSource(cmd, data)
