"""Kukur connection to Redshift."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Dict

from kukur.exceptions import InvalidSourceException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig

try:
    import redshift_connector

    HAS_REDSHIFT = True
except ImportError:
    HAS_REDSHIFT = False


class RedshiftSource(BaseSQLSource):
    """Kukur source for Redshift."""

    def __init__(
        self,
        data: Dict,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        config = SQLConfig.from_dict(data)
        if "connection" not in data:
            raise InvalidSourceException(
                "'redshift' sources require a 'connection' dictionary."
            )
        self.__connection_options: Dict = data["connection"]
        super().__init__(config, metadata_value_mapper, quality_mapper)

    def connect(self) -> redshift_connector.Connection:
        """Create a connection to Redshift."""
        connection_options = self.__connection_options.copy()
        if self._config.query_timeout_seconds is not None:
            connection_options["timeout"] = self._config.query_timeout_seconds
        return redshift_connector.connect(**self.__connection_options)


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
) -> RedshiftSource:
    """Create a new Redshift source from a configuration dictionary."""
    return RedshiftSource(data, metadata_value_mapper, quality_mapper)
