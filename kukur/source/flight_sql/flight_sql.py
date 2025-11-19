"""Kukur source for Apache Arrow Flight SQL.

This uses the DBAPI, not the Arrow native response in order to reuse the existing SQL code.
"""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from kukur.exceptions import InvalidSourceException, MissingModuleException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig

import adbc_driver_flightsql.dbapi
import adbc_driver_manager

try:
    import adbc_driver_flightsql.dbapi
    import adbc_driver_manager

    HAS_FLIGHT_SQL = True
except ImportError:
    HAS_FLIGHT_SQL = False


class FlightSQLSource(BaseSQLSource):
    """Kukur source for Flight SQL."""

    def __init__(
        self,
        data: dict,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        if not HAS_FLIGHT_SQL:
            raise MissingModuleException("adbc_driver_flight_sql", "flight-sql")
        config = SQLConfig.from_dict(data)
        super().__init__(config, metadata_value_mapper, quality_mapper)

    def connect(self):
        """Create a connection to Flight SQL."""
        return adbc_driver_flightsql.dbapi.connect(self._config.connection_string, autocommit=True)


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
) -> FlightSQLSource:
    """Create a new Flight SQL source from a configuration dictionary."""
    if not HAS_FLIGHT_SQL:
        raise MissingModuleException("adbc_driver_flight_sql", "flight-sql")
    return FlightSQLSource(data, metadata_value_mapper, quality_mapper)
