"""Kukur source for Apache Arrow Flight SQL.

This uses the DBAPI, not the Arrow native response in order to reuse the existing SQL code.
"""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

from kukur.exceptions import MissingModuleException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig

try:
    import adbc_driver_flightsql.dbapi
    import adbc_driver_manager  # noqa: F401
    from adbc_driver_flightsql import DatabaseOptions

    HAS_FLIGHT_SQL = True
except ImportError:
    HAS_FLIGHT_SQL = False


@dataclass
class FlightSQLOptions:
    """Options specific to the Flight SQL source."""

    username: str | None = None
    password: str | None = None
    verify_ssl: bool = True

    @classmethod
    def from_data(cls, data: dict) -> "FlightSQLOptions":
        """Create Flight SQL options from a dictionary."""
        return cls(
            username=data.get("username"),
            password=data.get("password"),
            verify_ssl=data.get("verify_ssl", True),
        )


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
        self.__flight_options = FlightSQLOptions.from_data(data)

    def connect(self):
        """Create a connection to Flight SQL."""
        db_kwargs = {}
        if self.__flight_options.username is not None:
            db_kwargs[adbc_driver_manager.DatabaseOptions.USERNAME.value] = (
                self.__flight_options.username
            )
        if self.__flight_options.password is not None:
            db_kwargs[adbc_driver_manager.DatabaseOptions.PASSWORD.value] = (
                self.__flight_options.password
            )
        if not self.__flight_options.verify_ssl:
            db_kwargs[DatabaseOptions.TLS_SKIP_VERIFY.value] = "true"
        if (
            self._config.query_timeout_seconds is not None
            and self._config.query_timeout_seconds > 0
        ):
            db_kwargs[DatabaseOptions.TIMEOUT_QUERY.value] = str(
                self._config.query_timeout_seconds
            )
            db_kwargs[DatabaseOptions.TIMEOUT_FETCH.value] = str(
                self._config.query_timeout_seconds
            )
        return adbc_driver_flightsql.dbapi.connect(
            self._config.connection_string, db_kwargs, autocommit=True
        )


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
) -> FlightSQLSource:
    """Create a new Flight SQL source from a configuration dictionary."""
    if not HAS_FLIGHT_SQL:
        raise MissingModuleException("adbc_driver_flight_sql", "flight-sql")
    return FlightSQLSource(data, metadata_value_mapper, quality_mapper)
