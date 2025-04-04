"""Test the Inspect functions for databases."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import pytest

from kukur.inspect import Connection, DataOptions, InspectedPath, ResourceType
from kukur.inspect.database import (
    inspect_database,
    preview_database,
    read_database,
)

pytestmark = pytest.mark.postgresql


def _sort_by_path(path: InspectedPath) -> str:
    return path.path


def _get_connection_string() -> str:
    return "host=localhost port=5431 user=postgres password=Timeseer!AI"


def _get_connection_options() -> dict:
    return {
        "host": "localhost",
        "port": 5431,
        "user": "postgres",
        "password": "Timeseer!AI",
    }


def test_inspect_database_schema() -> None:
    connection = Connection("postgresql", None, _get_connection_string(), None, None)
    results = inspect_database(connection)
    assert len(results) == 3
    results = sorted(results, key=_sort_by_path)
    assert results[0].path == "information_schema"
    assert results[0].resource_type == ResourceType.DIRECTORY
    assert results[1].path == "pg_catalog"
    assert results[1].resource_type == ResourceType.DIRECTORY
    assert results[2].path == "public"
    assert results[2].resource_type == ResourceType.DIRECTORY


def test_inspect_database_tables() -> None:
    connection = Connection(
        "postgresql",
        None,
        _get_connection_string(),
        None,
        None,
    )
    results = inspect_database(connection, "public")
    assert len(results) == 2
    results = sorted(results, key=_sort_by_path)
    assert results[0].path == "public/data"
    assert results[0].resource_type == ResourceType.TABLE
    assert results[1].path == "public/metadata"
    assert results[1].resource_type == ResourceType.TABLE


def test_preview_database() -> None:
    connection = Connection("postgresql", None, _get_connection_string(), None, None)
    results = preview_database(connection, "public/data")
    assert results is not None
    assert len(results) == 3


def test_preview_database_selected_columns() -> None:
    connection = Connection("postgresql", None, _get_connection_string(), None, None)
    results = preview_database(
        connection, "public/data", 5000, options=DataOptions(["ts", "value"])
    )
    assert results is not None
    assert len(results) == 3
    assert results.num_columns == 2


def test_preview_database_limit_rows() -> None:
    connection = Connection("postgresql", None, _get_connection_string(), None, None)
    results = preview_database(connection, "public/data", 2)
    assert results is not None
    assert len(results) == 2


def test_read_database() -> None:
    connection = Connection("postgresql", None, _get_connection_string(), None, None)
    batches = list(read_database(connection, "public/data"))
    assert len(batches) == 1
    assert len(batches[0]) == 3


def test_inspect_database_schema_pg8000() -> None:
    connection = Connection("postgresql", None, None, _get_connection_options(), None)
    results = inspect_database(connection)
    assert len(results) == 3
    results = sorted(results, key=_sort_by_path)
    assert results[0].path == "information_schema"
    assert results[0].resource_type == ResourceType.DIRECTORY
    assert results[1].path == "pg_catalog"
    assert results[1].resource_type == ResourceType.DIRECTORY
    assert results[2].path == "public"
    assert results[2].resource_type == ResourceType.DIRECTORY


def test_inspect_database_tables_pg8000() -> None:
    connection = Connection("postgresql", None, None, _get_connection_options(), None)
    results = inspect_database(connection, "public")
    assert len(results) == 2
    results = sorted(results, key=_sort_by_path)
    assert results[0].path == "public/data"
    assert results[0].resource_type == ResourceType.TABLE
    assert results[1].path == "public/metadata"
    assert results[1].resource_type == ResourceType.TABLE


def test_preview_database_pg8000() -> None:
    connection = Connection("postgresql", None, None, _get_connection_options(), None)
    results = preview_database(connection, "public/data")
    assert results is not None
    assert len(results) == 3


def test_preview_database_selected_columns_pg8000() -> None:
    connection = Connection("postgresql", None, None, _get_connection_options(), None)
    results = preview_database(
        connection, "public/data", 5000, options=DataOptions(["ts", "value"])
    )
    assert results is not None
    assert len(results) == 3
    assert results.num_columns == 2


def test_preview_database_limit_rows_pg8000() -> None:
    connection = Connection("postgresql", None, None, _get_connection_options(), None)
    results = preview_database(connection, "public/data", 2)
    assert results is not None
    assert len(results) == 2


def test_read_database_pg8000() -> None:
    connection = Connection(
        "postgresql",
        None,
        None,
        _get_connection_options(),
        None,
    )
    batches = list(read_database(connection, "public/data"))
    assert len(batches) == 1
    assert len(batches[0]) == 3
