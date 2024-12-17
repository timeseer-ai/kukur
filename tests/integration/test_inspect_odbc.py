"""Test the Inspect functions for databases."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import os

import pytest

from kukur.inspect import Connection, DataOptions, InspectedPath, ResourceType
from kukur.inspect.database import (
    inspect_database,
    preview_database,
    read_database,
)

pytestmark = pytest.mark.odbc


def _sort_by_path(path: InspectedPath) -> str:
    return path.path


def _get_connection_string() -> str:
    if "KUKUR_INTEGRATION_TARGET" in os.environ:
        if os.environ["KUKUR_INTEGRATION_TARGET"] == "local":
            return (
                "Driver={/usr/local/lib/libtdsodbc.so};Server=localhost;Port=1433;"
                "UID=sa;PWD=Timeseer!AI;TDS_Version=8.0;ClientCharset=UTF-8"
            )
        if os.environ["KUKUR_INTEGRATION_TARGET"] == "linux":
            return (
                "Driver={/usr/lib/libtdsodbc.so};Server=localhost;Port=1433;"
                "UID=sa;PWD=Timeseer!AI;TDS_Version=8.0;ClientCharset=UTF-8"
            )
    return (
        "Driver={/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so};Server=localhost;Port=1433;"
        "UID=sa;PWD=Timeseer!AI;TDS_Version=8.0;ClientCharset=UTF-8"
    )


def test_inspect_database_schema() -> None:
    connection = Connection("odbc", "TestData", _get_connection_string(), None, "top")
    results = inspect_database(connection)
    assert len(results) == 1
    results = sorted(results, key=_sort_by_path)
    assert results[0].path == "dbo"
    assert results[0].resource_type == ResourceType.DIRECTORY


def test_inspect_database_tables() -> None:
    connection = Connection(
        "odbc",
        "TestData",
        _get_connection_string(),
        None,
        "top",
    )
    results = inspect_database(connection, "dbo")
    assert len(results) == 3
    results = sorted(results, key=_sort_by_path)
    assert results[0].path == "dbo/Data"
    assert results[0].resource_type == ResourceType.TABLE
    assert results[1].path == "dbo/Dictionary"
    assert results[1].resource_type == ResourceType.TABLE
    assert results[2].path == "dbo/Metadata"
    assert results[2].resource_type == ResourceType.TABLE


def test_preview_database() -> None:
    connection = Connection(
        "odbc",
        "TestData",
        _get_connection_string(),
        None,
        "top",
    )
    results = preview_database(connection, "dbo/Data")
    assert results is not None
    assert len(results) == 17


def test_preview_database_selected_columns() -> None:
    connection = Connection("odbc", "TestData", _get_connection_string(), None, "top")
    results = preview_database(
        connection, "dbo/data", 5000, options=DataOptions(["ts", "value"])
    )
    assert results is not None
    assert len(results) == 17
    assert results.num_columns == 2


def test_preview_database_limit_rows() -> None:
    connection = Connection("odbc", "TestData", _get_connection_string(), None, "top")
    results = preview_database(connection, "dbo/data", 10)
    assert results is not None
    assert len(results) == 10


def test_read_database() -> None:
    connection = Connection("odbc", "TestData", _get_connection_string(), None, "top")
    batches = list(read_database(connection, "dbo/data"))
    assert len(batches) == 1
    assert len(batches[0]) == 17
