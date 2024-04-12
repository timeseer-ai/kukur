"""Test the Inspect functions for filesystems."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from kukur.inspect import ResourceType
from kukur.inspect.filesystem import (
    inspect_filesystem,
    preview_filesystem,
    read_filesystem,
)


def test_inspect_filesystem() -> None:
    path = Path("tests/test_data/feather/dir")
    results = inspect_filesystem(path)
    assert len(results) == 2
    assert results[0].path == str(path / Path("test-tag-1.feather"))
    assert results[0].resource_type == ResourceType.ARROW
    assert results[1].path == str(path / Path("test-tag-5.feather"))
    assert results[1].resource_type == ResourceType.ARROW


def test_preview_filesystem() -> None:
    path = Path("tests/test_data/feather/row.feather")
    results = preview_filesystem(path)
    assert len(results) == 47


def test_preview_filesystem_limit_rows() -> None:
    path = Path("tests/test_data/feather/row.feather")
    results = preview_filesystem(path, 10)
    assert len(results) == 10


def test_read_filesystem() -> None:
    path = Path("tests/test_data/feather/row.feather")
    results = list(read_filesystem(path))

    assert (len(results)) == 1
    assert results[0].num_columns == 3
    assert results[0].num_rows == 47


def test_read_filesystem_series_column() -> None:
    path = Path("tests/test_data/feather/row.feather")
    results = list(read_filesystem(path, column_names=["series name"]))

    assert (len(results)) == 1
    assert results[0].num_columns == 1
    assert results[0].num_rows == 47


def test_inspect_filesystem_delta_table() -> None:
    path = Path("tests/test_data/delta/delta-row")
    results = inspect_filesystem(path)
    assert len(results) == 2
    assert results[0].path == str(path / Path("_delta_log"))
    assert results[0].resource_type == ResourceType.DIRECTORY
    assert results[1].path == str(
        path
        / Path("part-00000-b40d68b8-87cd-4350-b489-e615408c98d5-c000.snappy.parquet")
    )
    assert results[1].resource_type == ResourceType.PARQUET


def test_preview_filesystem_delta_table() -> None:
    path = Path("tests/test_data/delta/delta-row")
    results = preview_filesystem(path)
    assert len(results) == 47


def test_preview_filesystem_delta_table_limit_rows() -> None:
    path = Path("tests/test_data/delta/delta-row")
    results = preview_filesystem(path, 10)
    assert len(results) == 10


def test_read_filesystem_delta_table() -> None:
    path = Path("tests/test_data/delta/delta-row")
    results = list(read_filesystem(path))

    assert (len(results)) == 1
    assert results[0].num_columns == 3
    assert results[0].num_rows == 47


def test_read_filesystem_delta_table_series_column() -> None:
    path = Path("tests/test_data/delta/delta-row")
    results = list(read_filesystem(path, column_names=["name"]))

    assert (len(results)) == 1
    assert results[0].num_columns == 1
    assert results[0].num_rows == 47
