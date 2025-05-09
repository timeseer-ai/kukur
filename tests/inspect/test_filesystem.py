"""Test the Inspect functions for filesystems."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from kukur.inspect import DataOptions, FileOptions, InspectedPath, ResourceType
from kukur.inspect.filesystem import (
    inspect_filesystem,
    preview_filesystem,
    read_filesystem,
)


def _sort_by_path(path: InspectedPath) -> str:
    return path.path


def test_inspect_filesystem() -> None:
    path = Path("tests/test_data/feather/dir")
    results = inspect_filesystem(path)
    assert len(results) == 2
    results = sorted(results, key=_sort_by_path)
    assert results[0].path == str(path / Path("test-tag-1.feather"))
    assert results[0].resource_type == ResourceType.ARROW
    assert results[1].path == str(path / Path("test-tag-5.feather"))
    assert results[1].resource_type == ResourceType.ARROW


def test_preview_filesystem() -> None:
    path = Path("tests/test_data/feather/row.feather")
    results = preview_filesystem(path)
    assert results is not None
    assert len(results) == 47


def test_preview_filesystem_limit_rows() -> None:
    path = Path("tests/test_data/feather/row.feather")
    results = preview_filesystem(path, 10)
    assert results is not None
    assert len(results) == 10


def test_read_filesystem() -> None:
    path = Path("tests/test_data/feather/row.feather")
    results = list(read_filesystem(path))

    assert (len(results)) == 1
    assert results[0].num_columns == 3
    assert results[0].num_rows == 47


def test_read_filesystem_series_column() -> None:
    path = Path("tests/test_data/feather/row.feather")
    results = list(read_filesystem(path, DataOptions(column_names=["series name"])))

    assert (len(results)) == 1
    assert results[0].num_columns == 1
    assert results[0].num_rows == 47


def test_inspect_filesystem_detect_delta_table() -> None:
    path = Path("tests/test_data/delta/")

    results = inspect_filesystem(path, options=FileOptions(detect_delta=True))
    assert any(result.resource_type == ResourceType.DELTA for result in results)

    results = inspect_filesystem(path)
    assert not any(result.resource_type == ResourceType.DELTA for result in results)


def test_inspect_filesystem_delta_table() -> None:
    path = Path("tests/test_data/delta/delta-row")
    results = inspect_filesystem(path)
    assert len(results) == 2
    results = sorted(results, key=_sort_by_path)
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
    assert results is not None
    assert len(results) == 47


def test_preview_filesystem_delta_table_limit_rows() -> None:
    path = Path("tests/test_data/delta/delta-row")
    results = preview_filesystem(path, 10)
    assert results is not None
    assert len(results) == 10


def test_read_filesystem_delta_table() -> None:
    path = Path("tests/test_data/delta/delta-row")
    results = list(read_filesystem(path))

    assert len(results) == 1
    assert results[0].num_columns == 3
    assert results[0].num_rows == 47


def test_read_filesystem_delta_table_series_column() -> None:
    path = Path("tests/test_data/delta/delta-row")
    results = list(read_filesystem(path, DataOptions(column_names=["name"])))

    assert len(results) == 1
    assert results[0].num_columns == 1
    assert results[0].num_rows == 47


def test_read_filesystem_csv_delimiter_semicolon() -> None:
    path = Path("tests/test_data/csv/row-semicolon.csv")
    results = list(read_filesystem(path, DataOptions(csv_delimiter=";")))

    assert len(results) == 1
    assert results[0].num_columns == 3
    assert results[0].num_rows == 60


def test_read_filesystem_csv_no_header_row() -> None:
    path = Path("tests/test_data/csv/dir/test-tag-1.csv")
    results = list(read_filesystem(path, DataOptions(csv_header_row=False)))
    assert len(results) == 1
    assert results[0].num_columns == 2
    assert results[0].num_rows == 5


def test_read_filesystem_parquet() -> None:
    path = Path("tests/test_data/parquet/row.parquet")
    batches = list(read_filesystem(path))
    assert len(batches) == 1
    assert len(batches[0]) == 47


def test_read_filesystem_ndjson() -> None:
    path = Path("tests/test_data/ndjson/inspect.ndjson")
    batches = list(read_filesystem(path))
    assert len(batches) == 1
    assert len(batches[0]) == 5


def test_read_filesystem_orc() -> None:
    path = Path("tests/test_data/orc/row.orc")
    batches = list(read_filesystem(path))
    assert len(batches) == 1
    assert len(batches[0]) == 47


def test_recursive() -> None:
    path = Path("tests/test_data/csv/recursive")
    paths = inspect_filesystem(path, options=FileOptions(recursive=True))
    assert len(paths) == 4
    csv_paths = [blob.path for blob in paths if blob.resource_type == ResourceType.CSV]
    assert len(csv_paths) == 2
    assert "tests/test_data/csv/recursive/dt=2024-01-01/data.csv" in csv_paths
    assert "tests/test_data/csv/recursive/dt=2024-01-02/data.csv" in csv_paths


def test_default_resource_type() -> None:
    path = Path("tests/test_data/csv/no_extension")
    paths = inspect_filesystem(
        path, options=FileOptions(default_resource_type=ResourceType.CSV)
    )
    assert len(paths) == 1
    assert paths[0].resource_type == ResourceType.CSV
