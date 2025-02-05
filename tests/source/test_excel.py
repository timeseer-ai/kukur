"""Test importing xlsx files."""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from kukur.inspect import DataOptions
from kukur.source.excel import parse_excel


def test_excel() -> None:
    path = Path("tests/test_data/excel/data.xlsx").resolve()
    table = parse_excel(path, "data", DataOptions())
    assert len(table) == 251
    assert "series name" in table.column_names
    assert "ts" in table.column_names
    assert "value" in table.column_names


def test_excel_without_header_row() -> None:
    path = Path("tests/test_data/excel/data.xlsx").resolve()
    table = parse_excel(path, "data", DataOptions(excel_header_row=False))
    assert len(table) == 252
    assert "0" in table.column_names
    assert "1" in table.column_names
    assert "2" in table.column_names
