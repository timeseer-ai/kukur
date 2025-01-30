"""Test importing xlsx files."""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from kukur.source.excel import parse_excel


def test_gpx() -> None:
    path = Path("tests/test_data/excel/data.xlsx").resolve()
    table = parse_excel(path)
    assert len(table) == 251
    assert "series name" in table.column_names
    assert "ts" in table.column_names
    assert "value" in table.column_names
