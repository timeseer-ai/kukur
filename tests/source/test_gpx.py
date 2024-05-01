"""Test importing GPX files."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from kukur.source.gpx import parse_gpx


def test_gpx() -> None:
    table = parse_gpx("tests/test_data/gpx/20240501.gpx")
    assert len(table) == 2263
    assert "ele" in table.column_names
    assert "lon" in table.column_names
    assert "lat" in table.column_names
