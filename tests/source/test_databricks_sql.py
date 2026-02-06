"""Tests for the Statement Execution API source."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from io import BytesIO
from unittest.mock import patch

import pyarrow as pa
import pytest
from pyarrow import ipc

from kukur import SeriesSearch, SeriesSelector
from kukur.metadata import Metadata
from kukur.source.databricks_sql.databricks_rest import (
    DatabricksError,
    DatabricksStatementExecutionSource,
    StatementExecutionConfiguration,
)
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper

HOST = "example.org"
WAREHOUSE_ID = "4t2"
PASSWORD = "hunter2"

STATEMENT_ID = "01ed9db9-24c4-1cb6-a320-fb6ebbe7410d"

STATEMENT_RUNNING = {"statement_id": STATEMENT_ID, "status": {"state": "RUNNING"}}

STATEMENT_FAILED = {
    "statement_id": STATEMENT_ID,
    "status": {
        "state": "FAILED",
        "error": {"error_code": "BAD_REQUEST", "message": "Syntax Error"},
    },
}

STATEMENT_COMPLETE = {
    "statement_id": STATEMENT_ID,
    "status": {"state": "SUCCEEDED"},
    "result": {
        "external_links": [
            {
                "chunk_index": 0,
                "external_link": "https://provider/data/1",
                "next_chunk_internal_link": f"/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/2",
            }
        ]
    },
}

STATEMENT_NO_DATA = {
    "statement_id": STATEMENT_ID,
    "status": {"state": "SUCCEEDED"},
    "result": {},
}

LAST_CHUNK = {
    "external_links": [
        {
            "chunk_index": 1,
            "external_link": "https://provider/data/2",
        }
    ]
}

METADATA_CHUNK_1 = [["test-tag-1", "test A"]]

METADATA_CHUNK_2 = [["test-tag-2", "test B"]]


DATA_STATEMENT_COMPLETE = {
    "statement_id": STATEMENT_ID,
    "status": {"state": "SUCCEEDED"},
    "result": {
        "external_links": [
            {
                "chunk_index": 0,
                "external_link": "https://provider/data/1",
                "next_chunk_internal_link": f"/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/2",
            }
        ]
    },
}


class MockResponse:
    def __init__(self, json_data: dict | list, status_code: int):
        self.json_data = json_data
        self.status_code = status_code

    def raise_for_status(self):
        return

    def json(self):
        return self.json_data


class MockBinaryResponse:
    def __init__(self, data: BytesIO, status_code: int):
        self.content = data
        self.status_code = status_code

    def raise_for_status(self):
        return

    def json(self):
        return self.json_data


def mock_post(*args, **kwargs) -> MockResponse:
    assert PASSWORD in kwargs["headers"]["Authorization"]
    body = kwargs["json"]
    assert body["warehouse_id"] == WAREHOUSE_ID
    assert body["disposition"] == "EXTERNAL_LINKS"
    assert body["wait_timeout"] == "50s"
    assert "statement" in body
    if "metadata" in body["statement"]:
        assert body["format"] == "JSON_ARRAY"
        return MockResponse(STATEMENT_RUNNING, 200)
    if "data" in body["statement"]:
        assert body["format"] == "ARROW_STREAM"
        assert len(body["parameters"]) == 3
        return MockResponse(DATA_STATEMENT_COMPLETE, 200)
    raise Exception("unknown POST request")


class GetMetadataMock:
    def __init__(self):
        self.index = -1

    def mock_get(self, *args, **kwargs) -> MockResponse:
        self.index = self.index + 1
        url = args[0]
        if url == f"https://{HOST}/api/2.0/sql/statements/{STATEMENT_ID}":
            if self.index == 0:
                return MockResponse(STATEMENT_RUNNING, 200)
            if self.index == 1:
                return MockResponse(STATEMENT_COMPLETE, 200)
        if (
            url
            == f"https://{HOST}/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/2"
        ):
            return MockResponse(LAST_CHUNK, 200)
        if url == "https://provider/data/1":
            return MockResponse(METADATA_CHUNK_1, 200)
        if url == "https://provider/data/2":
            return MockResponse(METADATA_CHUNK_2, 200)
        raise Exception("unknown GET request")


class GetFailMock:
    def __init__(self):
        self.index = -1

    def mock_get(self, *args, **kwargs) -> MockResponse:
        self.index = self.index + 1
        url = args[0]
        if url == f"https://{HOST}/api/2.0/sql/statements/{STATEMENT_ID}":
            if self.index == 0:
                return MockResponse(STATEMENT_RUNNING, 200)
            if self.index == 1:
                return MockResponse(STATEMENT_FAILED, 200)
        raise Exception("unknown GET request")


class GetDataMock:
    def __init__(self):
        self.index = -1

    def mock_get(self, *args, **kwargs) -> MockResponse | MockBinaryResponse:
        self.index = self.index + 1
        url = args[0]
        if (
            url
            == f"https://{HOST}/api/2.0/sql/statements/{STATEMENT_ID}/result/chunks/2"
        ):
            return MockResponse(LAST_CHUNK, 200)
        if url == "https://provider/data/1":
            table = pa.Table.from_pydict(
                {
                    "ts": [datetime.fromisoformat("2026-01-01T00:00:00+00:00")],
                    "value": [1],
                }
            )
            return MockBinaryResponse(_get_ipc_bytes(table), 200)
        if url == "https://provider/data/2":
            table = pa.Table.from_pydict(
                {
                    "ts": [datetime.fromisoformat("2026-01-02T00:00:00+00:00")],
                    "value": [2],
                }
            )
            return MockBinaryResponse(_get_ipc_bytes(table), 200)
        raise Exception("unknown GET request")


def mock_no_data_post(*args, **kwargs) -> MockResponse:
    assert PASSWORD in kwargs["headers"]["Authorization"]
    body = kwargs["json"]
    assert body["warehouse_id"] == WAREHOUSE_ID
    assert body["disposition"] == "EXTERNAL_LINKS"
    assert body["wait_timeout"] == "50s"
    assert "statement" in body
    return MockResponse(STATEMENT_NO_DATA, 200)


def get_source() -> DatabricksStatementExecutionSource:
    config = StatementExecutionConfiguration.from_data(
        {
            "connection": {
                "host": HOST,
                "warehouse_id": WAREHOUSE_ID,
                "password": PASSWORD,
            },
            "tag_columns": ["series name"],
            "metadata_columns": ["description"],
            "list_query": "select name, description from metadata",
            "list_columns": ["series name", "description"],
            "data_query": """
                select ts, value
                from data
                where name = :series_name
                  and ts >= :start_date and ts < :end_date""",
        }
    )
    return DatabricksStatementExecutionSource(
        config, MetadataValueMapper(), QualityMapper()
    )


@patch("requests.Session.post", mock_post)
@patch("requests.Session.get", GetMetadataMock().mock_get)
def test_search_ok() -> None:
    source = get_source()
    all_metadata = list(source.search(SeriesSearch("databricks")))
    assert len(all_metadata) == 2
    first_metadata = all_metadata[0]
    assert isinstance(first_metadata, Metadata)
    assert first_metadata.series.tags["series name"] == "test-tag-1"
    assert first_metadata.get_field_by_name("description") == "test A"


@patch("requests.Session.post", mock_post)
@patch("requests.Session.get", GetFailMock().mock_get)
def test_search_error() -> None:
    source = get_source()
    with pytest.raises(DatabricksError):
        list(source.search(SeriesSearch("databricks")))


@patch("requests.Session.post", mock_post)
@patch("requests.Session.get", GetDataMock().mock_get)
def test_data() -> None:
    source = get_source()
    data = source.get_data(
        SeriesSelector("databricks", "test-tag-1"),
        datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2026-02-01T00:00:00+00:00"),
    )
    assert len(data) == 2
    assert data["value"].to_pylist() == [1, 2]


@patch("requests.Session.post", mock_no_data_post)
def test_no_data() -> None:
    source = get_source()
    data = source.get_data(
        SeriesSelector("databricks", "test-tag-1"),
        datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2026-02-01T00:00:00+00:00"),
    )
    assert len(data) == 0


def _get_ipc_bytes(table: pa.Table) -> BytesIO:
    data = BytesIO()
    writer = ipc.new_stream(data, table.schema)
    writer.write_table(table)
    writer.close()
    data.seek(0)
    return data
