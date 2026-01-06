# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import re
from datetime import datetime, timedelta, timezone
from random import random
from typing import Any
from unittest.mock import patch

from azure.kusto.data.exceptions import KustoMultiApiError

from kukur import SeriesSearch, SeriesSelector
from kukur.metadata import Metadata
from kukur.source.azure_data_explorer import from_config
from kukur.source.metadata import MetadataMapper, MetadataValueMapper


class MockKustoResponse:
    primary_results: list[list[dict[str, Any] | list[str]]]

    def __init__(self, results_list: list[dict[str, Any] | list[str]]):
        self.primary_results = [results_list]


def source_structure_queries(_, query) -> MockKustoResponse:
    if query == "['telemetry-data'] | distinct deviceId, plant, location":
        return MockKustoResponse(
            [
                {"deviceId": "sim000001", "plant": "Plant01", "location": "Antwerp"},
                {"deviceId": "sim000002", "plant": "Plant02", "location": "Antwerp"},
                {"deviceId": "sim000003", "plant": "Plant03", "location": "Curitiba"},
            ]
        )
    if "summarize" in query:
        return MockKustoResponse(
            [
                {
                    "deviceId": "sim000001",
                    "plant": "Plant01",
                    "sensorModel": "AST20PT",
                    "location": "Antwerp",
                },
                {
                    "deviceId": "sim000002",
                    "plant": "Plant02",
                    "sensorModel": "AST20PT",
                    "location": "Antwerp",
                },
                {
                    "deviceId": "sim000003",
                    "plant": "Plant03",
                    "sensorModel": "AST20PT",
                    "location": "Curitiba",
                },
            ]
        )
    if "deviceId" in query and "distinct" in query:
        return MockKustoResponse([["sim000001"], ["sim000002"], ["sim000003"]])
    if "plant" in query and "distinct" in query:
        return MockKustoResponse([["Plant01"], ["Plant02"], ["Plant03"]])
    if "location" in query and "distinct" in query:
        return MockKustoResponse([["Antwerp"], ["Curitiba"]])
    if "sensorModel" in query and "distinct" in query:
        return MockKustoResponse([["AST20PT"]])
    return MockKustoResponse([])


def get_data_response(_database, query, _params):
    limit = _get_limit_from_query(query)
    offset = _get_offset_from_query(query)
    response_data = [
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=10),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=9),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=8),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=7),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=6),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=5),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=4),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=3),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=2),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=1),
            "pressure": random() * 100,
        },
    ]
    if offset is not None:
        response_data = response_data[offset:]
    if limit is not None:
        response_data = response_data[:limit]

    return MockKustoResponse(response_data)


def result_set_too_large_response(_database, query, _params):
    limit = _get_limit_from_query(query)
    offset = _get_offset_from_query(query)

    if limit == 6:
        raise KustoMultiApiError(
            [
                {
                    "OneApiErrors": [
                        {
                            "error": {
                                "code": "",
                                "message": "",
                                "@message": "E_QUERY_RESULT_SET_TOO_LARGE",
                            }
                        }
                    ]
                }
            ]
        )
    assert limit == 3

    response_data = [
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=10),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=9),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=8),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=7),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=6),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=5),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=4),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=3),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=2),
            "pressure": random() * 100,
        },
        {
            "ts": datetime.now(tz=timezone.utc) - timedelta(minutes=1),
            "pressure": random() * 100,
        },
    ]
    if offset is not None:
        response_data = response_data[offset:]
    if limit is not None:
        response_data = response_data[:limit]

    return MockKustoResponse(response_data)


def _get_limit_from_query(query) -> int | None:
    limit = None
    match = re.search(r"\btake (\d+)", query)
    if match is not None:
        limit = int(match.group(1))
    return limit


def _get_offset_from_query(query) -> int | None:
    offset = None
    match = re.search(r"\brow_number\(\) > (\d+)", query)
    if match is not None:
        offset = int(match.group(1))
    return offset


@patch("azure.kusto.data.KustoClient.execute", side_effect=get_data_response)
def test_get_data(kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "table": "telemetry-data",
            "tag_columns": ["plant", "location"],
            "field_columns": ["pressure"],
        },
        MetadataMapper(),
        MetadataValueMapper(),
    )
    selector = SeriesSelector(
        "my_source", {"location": "Curitiba", "plant": "Plant02"}, "pressure"
    )
    initial_date = datetime.now(tz=timezone.utc) - timedelta(minutes=20)
    final_date = datetime.now(tz=timezone.utc)
    data = source.get_data(selector, initial_date, final_date)
    args = kusto_client.call_args.args
    assert args[0] == "telemetry"
    assert "['location']==tag_1" in args[1]
    assert "['plant']==tag_0" in args[1]
    assert "['ts'] >= todatetime(startDate)" in args[1]
    assert "['ts'] <= todatetime(endDate)" in args[1]
    assert "Curitiba" in args[2].get_parameter("tag_1", None)
    assert "Plant02" in args[2].get_parameter("tag_0", None)
    assert "project ['ts'], ['pressure']" in args[1]
    assert len(data) == 10


@patch("azure.kusto.data.KustoClient.execute", side_effect=get_data_response)
def test_get_data_multiple_calls(kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "table": "telemetry-data",
            "tag_columns": ["plant", "location"],
            "field_columns": ["pressure", "temperature"],
            "max_items_per_call": 3,
        },
        MetadataMapper(),
        MetadataValueMapper(),
    )
    selector = SeriesSelector(
        "my_source", {"location": "Curitiba", "plant": "Plant02"}, "pressure"
    )
    initial_date = datetime.now(tz=timezone.utc) - timedelta(minutes=20)
    final_date = datetime.now(tz=timezone.utc)
    data = source.get_data(selector, initial_date, final_date)
    args = kusto_client.call_args.args
    assert args[0] == "telemetry"
    assert "['location']==tag_1" in args[1]
    assert "['plant']==tag_0" in args[1]
    assert "['ts'] >= todatetime(startDate)" in args[1]
    assert "['ts'] <= todatetime(endDate)" in args[1]
    assert "Curitiba" in args[2].get_parameter("tag_1", None)
    assert "Plant02" in args[2].get_parameter("tag_0", None)
    assert len(data) == 10


@patch(
    "azure.kusto.data.KustoClient.execute", side_effect=result_set_too_large_response
)
def test_result_set_too_large(kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "table": "telemetry-data",
            "tag_columns": ["plant", "location"],
            "field_columns": ["pressure", "temperature"],
            "max_items_per_call": 6,
        },
        MetadataMapper(),
        MetadataValueMapper(),
    )
    selector = SeriesSelector(
        "my_source", {"location": "Curitiba", "plant": "Plant02"}, "pressure"
    )
    initial_date = datetime.now(tz=timezone.utc) - timedelta(minutes=20)
    final_date = datetime.now(tz=timezone.utc)
    data = source.get_data(selector, initial_date, final_date)
    assert kusto_client.call_count == 5
    assert len(data) == 10


@patch("azure.kusto.data.KustoClient.execute", side_effect=source_structure_queries)
def test_search_with_metadata(_kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "table": "telemetry-data",
            "tag_columns": ["deviceId", "plant", "location"],
            "field_columns": ["pressure", "temperature"],
            "metadata_columns": ["sensorModel"],
        },
        MetadataMapper(),
        MetadataValueMapper(),
    )
    metadata_list = list(source.search(SeriesSearch("my_source")))
    assert len(metadata_list) == 6
    for metadata in metadata_list:
        assert isinstance(metadata, Metadata)
        assert metadata.get_field_by_name("sensorModel") == "AST20PT"


@patch("azure.kusto.data.KustoClient.execute", side_effect=source_structure_queries)
def test_search_without_metadata(_kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "table": "telemetry-data",
            "tag_columns": ["deviceId", "plant", "location"],
            "field_columns": ["pressure", "temperature"],
        },
        MetadataMapper(),
        MetadataValueMapper(),
    )
    metadata_list = list(source.search(SeriesSearch("my_source")))
    assert len(metadata_list) == 6
    for metadata in metadata_list:
        assert isinstance(metadata, Metadata)
        assert "plant" in metadata.series.tags
        assert "location" in metadata.series.tags
