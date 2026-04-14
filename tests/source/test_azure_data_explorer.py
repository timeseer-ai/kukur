# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from random import random
from typing import Any
from unittest.mock import patch

import pytest
from azure.kusto.data.exceptions import KustoMultiApiError, KustoThrottlingError

from kukur import DataType, SeriesSearch, SeriesSelector
from kukur.metadata import Metadata, fields
from kukur.source.azure_data_explorer import from_config
from kukur.source.azure_data_explorer.azure_data_explorer import _Sleeper
from kukur.source.metadata import MetadataMapper, MetadataValueMapper
from kukur.source.token_cache import NullTokenCache


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
    if (
        query
        == "['telemetry-metadata'] | distinct deviceId, plant, location, ['data type']"
    ):
        return MockKustoResponse(
            [
                {
                    "deviceId": "sim000001",
                    "plant": "Plant01",
                    "location": "Antwerp",
                    "data type": "float",
                },
                {
                    "deviceId": "sim000002",
                    "plant": "Plant02",
                    "location": "Antwerp",
                    "data type": None,
                },
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


def get_data_response_custom_query(_database, query, props):
    assert "telemetry-custom-data" in query
    assert "loc" in query
    assert props.get_parameter("loc", "") == "Antwerp"
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
    response_data = response_data[offset:]
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


def get_throttle_all(_database, query, props):
    raise KustoThrottlingError()


class _ThrottleMock:
    def __init__(self):
        self.count = 0

    def get_throttle_3_times(self, _database, query, props):
        if self.count < 3:
            self.count = self.count + 1
            raise KustoThrottlingError()
        return get_data_response_custom_query(_database, query, props)


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
        NullTokenCache(),
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
        NullTokenCache(),
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
        NullTokenCache(),
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
        NullTokenCache(),
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
        NullTokenCache(),
    )
    metadata_list = list(source.search(SeriesSearch("my_source")))
    assert len(metadata_list) == 6
    for metadata in metadata_list:
        assert isinstance(metadata, Metadata)
        assert "plant" in metadata.series.tags
        assert "location" in metadata.series.tags


@patch("azure.kusto.data.KustoClient.execute", side_effect=source_structure_queries)
def test_search_with_custom_query(_kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "tag_columns": ["deviceId", "plant", "location"],
            "field_columns": ["pressure", "temperature"],
            "metadata_columns": ["data type"],
            "list_query": "['telemetry-metadata'] | distinct deviceId, plant, location, ['data type']",
        },
        MetadataMapper(),
        MetadataValueMapper.from_config({"data type": {"FLOAT64": ["float"]}}),
        NullTokenCache(),
    )
    all_metadata = list(source.search(SeriesSearch("my_source")))
    assert len(all_metadata) == 4
    field_counter = Counter([metadata.series.field for metadata in all_metadata])
    assert field_counter["pressure"] == 2
    assert field_counter["temperature"] == 2
    for idx, metadata in enumerate(all_metadata):
        assert isinstance(metadata, Metadata)
        assert "plant" in metadata.series.tags
        assert "location" in metadata.series.tags
        if idx == 0:
            assert metadata.get_field(fields.DataType) == DataType.FLOAT64


@patch(
    "azure.kusto.data.KustoClient.execute", side_effect=get_data_response_custom_query
)
def test_get_data_custom_query(kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "tag_columns": ["deviceId", "plant", "location"],
            "field_columns": ["pressure", "temperature"],
            "data_query": "['telemetry-custom-data'] | where ['location'] == loc",
            "data_query_named_parameters": {"loc": "location"},
            "max_items_per_call": 3,
        },
        MetadataMapper(),
        MetadataValueMapper(),
        NullTokenCache(),
    )
    selector = SeriesSelector(
        "my_source", {"location": "Antwerp", "plant": "Plant02"}, "pressure"
    )
    initial_date = datetime.now(tz=timezone.utc) - timedelta(minutes=20)
    final_date = datetime.now(tz=timezone.utc)
    data = source.get_data(selector, initial_date, final_date)
    assert len(data) == 10


class _TestSleeper(_Sleeper):
    def __init__(self):
        self.count = 0

    def sleep(self, _sleep_seconds: int):
        self.count = self.count + 1


@patch("azure.kusto.data.KustoClient.execute", side_effect=get_throttle_all)
def test_get_data_throttle(kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "tag_columns": ["deviceId", "plant", "location"],
            "field_columns": ["pressure", "temperature"],
            "data_query": "['telemetry-custom-data'] | where ['location'] == loc",
            "data_query_named_parameters": {"loc": "location"},
            "max_items_per_call": 3,
            "throttle_backoff_count": 7,
        },
        MetadataMapper(),
        MetadataValueMapper(),
        NullTokenCache(),
    )
    source._sleeper = _TestSleeper()
    selector = SeriesSelector(
        "my_source", {"location": "Antwerp", "plant": "Plant02"}, "pressure"
    )
    initial_date = datetime.now(tz=timezone.utc) - timedelta(minutes=20)
    final_date = datetime.now(tz=timezone.utc)

    with pytest.raises(KustoThrottlingError):
        source.get_data(selector, initial_date, final_date)
    assert source._sleeper.count == 7


@patch(
    "azure.kusto.data.KustoClient.execute",
    side_effect=_ThrottleMock().get_throttle_3_times,
)
def test_get_data_throttle_resolved(kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "tag_columns": ["deviceId", "plant", "location"],
            "field_columns": ["pressure", "temperature"],
            "data_query": "['telemetry-custom-data'] | where ['location'] == loc",
            "data_query_named_parameters": {"loc": "location"},
            "max_items_per_call": 3,
        },
        MetadataMapper(),
        MetadataValueMapper(),
        NullTokenCache(),
    )
    source._sleeper = _TestSleeper()
    selector = SeriesSelector(
        "my_source", {"location": "Antwerp", "plant": "Plant02"}, "pressure"
    )
    initial_date = datetime.now(tz=timezone.utc) - timedelta(minutes=20)
    final_date = datetime.now(tz=timezone.utc)

    data = source.get_data(selector, initial_date, final_date)
    assert len(data) == 10
    assert source._sleeper.count == 3
