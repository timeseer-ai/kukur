# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections import Counter
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from random import random
from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pytest
from azure.kusto.data import ClientRequestProperties
from azure.kusto.data.exceptions import KustoMultiApiError, KustoThrottlingError

from kukur import DataType, SeriesSearch, SeriesSelector
from kukur.metadata import Metadata, fields
from kukur.source.azure_data_explorer import (
    DataExplorerConnection,
    DataExplorerConnectionConfiguration,
    from_config,
)
from kukur.source.azure_data_explorer.azure_data_explorer import (
    DataExplorerSource,
    _Sleeper,
)
from kukur.source.metadata import MetadataMapper, MetadataValueMapper
from kukur.source.token_cache import NullTokenCache


class MockStreamingResponse:
    def __init__(self, rows: Iterable[dict[str, Any]]):
        self.__rows = rows

    def iter_primary_results(self) -> Iterator[Iterable[dict[str, Any]]]:
        return iter([self.__rows])


def search_queries(_database, query, properties=None) -> MockStreamingResponse:
    if query == "['telemetry-data'] | distinct deviceId, plant, location":
        return MockStreamingResponse(
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
        return MockStreamingResponse(
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
        return MockStreamingResponse(
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
    return MockStreamingResponse([])


def _data_rows() -> list[dict[str, Any]]:
    now = datetime.now(tz=UTC)
    return [
        {"ts": now - timedelta(minutes=minutes), "pressure": random() * 100}
        for minutes in range(10, 0, -1)
    ]


def get_data_response(_database, query, properties) -> MockStreamingResponse:
    return MockStreamingResponse(_data_rows())


def get_data_response_custom_query(
    _database, query, properties
) -> MockStreamingResponse:
    assert "telemetry-custom-data" in query
    assert "loc" in query
    assert properties.get_parameter("loc", "") == "Antwerp"
    return MockStreamingResponse(_data_rows())


def get_throttle_all(_database, query, properties):
    raise KustoThrottlingError()


class _ThrottleMock:
    def __init__(self):
        self.count = 0

    def get_throttle_3_times(self, _database, query, properties):
        if self.count < 3:
            self.count = self.count + 1
            raise KustoThrottlingError()
        return get_data_response_custom_query(_database, query, properties)


def _source(**config: Any) -> DataExplorerSource:
    return from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "table": "telemetry-data",
            "tag_columns": ["plant", "location"],
            "field_columns": ["pressure", "temperature"],
            **config,
        },
        MetadataMapper(),
        MetadataValueMapper(),
        NullTokenCache(),
    )


def _get_data(source: DataExplorerSource) -> pa.Table:
    selector = SeriesSelector(
        "my_source", {"location": "Curitiba", "plant": "Plant02"}, "pressure"
    )
    return source.get_data(
        selector, datetime.now(tz=UTC) - timedelta(minutes=20), datetime.now(tz=UTC)
    )


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query",
    side_effect=get_data_response,
)
def test_get_data(kusto_client) -> None:
    data = _get_data(_source(field_columns=["pressure"]))
    args = kusto_client.call_args.args
    assert args[0] == "telemetry"
    assert "['location']==tag_1" in args[1]
    assert "['plant']==tag_0" in args[1]
    assert "['ts'] >= todatetime(startDate)" in args[1]
    assert "['ts'] <= todatetime(endDate)" in args[1]
    properties = kusto_client.call_args.kwargs["properties"]
    assert "Curitiba" in properties.get_parameter("tag_1", None)
    assert "Plant02" in properties.get_parameter("tag_0", None)
    assert "project ['ts'], ['pressure']" in args[1]
    assert len(data) == 10


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query",
    side_effect=get_data_response,
)
def test_get_data_runs_one_untruncated_query(kusto_client) -> None:
    data = _get_data(_source(max_items_per_call=3))
    assert kusto_client.call_count == 1
    query = kusto_client.call_args.args[1]
    assert "row_number()" not in query
    assert "take" not in query
    properties = kusto_client.call_args.kwargs["properties"]
    assert properties.get_option("notruncation", False) is True
    assert len(data) == 10


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query",
    return_value=MockStreamingResponse([]),
)
def test_get_data_without_result(_kusto_client) -> None:
    data = _get_data(_source())
    assert len(data) == 0
    assert data.column_names == ["ts", "value"]


def _rows_failing_after_first() -> Iterator[dict[str, Any]]:
    yield _data_rows()[0]
    raise KustoMultiApiError(
        [
            {
                "OneApiErrors": [
                    {
                        "error": {
                            "code": "",
                            "message": "",
                            "@message": "E_RUNAWAY_QUERY",
                        }
                    }
                ]
            }
        ]
    )


@patch("azure.kusto.data.KustoClient.execute_streaming_query")
def test_get_data_error_in_stream(kusto_client) -> None:
    kusto_client.return_value = MockStreamingResponse(_rows_failing_after_first())
    with pytest.raises(KustoMultiApiError):
        _get_data(_source())


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query",
    side_effect=get_data_response,
)
def test_get_data_default_query_timeout(kusto_client) -> None:
    _get_data(_source())
    properties = kusto_client.call_args.kwargs["properties"]
    assert (
        properties.get_option(ClientRequestProperties.request_timeout_option_name, None)
        is None
    )


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query",
    side_effect=get_data_response,
)
def test_get_data_query_timeout(kusto_client) -> None:
    _get_data(_source(query_timeout_seconds=900))
    properties = kusto_client.call_args.kwargs["properties"]
    assert properties.get_option(
        ClientRequestProperties.request_timeout_option_name, None
    ) == timedelta(seconds=900)


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query", side_effect=search_queries
)
def test_search_runs_untruncated_query_with_timeout(kusto_client) -> None:
    source = _source(
        tag_columns=["deviceId", "plant", "location"], query_timeout_seconds=900
    )
    list(source.search(SeriesSearch("my_source")))
    properties = kusto_client.call_args.kwargs["properties"]
    assert properties.get_option("notruncation", False) is True
    assert properties.get_option(
        ClientRequestProperties.request_timeout_option_name, None
    ) == timedelta(seconds=900)


@patch("azure.kusto.data.KustoClient.close")
@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query", side_effect=search_queries
)
def test_search_keeps_connection_open_while_streaming(_kusto_client, close) -> None:
    source = _source(tag_columns=["deviceId", "plant", "location"])
    search = source.search(SeriesSearch("my_source"))
    next(search)
    assert close.call_count == 0
    list(search)
    assert close.call_count == 1


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query",
    return_value=MockStreamingResponse([]),
)
def test_search_without_result(_kusto_client) -> None:
    source = _source(tag_columns=["deviceId", "plant", "location"])
    assert list(source.search(SeriesSearch("my_source"))) == []


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query", side_effect=search_queries
)
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


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query", side_effect=search_queries
)
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


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query", side_effect=search_queries
)
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
    "azure.kusto.data.KustoClient.execute_streaming_query",
    side_effect=get_data_response_custom_query,
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
        },
        MetadataMapper(),
        MetadataValueMapper(),
        NullTokenCache(),
    )
    selector = SeriesSelector(
        "my_source", {"location": "Antwerp", "plant": "Plant02"}, "pressure"
    )
    initial_date = datetime.now(tz=UTC) - timedelta(minutes=20)
    final_date = datetime.now(tz=UTC)
    data = source.get_data(selector, initial_date, final_date)
    assert len(data) == 10


class _TestSleeper(_Sleeper):
    def __init__(self):
        self.count = 0

    def sleep(self, _sleep_seconds: int):
        self.count = self.count + 1


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query", side_effect=get_throttle_all
)
def test_get_data_throttle(kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "tag_columns": ["deviceId", "plant", "location"],
            "field_columns": ["pressure", "temperature"],
            "data_query": "['telemetry-custom-data'] | where ['location'] == loc",
            "data_query_named_parameters": {"loc": "location"},
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
    initial_date = datetime.now(tz=UTC) - timedelta(minutes=20)
    final_date = datetime.now(tz=UTC)

    with pytest.raises(KustoThrottlingError):
        source.get_data(selector, initial_date, final_date)
    assert source._sleeper.count == 7


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query",
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
        },
        MetadataMapper(),
        MetadataValueMapper(),
        NullTokenCache(),
    )
    source._sleeper = _TestSleeper()
    selector = SeriesSelector(
        "my_source", {"location": "Antwerp", "plant": "Plant02"}, "pressure"
    )
    initial_date = datetime.now(tz=UTC) - timedelta(minutes=20)
    final_date = datetime.now(tz=UTC)

    data = source.get_data(selector, initial_date, final_date)
    assert len(data) == 10
    assert source._sleeper.count == 3


def _connection(throttle_backoff_count: int = 8) -> DataExplorerConnection:
    return DataExplorerConnection(
        DataExplorerConnectionConfiguration.from_config(
            {
                "connection_string": "https://test_cluster.net",
                "database": "telemetry",
                "throttle_backoff_count": throttle_backoff_count,
            }
        ),
        NullTokenCache(),
        _TestSleeper().sleep,
    )


@patch("azure.kusto.data.KustoClient.execute_streaming_query")
def test_connection_execute_streaming(kusto_client) -> None:
    props = ClientRequestProperties()
    with _connection() as connection:
        response = connection.execute_streaming("['telemetry-data']", props)

    assert response is kusto_client.return_value
    assert kusto_client.call_args.args == ("telemetry", "['telemetry-data']")
    assert kusto_client.call_args.kwargs["properties"] is props


@patch(
    "azure.kusto.data.KustoClient.execute_streaming_query",
    side_effect=KustoThrottlingError(),
)
def test_connection_execute_streaming_throttle(kusto_client) -> None:
    with _connection(throttle_backoff_count=2) as connection:
        with pytest.raises(KustoThrottlingError):
            connection.execute_streaming("['telemetry-data']")

    assert kusto_client.call_count == 2
