from datetime import datetime, timedelta
from random import random
from typing import Any, Dict, List, Union
from kukur.metadata import Metadata

from kukur.source.azure_data_explorer import from_config
from kukur import SeriesSelector
from kukur.source.metadata import MetadataMapper, MetadataValueMapper

from unittest.mock import patch


class MockKustoResponse:
    primary_results: List[List[Union[Dict[str, Any], List[str]]]]

    def __init__(self, results_list: List[Union[Dict[str, Any], List[str]]]):
        self.primary_results = [results_list]


def source_structure_queries(_, query) -> MockKustoResponse:
    if query == f".show table ['telemetry-data'] schema as json":
        return MockKustoResponse(
            [
                {
                    "Schema": """{\"OrderedColumns\": [
                        {\"Name\": \"ts\"},
                        {\"Name\": \"deviceId\"},
                        {\"Name\": \"plant\"},
                        {\"Name\": \"temperature\"},
                        {\"Name\": \"pressure\"},
                        {\"Name\": \"location\"},
                        {\"Name\": \"sensorModel\"},
                        {\"Name\": \"pressureUnit\"},
                        {\"Name\": \"temperatureUnit\"}
                    ]}"""
                }
            ]
        )
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


@patch("azure.kusto.data.KustoClient.execute", side_effect=source_structure_queries)
def test_source_structure(_kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "table": "telemetry-data",
            "tag_columns": ["deviceId", "plant", "sensorModel", "location"],
            "ignored_columns": ["pressureUnit", "temperatureUnit"],
        },
        None,
        None,
    )
    source_structure = source.get_source_structure(None)
    assert set(source_structure.fields) == {"pressure", "temperature"}
    assert set(source_structure.tag_keys) == {
        "deviceId",
        "plant",
        "sensorModel",
        "location",
    }
    device_id_values = {
        item["value"]
        for item in source_structure.tag_values
        if item["key"] == "deviceId"
    }
    plant_values = {
        item["value"] for item in source_structure.tag_values if item["key"] == "plant"
    }
    location_values = {
        item["value"]
        for item in source_structure.tag_values
        if item["key"] == "location"
    }
    assert device_id_values == {"sim000001", "sim000002", "sim000003"}
    assert plant_values == {"Plant01", "Plant02", "Plant03"}
    assert location_values == {"Antwerp", "Curitiba"}


get_data_response = MockKustoResponse(
    [
        {"ts": datetime.utcnow() - timedelta(minutes=10), "pressure": random() * 100},
        {"ts": datetime.utcnow() - timedelta(minutes=9), "pressure": random() * 100},
        {"ts": datetime.utcnow() - timedelta(minutes=8), "pressure": random() * 100},
        {"ts": datetime.utcnow() - timedelta(minutes=7), "pressure": random() * 100},
    ]
)


@patch("azure.kusto.data.KustoClient.execute", return_value=get_data_response)
def test_get_data(kusto_client) -> None:
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "table": "telemetry-data",
            "tag_columns": ["deviceId", "plant", "sensorModel", "location"],
            "ignored_columns": ["pressureUnit", "temperatureUnit"],
        },
        None,
        None,
    )
    selector = SeriesSelector(
        "my_source", {"location": "Curitiba", "plant": "Plant02"}, "pressure"
    )
    initial_date = datetime.utcnow() - timedelta(minutes=20)
    final_date = datetime.utcnow()
    data = source.get_data(selector, initial_date, final_date)
    args = kusto_client.call_args.args
    assert args[0] == "telemetry"
    assert "['location']=='Curitiba'" in args[1]
    assert "['plant']=='Plant02'" in args[1]
    assert f"['ts'] >= todatetime('{initial_date}')" in args[1]
    assert f"['ts'] <= todatetime('{final_date}')" in args[1]
    assert len(data) == 4


@patch("azure.kusto.data.KustoClient.execute", side_effect=source_structure_queries)
def test_search_with_metadata(_kusto_client) -> None:
    mm = MetadataMapper.from_config({})
    mvp = MetadataValueMapper.from_config({})
    source = from_config(
        {
            "connection_string": "https://test_cluster.net",
            "database": "telemetry",
            "table": "telemetry-data",
            "tag_columns": ["deviceId", "plant", "location"],
            "metadata_columns": ["sensorModel"],
            "ignored_columns": ["pressureUnit", "temperatureUnit"],
        },
        mm,
        mvp,
    )
    selector = SeriesSelector("my_source", {}, "pressure")
    metadata_list = list(source.search(selector))
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
            "ignored_columns": ["pressureUnit", "temperatureUnit", "sensorModel"],
        },
        None,
        None,
    )
    selector = SeriesSelector("my_source", {}, "pressure")
    series = list(source.search(selector))
    assert len(series) == 6
