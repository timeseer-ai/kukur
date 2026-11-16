"""Test the Cognite Data Fusion source."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import math
import tomllib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from kukur import DataType, InterpolationType, SeriesSearch, SeriesSelector, quality
from kukur.exceptions import (
    DataNotFoundException,
    InvalidDataError,
    InvalidSourceException,
)
from kukur.metadata import fields
from kukur.source import SourceFactory
from kukur.source.cognite import cognite
from kukur.source.token_cache import InMemoryTokenCacheFactory, Token

BASE_URL = "https://example.cognitedata.com"
TOKEN_URL = "https://login.example.org/tenant/oauth2/v2.0/token"
CONFIG = {
    "type": "cognite",
    "base_url": BASE_URL,
    "project": "test-project",
    "oidc_token_url": TOKEN_URL,
    "client_id": "client",
    "client_secret": "secret",
}

START = datetime(2026, 11, 1, tzinfo=UTC)
END = datetime(2026, 11, 2, tzinfo=UTC)
START_MS = 1793491200000
END_MS = START_MS + 86400000

GOOD_CLAMPED = 0x00300000
UNCERTAIN = 0x40000000
BAD = 0x80000000
BAD_COMMUNICATION_ERROR = 0x80050000

TS_VIEW_KEY = "CogniteTimeSeries/v1"


class MockResponse:
    def __init__(self, json_data: Any, status_code: int = 200):
        self.json_data = json_data
        self.status_code = status_code
        self.text = str(json_data)

    def json(self) -> Any:
        return self.json_data


class FakeCognite:
    """Answer POST requests from a queue of responses per path."""

    def __init__(self, responses: dict[str, list[MockResponse]]):
        self.responses = responses
        self.requests: list[tuple[str, Any]] = []

    def post(self, url: str, json=None, data=None, timeout=None) -> MockResponse:
        assert timeout is not None
        for path, queue in self.responses.items():
            if url.endswith(path):
                self.requests.append((path, json if json is not None else data))
                return queue.pop(0)
        raise AssertionError(f"unexpected request: {url}")

    def bodies(self, path: str) -> list[Any]:
        return [body for request_path, body in self.requests if request_path == path]


class StaticTokenCache:
    def get_token(self, token_fn: Callable[[str | None], Token]) -> str:
        return "token"


class FakeSleeper:
    def __init__(self):
        self.sleeps: list[float] = []

    def sleep(self, sleep_seconds: float):
        self.sleeps.append(sleep_seconds)


def make_source(**options) -> cognite.CogniteSource:
    source = cognite.from_config(CONFIG | options, StaticTokenCache())
    source._sleeper = FakeSleeper()
    return source


def node(space: str, external_id: str, **properties) -> dict:
    return {
        "instanceType": "node",
        "space": space,
        "externalId": external_id,
        "properties": {"cdf_cdm": {TS_VIEW_KEY: properties}},
    }


def unit_node(external_id: str, symbol: str) -> dict:
    return {
        "instanceType": "node",
        "space": "cdf_cdm_units",
        "externalId": external_id,
        "properties": {"cdf_cdm": {"CogniteUnit/v1": {"symbol": symbol}}},
    }


def data_response(datapoints: list[dict], **series) -> MockResponse:
    item = {
        "instanceId": {"space": "plant", "externalId": "pump_1_flow"},
        "type": "numeric",
        "isString": False,
        "isStep": False,
        "datapoints": datapoints,
    } | series
    return MockResponse({"items": [item]})


@pytest.mark.parametrize(
    "key", ["base_url", "project", "oidc_token_url", "client_id", "client_secret"]
)
def test_required_keys(key: str):
    config = dict(CONFIG)
    del config[key]
    with pytest.raises(InvalidSourceException, match=key):
        cognite.from_config(config, StaticTokenCache())


def test_spaces_must_be_a_list():
    with pytest.raises(InvalidSourceException):
        cognite.from_config(CONFIG | {"spaces": "plant"}, StaticTokenCache())


def test_default_scope():
    config = cognite.CogniteConfiguration.from_data(
        CONFIG | {"base_url": BASE_URL + "/"}
    )
    assert config.base_url == BASE_URL
    assert config.oidc.scope == f"{BASE_URL}/.default"


def test_configured_scope():
    config = cognite.CogniteConfiguration.from_data(CONFIG | {"scope": "custom"})
    assert config.oidc.scope == "custom"


def test_empty_scope_uses_default():
    config = cognite.CogniteConfiguration.from_data(CONFIG | {"scope": ""})
    assert config.oidc.scope == f"{BASE_URL}/.default"


def test_documented_configuration():
    """The configuration template in the documentation works when copied as-is."""
    documentation = Path("docs/source/cognite.asciidoc").read_text(encoding="utf-8")
    template = documentation.split("```toml\n")[1].split("```")[0]
    source_config = tomllib.loads(template.replace("<name>", "Cognite"))["source"][
        "Cognite"
    ]

    config = cognite.CogniteConfiguration.from_data(source_config)

    assert config.oidc.scope == "https://<cluster>.cognitedata.com/.default"
    assert config.spaces == []
    assert config.proxy == ""
    session = cognite._new_session(config)
    assert session.proxies == {}
    assert session.verify is True


def test_factory():
    source = SourceFactory({"source": {"Cognite": CONFIG}}).get_source("Cognite")
    assert source is not None


def test_search_pages_through_all_spaces():
    fake = FakeCognite(
        {
            "models/instances/list": [
                MockResponse(
                    {
                        "items": [
                            node(
                                "plant",
                                "pump_1_flow",
                                type="numeric",
                                isStep=False,
                                description="Flow of pump 1",
                                unit={
                                    "space": "cdf_cdm_units",
                                    "externalId": "volume_flow_rate:m3-per-hr",
                                },
                                sourceUnit="m3/h",
                            ),
                            node("plant", "pump_1_state", type="state"),
                        ],
                        "nextCursor": "cursor-1",
                    }
                ),
                MockResponse(
                    {
                        "items": [
                            node(
                                "lab",
                                "sample_id",
                                type="string",
                                isStep=True,
                                sourceUnit="",
                            )
                        ]
                    }
                ),
            ],
            "models/instances/byids": [
                MockResponse(
                    {"items": [unit_node("volume_flow_rate:m3-per-hr", "m³/h")]}
                )
            ],
        }
    )
    source = make_source()
    with patch("requests.Session.post", side_effect=fake.post):
        results = list(source.search(SeriesSearch("Cognite")))

    assert [result.series.name for result in results] == [
        "pump_1_flow,space=plant",
        "sample_id,space=lab",
    ]
    flow, sample = results
    assert flow.get_field(fields.Description) == "Flow of pump 1"
    assert flow.get_field(fields.Unit) == "m³/h"
    assert flow.get_field(fields.DataType) == DataType.FLOAT64
    assert flow.get_field(fields.InterpolationType) == InterpolationType.LINEAR
    assert sample.get_field(fields.Unit) == ""
    assert sample.get_field(fields.DataType) == DataType.STRING
    assert sample.get_field(fields.InterpolationType) == InterpolationType.STEPPED

    first, second = fake.bodies("models/instances/list")
    assert first["instanceType"] == "node"
    assert first["limit"] == 1000
    assert first["sources"] == [{"source": cognite.TIME_SERIES_VIEW}]
    assert first["filter"] == {"hasData": [cognite.TIME_SERIES_VIEW]}
    assert "cursor" not in first
    assert second["cursor"] == "cursor-1"
    (units_body,) = fake.bodies("models/instances/byids")
    assert units_body["items"] == [
        {
            "instanceType": "node",
            "space": "cdf_cdm_units",
            "externalId": "volume_flow_rate:m3-per-hr",
        }
    ]
    assert units_body["sources"] == [{"source": cognite.UNIT_VIEW}]


def test_search_one_space_has_no_space_tag():
    fake = FakeCognite(
        {
            "models/instances/list": [
                MockResponse({"items": [node("plant", "pump_1_flow", type="numeric")]})
            ]
        }
    )
    source = make_source(spaces=["plant"])
    with patch("requests.Session.post", side_effect=fake.post):
        results = list(source.search(SeriesSearch("Cognite")))

    assert [result.series.name for result in results] == ["pump_1_flow"]
    assert results[0].series.tags == {"series name": "pump_1_flow"}
    (body,) = fake.bodies("models/instances/list")
    assert body["filter"] == {
        "and": [
            {"hasData": [cognite.TIME_SERIES_VIEW]},
            {"in": {"property": ["node", "space"], "values": ["plant"]}},
        ]
    }


def test_search_several_spaces_has_space_tag():
    fake = FakeCognite(
        {
            "models/instances/list": [
                MockResponse({"items": [node("plant", "pump_1_flow", type="numeric")]})
            ]
        }
    )
    source = make_source(spaces=["plant", "lab"])
    with patch("requests.Session.post", side_effect=fake.post):
        results = list(source.search(SeriesSearch("Cognite")))

    assert results[0].series.tags == {"series name": "pump_1_flow", "space": "plant"}


def test_search_uses_source_unit_when_units_cannot_be_read():
    unit = {"space": "cdf_cdm_units", "externalId": "temperature:deg_c"}
    other_unit = {"space": "cdf_cdm_units", "externalId": "pressure:bar"}
    fake = FakeCognite(
        {
            "models/instances/list": [
                MockResponse(
                    {
                        "items": [
                            node(
                                "plant", "t1", type="numeric", unit=unit, sourceUnit="C"
                            )
                        ],
                        "nextCursor": "cursor-1",
                    }
                ),
                MockResponse(
                    {
                        "items": [
                            node(
                                "plant",
                                "p1",
                                type="numeric",
                                unit=other_unit,
                                sourceUnit="bar",
                            )
                        ]
                    }
                ),
            ],
            "models/instances/byids": [
                MockResponse({"error": {"code": 403, "message": "Forbidden"}}, 403)
            ],
        }
    )
    source = make_source()
    with patch("requests.Session.post", side_effect=fake.post):
        results = list(source.search(SeriesSearch("Cognite")))

    assert [result.get_field(fields.Unit) for result in results] == ["C", "bar"]
    assert len(fake.bodies("models/instances/byids")) == 1


def test_get_metadata():
    fake = FakeCognite(
        {
            "models/instances/byids": [
                MockResponse(
                    {
                        "items": [
                            node(
                                "plant",
                                "pump_1_flow",
                                type="numeric",
                                isStep=True,
                                description="Flow of pump 1",
                                unit={
                                    "space": "cdf_cdm_units",
                                    "externalId": "volume_flow_rate:m3-per-hr",
                                },
                            )
                        ]
                    }
                ),
                MockResponse(
                    {"items": [unit_node("volume_flow_rate:m3-per-hr", "m³/h")]}
                ),
            ]
        }
    )
    source = make_source()
    selector = SeriesSelector(
        "Cognite", {"series name": "pump_1_flow", "space": "plant"}
    )
    with patch("requests.Session.post", side_effect=fake.post):
        metadata = source.get_metadata(selector)

    assert metadata.get_field(fields.Description) == "Flow of pump 1"
    assert metadata.get_field(fields.Unit) == "m³/h"
    assert metadata.get_field(fields.InterpolationType) == InterpolationType.STEPPED
    series_body, _ = fake.bodies("models/instances/byids")
    assert series_body == {
        "items": [
            {"instanceType": "node", "space": "plant", "externalId": "pump_1_flow"}
        ],
        "sources": [{"source": cognite.TIME_SERIES_VIEW}],
    }


def test_get_metadata_unknown_series():
    fake = FakeCognite({"models/instances/byids": [MockResponse({"items": []})]})
    source = make_source()
    selector = SeriesSelector("Cognite", {"series name": "unknown", "space": "plant"})
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(DataNotFoundException),
    ):
        source.get_metadata(selector)


def test_space_tag_required_without_one_space():
    source = make_source()
    with pytest.raises(DataNotFoundException, match="space"):
        source.get_metadata(SeriesSelector("Cognite", "pump_1_flow"))


def test_one_space_does_not_need_space_tag():
    fake = FakeCognite(
        {
            "models/instances/byids": [
                MockResponse({"items": [node("plant", "pump_1_flow", type="numeric")]})
            ]
        }
    )
    source = make_source(spaces=["plant"])
    with patch("requests.Session.post", side_effect=fake.post):
        source.get_metadata(SeriesSelector("Cognite", "pump_1_flow"))

    (body,) = fake.bodies("models/instances/byids")
    assert body["items"][0]["space"] == "plant"


@pytest.mark.parametrize("spaces", [["plant"], ["plant", "lab"]])
def test_space_tag_must_be_a_configured_space(spaces: list[str]):
    source = make_source(spaces=spaces)
    selector = SeriesSelector(
        "Cognite", {"series name": "pump_1_flow", "space": "other"}
    )
    with patch("requests.Session.post") as post:
        with pytest.raises(DataNotFoundException, match="other"):
            source.get_metadata(selector)
        with pytest.raises(DataNotFoundException, match="other"):
            source.get_data(selector, START, END)
        with pytest.raises(DataNotFoundException, match="other"):
            source.get_plot_data(selector, START, END, 10)

    post.assert_not_called()


def test_get_metadata_state_series():
    fake = FakeCognite(
        {
            "models/instances/byids": [
                MockResponse({"items": [node("plant", "pump_1_state", type="state")]})
            ]
        }
    )
    source = make_source(spaces=["plant"])
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(InvalidDataError, match="state"),
    ):
        source.get_metadata(SeriesSelector("Cognite", "pump_1_state"))


def test_get_data_pages_and_quality():
    fake = FakeCognite(
        {
            "timeseries/data/list": [
                data_response(
                    [
                        {"timestamp": START_MS, "value": 5},
                        {
                            "timestamp": START_MS + 1000,
                            "value": 6.5,
                            "status": {"code": GOOD_CLAMPED, "symbol": "GoodClamped"},
                        },
                        {
                            "timestamp": START_MS + 2000,
                            "value": 7.0,
                            "status": {
                                "code": UNCERTAIN,
                                "symbol": "Uncertain",
                            },
                        },
                    ],
                    nextCursor="cursor-1",
                ),
                data_response(
                    [
                        {
                            "timestamp": START_MS + 3000,
                            "value": "NaN",
                            "status": {
                                "code": BAD_COMMUNICATION_ERROR,
                                "symbol": "BadCommunicationError",
                            },
                        },
                        {
                            "timestamp": START_MS + 4000,
                            "value": "-Infinity",
                            "status": {"code": BAD, "symbol": "Bad"},
                        },
                        {
                            "timestamp": START_MS + 5000,
                            "status": {"symbol": "Bad"},
                        },
                    ]
                ),
            ]
        }
    )
    source = make_source()
    selector = SeriesSelector(
        "Cognite", {"series name": "pump_1_flow", "space": "plant"}
    )
    with patch("requests.Session.post", side_effect=fake.post):
        table = source.get_data(selector, START, END)

    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"].to_pylist() == [
        START + timedelta(seconds=seconds) for seconds in range(6)
    ]
    values = table["value"].to_pylist()
    assert values[:3] == [5.0, 6.5, 7.0]
    assert math.isnan(values[3])
    assert values[4] == -math.inf
    assert values[5] is None
    assert table["quality"].to_pylist() == [0, 0, 1, 1, 1, 1]
    assert quality.get_mapping(table) == quality.DEFAULT_MAPPING

    first, second = fake.bodies("timeseries/data/list")
    assert first == {
        "items": [
            {
                "instanceId": {"space": "plant", "externalId": "pump_1_flow"},
                "start": START_MS,
                "end": END_MS,
                "limit": 100000,
                "includeStatus": True,
                "ignoreBadDataPoints": False,
                "treatUncertainAsBad": False,
            }
        ],
        "ignoreUnknownIds": True,
    }
    assert second["items"][0]["cursor"] == "cursor-1"


def test_get_data_string_series():
    fake = FakeCognite(
        {
            "timeseries/data/list": [
                data_response(
                    [
                        {"timestamp": START_MS, "value": "open"},
                        {"timestamp": START_MS + 1000, "status": {"code": BAD}},
                    ],
                    type="string",
                    isString=True,
                )
            ]
        }
    )
    source = make_source(spaces=["plant"])
    with patch("requests.Session.post", side_effect=fake.post):
        table = source.get_data(SeriesSelector("Cognite", "pump_1_flow"), START, END)

    assert table["value"].to_pylist() == ["open", None]
    assert table["quality"].to_pylist() == [0, 1]


def test_get_data_state_series():
    fake = FakeCognite(
        {
            "timeseries/data/list": [
                data_response(
                    [{"timestamp": START_MS, "numericValue": 1}],
                    type="state",
                    nextCursor="cursor-1",
                )
            ]
        }
    )
    source = make_source(spaces=["plant"])
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(InvalidDataError),
    ):
        source.get_data(SeriesSelector("Cognite", "pump_1_flow"), START, END)

    # The first page is enough to reject a state time series.
    assert len(fake.bodies("timeseries/data/list")) == 1


def test_get_data_unknown_series():
    fake = FakeCognite({"timeseries/data/list": [MockResponse({"items": []})]})
    source = make_source(spaces=["plant"])
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(DataNotFoundException),
    ):
        source.get_data(SeriesSelector("Cognite", "unknown"), START, END)


def test_get_data_empty_range():
    source = make_source(spaces=["plant"])
    with patch("requests.Session.post") as post:
        table = source.get_data(SeriesSelector("Cognite", "pump_1_flow"), START, START)

    post.assert_not_called()
    assert len(table) == 0
    assert table.column_names == ["ts", "value", "quality"]


def test_get_data_limits_range_to_supported_timestamps():
    fake = FakeCognite({"timeseries/data/list": [data_response([])]})
    source = make_source(spaces=["plant"])
    with patch("requests.Session.post", side_effect=fake.post):
        table = source.get_data(
            SeriesSelector("Cognite", "pump_1_flow"),
            datetime(1800, 1, 1, tzinfo=UTC),
            datetime(2200, 1, 1, tzinfo=UTC),
        )

    assert len(table) == 0
    (body,) = fake.bodies("timeseries/data/list")
    assert body["items"][0]["start"] == -2208988800000
    assert body["items"][0]["end"] == 4102444800000


def test_get_plot_data_uses_min_and_max():
    hour_ms = 3600000
    fake = FakeCognite(
        {
            "timeseries/data/list": [
                data_response(
                    [
                        {
                            "timestamp": START_MS,
                            "minDatapoint": {
                                "timestamp": START_MS + 3000,
                                "value": 1.0,
                            },
                            "maxDatapoint": {
                                "timestamp": START_MS + 1000,
                                "value": 9.0,
                                "status": {"code": UNCERTAIN},
                            },
                        },
                        {
                            "timestamp": START_MS + 2 * hour_ms,
                            "minDatapoint": {
                                "timestamp": START_MS + 2 * hour_ms,
                                "value": 4.0,
                            },
                            "maxDatapoint": {
                                "timestamp": START_MS + 2 * hour_ms,
                                "value": 4.0,
                            },
                        },
                    ],
                    nextCursor="cursor-1",
                ),
                data_response(
                    [
                        {
                            "timestamp": END_MS - 2 * hour_ms,
                            "minDatapoint": {
                                "timestamp": END_MS - 1000,
                                "value": 2.0,
                            },
                            "maxDatapoint": {"timestamp": END_MS + 1000, "value": 3.0},
                        }
                    ]
                ),
            ]
        }
    )
    source = make_source(spaces=["plant"])
    with patch("requests.Session.post", side_effect=fake.post):
        table = source.get_plot_data(
            SeriesSelector("Cognite", "pump_1_flow"), START, END, 10
        )

    assert table["ts"].to_pylist() == [
        START + timedelta(seconds=1),
        START + timedelta(seconds=3),
        START + timedelta(hours=2),
        END - timedelta(seconds=1),
    ]
    assert table["value"].to_pylist() == [9.0, 1.0, 4.0, 2.0]
    assert table["quality"].to_pylist() == [1, 0, 0, 0]

    first, second = fake.bodies("timeseries/data/list")
    query = first["items"][0]
    assert query["aggregates"] == ["minDatapoint", "maxDatapoint"]
    assert query["granularity"] == "2h"
    assert query["limit"] == 10000
    assert query["includeStatus"] is True
    assert second["items"][0]["cursor"] == "cursor-1"


AGGREGATES_REJECTED = MockResponse(
    {"error": {"code": 400, "message": "Aggregates not supported"}}, 400
)


def test_get_plot_data_of_string_series_returns_raw_data():
    fake = FakeCognite(
        {
            "timeseries/data/list": [
                AGGREGATES_REJECTED,
                data_response(
                    [{"timestamp": START_MS - 1000, "value": "closed"}],
                    type="string",
                    isString=True,
                ),
                data_response(
                    [{"timestamp": START_MS, "value": "open"}],
                    type="string",
                    isString=True,
                ),
            ]
        }
    )
    source = make_source(spaces=["plant"])
    with patch("requests.Session.post", side_effect=fake.post):
        table = source.get_plot_data(
            SeriesSelector("Cognite", "pump_1_flow"), START, END, 10
        )

    assert table["value"].to_pylist() == ["open"]
    aggregate_query, type_query, raw_query = fake.bodies("timeseries/data/list")
    assert "aggregates" in aggregate_query["items"][0]
    assert type_query["items"] == [
        {"instanceId": {"space": "plant", "externalId": "pump_1_flow"}, "limit": 1}
    ]
    assert "aggregates" not in raw_query["items"][0]


def test_get_plot_data_of_numeric_series_does_not_return_raw_data():
    fake = FakeCognite(
        {"timeseries/data/list": [AGGREGATES_REJECTED, data_response([])]}
    )
    source = make_source(spaces=["plant"])
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(cognite.CogniteError, match="Aggregates not supported"),
    ):
        source.get_plot_data(SeriesSelector("Cognite", "pump_1_flow"), START, END, 10)

    assert len(fake.bodies("timeseries/data/list")) == 2


def test_get_plot_data_error_on_later_page_does_not_return_raw_data():
    fake = FakeCognite(
        {
            "timeseries/data/list": [
                data_response([], nextCursor="cursor-1"),
                AGGREGATES_REJECTED,
            ]
        }
    )
    source = make_source(spaces=["plant"])
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(cognite.CogniteError, match="Aggregates not supported"),
    ):
        source.get_plot_data(SeriesSelector("Cognite", "pump_1_flow"), START, END, 10)

    assert len(fake.bodies("timeseries/data/list")) == 2


def test_get_plot_data_state_series():
    fake = FakeCognite({"timeseries/data/list": [data_response([], type="state")]})
    source = make_source(spaces=["plant"])
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(InvalidDataError, match="state"),
    ):
        source.get_plot_data(SeriesSelector("Cognite", "pump_1_flow"), START, END, 10)


def test_get_plot_data_short_range_uses_raw_data():
    fake = FakeCognite({"timeseries/data/list": [data_response([])]})
    source = make_source(spaces=["plant"])
    with patch("requests.Session.post", side_effect=fake.post):
        source.get_plot_data(
            SeriesSelector("Cognite", "pump_1_flow"),
            START,
            START + timedelta(seconds=10),
            100,
        )

    (body,) = fake.bodies("timeseries/data/list")
    assert "aggregates" not in body["items"][0]


@pytest.mark.parametrize(
    ("seconds", "interval_count", "granularity"),
    [
        (1, 2, None),
        (1, 1, "1s"),
        (119, 1, "119s"),
        (120, 1, "2m"),
        (7199, 1, "119m"),
        (7200, 1, "2h"),
        (86400, 10, "2h"),
        (100 * 365 * 86400, 1, "100000h"),
    ],
)
def test_granularity(seconds: int, interval_count: int, granularity: str | None):
    assert cognite._get_granularity(0, seconds * 1000, interval_count) == granularity


def test_throttled_requests_are_retried():
    throttled = {"error": {"code": 429, "message": "Too many requests"}}
    fake = FakeCognite(
        {
            "timeseries/data/list": [
                MockResponse(throttled, 429),
                MockResponse({"error": {"code": 503, "message": "Unavailable"}}, 503),
                data_response([{"timestamp": START_MS, "value": 1.0}]),
            ]
        }
    )
    source = make_source(spaces=["plant"])
    with patch("requests.Session.post", side_effect=fake.post):
        table = source.get_data(SeriesSelector("Cognite", "pump_1_flow"), START, END)

    assert table["value"].to_pylist() == [1.0]
    assert source._sleeper.sleeps == [2, 4]


def test_throttled_requests_give_up():
    throttled = {"error": {"code": 429, "message": "Too many requests"}}
    fake = FakeCognite(
        {"timeseries/data/list": [MockResponse(throttled, 429) for _ in range(3)]}
    )
    source = make_source(spaces=["plant"], throttle_backoff_count=2)
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(cognite.CogniteError, match="Too many requests") as err,
    ):
        source.get_data(SeriesSelector("Cognite", "pump_1_flow"), START, END)

    assert err.value.status_code == 429
    assert source._sleeper.sleeps == [2, 4]


def test_error_message():
    fake = FakeCognite(
        {
            "models/instances/list": [
                MockResponse({"error": {"code": 401, "message": "Unauthorized"}}, 401)
            ]
        }
    )
    source = make_source()
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(cognite.CogniteError, match="401: Unauthorized"),
    ):
        list(source.search(SeriesSearch("Cognite")))


class FakeRequest:
    def __init__(self):
        self.headers: dict[str, str] = {}


def test_token_is_requested_once_and_cached():
    fake = FakeCognite(
        {TOKEN_URL: [MockResponse({"access_token": "abc", "expires_in": 3600})]}
    )
    config = cognite.CogniteConfiguration.from_data(CONFIG)
    auth = cognite._TokenCacheAuth(
        config, InMemoryTokenCacheFactory().get_cache("data-Cognite")
    )
    with patch("requests.Session.post", side_effect=fake.post):
        first = auth(FakeRequest())
        second = auth(FakeRequest())

    assert first.headers["Authorization"] == "Bearer abc"
    assert second.headers["Authorization"] == "Bearer abc"
    (body,) = fake.bodies(TOKEN_URL)
    assert body == {
        "grant_type": "client_credentials",
        "client_id": "client",
        "client_secret": "secret",
        "scope": f"{BASE_URL}/.default",
    }


def test_token_error():
    fake = FakeCognite(
        {
            TOKEN_URL: [
                MockResponse(
                    {
                        "error": "invalid_client",
                        "error_description": "AADSTS7000215: Invalid client secret",
                    },
                    401,
                )
            ]
        }
    )
    config = cognite.CogniteConfiguration.from_data(CONFIG)
    auth = cognite._TokenCacheAuth(
        config, InMemoryTokenCacheFactory().get_cache("data-Cognite")
    )
    with (
        patch("requests.Session.post", side_effect=fake.post),
        pytest.raises(cognite.CogniteError, match="AADSTS7000215"),
    ):
        auth(FakeRequest())
