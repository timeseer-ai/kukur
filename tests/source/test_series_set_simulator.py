"""Unit tests for the series set simulator source."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import yaml
from pytest import raises

from kukur import Metadata, SeriesSearch
from kukur.exceptions import InvalidSourceException
from kukur.metadata import fields
from kukur.source.series_set_simulator import from_config
from kukur.source.series_set_simulator.series_set_simulator import (
    SeriesSetSimulatorSource,
)

START_DATE = datetime(2026, 9, 1, tzinfo=timezone.utc)
END_DATE = START_DATE + timedelta(days=30)


def _make_source(tmp_path: Path, **overrides) -> SeriesSetSimulatorSource:
    config = {
        "seed": 42,
        "seriesSets": 3,
        "seriesPerSet": 4,
        "samplingInterval": {"intervalSecondsMin": 10, "intervalSecondsMax": 300},
        "severity": 0.3,
        "controlAttribute": "active",
    }
    config.update(overrides)
    path = tmp_path / "simulator.yml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return from_config({"path": str(path)})


def _series(source: SeriesSetSimulatorSource) -> list[Metadata]:
    return list(source.search(SeriesSearch("simulator")))


def _intervals(table: pa.Table) -> np.ndarray:
    return np.diff(table["ts"].cast(pa.int64()).to_numpy()) / 1e6


def test_search_yields_one_control_series_per_set(tmp_path: Path):
    series = _series(_make_source(tmp_path))
    assert len(series) == 3 * (4 + 1)
    elements = {metadata.series.tags["series name"] for metadata in series}
    assert elements == {"Plant 1", "Plant 2", "Plant 3"}
    for element in elements:
        attributes = [
            metadata.series.tags["Attribute"]
            for metadata in series
            if metadata.series.tags["series name"] == element
        ]
        assert attributes == [
            "active",
            "Sensor 1",
            "Sensor 2",
            "Sensor 3",
            "Sensor 4",
        ]
    for metadata in series:
        assert metadata.series.field == metadata.series.tags["Attribute"]


def test_every_series_has_a_web_id_like_pi_af(tmp_path: Path):
    series = _series(_make_source(tmp_path))
    web_ids = [metadata.series.tags["__id__"] for metadata in series]
    assert len(set(web_ids)) == len(series)
    assert all(len(web_id) == 161 for web_id in web_ids)
    assert web_ids == [
        metadata.series.tags["__id__"] for metadata in _series(_make_source(tmp_path))
    ]


def test_control_attribute_defaults_to_active(tmp_path: Path):
    config_path = tmp_path / "simulator.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "seriesSets": 1,
                "seriesPerSet": 1,
                "samplingInterval": {
                    "intervalSecondsMin": 60,
                    "intervalSecondsMax": 60,
                },
            }
        ),
        encoding="utf-8",
    )
    series = _series(from_config({"path": str(config_path)}))
    assert [metadata.series.tags["Attribute"] for metadata in series] == [
        "active",
        "Sensor 1",
    ]


def test_same_configuration_generates_same_data(tmp_path: Path):
    first = _make_source(tmp_path)
    second = _make_source(tmp_path)
    for metadata in _series(first):
        assert first.get_data(metadata.series, START_DATE, END_DATE).equals(
            second.get_data(metadata.series, START_DATE, END_DATE)
        )


def test_seed_changes_data(tmp_path: Path):
    first = _make_source(tmp_path)
    selector = _series(first)[1].series
    data = first.get_data(selector, START_DATE, END_DATE)
    other = _make_source(tmp_path, seed=43).get_data(selector, START_DATE, END_DATE)
    assert not data.equals(other)


def test_split_queries_concatenate_to_full_query(tmp_path: Path):
    source = _make_source(tmp_path)
    midnight = START_DATE + timedelta(days=3)
    noon = START_DATE + timedelta(days=5, hours=12, seconds=7)
    for metadata in _series(source):
        full = source.get_data(metadata.series, START_DATE, END_DATE)
        parts = pa.concat_tables(
            [
                source.get_data(metadata.series, START_DATE, midnight),
                source.get_data(metadata.series, midnight, noon),
                source.get_data(metadata.series, noon, END_DATE),
            ]
        )
        assert parts.equals(full)


def test_data_is_within_the_half_open_window(tmp_path: Path):
    source = _make_source(tmp_path)
    selector = _series(source)[0].series
    data = source.get_data(selector, START_DATE, END_DATE)
    timestamps = data["ts"].to_pylist()
    assert timestamps[0] >= START_DATE
    assert timestamps[-1] < END_DATE
    assert np.all(_intervals(data) > 0)


def test_median_interval_within_sampling_interval(tmp_path: Path):
    source = _make_source(tmp_path, severity=0)
    for metadata in _series(source):
        median = np.median(
            _intervals(source.get_data(metadata.series, START_DATE, END_DATE))
        )
        assert 10 <= median <= 300


def test_regular_sampling_without_severity(tmp_path: Path):
    source = _make_source(tmp_path, severity=0)
    for metadata in _series(source):
        intervals = _intervals(source.get_data(metadata.series, START_DATE, END_DATE))
        assert intervals.max() - intervals.min() <= 2e-6


def _gap_statistics(source: SeriesSetSimulatorSource) -> tuple[int, float]:
    count = 0
    duration = 0.0
    for metadata in _series(source):
        intervals = _intervals(source.get_data(metadata.series, START_DATE, END_DATE))
        gaps = intervals[intervals > 2 * np.median(intervals)]
        count += len(gaps)
        duration += gaps.sum()
    return count, duration


def test_gaps_grow_with_severity(tmp_path: Path):
    low_count, low_duration = _gap_statistics(_make_source(tmp_path, severity=0.2))
    high_count, high_duration = _gap_statistics(_make_source(tmp_path, severity=1))
    assert 0 < low_count < high_count
    assert low_duration < high_duration


def test_control_series_has_two_levels(tmp_path: Path):
    source = _make_source(tmp_path, severity=0)
    selector = _series(source)[0].series
    values = source.get_data(selector, START_DATE, END_DATE)["value"].to_numpy()
    low = np.abs(values) < 10
    high = np.abs(values - 50) < 10
    assert np.all(low | high)
    assert low.any()
    assert high.any()


def test_value_series_use_every_function(tmp_path: Path):
    source = _make_source(tmp_path, seriesSets=5, seriesPerSet=10)
    descriptions = [
        metadata.get_field(fields.Description)
        for metadata in _series(source)
        if metadata.series.tags["Attribute"] != "active"
    ]
    functions = {
        description.rsplit("(", 1)[1].rstrip(")") for description in descriptions
    }
    assert functions == {"white noise", "drift", "sine"}


def test_severity_out_of_range_is_invalid(tmp_path: Path):
    with raises(InvalidSourceException):
        _make_source(tmp_path, severity=1.5)


def test_minimum_interval_above_maximum_is_invalid(tmp_path: Path):
    with raises(InvalidSourceException):
        _make_source(
            tmp_path,
            samplingInterval={"intervalSecondsMin": 60, "intervalSecondsMax": 10},
        )
