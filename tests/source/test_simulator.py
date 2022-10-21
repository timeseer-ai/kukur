"""Unit tests for the simulator source."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0


from datetime import datetime, timedelta

import pyarrow as pa

from dateutil.parser import parse as parse_date
from pytest import approx, fixture

from kukur.base import SeriesSearch, SeriesSelector
from kukur.source.simulator.simulator import (
    SineSignalGenerator,
    StepSignalGenerator,
    WhiteNoiseSignalGenerator,
    calculate_sine,
)


START_DATE = parse_date("2020-01-01T01:00:00Z")
END_DATE = parse_date("2020-11-01T00:00:00Z")


@fixture
def step_signal_generator() -> StepSignalGenerator:
    return StepSignalGenerator()


@fixture
def whitenoise_signal_generator() -> WhiteNoiseSignalGenerator:
    return WhiteNoiseSignalGenerator()


@fixture
def sine_signal_generator() -> SineSignalGenerator:
    return SineSignalGenerator()


@fixture
def step_signal_selector() -> SeriesSelector:
    return SeriesSelector(
        "",
        {
            "series name": "step-signal-test",
            "signal_type": "step",
            "seed": "0",
            "interval_seconds_min": "600",
            "interval_seconds_max": "3600",
            "min_value": "0",
            "max_value": "100",
            "number_of_steps": "10",
        },
    )


@fixture
def whitenoise_signal_selector() -> SeriesSelector:
    return SeriesSelector(
        "",
        {
            "series name": "whitenoise-signal-test",
            "signal_type": "whitenoise",
            "seed": "0",
            "interval_seconds_min": "600",
            "interval_seconds_max": "3600",
            "mean": "10",
            "standard_deviation": "10",
        },
    )


@fixture
def sine_signal_selector() -> SeriesSelector:
    return SeriesSelector(
        "",
        {
            "series name": "sine-signal-test",
            "signal_type": "sine",
            "seed": "0",
            "interval_seconds_min": "600",
            "interval_seconds_max": "3600",
            "period_seconds": "2",
            "amplitude": "10",
            "phase_seconds": "10",
            "shift": "0",
        },
    )


def test_step_signal_generator_produces_same_data(
    step_signal_generator: StepSignalGenerator, step_signal_selector: SeriesSelector
):
    first_run = step_signal_generator.generate(
        step_signal_selector, START_DATE, END_DATE
    )
    second_run = step_signal_generator.generate(
        step_signal_selector, START_DATE, END_DATE
    )
    assert first_run == second_run


def test_step_signal_generator_consistency(
    step_signal_generator: StepSignalGenerator, step_signal_selector: SeriesSelector
):
    bigger_data = step_signal_generator.generate(
        step_signal_selector, START_DATE, END_DATE
    )

    smaller_start_date = START_DATE + timedelta(weeks=4)
    smaller_end_date = END_DATE - timedelta(weeks=4)
    smaller_data = step_signal_generator.generate(
        step_signal_selector, smaller_start_date, smaller_end_date
    )
    assert (
        drop_data_after_and_before(bigger_data, smaller_start_date, smaller_end_date)
        == smaller_data
    )


def test_step_signal_generator_data(
    step_signal_generator: StepSignalGenerator, step_signal_selector: SeriesSelector
):
    data = step_signal_generator.generate(step_signal_selector, START_DATE, END_DATE)
    for value in data["value"]:
        assert value.as_py() in list(range(0, 101, 10))


def test_step_signal_generator_series() -> None:
    search = SeriesSearch("")
    assert len(list(StepSignalGenerator().list_series(search))) == 0
    generator = StepSignalGenerator(
        {
            "seriesName": "step",
            "type": "step",
            "samplingInterval": {
                "intervalSecondsMin": 1,
                "intervalSecondsMax": 2,
            },
            "metadata": {"description": "step function"},
            "values": [
                {
                    "min": 0,
                    "max": 10,
                    "numberOfSteps": 10,
                },
            ],
        }
    )
    one_series = list(generator.list_series(search))
    assert len(one_series) == 1
    assert one_series[0].get_field_by_name("description") == "step function"
    assert one_series[0].series.tags == {
        "series name": "step",
        "signal_type": "step",
        "seed": "7106521602475165645",
        "interval_seconds_min": "1",
        "interval_seconds_max": "2",
        "min_value": "0",
        "max_value": "10",
        "number_of_steps": "10",
    }

    generator = StepSignalGenerator(
        {
            "seriesName": "step",
            "type": "step",
            "samplingInterval": {
                "intervalSecondsMin": 1,
                "intervalSecondsMax": 2,
            },
            "metadata": {"description": "step function"},
            "values": [
                {
                    "min": 0,
                    "max": 10,
                    "numberOfSteps": [5, 10],
                },
            ],
        }
    )
    two_series = list(generator.list_series(search))
    assert len(two_series) == 2
    assert [metadata.series.tags["number_of_steps"] for metadata in two_series] == [
        "5",
        "10",
    ]
    for metadata in two_series:
        assert metadata.get_field_by_name("description") == "step function"
        del metadata.series.tags["number_of_steps"]
        del metadata.series.tags["seed"]
        assert metadata.series.tags == {
            "series name": "step",
            "signal_type": "step",
            "interval_seconds_min": "1",
            "interval_seconds_max": "2",
            "min_value": "0",
            "max_value": "10",
        }

    generator = StepSignalGenerator(
        {
            "seriesName": "step",
            "type": "step",
            "samplingInterval": {
                "intervalSecondsMin": 1,
                "intervalSecondsMax": 2,
            },
            "metadata": {"description": "step function"},
            "values": [
                {
                    "min": 0,
                    "max": 10,
                    "numberOfSteps": [5, 10],
                },
                {
                    "min": 20,
                    "max": 50,
                    "numberOfSteps": [5, 10],
                },
            ],
        }
    )
    four_series = list(generator.list_series(search))
    assert len(four_series) == 4
    assert {metadata.series.tags["number_of_steps"] for metadata in four_series} == set(
        [
            "5",
            "10",
        ]
    )
    for metadata in four_series:
        assert metadata.get_field_by_name("description") == "step function"
        del metadata.series.tags["number_of_steps"]
        del metadata.series.tags["seed"]
        assert (
            metadata.series.tags["min_value"] == "0"
            and metadata.series.tags["max_value"] == "10"
        ) or (
            metadata.series.tags["min_value"] == "20"
            and metadata.series.tags["max_value"] == "50"
        )

        del metadata.series.tags["min_value"]
        del metadata.series.tags["max_value"]
        assert metadata.series.tags == {
            "series name": "step",
            "signal_type": "step",
            "interval_seconds_min": "1",
            "interval_seconds_max": "2",
        }


def test_whitenoise_signal_generator_produces_same_data(
    whitenoise_signal_generator: WhiteNoiseSignalGenerator,
    whitenoise_signal_selector: SeriesSelector,
):
    first_run = whitenoise_signal_generator.generate(
        whitenoise_signal_selector, START_DATE, END_DATE
    )
    second_run = whitenoise_signal_generator.generate(
        whitenoise_signal_selector, START_DATE, END_DATE
    )
    assert first_run == second_run


def test_whitenoise_signal_generator_consistency(
    whitenoise_signal_generator: WhiteNoiseSignalGenerator,
    whitenoise_signal_selector: SeriesSelector,
):
    bigger_data = whitenoise_signal_generator.generate(
        whitenoise_signal_selector, START_DATE, END_DATE
    )

    smaller_start_date = START_DATE + timedelta(weeks=4)
    smaller_end_date = END_DATE - timedelta(weeks=4)
    smaller_data = whitenoise_signal_generator.generate(
        whitenoise_signal_selector, smaller_start_date, smaller_end_date
    )
    assert (
        drop_data_after_and_before(bigger_data, smaller_start_date, smaller_end_date)
        == smaller_data
    )


def test_white_noise_signal_generator_series() -> None:
    search = SeriesSearch("")
    assert len(list(WhiteNoiseSignalGenerator().list_series(search))) == 0
    generator = WhiteNoiseSignalGenerator(
        {
            "seriesName": "white noise",
            "type": "white noise",
            "samplingInterval": {
                "intervalSecondsMin": 1,
                "intervalSecondsMax": 2,
            },
            "metadata": {"description": "white noise"},
            "values": {
                "mean": 10,
                "standardDeviation": 1,
            },
        }
    )
    one_series = list(generator.list_series(search))
    assert len(one_series) == 1
    assert one_series[0].get_field_by_name("description") == "white noise"
    assert one_series[0].series.tags == {
        "series name": "white noise",
        "signal_type": "white noise",
        "seed": "7106521602475165645",
        "interval_seconds_min": "1",
        "interval_seconds_max": "2",
        "mean": "10",
        "standard_deviation": "1",
    }
    generator = WhiteNoiseSignalGenerator(
        {
            "seriesName": "white noise",
            "type": "white noise",
            "samplingInterval": {
                "intervalSecondsMin": 1,
                "intervalSecondsMax": 2,
            },
            "metadata": {"description": "white noise"},
            "values": {
                "mean": 10,
                "standardDeviation": [1, 2],
            },
        }
    )
    two_series = list(generator.list_series(search))
    assert len(two_series) == 2
    assert [metadata.series.tags["standard_deviation"] for metadata in two_series] == [
        "1",
        "2",
    ]
    for metadata in two_series:
        assert metadata.get_field_by_name("description") == "white noise"
        del metadata.series.tags["standard_deviation"]
        del metadata.series.tags["seed"]
        assert metadata.series.tags == {
            "series name": "white noise",
            "signal_type": "white noise",
            "interval_seconds_min": "1",
            "interval_seconds_max": "2",
            "mean": "10",
        }


def test_sine_signal_generator_series() -> None:
    search = SeriesSearch("")
    assert len(list(SineSignalGenerator().list_series(search))) == 0
    generator = SineSignalGenerator(
        {
            "seriesName": "sine",
            "type": "sine",
            "samplingInterval": {
                "intervalSecondsMin": 1,
                "intervalSecondsMax": 2,
            },
            "metadata": {"description": "sine wave"},
            "values": {
                "periodSeconds": 3600,
                "phaseSeconds": 0,
                "amplitude": 2,
                "shift": 1,
            },
        }
    )
    one_series = list(generator.list_series(search))
    assert len(one_series) == 1
    assert one_series[0].get_field_by_name("description") == "sine wave"
    assert one_series[0].series.tags == {
        "series name": "sine",
        "signal_type": "sine",
        "seed": "7106521602475165645",
        "interval_seconds_min": "1",
        "interval_seconds_max": "2",
        "period_seconds": "3600",
        "phase_seconds": "0",
        "amplitude": "2",
        "shift": "1",
    }
    generator = SineSignalGenerator(
        {
            "seriesName": "sine",
            "type": "sine",
            "samplingInterval": {
                "intervalSecondsMin": 1,
                "intervalSecondsMax": 2,
            },
            "metadata": {"description": "sine wave"},
            "values": {
                "periodSeconds": 3600,
                "phaseSeconds": 0,
                "amplitude": 2,
                "shift": [1, 2],
            },
        }
    )
    two_series = list(generator.list_series(search))
    assert len(two_series) == 2
    assert [metadata.series.tags["shift"] for metadata in two_series] == ["1", "2"]
    for metadata in two_series:
        assert metadata.get_field_by_name("description") == "sine wave"
        del metadata.series.tags["shift"]
        del metadata.series.tags["seed"]
        assert metadata.series.tags == {
            "series name": "sine",
            "signal_type": "sine",
            "interval_seconds_min": "1",
            "interval_seconds_max": "2",
            "period_seconds": "3600",
            "phase_seconds": "0",
            "amplitude": "2",
        }


def test_sine_signal_generator_produces_same_data(
    sine_signal_generator: SineSignalGenerator, sine_signal_selector: SeriesSelector
):
    bigger_data = sine_signal_generator.generate(
        sine_signal_selector, START_DATE, END_DATE
    )

    smaller_start_date = START_DATE + timedelta(weeks=4)
    smaller_end_date = END_DATE - timedelta(weeks=4)
    smaller_data = sine_signal_generator.generate(
        sine_signal_selector, smaller_start_date, smaller_end_date
    )
    assert (
        drop_data_after_and_before(bigger_data, smaller_start_date, smaller_end_date)
        == smaller_data
    )


def test_sine_signal_generator_consistency(
    sine_signal_generator: SineSignalGenerator, sine_signal_selector: SeriesSelector
):
    bigger_data = sine_signal_generator.generate(
        sine_signal_selector, START_DATE, END_DATE
    )

    smaller_start_date = START_DATE + timedelta(weeks=4)
    smaller_end_date = END_DATE - timedelta(weeks=4)
    smaller_data = sine_signal_generator.generate(
        sine_signal_selector, smaller_start_date, smaller_end_date
    )
    assert (
        drop_data_after_and_before(bigger_data, smaller_start_date, smaller_end_date)
        == smaller_data
    )


def drop_data_after_and_before(
    table: pa.Table, start_date: datetime, end_date: datetime
) -> pa.Table:
    keep_after = pa.compute.greater_equal(table["ts"], pa.scalar(start_date))
    keep_before = pa.compute.less_equal(table["ts"], pa.scalar(end_date))
    return table.filter(pa.compute.and_(keep_before, keep_after))


def test_sine_zero() -> None:
    t1 = datetime.fromtimestamp(0)

    v0 = calculate_sine(t1, period_seconds=3600)
    v1 = calculate_sine(t1 + timedelta(minutes=15), period_seconds=3600)
    v2 = calculate_sine(t1 + timedelta(minutes=30), period_seconds=3600)
    v3 = calculate_sine(t1 + timedelta(minutes=45), period_seconds=3600)
    v4 = calculate_sine(t1 + timedelta(minutes=60), period_seconds=3600)

    assert v0 == approx(0)
    assert v1 == approx(1)
    assert v2 == approx(0)
    assert v3 == approx(-1)
    assert v4 == approx(0)


def test_sine_phase() -> None:
    t1 = datetime.fromtimestamp(0)

    v0 = calculate_sine(t1, period_seconds=3600, phase_seconds=900)
    v1 = calculate_sine(
        t1 + timedelta(minutes=15), period_seconds=3600, phase_seconds=900
    )
    v2 = calculate_sine(
        t1 + timedelta(minutes=30), period_seconds=3600, phase_seconds=900
    )
    v3 = calculate_sine(
        t1 + timedelta(minutes=45), period_seconds=3600, phase_seconds=900
    )
    v4 = calculate_sine(
        t1 + timedelta(minutes=60), period_seconds=3600, phase_seconds=900
    )

    assert v0 == approx(1)
    assert v1 == approx(0)
    assert v2 == approx(-1)
    assert v3 == approx(0)
    assert v4 == approx(1)


def test_sine_phase_half() -> None:
    t1 = datetime.fromtimestamp(0)

    v0 = calculate_sine(
        t1 + timedelta(seconds=7 * 60 + 30), period_seconds=3600, phase_seconds=450
    )
    v1 = calculate_sine(
        t1 + timedelta(seconds=22 * 60 + 30), period_seconds=3600, phase_seconds=450
    )
    v2 = calculate_sine(
        t1 + timedelta(seconds=37 * 60 + 30), period_seconds=3600, phase_seconds=450
    )
    v3 = calculate_sine(
        t1 + timedelta(seconds=52 * 60 + 30), period_seconds=3600, phase_seconds=450
    )
    v4 = calculate_sine(
        t1 + timedelta(seconds=67 * 60 + 30), period_seconds=3600, phase_seconds=450
    )

    assert v0 == approx(1)
    assert v1 == approx(0)
    assert v2 == approx(-1)
    assert v3 == approx(0)
    assert v4 == approx(1)


def test_sine_shift() -> None:
    t1 = datetime.fromtimestamp(0)

    v0 = calculate_sine(t1, period_seconds=3600, shift=1)
    v1 = calculate_sine(t1 + timedelta(minutes=15), period_seconds=3600, shift=1)
    v2 = calculate_sine(t1 + timedelta(minutes=30), period_seconds=3600, shift=1)
    v3 = calculate_sine(t1 + timedelta(minutes=45), period_seconds=3600, shift=1)
    v4 = calculate_sine(t1 + timedelta(minutes=60), period_seconds=3600, shift=1)

    assert v0 == approx(1)
    assert v1 == approx(2)
    assert v2 == approx(1)
    assert v3 == approx(0)
    assert v4 == approx(1)


def test_sine_amplitude() -> None:
    t1 = datetime.fromtimestamp(0)

    v0 = calculate_sine(t1, period_seconds=3600, amplitude=2)
    v1 = calculate_sine(t1 + timedelta(minutes=15), period_seconds=3600, amplitude=2)
    v2 = calculate_sine(t1 + timedelta(minutes=30), period_seconds=3600, amplitude=2)
    v3 = calculate_sine(t1 + timedelta(minutes=45), period_seconds=3600, amplitude=2)
    v4 = calculate_sine(t1 + timedelta(minutes=60), period_seconds=3600, amplitude=2)

    assert v0 == approx(0)
    assert v1 == approx(2)
    assert v2 == approx(0)
    assert v3 == approx(-2)
    assert v4 == approx(0)
