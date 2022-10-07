"""Unit tests for the simulator source."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta
from pytest import fixture
import pyarrow as pa

from dateutil.parser import parse as parse_date
from kukur.base import SeriesSelector

from kukur.source.simulator.simulator import (
    SineSignalGenerator,
    StepSignalGenerator,
    WhiteNoiseSignalGenerator,
    WhiteNoiseSignalGeneratorConfig,
)


START_DATE = parse_date("2020-01-01T00:00:00Z")
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
            "min_interval": "600",
            "max_interval": "3600",
            "min_value": "0",
            "max_value": "100",
            "min_step": "0",
            "max_step": "10",
        },
    )


@fixture
def whitenoise_signal_selector() -> SeriesSelector:
    return SeriesSelector(
        "",
        {
            "series name": "whitenoise-signal-test",
            "signal_type": "whitenoise",
            "min_interval": "600",
            "max_interval": "3600",
            "mean": "10",
            "standard_deviation": "10",
        },
    )


@fixture
def sine_signal_selector() -> SeriesSelector:
    return SeriesSelector(
        "",
        {
            "series name": "whitenoise-signal-test",
            "signal_type": "whitenoise",
            "min_interval": "600",
            "max_interval": "3600",
            "period": "2",
            "amplitude": "10",
            "phase": "10",
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
    first_run = whitenoise_signal_generator.generate(
        whitenoise_signal_selector, START_DATE, END_DATE
    )

    second_run = whitenoise_signal_generator.generate(
        whitenoise_signal_selector, START_DATE, END_DATE
    )

    assert first_run == second_run


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
