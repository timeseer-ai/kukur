"""Simulate a data source by generating data for Timeseer."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import itertools
import operator
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from hashlib import sha1
from pathlib import Path
from random import Random
from typing import Any, Dict, Generator, List, Optional, Union

import numpy
import pyarrow as pa

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False

from kukur import (
    Metadata,
    SeriesSearch,
    SeriesSelector,
    SignalGenerator,
    SourceStructure,
)
from kukur.exceptions import (
    InvalidSourceException,
    MissingModuleException,
)

operator_functions = {
    "+": operator.add,
    "-": operator.sub,
}


class UnknownSignalTypeError(Exception):
    """Raised when a simulator type is unknown."""


@dataclass
class SimulatorConfiguration:
    """Simulator source configuration."""

    path: Optional[str]


def from_config(
    config: Dict[str, Any],
):
    """Create a new Simulator data source from the given configuration dictionary."""
    return SimulatorSource(SimulatorConfiguration(config.get("path")))


@dataclass
class SignalGeneratorConfig:  # pylint: disable=too-many-instance-attributes
    """Base signal generator configuration."""

    series_name: str
    signal_type: str
    initial_seed: int
    number_of_seeds: int
    interval_seconds_min: Union[List[float], float]
    interval_seconds_max: Union[List[float], float]
    metadata: Dict[str, str]
    fields: List[str]


@dataclass
class SignalConfig:
    """Base signal configuration."""

    series_name: str
    signal_type: str
    seed: int
    interval_seconds_min: int
    interval_seconds_max: int

    def to_bytes(self) -> bytes:
        """Convert to a unique bytes representation."""
        return bytes(str(self), "UTF-8")


class SignalDataType(Enum):
    """Data types for step signal generator."""

    STRING = "string"
    NUMERIC = "numeric"


@dataclass
class StepSignalGeneratorConfigValue:
    """One possible step configuration."""

    min: float
    max: float
    number_of_steps: List[int]
    data_type: SignalDataType


@dataclass
class StepSignalGeneratorConfig(SignalGeneratorConfig):
    """Configuration for the step signal generator."""

    values: List[StepSignalGeneratorConfigValue]


@dataclass
class StepSignalConfig(SignalConfig):
    """Configuration for the step signal."""

    min_value: float
    max_value: float
    number_of_steps: int
    data_type: SignalDataType


@dataclass
class WhiteNoiseSignalGeneratorConfig(SignalGeneratorConfig):
    """Configuration for the white noise signal generator."""

    mean: Union[List[float], float]
    standard_deviation: Union[List[float], float]


@dataclass
class WhiteNoiseSignalConfig(SignalConfig):
    """Configuration for the white noise signal."""

    mean: float
    standard_deviation: float


@dataclass
class SineSignalGeneratorConfig(SignalGeneratorConfig):
    """Configuration for the sine signal generator."""

    period_seconds: Union[List[float], float]
    phase_seconds: Union[List[float], float]
    amplitude: Union[List[float], float]
    shift: Union[List[float], float]


@dataclass
class SineSignalConfig(SignalConfig):
    """Configuration for one sine signal."""

    period_seconds: float
    phase_seconds: float
    amplitude: float
    shift: float


@dataclass
class CounterSignalGeneratorConfigValue:
    """One possible counter configuration."""

    min: float
    max: float
    increase_value: List[float]
    interval_seconds: List[int]


@dataclass
class CounterSignalGeneratorConfig(SignalGeneratorConfig):
    """Configuration for the counter signal generator."""

    values: List[CounterSignalGeneratorConfigValue]


@dataclass
class CounterSignalConfig(SignalConfig):
    """Configuration for the step signal."""

    min_value: float
    max_value: float
    increase_value: float
    interval_seconds: int


class StepSignalGenerator:
    """Step signal generator."""

    __config: Optional[StepSignalGeneratorConfig] = None

    def __init__(self, config: Optional[Dict] = None):
        if config is not None:
            self.__config = StepSignalGeneratorConfig(
                config["seriesName"],
                config["type"],
                config.get("initialSeed", 0),
                config.get("numberOfSeeds", 1),
                config["samplingInterval"]["intervalSecondsMin"],
                config["samplingInterval"]["intervalSecondsMax"],
                config.get("metadata", {}),
                config.get("fields", ["value"]),
                [
                    StepSignalGeneratorConfigValue(
                        value["min"],
                        value["max"],
                        _ensure_list(value["numberOfSteps"]),
                        SignalDataType(value.get("dataType", "numeric")),
                    )
                    for value in config["values"]
                ],
            )

    def generate(  # pylint: disable=no-self-use, too-many-locals
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generate data in steps based on a selector start and end date."""
        current_time = _get_start_of_day(start_date)
        configuration = _get_step_configuration(selector)
        ts = []
        value = []
        rng = Random(_get_hex_digest(current_time, configuration.to_bytes()))

        step_size = (
            configuration.max_value - configuration.min_value
        ) / configuration.number_of_steps
        current_value = (
            configuration.min_value
            + rng.randint(0, configuration.number_of_steps) * step_size
        )
        while current_time <= end_date:
            generated_step = (
                rng.randint(0, int(configuration.number_of_steps / 2)) * step_size
            )
            random_operator = rng.choice(["+", "-"])
            new_value = operator_functions[random_operator](
                current_value, generated_step
            )
            if (
                new_value > configuration.max_value
                or new_value < configuration.min_value
            ):
                random_operator = "+" if random_operator == "-" else "-"
                new_value = operator_functions[random_operator](
                    current_value, generated_step
                )
            if configuration.data_type == SignalDataType.STRING:
                value.append(f"string_{new_value}")
            elif configuration.data_type == SignalDataType.NUMERIC:
                value.append(new_value)
            else:
                raise InvalidSourceException("Unknown data type")

            ts.append(current_time)
            current_value = new_value

            time_increment = rng.randint(
                configuration.interval_seconds_min, configuration.interval_seconds_max
            )
            new_time = current_time + timedelta(seconds=time_increment)

            if new_time.date() != current_time.date():
                new_time = _get_start_of_day(new_time)
                rng.seed(_get_hex_digest(new_time, configuration.to_bytes()))
                current_value = (
                    configuration.min_value
                    + rng.randint(0, configuration.number_of_steps) * step_size
                )

            current_time = new_time

        return _drop_data_before(
            pa.Table.from_pydict({"ts": ts, "value": value}), start_date
        )

    def list_series(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Yield all possible metadata combinations using the signal configuration and the provided selector."""
        arg_list = []
        if self.__config is None:
            return

        rng = Random(self.__config.initial_seed)
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "seed",
                [
                    rng.randint(0, sys.maxsize)
                    for _ in range(self.__config.number_of_seeds)
                ],
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "interval_seconds_min",
                self.__config.interval_seconds_min,
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "interval_seconds_max",
                self.__config.interval_seconds_max,
            )
        )
        arg_list.append(
            [
                dict(
                    min=value.min,
                    max=value.max,
                    number_of_steps=number_of_steps,
                    data_type=value.data_type.value,
                )
                for value in self.__config.values  # noqa: PD
                for number_of_steps in value.number_of_steps
            ]
        )

        for entry in itertools.product(*arg_list):
            for field in self.__config.fields:
                yield _build_step_search_result(
                    self.__config, entry, selector.source, field
                )


def _ensure_list(value) -> List:
    if isinstance(value, List):
        return value
    return [value]


def _build_step_search_result(
    config: StepSignalGeneratorConfig, entry: tuple, source_name: str, field: str
) -> Metadata:
    series_selector = SeriesSelector(
        source_name,
        {
            "series name": config.series_name,
            "signal_type": config.signal_type,
            "seed": str(entry[0]),
            "interval_seconds_min": str(entry[1]),
            "interval_seconds_max": str(entry[2]),
            "min_value": str(entry[3]["min"]),
            "max_value": str(entry[3]["max"]),
            "number_of_steps": str(entry[3]["number_of_steps"]),
            "data_type": str(entry[3]["data_type"]),
        },
        field,
    )
    metadata = Metadata(series_selector)
    for field_name, field_value in config.metadata.items():
        metadata.coerce_field(field_name, field_value)
    return metadata


def _get_step_configuration(selector: SeriesSelector) -> StepSignalConfig:
    return StepSignalConfig(
        selector.tags["series name"],
        selector.tags["signal_type"],
        int(selector.tags["seed"]),
        int(selector.tags["interval_seconds_min"]),
        int(selector.tags["interval_seconds_max"]),
        float(selector.tags["min_value"]),
        float(selector.tags["max_value"]),
        int(selector.tags["number_of_steps"]),
        SignalDataType(selector.tags.get("data_type", "numeric")),
    )


def _get_start_of_day(ts: datetime) -> datetime:
    current_day = ts.date()
    return datetime(
        current_day.year, current_day.month, current_day.day, tzinfo=timezone.utc
    )


class WhiteNoiseSignalGenerator:
    """White noise signal generator."""

    __config: Optional[WhiteNoiseSignalGeneratorConfig] = None

    def __init__(self, config: Optional[Dict] = None):
        if config is not None:
            self.__config = WhiteNoiseSignalGeneratorConfig(
                config["seriesName"],
                config["type"],
                config.get("initialSeed", 0),
                config.get("numberOfSeeds", 1),
                config["samplingInterval"]["intervalSecondsMin"],
                config["samplingInterval"]["intervalSecondsMax"],
                config.get("metadata", {}),
                config.get("fields", ["value"]),
                config["values"]["mean"],
                config["values"]["standardDeviation"],
            )

    def generate(  # pylint: disable=no-self-use
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generate white noise based on a selector start and end date."""
        current_time = _get_start_of_day(start_date)
        configuration = _get_white_noise_configuration(selector)
        ts = []
        value = []

        rng = Random(_get_hex_digest(current_time, configuration.to_bytes()))

        numpy_rng = numpy.random.default_rng(
            _get_int_digest(current_time, configuration.to_bytes())
        )

        while current_time <= end_date:
            generated_value = numpy_rng.normal(
                configuration.mean, configuration.standard_deviation, 1
            )[0]
            value.append(generated_value)
            ts.append(current_time)

            time_increment = rng.randint(
                configuration.interval_seconds_min, configuration.interval_seconds_max
            )
            new_time = current_time + timedelta(seconds=time_increment)

            if new_time.date() != current_time.date():
                new_time = _get_start_of_day(new_time)
                rng.seed(_get_hex_digest(new_time, configuration.to_bytes()))
                numpy_rng = numpy.random.default_rng(
                    _get_int_digest(new_time, configuration.to_bytes())
                )

            current_time = new_time

        return _drop_data_before(
            pa.Table.from_pydict({"ts": ts, "value": value}), start_date
        )

    def list_series(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Yield all possible metadata combinations using the signal configuration and the provided selector."""
        arg_list = []
        if self.__config is None:
            return

        rng = Random(self.__config.initial_seed)
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "seed",
                [
                    rng.randint(0, sys.maxsize)
                    for _ in range(self.__config.number_of_seeds)
                ],
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "interval_seconds_min",
                self.__config.interval_seconds_min,
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "interval_seconds_max",
                self.__config.interval_seconds_max,
            )
        )
        arg_list.append(_extract_from_tag(selector.tags, "mean", self.__config.mean))
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "standard_deviation",
                self.__config.standard_deviation,
            )
        )

        for entry in itertools.product(*arg_list):
            for field in self.__config.fields:
                yield _build_white_noise_search_result(
                    self.__config, entry, selector.source, field
                )


def _build_white_noise_search_result(
    config: WhiteNoiseSignalGeneratorConfig, entry: tuple, source_name: str, field: str
) -> Metadata:
    series_selector = SeriesSelector(
        source_name,
        {
            "series name": config.series_name,
            "signal_type": config.signal_type,
            "seed": str(entry[0]),
            "interval_seconds_min": str(entry[1]),
            "interval_seconds_max": str(entry[2]),
            "mean": str(entry[3]),
            "standard_deviation": str(entry[4]),
        },
        field,
    )
    metadata = Metadata(series_selector)
    for field_name, field_value in config.metadata.items():
        metadata.coerce_field(field_name, field_value)
    return metadata


def _get_white_noise_configuration(
    selector: SeriesSelector,
) -> WhiteNoiseSignalConfig:
    return WhiteNoiseSignalConfig(
        selector.tags["series name"],
        selector.tags["signal_type"],
        int(selector.tags["seed"]),
        int(selector.tags["interval_seconds_min"]),
        int(selector.tags["interval_seconds_max"]),
        float(selector.tags["mean"]),
        float(selector.tags["standard_deviation"]),
    )


class SineSignalGenerator:
    """Sine signal generator."""

    __config: Optional[SineSignalGeneratorConfig] = None

    def __init__(self, config: Optional[Dict] = None):
        if config is not None:
            self.__config = SineSignalGeneratorConfig(
                config["seriesName"],
                config["type"],
                config.get("initialSeed", 0),
                config.get("numberOfSeeds", 1),
                config["samplingInterval"]["intervalSecondsMin"],
                config["samplingInterval"]["intervalSecondsMax"],
                config.get("metadata", {}),
                config.get("fields", ["value"]),
                config["values"]["periodSeconds"],
                config["values"]["phaseSeconds"],
                config["values"]["amplitude"],
                config["values"]["shift"],
            )

    def generate(  # pylint: disable=no-self-use
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generate a sine function based on a selector start and end date."""
        current_time = _get_start_of_day(start_date)
        configuration = _get_sine_configuration(selector)
        ts = []
        value = []

        rng = Random(_get_hex_digest(current_time, configuration.to_bytes()))

        while current_time <= end_date:
            value.append(
                calculate_sine(
                    current_time,
                    period_seconds=configuration.period_seconds,
                    phase_seconds=configuration.phase_seconds,
                    amplitude=configuration.amplitude,
                    shift=configuration.shift,
                )
            )
            ts.append(current_time)

            time_increment = rng.randint(
                configuration.interval_seconds_min, configuration.interval_seconds_max
            )
            new_time = current_time + timedelta(seconds=time_increment)

            if new_time.date() != current_time.date():
                new_time = _get_start_of_day(new_time)

                rng.seed(_get_hex_digest(new_time, configuration.to_bytes()))

            current_time = new_time

        return _drop_data_before(
            pa.Table.from_pydict({"ts": ts, "value": value}), start_date
        )

    def list_series(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Yield all possible metadata combinations using the signal configuration and the provided selector."""
        if self.__config is None:
            return

        arg_list = []
        rng = Random(self.__config.initial_seed)
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "seed",
                [
                    rng.randint(0, sys.maxsize)
                    for _ in range(self.__config.number_of_seeds)
                ],
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "interval_seconds_min",
                self.__config.interval_seconds_min,
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "interval_seconds_max",
                self.__config.interval_seconds_max,
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags, "period_seconds", self.__config.period_seconds
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "amplitude",
                self.__config.amplitude,
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags, "phase_seconds", self.__config.phase_seconds
            )
        )
        arg_list.append(_extract_from_tag(selector.tags, "shift", self.__config.shift))

        for entry in itertools.product(*arg_list):
            for field in self.__config.fields:
                yield _build_sine_search_result(
                    self.__config, entry, selector.source, field
                )


def _build_sine_search_result(
    config: SineSignalGeneratorConfig, entry: tuple, source_name: str, field: str
) -> Metadata:
    series_selector = SeriesSelector(
        source_name,
        {
            "series name": config.series_name,
            "signal_type": config.signal_type,
            "seed": str(entry[0]),
            "interval_seconds_min": str(entry[1]),
            "interval_seconds_max": str(entry[2]),
            "period_seconds": str(entry[3]),
            "amplitude": str(entry[4]),
            "phase_seconds": str(entry[5]),
            "shift": str(entry[6]),
        },
        field,
    )
    metadata = Metadata(series_selector)
    for field_name, field_value in config.metadata.items():
        metadata.coerce_field(field_name, field_value)
    return metadata


def _get_sine_configuration(selector: SeriesSelector) -> SineSignalConfig:
    return SineSignalConfig(
        selector.tags["series name"],
        selector.tags["signal_type"],
        int(selector.tags["seed"]),
        int(selector.tags["interval_seconds_min"]),
        int(selector.tags["interval_seconds_max"]),
        float(selector.tags["period_seconds"]),
        float(selector.tags["phase_seconds"]),
        float(selector.tags["amplitude"]),
        float(selector.tags["shift"]),
    )


def calculate_sine(
    ts: datetime,
    *,
    period_seconds: float,
    phase_seconds: float = 0,
    amplitude: float = 1,
    shift: float = 0,
) -> float:
    """Calculate the sine function at unix timestamp ts."""
    return (
        amplitude
        * numpy.sin(2 * numpy.pi * (ts.timestamp() + phase_seconds) / period_seconds)
        + shift
    )


class CounterSignalGenerator:
    """Count signal generator."""

    __config: Optional[CounterSignalGeneratorConfig] = None

    def __init__(self, config: Optional[Dict] = None):
        if config is not None:
            self.__config = CounterSignalGeneratorConfig(
                config["seriesName"],
                config["type"],
                config.get("initialSeed", 0),
                config.get("numberOfSeeds", 1),
                config.get("samplingInterval", {}).get("intervalSecondsMin", 0),
                config.get("samplingInterval", {}).get("intervalSecondsMax", 0),
                config.get("metadata", {}),
                config.get("fields", ["value"]),
                [
                    CounterSignalGeneratorConfigValue(
                        value["min"],
                        value["max"],
                        _ensure_list(value["increaseValue"]),
                        _ensure_list(value["intervalSeconds"]),
                    )
                    for value in config["values"]
                ],
            )

    def generate(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generate data as counter based on a selector start and end date."""
        configuration = _get_counter_configuration(selector)
        ts: List[datetime] = []
        value: List[float] = []
        period_in_seconds = (
            int(
                (configuration.max_value - configuration.min_value)
                / configuration.increase_value
            )
            * configuration.interval_seconds
        )
        rem = start_date.timestamp() % period_in_seconds
        period_start = current_time = start_date - timedelta(seconds=rem)
        current_value = configuration.min_value
        while current_time <= end_date:
            value.append(current_value)
            ts.append(current_time)

            time_increment = configuration.interval_seconds
            current_time += timedelta(seconds=time_increment)
            current_value += configuration.increase_value
            if current_time >= (period_start + timedelta(seconds=period_in_seconds)):
                period_start = current_time = period_start + timedelta(
                    seconds=period_in_seconds
                )
                current_value = configuration.min_value

        return _drop_data_before(
            pa.Table.from_pydict({"ts": ts, "value": value}), start_date
        )

    def list_series(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Yield all possible metadata combinations using the counter configuration and the provided selector."""
        arg_list = []
        if self.__config is None:
            return

        rng = Random(self.__config.initial_seed)
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "seed",
                [
                    rng.randint(0, sys.maxsize)
                    for _ in range(self.__config.number_of_seeds)
                ],
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "interval_seconds_min",
                self.__config.interval_seconds_min,
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "interval_seconds_max",
                self.__config.interval_seconds_max,
            )
        )
        arg_list.append(
            [
                {
                    "min": value.min,
                    "max": value.max,
                    "increase_value": increase_value,
                    "interval_seconds": interval_seconds,
                }
                for value in self.__config.values  # noqa: PD011
                for increase_value in value.increase_value
                for interval_seconds in value.interval_seconds
            ]
        )

        for entry in itertools.product(*arg_list):
            for field in self.__config.fields:
                yield _build_counter_search_result(
                    self.__config, entry, selector.source, field
                )


def _build_counter_search_result(
    config: CounterSignalGeneratorConfig, entry: tuple, source_name: str, field: str
) -> Metadata:
    series_selector = SeriesSelector(
        source_name,
        {
            "series name": config.series_name,
            "signal_type": config.signal_type,
            "seed": str(entry[0]),
            "interval_seconds_min": str(entry[1]),
            "interval_seconds_max": str(entry[2]),
            "min_value": str(entry[3]["min"]),
            "max_value": str(entry[3]["max"]),
            "increase_value": str(entry[3]["increase_value"]),
            "interval_seconds": str(entry[3]["interval_seconds"]),
        },
        field,
    )
    metadata = Metadata(series_selector)
    for field_name, field_value in config.metadata.items():
        metadata.coerce_field(field_name, field_value)
    return metadata


def _get_counter_configuration(selector: SeriesSelector) -> CounterSignalConfig:
    return CounterSignalConfig(
        selector.tags["series name"],
        selector.tags["signal_type"],
        int(selector.tags["seed"]),
        int(selector.tags["interval_seconds_min"]),
        int(selector.tags["interval_seconds_max"]),
        float(selector.tags["min_value"]),
        float(selector.tags["max_value"]),
        float(selector.tags["increase_value"]),
        int(selector.tags["interval_seconds"]),
    )


class SimulatorSource:
    """A simulator data source."""

    __signal_generators: Dict[str, List[SignalGenerator]]

    __yaml_path: Optional[Path] = None

    def __init__(self, config: SimulatorConfiguration):
        self.__signal_generators = defaultdict(list)

        if config.path is not None:
            if not HAS_YAML:
                raise MissingModuleException("PyYAML")
            self.__yaml_path = Path(config.path)

        if self.__yaml_path is None:
            self.__signal_generators["step"].append(StepSignalGenerator())
            self.__signal_generators["white noise"].append(WhiteNoiseSignalGenerator())
            self.__signal_generators["sine"].append(SineSignalGenerator())
            self.__signal_generators["counter"].append(CounterSignalGenerator())
        else:
            with self.__yaml_path.open(encoding="utf-8") as file:
                yaml_data = yaml.safe_load(file)
                for signal_config in yaml_data.get("signals", []):
                    self._register_generator(signal_config)

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Yield all possible metadata combinations using the signal configuration and the provided selector."""
        all_series = []
        if "signal_type" in selector.tags:
            for generator in self.__signal_generators[selector.tags["signal_type"]]:
                all_series.append(generator.list_series(selector))
        else:
            for generators in self.__signal_generators.values():
                for generator in generators:
                    all_series.append(generator.list_series(selector))

        for series in all_series:
            yield from series

    # pylint: disable=no-self-use
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generate data in steps based on a selector start and end date."""
        if "signal_type" not in selector.tags:
            return pa.Table.from_pydict({"ts": [], "value": []})
        for generator in self.__signal_generators[selector.tags["signal_type"]]:
            return generator.generate(selector, start_date, end_date)
        raise UnknownSignalTypeError(selector.tags["signal_type"])

    def get_source_structure(self, _: SeriesSelector) -> Optional[SourceStructure]:
        """Return the structure of a source."""

    def _register_generator(self, config: Dict):
        """Register a generator."""
        signal_type = config["type"]
        if signal_type == "step":
            self.__signal_generators[signal_type].append(StepSignalGenerator(config))
        if signal_type == "white noise":
            self.__signal_generators[signal_type].append(
                WhiteNoiseSignalGenerator(config)
            )
        if signal_type == "sine":
            self.__signal_generators[signal_type].append(SineSignalGenerator(config))
        if signal_type == "counter":
            self.__signal_generators[signal_type].append(CounterSignalGenerator(config))


def _extract_from_tag(
    tags: Dict[str, str], key: str, fallback: Union[List[Any], Any]
) -> List[Any]:
    if key in tags:
        return [tags[key]]
    if not isinstance(fallback, List):
        return [fallback]
    return fallback


def _drop_data_before(table: pa.Table, ts: datetime) -> pa.Table:
    # pylint: disable=no-member
    keep_after = pa.compute.greater_equal(table["ts"], pa.scalar(ts))
    return table.filter(keep_after)


def _get_hex_digest(ts: datetime, extra_bytes: bytes) -> str:
    return _get_hash(ts, extra_bytes).hexdigest()


def _get_int_digest(ts: datetime, extra_bytes) -> int:
    return int.from_bytes(_get_hash(ts, extra_bytes).digest()[:4], "little")


def _get_hash(ts: datetime, extra_bytes: bytes):
    return sha1(extra_bytes + bytes(ts.isoformat(), "UTF-8"))
