"""Simulate a data source by generating data for Timeseer."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from hashlib import sha1
from pathlib import Path

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional, Union

import itertools
import operator
import random

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
from kukur.exceptions import MissingModuleException
from kukur.source.metadata import MetadataMapper, MetadataValueMapper


operator_functions = {
    "+": operator.add,
    "-": operator.sub,
}


@dataclass
class SimulatorConfiguration:
    """Simulator source configuration."""

    path: Optional[str]


def from_config(
    config: Dict[str, Any],
    _metadata_mapper: MetadataMapper,
    _metadata_value_mapper: MetadataValueMapper,
):
    """Create a new Simulator data source from the given configuration dictionary."""
    return SimulatorSource(SimulatorConfiguration(config.get("path")))


@dataclass
class SignalGeneratorConfig:
    """Base signal configuration."""

    series_name: str
    signal_type: str
    min_interval: Union[List[int], int]
    max_interval: Union[List[int], int]
    metadata: dict[str, str]


@dataclass
class StepSignalGeneratorConfig(SignalGeneratorConfig):
    """Configuration for the step signal."""

    min_value: Union[List[int], int]
    max_value: Union[List[int], int]
    min_step: Union[List[int], int]
    max_step: Union[List[int], int]

    def to_bytes(self):
        """To bytes"""
        return bytes(
            self.series_name
            + str(self.min_value)
            + str(self.max_value)
            + str(self.min_step)
            + str(self.max_step)
            + str(self.min_interval)
            + str(self.max_interval),
            "UTF-8",
        )


@dataclass
class WhiteNoiseSignalGeneratorConfig(SignalGeneratorConfig):
    """Configuration for the step signal."""

    mean: Union[List[int], int]
    standard_deviation: Union[List[int], int]

    def to_bytes(self):
        """To bytes"""
        return bytes(
            self.series_name
            + str(self.mean)
            + str(self.standard_deviation)
            + str(self.min_interval)
            + str(self.max_interval),
            "UTF-8",
        )


@dataclass
class SineSignalGeneratorConfig(SignalGeneratorConfig):
    """Configuration for the sine signal."""

    period: Union[List[int], int]
    amplitude: Union[List[int], int]
    phase: Union[List[int], int]
    shift: Union[List[int], int]

    def to_bytes(self):
        """To bytes"""
        return bytes(
            self.series_name
            + str(self.period)
            + str(self.amplitude)
            + str(self.phase)
            + str(self.shift)
            + str(self.min_interval)
            + str(self.max_interval),
            "UTF-8",
        )


class StepSignalGenerator(SignalGenerator):
    """Step signal generator."""

    __default_config: Optional[StepSignalGeneratorConfig] = None

    def __init__(self, config: Optional[Dict] = None):
        super().__init__()
        if config is not None:
            self.__default_config = StepSignalGeneratorConfig(
                config["seriesName"],
                config["type"],
                config["samplingInterval"]["minInterval"],
                config["samplingInterval"]["maxInterval"],
                config.get("metadata", {}),
                config["values"]["minValue"],
                config["values"]["maxValue"],
                config["values"]["minStep"],
                config["values"]["maxStep"],
            )

    def generate(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generates data in steps based on a selector start and end date."""
        current_time = _get_start_of_day(start_date)
        configuration = self._get_configuration(selector)
        ts = []
        value = []
        random.seed(
            sha1(
                configuration.to_bytes()
                + bytes(current_time.date().isoformat(), "UTF-8")
            ).hexdigest()
        )

        assert isinstance(configuration.min_step, int)
        assert isinstance(configuration.max_step, int)
        assert isinstance(configuration.min_value, int)
        assert isinstance(configuration.max_value, int)
        assert isinstance(configuration.min_interval, int)
        assert isinstance(configuration.max_interval, int)

        current_value = random.uniform(
            float(configuration.min_step), float(configuration.max_step)
        )
        while current_time <= end_date:
            generated_step = random.uniform(
                float(configuration.min_step), float(configuration.max_step)
            )
            random_operator = random.choice(["+", "-"])
            generated_step = operator_functions[random_operator](
                current_value, generated_step
            )

            clamped_step = max(
                min(generated_step, float(configuration.max_value)),
                float(configuration.min_value),
            )
            current_value = clamped_step
            value.append(clamped_step)
            ts.append(current_time)

            time_increment = random.randint(
                configuration.min_interval, configuration.max_interval
            )
            new_time = current_time + timedelta(seconds=time_increment)

            if new_time.date() != current_time.date():
                new_time = _get_start_of_day(new_time)

                random.seed(
                    sha1(
                        configuration.to_bytes()
                        + bytes(new_time.date().isoformat(), "UTF-8")
                    ).hexdigest()
                )

            current_time = new_time

        return _drop_data_before(
            pa.Table.from_pydict({"ts": ts, "value": value}), start_date
        )

    def list_series(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Yields all possible metadata combinations using the signal configuration and the provided selector."""
        arg_list = []
        assert self.__default_config is not None
        arg_list.append(
            _extract_from_tag(
                selector.tags, "min_interval", self.__default_config.min_interval
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags, "max_interval", self.__default_config.max_interval
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags, "min_value", self.__default_config.min_value
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags, "max_value", self.__default_config.max_value
            )
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "min_step", self.__default_config.min_step)
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "max_step", self.__default_config.max_step)
        )

        for entry in itertools.product(*arg_list):
            yield self._build_search_result(entry, selector.source)

    def _build_search_result(
        self, entry: tuple, source_name: str
    ) -> Union[SeriesSelector, Metadata]:
        """Builds a series selector or metadata from a combination of configuration parameters."""
        assert self.__default_config is not None
        series_selector = SeriesSelector(
            source_name,
            {
                "series name": self.__default_config.series_name,
                "signal_type": self.__default_config.signal_type,
                "min_interval": entry[0],
                "max_interval": entry[1],
                "min_value": entry[2],
                "max_value": entry[3],
                "min_step": entry[4],
                "max_step": entry[5],
            },
        )
        if len(self.__default_config.metadata) > 0:
            metadata = Metadata(series_selector)
            for field_name, field_value in self.__default_config.metadata.items():
                metadata.coerce_field(field_name, field_value)
            return metadata
        return series_selector

    def _get_configuration(self, selector: SeriesSelector) -> StepSignalGeneratorConfig:
        return StepSignalGeneratorConfig(
            selector.tags["series name"],
            selector.tags["signal_type"],
            int(selector.tags["min_interval"]),
            int(selector.tags["max_interval"]),
            {},
            int(selector.tags["min_value"]),
            int(selector.tags["max_value"]),
            int(selector.tags["min_step"]),
            int(selector.tags["max_step"]),
        )


def _get_start_of_day(ts: datetime) -> datetime:
    current_day = ts.date()
    return datetime(
        current_day.year, current_day.month, current_day.day, tzinfo=timezone.utc
    )


class WhiteNoiseSignalGenerator(SignalGenerator):
    """White noise signal generator."""

    __default_config: Optional[WhiteNoiseSignalGeneratorConfig] = None

    def __init__(self, config: Optional[Dict] = None):
        super().__init__()
        if config is not None:
            self.__default_config = WhiteNoiseSignalGeneratorConfig(
                config["seriesName"],
                config["type"],
                config["samplingInterval"]["minInterval"],
                config["samplingInterval"]["maxInterval"],
                config.get("metadata", {}),
                config["values"]["mean"],
                config["values"]["standardDeviation"],
            )

    def generate(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generates white noise based on a selector start and end date."""
        current_time = start_date
        configuration = self._get_configuration(selector)
        ts = []
        value = []

        random.seed(
            sha1(
                configuration.to_bytes()
                + bytes(current_time.date().isoformat(), "UTF-8")
            ).hexdigest()
        )

        assert isinstance(configuration.mean, int)
        assert isinstance(configuration.standard_deviation, int)
        assert isinstance(configuration.min_interval, int)
        assert isinstance(configuration.max_interval, int)
        while current_time <= end_date:
            generated_value = numpy.random.normal(
                float(configuration.mean), float(configuration.standard_deviation), 1
            )[0]
            value.append(generated_value)
            ts.append(current_time)

            time_increment = random.randint(
                configuration.min_interval, configuration.max_interval
            )
            new_time = current_time + timedelta(seconds=time_increment)

            if new_time.date() != current_time.date():
                new_time = _get_start_of_day(new_time)

                random.seed(
                    sha1(
                        configuration.to_bytes()
                        + bytes(new_time.date().isoformat(), "UTF-8")
                    ).hexdigest()
                )

            current_time = new_time

        return _drop_data_before(
            pa.Table.from_pydict({"ts": ts, "value": value}), start_date
        )

    def list_series(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Yields all possible metadata combinations using the signal configuration and the provided selector."""
        arg_list = []
        assert self.__default_config is not None
        arg_list.append(
            _extract_from_tag(
                selector.tags, "min_interval", self.__default_config.min_interval
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags, "max_interval", self.__default_config.max_interval
            )
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "mean", self.__default_config.mean)
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "standard_deviation",
                self.__default_config.standard_deviation,
            )
        )

        for entry in itertools.product(*arg_list):
            yield self._build_search_result(entry, selector.source)

    def _build_search_result(
        self, entry: tuple, source_name: str
    ) -> Union[SeriesSelector, Metadata]:
        """Builds a series selector or metadata from a combination of configuration parameters."""
        assert self.__default_config is not None
        series_selector = SeriesSelector(
            source_name,
            {
                "series name": self.__default_config.series_name,
                "signal_type": self.__default_config.signal_type,
                "min_interval": entry[0],
                "max_interval": entry[1],
                "mean": str(entry[2]),
                "standard_deviation": str(entry[3]),
            },
        )
        if len(self.__default_config.metadata) > 0:
            metadata = Metadata(series_selector)
            for field_name, field_value in self.__default_config.metadata.items():
                metadata.coerce_field(field_name, field_value)
            return metadata
        return series_selector

    def _get_configuration(
        self, selector: SeriesSelector
    ) -> WhiteNoiseSignalGeneratorConfig:
        return WhiteNoiseSignalGeneratorConfig(
            selector.tags["series name"],
            selector.tags["signal_type"],
            int(selector.tags["min_interval"]),
            int(selector.tags["max_interval"]),
            {},
            int(selector.tags["mean"]),
            int(selector.tags["standard_deviation"]),
        )


class SineSignalGenerator(SignalGenerator):
    """Sine signal generator."""

    __default_config: Optional[SineSignalGeneratorConfig] = None

    def __init__(self, config: Optional[Dict] = None):
        super().__init__()
        if config is not None:
            self.__default_config = SineSignalGeneratorConfig(
                config["seriesName"],
                config["type"],
                config["samplingInterval"]["minInterval"],
                config["samplingInterval"]["maxInterval"],
                config.get("metadata", {}),
                config["values"]["period"],
                config["values"]["amplitude"],
                config["values"]["phase"],
                config["values"]["shift"],
            )

    def generate(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generates a sine function based on a selector start and end date."""
        current_time = start_date
        configuration = self._get_configuration(selector)
        ts = []
        value = []

        random.seed(
            sha1(
                configuration.to_bytes()
                + bytes(current_time.date().isoformat(), "UTF-8")
            ).hexdigest()
        )
        print(configuration)
        assert isinstance(configuration.period, int)
        assert isinstance(configuration.amplitude, int)
        assert isinstance(configuration.phase, int)
        assert isinstance(configuration.shift, int)
        assert isinstance(configuration.min_interval, int)
        assert isinstance(configuration.max_interval, int)

        current_value = 0
        while current_time <= end_date:
            multiplier = 2 * numpy.pi / configuration.period
            generated_value = (
                configuration.amplitude
                * numpy.sin((current_value + configuration.phase) * multiplier)
                + configuration.shift
            )
            value.append(generated_value)
            ts.append(current_time)

            time_increment = random.randint(
                configuration.min_interval, configuration.max_interval
            )
            current_value = current_value + time_increment
            new_time = current_time + timedelta(seconds=time_increment)

            if new_time.date() != current_time.date():
                new_time = _get_start_of_day(new_time)

                random.seed(
                    sha1(
                        configuration.to_bytes()
                        + bytes(new_time.date().isoformat(), "UTF-8")
                    ).hexdigest()
                )

            current_time = new_time

        return _drop_data_before(
            pa.Table.from_pydict({"ts": ts, "value": value}), start_date
        )

    def list_series(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Yields all possible metadata combinations using the signal configuration and the provided selector."""
        arg_list = []
        assert self.__default_config is not None
        arg_list.append(
            _extract_from_tag(
                selector.tags, "min_interval", self.__default_config.min_interval
            )
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags, "max_interval", self.__default_config.max_interval
            )
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "period", self.__default_config.period)
        )
        arg_list.append(
            _extract_from_tag(
                selector.tags,
                "amplitude",
                self.__default_config.amplitude,
            )
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "phase", self.__default_config.phase)
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "shift", self.__default_config.shift)
        )

        for entry in itertools.product(*arg_list):
            yield (self._build_search_result(entry, selector.source))

    def _build_search_result(
        self, entry: tuple, source_name: str
    ) -> Union[SeriesSelector, Metadata]:
        """Builds a series selector or metadata from a combination of configuration parameters."""
        series_selector = SeriesSelector(
            source_name,
            {
                "series name": self.__default_config.series_name,
                "signal_type": self.__default_config.signal_type,
                "min_interval": entry[0],
                "max_interval": entry[1],
                "period": str(entry[2]),
                "amplitude": str(entry[3]),
                "phase": str(entry[4]),
                "shift": str(entry[5]),
            },
        )
        if len(self.__default_config.metadata) > 0:
            metadata = Metadata(series_selector)
            for field_name, field_value in self.__default_config.metadata.items():
                metadata.coerce_field(field_name, field_value)
            return metadata
        else:
            return series_selector

    def _get_configuration(self, selector: SeriesSelector) -> SineSignalGeneratorConfig:
        return SineSignalGeneratorConfig(
            selector.tags["series name"],
            selector.tags["signal_type"],
            int(selector.tags["min_interval"]),
            int(selector.tags["max_interval"]),
            {},
            int(selector.tags["period"]),
            int(selector.tags["amplitude"]),
            int(selector.tags["phase"]),
            int(selector.tags["shift"]),
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
            self.__signal_generators["whitenoise"].append(WhiteNoiseSignalGenerator())
            self.__signal_generators["sine"].append(SineSignalGenerator())
        else:
            yaml_config = self.__load_yaml_config()
            for signal_config in yaml_config:
                self.register_generator(signal_config)

    def register_generator(self, config: dict):
        """Register a generator."""
        signal_type = config["type"]
        if signal_type == "step":
            self.__signal_generators[signal_type].append(StepSignalGenerator(config))
        if signal_type == "whitenoise":
            self.__signal_generators[signal_type].append(
                WhiteNoiseSignalGenerator(config)
            )
        if signal_type == "sine":
            print("Register sine")
            self.__signal_generators[signal_type].append(SineSignalGenerator(config))

    def __load_yaml_config(self) -> dict:
        assert self.__yaml_path is not None
        with self.__yaml_path.open(encoding="utf-8") as file:
            yaml_data = yaml.safe_load(file)
            return yaml_data["signals"]

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Yields all possible metadata combinations using the signal configuration and the provided selector."""
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
        """Data explorer currently always returns empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generates data in steps based on a selector start and end date."""
        if "signal_type" not in selector.tags:
            return pa.Table.from_pydict({"ts": [], "value": []})
        for generator in self.__signal_generators[selector.tags["signal_type"]]:
            return generator.generate(selector, start_date, end_date)

    def get_source_structure(self, _: SeriesSelector) -> Optional[SourceStructure]:
        """Return the structure of a source."""


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
