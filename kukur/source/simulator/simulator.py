"""Simulate a data source by generating data for Timeseer."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional, Protocol, Union

import itertools
import random
import numpy
import yaml

from schema import Optional as OptionalKey, Or, Schema

import pyarrow as pa

from kukur import Metadata, SeriesSearch, SeriesSelector, SourceStructure
from kukur.source.metadata import MetadataMapper, MetadataValueMapper


values_schema = Schema(
    {
        OptionalKey("period"): Or(int, [int]),
        OptionalKey("amplitude"): Or(int, [int]),
        OptionalKey("phase"): Or(int, [int]),
        OptionalKey("shift"): Or(int, [int]),
        OptionalKey("standardDeviation"): [int],
        OptionalKey("mean"): [int],
        OptionalKey("minValue"): int,
        OptionalKey("maxValue"): int,
        OptionalKey("minStep"): int,
        OptionalKey("maxStep"): int,
    }
)

sampling_interval_schema = Schema({"minInterval": int, "maxInterval": int})

block_schema = Schema(
    {
        "namePrefix": str,
        "values": values_schema,
        "samplingInterval": sampling_interval_schema,
        OptionalKey("randomSeed"): Or(int, [int]),
    }
)

signal_schema = Schema(
    {
        OptionalKey("sine"): block_schema,
        OptionalKey("whitenoise"): block_schema,
        OptionalKey("step"): block_schema,
    }
)

signals_schema = Schema(
    {
        "signals": [signal_schema],
    }
)


@dataclass
class SimulatorConfiguration:
    """Simulator source configuration."""

    signal_type: str
    path: str


def from_config(
    config: Dict[str, Any],
    _metadata_mapper: MetadataMapper,
    _metadata_value_mapper: MetadataValueMapper,
):
    """Create a new Simulator data source from the given configuration dictionary."""
    return SimulatorSource(
        SimulatorConfiguration(config["signal_type"], config["path"])
    )


class SignalGenerator(Protocol):
    """Protocol for generating signals."""

    def generate(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generates data based on a selector, start date and end date."""
        ...

    def list_series(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Yields all possible metadata combinations using the signal configuration and the provided selector."""
        ...


@dataclass
class SignalGeneratorConfig:
    """Base signal configuration."""

    name_prefix: str
    min_interval: int
    max_interval: int
    random_seed: List[int]


@dataclass
class StepSignalGeneratorConfig(SignalGeneratorConfig):
    """Configuration for the step signal."""

    min_value: List[int]
    max_value: List[int]
    min_step: List[int]
    max_step: List[int]


@dataclass
class WhiteNoiseSignalGeneratorConfig(SignalGeneratorConfig):
    """Configuration for the step signal."""

    mean: List[int]
    standard_deviation: List[int]


class StepSignalGenerator(SignalGenerator):
    """Step signal generator."""

    __config: StepSignalGeneratorConfig

    def __init__(self, config: dict):
        super().__init__()
        self.__config = StepSignalGeneratorConfig(
            config["namePrefix"],
            config["samplingInterval"]["minInterval"],
            config["samplingInterval"]["maxInterval"],
            config["randomSeed"],
            config["values"]["minValue"],
            config["values"]["maxValue"],
            config["values"]["minStep"],
            config["values"]["maxStep"],
        )

    def generate(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generates data in steps based on a selector start and end date."""
        current_time = start_date
        tags = selector.tags
        ts = []
        value = []
        random_seed = int(tags["random_seed"])
        random.seed(start_date.timestamp() * random_seed)
        current_value = (float(tags["max_value"]) + float(tags["min_value"])) / 2
        while current_time <= end_date:
            generated_step = random.uniform(
                float(tags["min_step"]), float(tags["max_step"])
            )
            generated_step += current_value
            clamped_step = max(
                min(generated_step, float(tags["max_value"])), float(tags["min_value"])
            )
            current_value = clamped_step
            value.append(clamped_step)
            ts.append(current_time)

            time_increment = random.randint(
                self.__config.min_interval, self.__config.max_interval
            )
            new_time = current_time + timedelta(seconds=time_increment)

            if new_time.date() != current_time.date():
                random.seed(new_time.timestamp() * random_seed)

            current_time = new_time

        return pa.Table.from_pydict({"ts": ts, "value": value})

    def list_series(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Yields all possible metadata combinations using the signal configuration and the provided selector."""
        arg_list = []
        arg_list.append(
            _extract_from_tag(selector.tags, "min_value", self.__config.min_value)
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "max_value", self.__config.max_value)
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "min_step", self.__config.min_step)
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "max_step", self.__config.max_step)
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "random_seed", self.__config.random_seed)
        )

        for entry in itertools.product(*arg_list):
            yield (
                SeriesSelector(
                    selector.source,
                    {
                        "series name": self.__config.name_prefix,
                        "min_value": str(entry[0]),
                        "max_value": str(entry[1]),
                        "min_step": str(entry[2]),
                        "max_step": str(entry[3]),
                        "random_seed": str(entry[4]),
                    },
                )
            )


class WhiteNoiseSignalGenerator(SignalGenerator):
    """White noise signal generator."""

    __config: WhiteNoiseSignalGeneratorConfig

    def __init__(self, config: dict):
        super().__init__()
        self.__config = WhiteNoiseSignalGeneratorConfig(
            config["namePrefix"],
            config["samplingInterval"]["minInterval"],
            config["samplingInterval"]["maxInterval"],
            config["randomSeed"],
            config["values"]["mean"],
            config["values"]["standardDeviation"],
        )

    def generate(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generates white noise based on a selector start and end date."""
        current_time = start_date
        tags = selector.tags
        ts = []
        value = []
        random_seed = int(tags["random_seed"])
        random.seed(start_date.timestamp() * random_seed)

        while current_time <= end_date:
            generated_value = numpy.random.normal(
                float(tags["mean"]), float(tags["standard_deviation"]), 1
            )[0]
            value.append(generated_value)
            ts.append(current_time)

            time_increment = random.randint(
                self.__config.min_interval, self.__config.max_interval
            )
            new_time = current_time + timedelta(seconds=time_increment)

            if new_time.date() != current_time.date():
                random.seed(new_time.timestamp() * random_seed)

            current_time = new_time

        return pa.Table.from_pydict({"ts": ts, "value": value})

    def list_series(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Yields all possible metadata combinations using the signal configuration and the provided selector."""
        arg_list = []
        arg_list.append(_extract_from_tag(selector.tags, "mean", self.__config.mean))
        arg_list.append(
            _extract_from_tag(
                selector.tags, "standard_deviation", self.__config.standard_deviation
            )
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "random_seed", self.__config.random_seed)
        )

        for entry in itertools.product(*arg_list):
            yield (
                SeriesSelector(
                    selector.source,
                    {
                        "series name": self.__config.name_prefix,
                        "mean": str(entry[0]),
                        "standard_deviation": str(entry[1]),
                        "random_seed": str(entry[2]),
                    },
                )
            )


class SimulatorSource:
    """A simulator data source."""

    __signal_type: str
    __signal_generator: SignalGenerator

    __yaml_path: Path

    def __init__(self, config: SimulatorConfiguration):
        self.__signal_type = config.signal_type
        self.__yaml_path = Path(config.path)

        if self.__signal_type == "step":
            self.__signal_generator = StepSignalGenerator(self.__load_yaml_config())
        else:
            self.__signal_generator = WhiteNoiseSignalGenerator(
                self.__load_yaml_config()
            )

    def __load_yaml_config(self) -> dict:
        with self.__yaml_path.open(encoding="utf-8") as file:
            yaml_data = yaml.safe_load(file)
            return yaml_data["signals"][self.__signal_type]

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Yields all possible metadata combinations using the signal configuration and the provided selector."""
        return self.__signal_generator.list_series(selector)

    # pylint: disable=no-self-use
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Data explorer currently always returns empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generates data in steps based on a selector start and end date."""
        return self.__signal_generator.generate(selector, start_date, end_date)

    def get_source_structure(self, _: SeriesSelector) -> Optional[SourceStructure]:
        """Return the structure of a source."""


def _extract_from_tag(tags: Dict[str, str], key: str, fallback: list) -> list[str]:
    if key in tags:
        return [tags[key]]
    return fallback
