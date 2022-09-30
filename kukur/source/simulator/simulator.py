"""Simulate a data source by generating data for Timeseer."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import yaml
import random
import itertools

from pathlib import Path

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional, Protocol, Union
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
        OptionalKey("sd"): [int],
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
    metadata_mapper: MetadataMapper,
    metadata_value_mapper: MetadataValueMapper,
):
    return SimulatorSource(
        SimulatorConfiguration(config["signal_type"], config["path"]),
        metadata_mapper,
        metadata_value_mapper,
    )


class SignalGenerator(Protocol):
    def generate(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        ...

    def list_series(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        ...


@dataclass
class SignalGeneratorConfig:
    name_prefix: str
    min_interval: List[int]
    max_interval: List[int]
    random_seed: List[int]


@dataclass
class StepSignalGeneratorConfig(SignalGeneratorConfig):
    min_value: List[int]
    max_value: List[int]
    min_step: List[int]
    max_step: List[int]


class StepSignalGenerator(SignalGenerator):
    __config: StepSignalGeneratorConfig

    def __init__(self, config: dict):
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
        current_time = start_date
        tags = selector.tags
        ts = []
        value = []
        random_seed = int(tags["random_seed"])
        random.seed(start_date.timestamp() * random_seed)
        while current_time <= end_date:
            generated_step = random.uniform(
                float(tags["min_step"]), float(tags["max_step"])
            )
            clamped_step = max(
                min(generated_step, float(tags["max_value"])), float(tags["min_value"])
            )

            value.append(clamped_step)
            ts.append(current_time)

            time_increment = random.randint(
                int(tags["min_interval"]), int(tags["max_interval"])
            )
            new_time = current_time + timedelta(seconds=time_increment)

            if new_time.date() != current_time.date():
                random.seed(new_time.timestamp() * random_seed)

            current_time = new_time

        return pa.Table.from_pydict({"ts": ts, "value": value})

    def list_series(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        arg_list = []
        arg_list.append(
            _extract_from_tag(selector.tags, "min_interval", self.__config.min_interval)
        )
        arg_list.append(
            _extract_from_tag(selector.tags, "max_interval", self.__config.max_interval)
        )
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
                        "min_interval": str(entry[0]),
                        "max_interval": str(entry[1]),
                        "min_value": str(entry[2]),
                        "max_value": str(entry[3]),
                        "min_step": str(entry[4]),
                        "max_step": str(entry[5]),
                        "random_seed": str(entry[6]),
                    },
                )
            )


class SimulatorSource:
    __signal_type: str
    __signal_generator: SignalGenerator

    __yaml_path: Path

    __metadata_mapper: MetadataMapper
    __metadata_value_mapper: MetadataValueMapper

    def __init__(
        self,
        config: SimulatorConfiguration,
        metadata_mapper: MetadataMapper,
        metadata_value_mapper: MetadataValueMapper,
    ):
        self.__signal_type = config.signal_type
        self.__yaml_path = Path(config.path)
        self.__metadata_mapper = metadata_mapper
        self.__metadata_value_mapper = metadata_value_mapper
        self.__signal_generator = StepSignalGenerator(self.__load_yaml_config())

    def __load_yaml_config(self) -> dict:
        with self.__yaml_path.open() as file:
            yaml_data = yaml.safe_load(file)
            return yaml_data["signals"][self.__signal_type]

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        return self.__signal_generator.list_series(selector)

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Data explorer currently always returns empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        return self.__signal_generator.generate(selector, start_date, end_date)

    def get_source_structure(self, _: SeriesSelector) -> Optional[SourceStructure]:
        pass


def _extract_from_tag(tags: Dict[str, str], key: str, fallback: list) -> list[str]:
    if key in tags:
        return [tags[key]]
    return fallback
