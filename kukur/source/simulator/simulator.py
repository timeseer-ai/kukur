"""Simulate a data source by generating data for Timeseer."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import yaml

from pathlib import Path

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, Optional, Protocol, Union
from schema import Optional as OptionalKey, Or, Schema

import pyarrow as pa

from kukur import Metadata, SeriesSearch, SeriesSelector, SourceStructure
from kukur.source.metadata import MetadataMapper, MetadataValueMapper


values_schema = Schema({
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
})

sampling_interval_schema = Schema({
    "minInterval": int,
    "maxInterval": int
})

block_schema = Schema({
    "namePrefix": str,
    "values": values_schema,
    "samplingInterval": sampling_interval_schema,
    OptionalKey("randomSeed"): Or(int, [int])
})

signal_schema = Schema({
    OptionalKey("sine"): block_schema,
    OptionalKey("whitenoise"): block_schema,
    OptionalKey("step"): block_schema,
})

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


def from_config(config: Dict[str, Any], metadata_mapper: MetadataMapper, metadata_value_mapper: MetadataValueMapper):
    return SimulatorSource(SimulatorConfiguration(config["signal_type"], config["path"]), metadata_mapper, metadata_value_mapper)


class SignalGenerator(Protocol):
    def generate(self) -> pa.Table:
        ...


class StepSignalGenerator(SignalGenerator):
    def __init__(self, config: dict):
        self.__config = config

    def generate(self) -> pa.Table:
        return pa.Table.from_pydict({"ts": [], "value": []})


class SimulatorSource:
    __signal_type: str
    __signal_generator: SignalGenerator

    __yaml_path: Path

    __metadata_mapper: MetadataMapper
    __metadata_value_mapper: MetadataValueMapper

    def __init__(self, config: SimulatorConfiguration, metadata_mapper: MetadataMapper, metadata_value_mapper: MetadataValueMapper):
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
        pass

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Data explorer currently always returns empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        return self.__signal_generator.generate()

    def get_source_structure(self, _: SeriesSelector) -> Optional[SourceStructure]:
        pass
