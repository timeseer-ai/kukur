"""Simulate series sets that each hold one control series and several other series.

The control series switches between two levels, so a threshold segments it in active periods.
Every other series follows white noise, a drift or a sine, picked at random.
Every series is sampled on its own regular grid and loses samples in archival gaps.
The number and the length of the gaps grow with a single severity parameter.
Data is deterministic: it only depends on the configuration and the requested time.
"""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import base64
import math
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256, shake_256
from pathlib import Path
from typing import Any

try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

import pyarrow as pa

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False

from kukur import DataType, InterpolationType, Metadata, SeriesSearch, SeriesSelector
from kukur.exceptions import InvalidSourceException, MissingModuleException
from kukur.metadata import fields

SOURCE_TYPE = "series-set-simulator"

DAY_SECONDS = 86400
DAY_MICROSECONDS = DAY_SECONDS * 1_000_000
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MICROSECOND = timedelta(microseconds=1)

ACTIVE_LOW = 0.0
ACTIVE_HIGH = 50.0
ACTIVE_JITTER = 1.0
ACTIVE_SWITCHES_PER_DAY = 4.0

WHITE_NOISE = "white noise"
DRIFT = "drift"
SINE = "sine"
FUNCTIONS = (WHITE_NOISE, DRIFT, SINE)

SINE_PERIOD_SECONDS_MIN = 600
SINE_PERIOD_SECONDS_MAX = 2 * DAY_SECONDS
DRIFT_SEGMENT_SECONDS_MIN = 6 * 3600
DRIFT_SEGMENT_SECONDS_MAX = 3 * DAY_SECONDS

MAX_GAPS_PER_DAY = 6.0
GAP_MULTIPLIER_MIN = 5.0
GAP_MULTIPLIER_RANGE = 995.0
GAP_MAX_SECONDS = 12 * 3600


@dataclass(frozen=True)
class SeriesSetSimulatorConfiguration:
    """Configuration of a series set simulator.

    Severity runs from 0.0 (no gaps) to 1.0 (frequent, long gaps).
    """

    seed: int
    series_sets: int
    series_per_set: int
    interval_seconds_min: float
    interval_seconds_max: float
    severity: float
    control_attribute: str

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> "SeriesSetSimulatorConfiguration":
        """Read the configuration from the YAML data."""
        sampling_interval = data.get("samplingInterval", {})
        config = cls(
            seed=int(data.get("seed", 0)),
            series_sets=int(data["seriesSets"]),
            series_per_set=int(data["seriesPerSet"]),
            interval_seconds_min=float(sampling_interval["intervalSecondsMin"]),
            interval_seconds_max=float(sampling_interval["intervalSecondsMax"]),
            severity=float(data.get("severity", 0.0)),
            control_attribute=str(data.get("controlAttribute", "active")),
        )
        config.validate()
        return config

    def validate(self):
        """Raise InvalidSourceException when the configuration is out of range."""
        if self.series_sets < 1:
            raise InvalidSourceException("seriesSets must be at least 1")
        if self.series_per_set < 1:
            raise InvalidSourceException("seriesPerSet must be at least 1")
        if not 0 < self.interval_seconds_min <= self.interval_seconds_max:
            raise InvalidSourceException(
                "samplingInterval must satisfy 0 < intervalSecondsMin <= intervalSecondsMax"
            )
        if not 0.0 <= self.severity <= 1.0:
            raise InvalidSourceException("severity must be between 0 and 1")


def from_config(config: dict[str, Any]) -> "SeriesSetSimulatorSource":
    """Create a new series set simulator source from the given configuration dictionary."""
    if not HAS_NUMPY:
        raise MissingModuleException("numpy", SOURCE_TYPE)
    if not HAS_YAML:
        raise MissingModuleException("PyYAML", SOURCE_TYPE)
    if "path" not in config:
        raise InvalidSourceException(f'"{SOURCE_TYPE}" sources require a "path"')
    with Path(config["path"]).open(encoding="utf-8-sig") as file:
        yaml_data = yaml.safe_load(file)
    return SeriesSetSimulatorSource(
        SeriesSetSimulatorConfiguration.from_data(yaml_data or {})
    )


@dataclass(frozen=True)
class _SeriesParameters:
    interval_us: int
    phase_us: int
    function: str
    scale: float
    offset: float
    noise: float
    sine_period_us: int
    sine_phase: float
    drift_segment_us: int
    gap_weight: float


class SeriesSetSimulatorSource:
    """A source of series sets, one per element, with a control series in each set."""

    def __init__(self, config: SeriesSetSimulatorConfiguration):
        self.__config = config

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Yield the metadata of every series of every series set."""
        attributes = [self.__config.control_attribute] + [
            _numbered("Sensor", i, self.__config.series_per_set)
            for i in range(1, self.__config.series_per_set + 1)
        ]
        for i in range(1, self.__config.series_sets + 1):
            element = _numbered("Plant", i, self.__config.series_sets)
            for attribute in attributes:
                yield self._get_metadata(
                    SeriesSelector(
                        selector.source,
                        {
                            "series name": element,
                            "Attribute": attribute,
                            "__id__": self._web_id(element, attribute),
                        },
                        attribute,
                    )
                )

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return the metadata of a simulated series."""
        return self._get_metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return the data of a simulated series in [start_date, end_date)."""
        element = selector.tags.get("series name")
        attribute = selector.tags.get("Attribute")
        if element is None or attribute is None:
            return _to_table(np.empty(0, dtype=np.int64), np.empty(0))
        start_us = _to_epoch_microseconds(start_date)
        end_us = _to_epoch_microseconds(end_date)
        if end_us <= start_us:
            return _to_table(np.empty(0, dtype=np.int64), np.empty(0))

        parameters = self._get_parameters(element, attribute)
        first_day = start_us // DAY_MICROSECONDS
        last_day = end_us // DAY_MICROSECONDS

        timestamps = []
        values = []
        for day in range(first_day, last_day + 1):
            day_timestamps, day_values = self._generate_day(
                element, attribute, parameters, day
            )
            timestamps.append(day_timestamps)
            values.append(day_values)
        ts_us = np.concatenate(timestamps)
        value = np.concatenate(values)

        keep = (ts_us >= start_us) & (ts_us < end_us)
        for gap_start, gap_end in self._gaps(
            element,
            attribute,
            parameters,
            first_day - 1,
            last_day,
        ):
            keep &= (ts_us <= gap_start) | (ts_us >= gap_end)
        return _to_table(ts_us[keep], value[keep])

    def _get_metadata(self, selector: SeriesSelector) -> Metadata:
        metadata = Metadata(selector)
        attribute = selector.tags.get("Attribute")
        element = selector.tags.get("series name")
        if attribute == self.__config.control_attribute:
            description = f"{attribute} of {element} (control series)"
        elif element is None or attribute is None:
            description = f"{attribute} of {element}"
        else:
            function = self._get_parameters(element, attribute).function
            description = f"{attribute} of {element} ({function})"
        metadata.set_field(fields.Description, description)
        metadata.set_field(fields.DataType, DataType.FLOAT64)
        metadata.set_field(fields.InterpolationType, InterpolationType.LINEAR)
        return metadata

    def _get_parameters(self, element: str, attribute: str) -> _SeriesParameters:
        rng = self._rng(element, attribute, "series")
        interval_us = max(
            1,
            round(
                1e6
                * rng.uniform(
                    self.__config.interval_seconds_min,
                    self.__config.interval_seconds_max,
                )
            ),
        )
        phase_us = int(rng.integers(0, interval_us))
        function = FUNCTIONS[int(rng.integers(0, len(FUNCTIONS)))]
        scale = rng.uniform(1, 100)
        return _SeriesParameters(
            interval_us=interval_us,
            phase_us=phase_us,
            function=function,
            scale=scale,
            offset=rng.uniform(-50, 100),
            noise=scale if function == WHITE_NOISE else scale * rng.uniform(0.01, 0.1),
            sine_period_us=round(
                1e6
                * math.exp(
                    rng.uniform(
                        math.log(SINE_PERIOD_SECONDS_MIN),
                        math.log(SINE_PERIOD_SECONDS_MAX),
                    )
                )
            ),
            sine_phase=rng.uniform(0, 2 * math.pi),
            drift_segment_us=int(
                1e6 * rng.uniform(DRIFT_SEGMENT_SECONDS_MIN, DRIFT_SEGMENT_SECONDS_MAX)
            ),
            gap_weight=rng.uniform(0, 2),
        )

    def _generate_day(
        self, element: str, attribute: str, parameters: _SeriesParameters, day: int
    ) -> tuple[Any, Any]:
        interval = parameters.interval_us
        phase = parameters.phase_us
        first_index = -((phase - day * DAY_MICROSECONDS) // interval)
        end_index = -((phase - (day + 1) * DAY_MICROSECONDS) // interval)
        timestamps = (
            phase + np.arange(first_index, end_index, dtype=np.int64) * interval
        )

        noise = self._rng(element, attribute, "noise", day).normal(
            0, 1, len(timestamps)
        )
        if attribute == self.__config.control_attribute:
            values = self._active(element, timestamps, day) + ACTIVE_JITTER * noise
        else:
            values = (
                self._signal(element, attribute, parameters, timestamps)
                + parameters.noise * noise
            )
        return timestamps, values

    def _active(self, element: str, timestamps: Any, day: int) -> Any:
        rng = self._rng(element, "control", day)
        initial = int(rng.integers(0, 2))
        count = rng.poisson(ACTIVE_SWITCHES_PER_DAY)
        switches = np.sort(
            day * DAY_MICROSECONDS + rng.integers(0, DAY_MICROSECONDS, count)
        )
        active = (initial + np.searchsorted(switches, timestamps, side="right")) % 2
        return np.where(active == 1, ACTIVE_HIGH, ACTIVE_LOW)

    def _signal(
        self,
        element: str,
        attribute: str,
        parameters: _SeriesParameters,
        timestamps: Any,
    ) -> Any:
        if parameters.function == SINE:
            period = parameters.sine_period_us
            angle = 2 * np.pi * (timestamps % period) / period + parameters.sine_phase
            return parameters.offset + parameters.scale * np.sin(angle)
        if parameters.function == DRIFT and len(timestamps) > 0:
            segment_us = parameters.drift_segment_us
            segments = timestamps // segment_us
            fraction = (timestamps - segments * segment_us) / segment_us
            first = int(segments[0])
            levels = np.array(
                [
                    self._rng(element, attribute, "drift", k).uniform(-1, 1)
                    for k in range(first, int(segments[-1]) + 2)
                ]
            )
            index = segments - first
            level = levels[index] * (1 - fraction) + levels[index + 1] * fraction
            return parameters.offset + parameters.scale * level
        return np.full(len(timestamps), parameters.offset)

    def _gaps(
        self,
        element: str,
        attribute: str,
        parameters: _SeriesParameters,
        first_day: int,
        last_day: int,
    ) -> Generator[tuple[int, int], None, None]:
        severity = self.__config.severity
        if severity == 0:
            return
        gaps_per_day = MAX_GAPS_PER_DAY * severity * parameters.gap_weight
        multiplier_min = math.log(GAP_MULTIPLIER_MIN)
        multiplier_max = math.log(GAP_MULTIPLIER_MIN + GAP_MULTIPLIER_RANGE * severity)
        for day in range(first_day, last_day + 1):
            rng = self._rng(element, attribute, "gaps", day)
            count = rng.poisson(gaps_per_day)
            starts = day * DAY_MICROSECONDS + rng.integers(0, DAY_MICROSECONDS, count)
            multipliers = np.exp(rng.uniform(multiplier_min, multiplier_max, count))
            durations = np.minimum(
                multipliers * parameters.interval_us, GAP_MAX_SECONDS * 1_000_000
            ).astype(np.int64)
            for gap_start, duration in zip(starts, durations, strict=True):
                yield int(gap_start), int(gap_start + duration)

    def _web_id(self, element: str, attribute: str) -> str:
        element_part = shake_256(f"{self.__config.seed}\x1f{element}".encode()).digest(
            48
        )
        attribute_part = shake_256(
            f"{self.__config.seed}\x1f{element}\x1f{attribute}".encode()
        ).digest(69)
        return (
            "F1AbE" + base64.urlsafe_b64encode(element_part + attribute_part).decode()
        )

    def _rng(self, *parts: str | int) -> Any:
        key = "\x1f".join(str(part) for part in (self.__config.seed, *parts))
        return np.random.default_rng(
            int.from_bytes(sha256(key.encode("utf-8")).digest()[:16], "little")
        )


def _numbered(prefix: str, index: int, count: int) -> str:
    return f"{prefix} {index:0{len(str(count))}d}"


def _to_epoch_microseconds(ts: datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (ts - EPOCH) // MICROSECOND


def _to_table(ts_us: Any, values: Any) -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array(ts_us, type=pa.int64()).cast(pa.timestamp("us", "UTC")),
            pa.array(values, type=pa.float64()),
        ],
        names=["ts", "value"],
    )
