"""Time series metadata.

Metadata in Kukur is flexible. Some fields are common to all time series.
They have been defined here. Users of Kukur can define their own metadata fields.
"""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Callable, Generator, Generic, Optional, TypeVar

from kukur import SeriesSelector

T = TypeVar("T")


class MetadataField(Generic[T]):
    """A generic base class for Timeseer metadata fields."""

    def __init__(
        self,
        name: str,
        *,
        default: T,
        serialized_name: str,
        serialize: Optional[Callable[[T], Any]] = None,
        deserialize: Optional[Callable[[Any], T]] = None,
    ):
        self.__name = name
        self.__default = default
        self.__serialized_name = serialized_name
        self.__serialize = serialize
        self.__deserialize = deserialize

    def __repr__(self) -> str:
        return f'MetadataField("{self.__name}")'

    def __hash__(self):
        return hash(self.__name)

    def name(self) -> str:
        """Return the name of the metadata field."""
        return self.__name

    def default(self) -> T:
        """Return the default (empty) value for this field."""
        return self.__default

    def serialized_name(self) -> str:
        """Return the name of the field in a JSON dictionary."""
        return self.__serialized_name

    def serialize(self, value: T) -> Any:
        """Convert a field of this type to JSON."""
        if self.__serialize is None:
            return value
        return self.__serialize(value)

    def deserialize(self, value: Any) -> T:
        """Convert the result of a serialization to a proper value."""
        if self.__deserialize is None:
            return value
        return self.__deserialize(value)


class MetadataFields:
    """Metadata fields.

    Can be extended."""

    _fields: list[MetadataField] = []

    @classmethod
    def register_field(cls, field: MetadataField[T]):
        """Register a new metadata field"""
        cls._fields.append(field)

    def __init__(self, values: Optional[dict[MetadataField, Any]] = None):
        self.__values = {k: k.default() for k in self._fields}
        if values is not None:
            for field, value in values.items():
                self.__values[field] = value

    def __iter__(self) -> Generator[tuple[MetadataField, Any], None, None]:
        for field in self._fields:
            yield (field, self.__values.get(field))

    def find_field(self, field_name: str) -> MetadataField:
        """Return the MetadataField with the given name.

        Name can either be the human readable name or a camelCase name for JSON."""
        for field in self._fields:
            if field.name() == field_name or field.serialized_name() == field_name:
                return field
        raise AttributeError()

    def set_field(self, field: MetadataField[T], value: T):
        """Set the given field to the specified value."""
        if field not in self._fields:
            raise AttributeError()
        self.__values[field] = value

    def iter_human(self) -> Generator[tuple[str, Any], None, None]:
        """Iterate over all metadata fields, but use the human readable names and serialized values."""
        for field in self._fields:
            yield (field.name(), field.serialize(self.__values.get(field)))

    def coerce_field(self, field_name: str, value: Any):
        """Set the field of the given name to the corresponding value.

        This tries to coerce the given value into a correctly typed one."""
        field = self.find_field(field_name)
        self.__values[field] = field.deserialize(value)

    def get_field(self, field: MetadataField[T]) -> T:
        """Return the value of the given field."""
        if field not in self._fields:
            raise AttributeError()
        return self.__values[field]

    def to_data(self) -> dict[str, Any]:
        """Convert the metadata to a Dictionary with camelcase keys as expected in JSON."""
        return {
            k.serialized_name(): k.serialize(self.get_field(k)) for k in self._fields
        }


class Metadata(MetadataFields):
    """Metadata fields about a time series."""

    series: SeriesSelector

    @classmethod
    def register_field(cls, field: MetadataField[T]):
        """Register a new metadata field"""
        super().register_field(field)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], series: Optional[SeriesSelector] = None
    ) -> "Metadata":
        """Create a new Metadata object from a dictionary produced by camelcase().

        This used the provided series as selector, otherwise the series is expected to be inside the data."""
        if series is None:
            if "series" not in data:
                raise AttributeError()
            series = SeriesSelector(data["series"]["source"], data["series"]["name"])
        metadata = cls(series)
        for k, v in data.items():
            if k == "series":
                continue
            metadata.coerce_field(k, v)
        return metadata

    def __init__(
        self,
        series: SeriesSelector,
        values: Optional[dict[MetadataField, Any]] = None,
    ):
        super().__init__(values)
        self.series = series

    def to_data(self) -> dict[str, Any]:
        """Convert the metadata to a Dictionary with camelcase keys as expected in JSON."""
        data = super().to_data()
        data["series"] = {
            "source": self.series.source,
            "name": self.series.name,
        }
        return data
