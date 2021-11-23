"""Time series metadata.

Metadata in Kukur is flexible. Some fields are common to all time series.
They have been defined here. Users of Kukur can define their own metadata fields.
"""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Generator, Optional, TypeVar, Union

from kukur import SeriesSelector

from .fields import MetadataField, register_default_fields

T = TypeVar("T")


class MetadataFields:
    """Metadata fields is a collection of custom metadata fields and their values.

    Fields can be typed and registered to MetadataFields by providing a MetadataField object.
    Alternatively, untyped methods that accept fields by name are also available.
    """

    _fields: list[MetadataField] = []
    __values: dict[Union[str, MetadataField], Any]

    @classmethod
    def register_field(
        cls, field: MetadataField[T], *, after_field: Optional[MetadataField] = None
    ):
        """Register a new metadata field.

        Optionally insert it right after the given field in the field ordering."""
        if after_field is not None:
            cls._fields.insert(cls._fields.index(after_field) + 1, field)
        else:
            cls._fields.append(field)

    def __init__(self, values: Optional[dict[MetadataField, Any]] = None):
        self.__values = {k: k.default() for k in self._fields}
        if values is not None:
            for field, value in values.items():
                self.__values[field] = value

    def iter_fields(self) -> Generator[tuple[MetadataField, Any], None, None]:
        """Iterate over all typed metadata fields and their values."""
        for field in self._fields:
            yield (field, field.calculated_value(self, self.__values.get(field)))

    def iter_names(self) -> Generator[tuple[str, Any], None, None]:
        """Iterate over all metadata fields (typed and untyped) and return their names and values."""
        for field, value in self.__values.items():
            if isinstance(field, MetadataField):
                yield (field.name(), field.calculated_value(self, value))
            else:
                yield (field, value)

    def iter_serialized(self) -> Generator[tuple[str, Any], None, None]:
        """Iterate over all metadata fields, but use the human readable names and serialized values."""
        for field, value in self.__values.items():
            if isinstance(field, MetadataField):
                yield (
                    field.name(),
                    field.serialize(field.calculated_value(self, value)),
                )
            else:
                yield (field, value)

    def find_field(self, field_name: str) -> MetadataField:
        """Return the MetadataField with the given name.

        Name can either be the human readable name or a camelCase name for JSON."""
        field = self._find_field(field_name)
        if field is None:
            raise AttributeError()
        return field

    def set_field(self, field: MetadataField[T], value: T):
        """Set the given field to the specified value."""
        if field not in self._fields:
            raise AttributeError()
        self.__values[field] = value

    def set_field_by_name(self, field_name: str, value: Any):
        """Set the field of the given name to the corresponding value."""
        field = self._find_field(field_name)
        if field is None:
            self.__values[field_name] = value
        else:
            self.__values[field] = value

    def coerce_field(self, field_name: str, value: Any):
        """Set the field of the given name to the corresponding value.

        This tries to coerce the given value into a correctly typed one."""
        field = self._find_field(field_name)
        if field is not None:
            self.__values[field] = field.deserialize(value)
        else:
            self.__values[field_name] = value

    def get_field(self, field: MetadataField[T]) -> T:
        """Return the value of the given field."""
        if field not in self._fields:
            raise AttributeError()
        return field.calculated_value(self, self.__values[field])

    def get_field_by_name(self, field_name: str) -> Any:
        """Return the value of the given field.

        When a field has not been defined, the value will be the serialized value."""
        field = self._find_field(field_name)
        if field is None:
            if field_name not in self.__values:
                return None
            return self.__values[field_name]
        return self.get_field(field)

    def to_data(self) -> dict[str, Any]:
        """Convert the metadata to a Dictionary with camelcase keys as expected in JSON."""
        data = {}
        for field, value in self.__values.items():
            if isinstance(field, MetadataField):
                data[field.serialized_name()] = field.serialize(value)
            else:
                data[field] = value
        return data

    def _find_field(self, field_name: str) -> Optional[MetadataField]:
        for field in self._fields:
            if field.name() == field_name or field.serialized_name() == field_name:
                return field
        return None


class Metadata(MetadataFields):
    """Metadata fields for one time series."""

    series: SeriesSelector

    @classmethod
    def from_data(
        cls, data: dict[str, Any], series: Optional[SeriesSelector] = None
    ) -> "Metadata":
        """Create a new Metadata object from a dictionary produced by to_data().

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

    def __repr__(self) -> str:
        data = dict(self.iter_names())
        return f"Metadata({self.series}, {data})"

    def to_data(self) -> dict[str, Any]:
        """Convert the metadata to a Dictionary with camelcase keys as expected in JSON."""
        data = super().to_data()
        data["series"] = {
            "source": self.series.source,
            "name": self.series.name,
        }
        return data


register_default_fields(Metadata)
