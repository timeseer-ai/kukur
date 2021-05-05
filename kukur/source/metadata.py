"""Metadata mappings for Kukur data sources."""

from typing import Dict, List, Union


class MetadataMapper:
    """MetadataMapper maps names for metadata fields used in a source to the names known by Kukur."""

    __mapping: Dict[str, str]

    @classmethod
    def from_config(cls, config: Dict[str, str]) -> "MetadataMapper":
        """Create a new mapper from a dictionary that maps Kukur names to external names."""
        mapper = cls()
        for k, v in config.items():
            mapper.add_mapping(k, v)
        return mapper

    def __init__(self):
        self.__mapping = {}

    def add_mapping(self, kukur_field_name: str, external_field_name: str):
        """Add a mapping"""
        self.__mapping[kukur_field_name] = external_field_name

    def from_kukur(self, kukur_field_name: str) -> str:
        """Map a metadata field name defined by kukur to an external field name.

        Returns the field name as is when no mapping is defined."""
        return self.__mapping.get(kukur_field_name, kukur_field_name)


class MetadataValueMapper:
    """MetadataFieldMapper maps values for a metadata field to values known by Kukur."""

    __mapping: Dict[str, Dict[Union[str, int], str]]

    def __init__(self):
        self.__mapping = {}

    @classmethod
    def from_config(
        cls, config: Dict[str, Dict[str, Union[str, List[str]]]]
    ) -> "MetadataValueMapper":
        """Create a new mapper from a double dictionary

        The double dictionary maps kukur field names and kukur field values to external values."""
        mapper = cls()
        for field_name, field_mapping in config.items():
            for field_value, external_field_value in field_mapping.items():
                if isinstance(external_field_value, (str, int)):
                    mapper.add_mapping(field_name, field_value, external_field_value)
                else:
                    for choice in external_field_value:
                        mapper.add_mapping(field_name, field_value, choice)
        return mapper

    def add_mapping(
        self,
        field_name: str,
        kukur_field_value: str,
        external_field_value: Union[str, int],
    ):
        """Add a mapping for a metadata field value known to kukur from the external value."""
        if field_name not in self.__mapping:
            self.__mapping[field_name] = {}
        self.__mapping[field_name][external_field_value] = kukur_field_value

    def from_source(
        self, field_name: str, external_field_value: Union[str, int]
    ) -> str:
        """Map a field value as it's known in an external source to one known by Kukur."""
        if field_name not in self.__mapping:
            return str(external_field_value)
        return self.__mapping[field_name].get(
            external_field_value, str(external_field_value)
        )
