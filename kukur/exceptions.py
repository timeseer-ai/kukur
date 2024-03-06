"""Generic exceptions for Kukur data sources."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Optional


class KukurException(Exception):  # noqa: N818
    """Base class for all Exceptions thrown by Kukur."""


class InvalidDataError(KukurException):
    """Raised when the data itself is invalid."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"invalid data: {message}")


class InvalidSourceException(KukurException):
    """Raised when the source configuration is invalid."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"invalid source: {message}")


class MissingModuleException(KukurException):
    """Raised when a required Python module is not available."""

    def __init__(self, module_name: str, source_type: Optional[str] = None):
        if source_type is not None:
            KukurException.__init__(
                self,
                f'source type "{source_type}" requires Python package: "{module_name}"',
            )
        else:
            KukurException.__init__(
                self,
                f'missing Python package: "{module_name}"',
            )


class UnknownSourceException(KukurException):
    """Raised when the source is not known."""

    def __init__(self, source_name: str):
        KukurException.__init__(self, f"source does not exist: {source_name}")


class InvalidLogLevelException(KukurException):
    """Raised when the logging level in the configuration is invalid."""

    def __init__(self):
        super().__init__("Configured log level unknown.")
