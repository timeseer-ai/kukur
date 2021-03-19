"""Generic exceptions for Kukur data sources."""


class InvalidDataError(Exception):
    """Raised when the data itself is invalid."""

    def __init__(self, message: str):
        Exception.__init__(self, f"invalid data: {message}")


class InvalidSourceException(Exception):
    """Raised when the source configuration is invalid."""

    def __init__(self, message: str):
        Exception.__init__(self, f"invalid source: {message}")


class UnknownSourceException(Exception):
    """Raised when the source is not known."""

    def __init__(self, source_name: str):
        Exception.__init__(self, f"source does not exist: {source_name}")
