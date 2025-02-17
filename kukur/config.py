"""Read the Kukur configuration."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import glob
import logging
import sys
from typing import Dict

if sys.version_info >= (3, 11):
    import tomllib as toml
else:
    import toml

from kukur.exceptions import KukurException

logger = logging.getLogger(__name__)


class InvalidIncludeException(KukurException):
    """Raised when the include configuration is invalid."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"invalid include: {message}")


def from_toml(path):
    """Read the configuration from a TOML file, processing includes."""
    config = _read_toml(path)
    for include_options in config.get("include", []):
        if "glob" not in include_options:
            raise InvalidIncludeException('"glob" is required')
        for include_path in sorted(glob.glob(include_options["glob"])):
            include_config = _read_toml(include_path)
            merge_fragment(config, include_config)
    return config


def merge_fragment(config: Dict, fragment: Dict):
    """Merge a configuration fragment into the main configuration.

    This extends lists and updates dictionaries two levels deep.
    """
    for k, v in fragment.items():
        if k not in config:
            config[k] = v
        elif isinstance(config[k], list):
            config[k].extend(v)
        elif isinstance(config[k], dict):
            _update_dict(config[k], v)
        else:
            config[k] = v


def _update_dict(existing: Dict, new: Dict):
    """Extend dictionaries, overwrite values and lists."""
    for k, v in new.items():
        if k in existing and isinstance(existing[k], dict):
            existing[k].update(v)
        else:
            existing[k] = v


def _read_toml(path):
    try:
        if sys.version_info >= (3, 11):
            with open(path, "rb") as config_file:
                return toml.load(config_file)
        else:
            return toml.load(path)
    except Exception as err:
        logger.error("error in %s", path)
        raise err
