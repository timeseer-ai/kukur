"""Read the Kukur configuration."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import glob
import logging

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
        for include_path in glob.glob(include_options["glob"]):
            include_config = _read_toml(include_path)
            for k, v in include_config.items():
                if k not in config:
                    config[k] = v
                elif isinstance(config[k], list):
                    config[k].extend(v)
                elif isinstance(config[k], dict):
                    config[k].update(v)
                else:
                    config[k] = v
    return config


def _read_toml(path):
    try:
        return toml.load(path)
    except Exception as err:
        logger.error("error in %s", path)
        raise err
