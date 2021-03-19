"""Read the Kukur configuration."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import glob

import toml


class InvalidIncludeException(Exception):
    """Raised when the include configuration is invalid."""

    def __init__(self, message: str):
        Exception.__init__(self, f"invalid include: {message}")


def from_toml(path):
    """Read the configuration from a TOML file, processing includes."""
    config = toml.load(path)
    for include_options in config.get("include", []):
        if "glob" not in include_options:
            raise InvalidIncludeException('"glob" is required')
        for include_path in glob.glob(include_options["glob"]):
            include_config = toml.load(include_path)
            for k, v in include_config.items():
                if k not in config:
                    config[k] = v
                elif isinstance(config[k], list):
                    config[k].append(v)
                elif isinstance(config[k], dict):
                    config[k].update(v)
                else:
                    config[k] = v
    return config
