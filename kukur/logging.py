"""Configure logging for Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
import logging.handlers

from typing import Any, Dict


LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
}


class InvalidLogLevelException(Exception):
    """Raised when the logging level in the configuration is invalid."""

    def __init__(self, level: str):
        levels = ", ".join(LEVELS)
        super().__init__(f"Configured log level {level} not in {levels}")


def configure(config: Dict[str, Any]):
    """Configure the Python logger."""
    log_format = "%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s"

    log_level = logging.INFO
    if "logging" in config and "level" in config["logging"]:
        level_text: str = config["logging"]["level"]
        if level_text not in LEVELS:
            raise InvalidLogLevelException(level_text)
        log_level = LEVELS[level_text]

    if "logging" in config and "path" in config["logging"]:
        path: str = config["logging"]["path"]
        handler = logging.handlers.TimedRotatingFileHandler(
            path, when="D", backupCount=7
        )
        logging.basicConfig(level=log_level, format=log_format, handlers=[handler])
    else:
        logging.basicConfig(level=log_level, format=log_format)
