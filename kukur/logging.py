"""Configure logging for Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
import logging.handlers
from typing import Any, Dict

from kukur.exceptions import InvalidLogLevelException

LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
}


def configure(config: Dict[str, Any]):
    """Configure the Python logger."""
    log_format = "%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s"
    level = _get_log_level(config)

    if "logging" in config and "path" in config["logging"]:
        path: str = config["logging"]["path"]
        handler = logging.handlers.TimedRotatingFileHandler(
            path, when="D", backupCount=7
        )
        logging.basicConfig(level=level, format=log_format, handlers=[handler])
    else:
        logging.basicConfig(level=level, format=log_format)

    logging.getLogger("azure.identity").setLevel(logging.WARNING)
    logging.getLogger("azure.core").setLevel(logging.WARNING)
    logging.getLogger("botocore.credentials").setLevel(logging.WARNING)


def _get_log_level(config: Dict[str, Any]):
    log_level = logging.INFO
    if "logging" in config and "level" in config["logging"]:
        level_text: str = config["logging"]["level"]
        if level_text not in LEVELS:
            raise InvalidLogLevelException()
        log_level = LEVELS[level_text]
    return log_level
