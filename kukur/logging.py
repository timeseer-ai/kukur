"""Configure logging for Kukur."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

import logging
import logging.handlers

from typing import Any, Dict


def configure(config: Dict[str, Any]):
    """Configure the Python logger."""
    log_format = "%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s"
    if "logging" in config and "path" in config["logging"]:
        path: str = config["logging"]["path"]
        handler = logging.handlers.TimedRotatingFileHandler(
            path, when="D", backupCount=7
        )
        logging.basicConfig(level=logging.INFO, format=log_format, handlers=[handler])
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)
