"""Configure logging for Timeseer."""

import logging
import logging.handlers

from typing import Any, Dict


def configure(config: Dict[str, Any], threaded=False):
    """Configure the Python logger."""
    log_format = '%(asctime)s %(levelname)s %(name)s %(processName)s : %(message)s'
    if threaded:
        log_format = '%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s'
    if 'logging' in config and 'path' in config['logging']:
        path: str = config['logging']['path']
        handler = logging.handlers.TimedRotatingFileHandler(path, when='D', backupCount=7)
        logging.basicConfig(level=logging.INFO, format=log_format, handlers=[handler])
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)
