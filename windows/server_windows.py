"""Server initialization code for Windows.

This starts a CherryPy server that runs Kukur."""


from typing import Any

import cherrypy

import kukur
import kukur.config
import kukur.logging

from kukur.app import Kukur


def start() -> Any:
    """"Start Kukur and serve it in a CherryPy web service."""
    config = kukur.config.from_toml('Kukur.toml')
    kukur.logging.configure(config, threaded=False)

    app = Kukur(config)
    app.start()

    cherrypy.config.update({
        'global': {'environment': 'production'},
        'engine.autoreload.on': False,
    })

    cherrypy.engine.signals.subscribe()

    cherrypy.engine.start()
    cherrypy.engine.block()


def stop() -> Any:
    """Stop the CherryPy web service that powers Kukur."""
    cherrypy.engine.exit()


if __name__ == '__main__':
    start()
