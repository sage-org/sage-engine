# start_server.py
# Author: Thomas MINIER - MIT License 2017-2019
import click
from os.path import isfile
from sage.http_server.server import sage_app
from gunicorn.app.base import BaseApplication
from gunicorn.six import iteritems


class StandaloneApplication(BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super(StandaloneApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


@click.command()
@click.argument("config")
@click.option("-p", "--port", type=int, default=8000, show_default=True, help="The port to bind")
@click.option("-w", "--workers", type=int, default=4, show_default=True, help="he number of server workers")
@click.option("--log-level", type=click.Choice(["debug", "info", "warning", "error"]), default="info", show_default=True, help="The granularity of log outputs")
def start_sage_server(config, port, workers, log_level):
    """Launch the Sage server using the CONFIG configuration file"""
    # check if config file exists
    if not isfile(config):
        print("Error: Configuration file not found: '{}'".format(config))
        print("Error: Sage server could not start, aborting...")
    else:
        options = {
            'bind': '%s:%s' % ('0.0.0.0', port),
            'workers': workers,
            'log-level': log_level
        }
        StandaloneApplication(sage_app(config), options).run()
