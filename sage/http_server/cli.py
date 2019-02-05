# cli.py
# Author: Thomas MINIER - MIT License 2017-2018
import argparse
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


def cli_sage():
    parser = argparse.ArgumentParser(description='Launch the Sage server using a configuration file')
    parser.add_argument('config', metavar='config', help='Path to the configuration file')
    parser.add_argument('-p', '--port', metavar='P', type=int, help='The port to bind (default: 8000)', default=8000)
    parser.add_argument('-w', '--workers', metavar='W', type=int, help='The number of server workers (default: 4)', default=4)
    parser.add_argument('--log-level', metavar='LEVEL', dest='log_level', help='The granularity of log outputs (default: info)', default='info')
    args = parser.parse_args()
    # check if config file exists
    if not isfile(args.config):
        print("Error: Configuration file not found: '{}'".format(args.config))
        print("Error: Sage server could not start, aborting...")
    else:
        options = {
            'bind': '%s:%s' % ('0.0.0.0', args.port),
            'workers': args.workers,
            'log-level': args.log_level
        }
        StandaloneApplication(sage_app(args.config), options).run()
