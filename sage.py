# sage.py
# Author: Thomas MINIER - MIT License 2017-2018
import argparse
import os
from http_server.server import sage_app
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


def start_sage():
    parser = argparse.ArgumentParser(description='Launch the Sage server')
    parser.add_argument('config', metavar='config', help='Path to the Sage configuration file to use')
    parser.add_argument('-p', '--port', metavar='P', type=int, help='The port to bind (default: 8000)', default=8000)
    parser.add_argument('-w', '--workers', metavar='W', type=int, help='The number of server workers (default: 4)', default=4)
    args = parser.parse_args()
    # check if config file exists
    if not os.path.isfile(args.config):
        print("Error: Configuration file not found: '{}'".format(args.config))
        print("Error: Sage server could not start, aborting...")
    else:
        options = {
            'bind': '%s:%s' % ('0.0.0.0', args.port),
            'workers': args.workers,
        }
        StandaloneApplication(sage_app(args.config), options).run()


if __name__ == '__main__':
    start_sage()
