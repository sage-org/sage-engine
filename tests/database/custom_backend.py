# custom_backend.py
# Author: Thomas MINIER - MIT License 2017-2018
from database.db_connector import DatabaseConnector


class SillyConnector(DatabaseConnector):
    """A custom database connector that does nothing"""

    def __init__(self, foo):
        super(SillyConnector, self).__init__()
        self._foo = foo

    def search(self, subject, predicate, obj, last_read=None):
        return self._foo

    def from_config(config):
        return SillyConnector(config['foo'])

    def foo(self):
        return self._foo
