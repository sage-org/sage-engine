# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator
# from itertools import filter


class DummyDataset:
    def __init__(self, doc, name):
        self._name = name
        self._doc = doc

    def get_graph(self, name):
        return self._doc

    def has_graph(self, name):
        return self._name == name


class SimpleIterator(DBIterator):
    """A DBIterator that iterates over a set of triples"""

    def __init__(self, triples, pattern, offset=0):
        super(SimpleIterator, self).__init__(pattern)
        self._triples = triples
        self._popped = offset

    def has_next(self):
        return len(self._triples) > 0

    def last_read(self):
        return "{}".format(self._popped)

    def next(self):
        self._popped += 1
        return self._triples.pop()


class MemoryDatabase(DatabaseConnector):
    """An in-memory RDF database"""

    def __init__(self):
        super(MemoryDatabase, self).__init__()
        self._triples = list()

    def from_config(config):
        return MemoryDatabase()

    def search(self, subject, predicate, obj, last_read=None):
        def __filter(triple):
            s, p, o = triple
            return (subject.startswith('?') or subject == s) and (predicate.startswith('?') or predicate == p) and (obj.startswith('?') or obj == o)
        pattern = {
            "subject": subject,
            "predicate": predicate,
            "object": obj
        }
        results = list(filter(__filter, self._triples))
        offset = 0 if last_read is None else int(float(last_read))
        results = results[offset:]
        return SimpleIterator(results, pattern, offset), len(results)

    def insert(self, subject, predicate, obj):
        self._triples.append((subject, predicate, obj))

    def delete(self, subject, predicate, obj):
        self._triples.remove((subject, predicate, obj))

    def close(self):
        self._triples = list()
