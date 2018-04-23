# rdf_index.py
# Author: Thomas MINIER - MIT License 2017-2018
from bisect import bisect_left
from math import inf


class TripleIndex(object):
    """docstring for TripleIndex."""
    def __init__(self):
        super(TripleIndex, self).__init__()
        self._keys = []
        self._values = []

    def insert(self, key, value):
        index = bisect_left(self._keys, key)
        self._keys.insert(index, key)
        self._values.insert(index, value)
        return index

    def index(self, key):
        return bisect_left(self._keys, key)

    def search_pattern(self, pattern, limit=inf, offset=0):
        def predicate(value):
            if (pattern[0] > 0 and value[0] != pattern[0]):
                return False
            if (pattern[1] > 0 and value[1] != pattern[1]):
                return False
            if (pattern[2] > 0 and value[2] != pattern[2]):
                return False
            return True

        def generator(startIndex):
            i = startIndex + offset
            nbRead = 0
            while i < len(self._keys) and nbRead <= limit and predicate(self._keys[i]):
                yield self._values[i]
                i += 1
                nbRead += 1
        return generator(self.index(pattern))
