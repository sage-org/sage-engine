# rdf_file_factory.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import Graph, URIRef, Literal
from database.db_connector import DatabaseConnector
from database.utils import TripleDictionnary
import os.path
from bisect import bisect_left, bisect_right
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

    def search_pattern(self, pattern, offset=0, limit=inf):
        def predicate(value):
            # TODO this thing is buggy :-p
            if (pattern[0] > 0 and value[0] == pattern[0]):
                return True
            if (pattern[1] > 0 and value[1] == pattern[1]):
                return True
            if (pattern[2] > 0 and value[2] == pattern[2]):
                return True
            return False

        def generator(startIndex):
            i = startIndex + offset
            nbRead = 0
            while i < len(self._keys) and nbRead <= limit and predicate(self._keys[i]):
                yield self._values[i]
                i += 1
                nbRead += 1
        return generator(self.index(pattern))


def strip_uri(v):
    return v[1:len(v) - 1] if v.startswith("<") else v


class RDFFileConnector(DatabaseConnector):
    """
        A RDFFileConnector search for RDF triples in a RDF file (N-triples, Turtle, N3, etc).
    """

    def __init__(self, file, format='nt'):
        self._dictionnary = TripleDictionnary()
        g = Graph()
        g.parse(file, format=format)
        self._triples = []
        self._spo_index = TripleIndex()
        self._ops_index = TripleIndex()
        self._pso_index = TripleIndex()
        for s, p, o in g.triples((None, None, None)):
            triple = self._dictionnary.insert_triple(strip_uri(s.n3()), strip_uri(p.n3()), strip_uri(o.n3()))
            self._spo_index.insert(triple, len(self._triples))
            self._ops_index.insert((triple[2], triple[1], triple[0]), len(self._triples))
            self._pso_index.insert((triple[1], triple[0], triple[1]), len(self._triples))
            self._triples.append(triple)

    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        def processor(i):
            s, p, o = self._triples[i]
            return self._dictionnary.bit_to_triple(s, p, o)
        btriple = self._dictionnary.triple_to_bit(subject, predicate, obj)
        iterator = None
        if subject is None:
            iterator = self._spo_index.search_pattern(btriple)
        elif predicate is None:
            iterator = self._ops_index.search_pattern((btriple[2], btriple[1], btriple[0]))
        else:
            iterator = self._spo_index.search_pattern(btriple)
        return map(processor, iterator), None

    def from_config(config):
        """Build a RawFileFactory from a config file"""
        if not os.path.isfile(config["file"]):
            raise Error("Configuration file not found: {}".format(config["file"]))
        return RDFFileConnector(config['file'], config['format'])
