# rdf_file_factory.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import Graph, URIRef, Literal
from database.db_connector import DatabaseConnector
from database.utils import TripleDictionnary
import os.path
from bisect import bisect_left, insort_left


def bisect_index(a, x):
    """Locate the leftmost value exactly equal to x"""
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    raise ValueError


def strip_uri(v):
    return v[1:len(v) - 1] if v.startswith("<") else v


class RDFFileConnector(DatabaseConnector):
    """
        A RDFFileConnector search for RDF triples in a RDF file (N-triples, Turtle, N3, etc).
        WARNING: currently very inefficient :-(
    """

    def __init__(self, file, format='nt'):
        self._dictionnary = TripleDictionnary()
        g = Graph()
        g.parse(file, format=format)
        self._triples = []
        for s, p, o in g.triples((None, None, None)):
            triple = self._dictionnary.insert_triple(strip_uri(s.n3()), strip_uri(p.n3()), strip_uri(o.n3()))
            insort_left(self._triples, triple)

    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        # convert subject, predicate and object to rdflib URIRef or Literal
        btriple = self._dictionnary.triple_to_bit(subject, predicate, obj)
        i = bisect_index(self._triples, btriple)
        print(self._dictionnary.bit_to_triple(btriple[0], btriple[1], btriple[2]))
        return None, None
        # subject = URIRef(subject) if subject is not None else None
        # predicate = URIRef(predicate) if predicate is not None else None
        # obj = string_to_literal(obj) if obj is not None else None
        # triples = list(self._graph.triples((subject, predicate, obj)))
        # triples.sort()  # sort triples as rdflib does not guarantee an order on results
        # triplesPage = triples[offset:offset + limit] if limit > 0 else triples[offset:]
        # return (triplesPage, len(triples))

    def from_config(config):
        """Build a RawFileFactory from a config file"""
        if not os.path.isfile(config["file"]):
            raise Error("Configuration file not found: {}".format(config["file"]))
        return RDFFileConnector(config['file'], config['format'])
