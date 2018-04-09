# fragment_factory.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import Graph, URIRef
from datasets.fragment_factory import FragmentFactory
from ldf.utils import string_to_literal


class RawFileFactory(FragmentFactory):
    """A RawFileFactory builds LDF fragments from a raw RDF file (N-Triples, NQuads, Trig, etc)"""

    def __init__(self, file, format='nt', triplesPerPage=100):
        self._graph = Graph()
        self._triplesPerPage = triplesPerPage
        self._graph.parse(file, format=format)

    def get_triples(self, subject, predicate, obj, page=1):
        # convert subject, predicate and object to rdflib URIRef or Literal
        subject = URIRef(subject) if subject is not None else None
        predicate = URIRef(predicate) if predicate is not None else None
        obj = string_to_literal(obj) if obj is not None else None
        triples = list(self._graph.triples((subject, predicate, obj)))
        triples.sort()  # sort triples as rdflib does not guarantee an order on results
        startIndex = self._triplesPerPage * (page - 1)
        triplesPage = triples[startIndex:startIndex + self._triplesPerPage]
        return (triplesPage, len(triples))

    def from_config(config):
        """Build a RawFileFactory from a config file"""
        # TODO add safeguard
        return RawFileFactory(config['file'], config['format'], config['pageSize'])
