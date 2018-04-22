# rdf_file_factory.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import Graph, URIRef, Literal
from database.db_connector import DatabaseConnector
from database.utils import string_to_literal
import os.path


class RDFFileConnector(DatabaseConnector):
    """
        A RDFFileConnector search for RDF triples in a RDF file (N-triples, Turtle, N3, etc).
        WARNING: currently very inefficient :-(
    """

    def __init__(self, file, format='nt'):
        self._graph = Graph()
        self._graph.parse(file, format=format)

    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        # convert subject, predicate and object to rdflib URIRef or Literal
        subject = URIRef(subject) if subject is not None else '?s'
        predicate = URIRef(predicate) if predicate is not None else '?p'
        obj = string_to_literal(obj) if obj is not None else None
        triples = list(self._graph.triples((subject, predicate, obj)))
        triples.sort()  # sort triples as rdflib does not guarantee an order on results
        triplesPage = triples[offset:offset + limit]
        return (triplesPage, len(triples))

    def from_config(config):
        """Build a RawFileFactory from a config file"""
        if not os.path.isfile(config["file"]):
            raise Error("Configuration file not found: {}".format(config["file"]))
        return RDFFileConnector(config['file'], config['format'])
