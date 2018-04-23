# rdf_file_factory.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import Graph
from rdflib.util import guess_format
from database.db_connector import DatabaseConnector
from database.rdf_index import TripleIndex
from database.utils import TripleDictionnary
from math import inf
from os.path import isfile


def strip_uri(v):
    return v[1:len(v) - 1] if v.startswith("<") else v


class RDFFileConnector(DatabaseConnector):
    """
        A RDFFileConnector search for RDF triples in a RDF file (N-triples, Turtle, N3, etc).
        Internally, it uses an Hexastore[1] based approach, with 6 B-tree indexes on SPO, SOP, PSO, POS, OSP and OPS.


        Reference:
            [1] Weiss, Cathrin, Panagiotis Karras, and Abraham Bernstein. "Hexastore: sextuple indexing for semantic web data management." Proceedings of the VLDB Endowment 1.1 (2008): 1008-1019.
    """

    def __init__(self, file, format=None):
        self._dictionnary = TripleDictionnary()
        self._triples = []
        self._indexes = {
            "spo": TripleIndex(),
            "sop": TripleIndex(),
            "osp": TripleIndex(),
            "ops": TripleIndex(),
            "pso": TripleIndex(),
            "pos": TripleIndex()
        }
        self.__loadFromFile(file, format)

    def search_triples(self, subject, predicate, obj, limit=inf, offset=0):
        def processor(i):
            s, p, o = self._triples[i]
            return self._dictionnary.bit_to_triple(s, p, o)
        btriple = self._dictionnary.triple_to_bit(subject, predicate, obj)
        iterator = None
        if subject is not None and predicate is not None:
            iterator = self._indexes["spo"].search_pattern(btriple, limit=limit, offset=offset)
        elif subject is not None and object is not None:
            iterator = self._indexes["sop"].search_pattern((btriple[0], btriple[2], btriple[1]), limit=limit, offset=offset)
        elif object is not None and subject is not None:
            iterator = self._indexes["osp"].search_pattern((btriple[2], btriple[0], btriple[1]), limit=limit, offset=offset)
        elif object is not None and predicate is not None:
            iterator = self._indexes["ops"].search_pattern((btriple[2], btriple[1], btriple[0]), limit=limit, offset=offset)
        elif predicate is not None and subject is not None:
            iterator = self._indexes["pso"].search_pattern((btriple[1], btriple[0], btriple[2]), limit=limit, offset=offset)
        elif predicate is not None and object is not None:
            iterator = self._indexes["pos"].search_pattern((btriple[1], btriple[2], btriple[0]), limit=limit, offset=offset)
        else:
            iterator = self._indexes["spo"].search_pattern((0, 0, 0), limit=limit, offset=offset)
        return map(processor, iterator), None

    def from_config(config):
        """Build a RDFFileConnector from a config file"""
        if not isfile(config["file"]):
            raise Error("Configuration file not found: {}".format(config["file"]))
        return RDFFileConnector(config['file'], config['format'])

    def __loadFromFile(self, file, format=None):
        if not isfile(file):
            raise Error("Cannot find RDF file to load: {}".format(file))
        if format is None:
            format = guess_format(file)
        # use a temporary graph to load from a RDF file
        g = Graph()
        g.parse(file, format=format)
        for s, p, o in g.triples((None, None, None)):
            # load RDF triples in the dictionnary, then index it
            triple = self._dictionnary.insert_triple(strip_uri(s.n3()), strip_uri(p.n3()), strip_uri(o.n3()))
            self._indexes["spo"].insert(triple, len(self._triples))
            self._indexes["sop"].insert((triple[0], triple[2], triple[1]), len(self._triples))
            self._indexes["osp"].insert((triple[2], triple[0], triple[1]), len(self._triples))
            self._indexes["ops"].insert((triple[2], triple[1], triple[0]), len(self._triples))
            self._indexes["pso"].insert((triple[1], triple[0], triple[2]), len(self._triples))
            self._indexes["pos"].insert((triple[1], triple[2], triple[0]), len(self._triples))
            self._triples.append(triple)
