# rdf_file_factory.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import Graph
from rdflib.util import guess_format
from database.db_connector import DatabaseConnector
from database.rdf_index import TripleIndex
from database.utils import TripleDictionary
from math import inf
import os
import fnmatch
import pickle


def strip_uri(v):
    return v[1:len(v) - 1] if v.startswith("<") else v


class RDFFileConnector(DatabaseConnector):
    """
        A RDFFileConnector search for RDF triples in a RDF file (N-triples, Turtle, N3, etc).
        Internally, it uses an Hexastore[1] based approach, with 6 B-tree indexes on SPO, SOP, PSO, POS, OSP and OPS.
        It also support caching, using the pickle protocol.

        Args:
            - file [string] - Path to the RDF file to load
            - format [string=None] - (Optional) Format of the RDF file ("ttl", "nt", "trig", etc)
            - useCache [boolean=False] (Optional) True if the cache should be used, False otherwise

        Reference:
            [1] Weiss, Cathrin, Panagiotis Karras, and Abraham Bernstein. "Hexastore: sextuple indexing for semantic web data management." Proceedings of the VLDB Endowment 1.1 (2008): 1008-1019.
    """

    def __init__(self, file, format=None, useCache=False):
        super(RDFFileConnector, self).__init__()
        file = os.path.abspath(file)
        self._dictionary = TripleDictionary()
        self._triples = []
        self._indexes = {
            "spo": TripleIndex(),
            "sop": TripleIndex(),
            "osp": TripleIndex(),
            "ops": TripleIndex(),
            "pso": TripleIndex(),
            "pos": TripleIndex()
        }
        if not useCache:
            self.__loadFromFile(file, format)
        else:
            # compute chache fingerprint
            cacheFile = "{}.v{}.cache".format(file, hash(os.path.getmtime(file)))
            if os.path.isfile(cacheFile):
                self.__loadFromCache(cacheFile)
            else:
                self.__loadFromFile(file, format)
                self.__purgeCache(file)
                self.__saveToCache(cacheFile)

    def search_triples(self, subject, predicate, obj, limit=inf, offset=0):
        def processor(i):
            s, p, o = self._triples[i]
            return self._dictionary.bit_to_triple(s, p, o)
        btriple = self._dictionary.triple_to_bit(subject, predicate, obj)
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
        if not os.path.isfile(config["file"]):
            raise Error("Configuration file not found: {}".format(config["file"]))
        return RDFFileConnector(config['file'], config['format'])

    def __loadFromFile(self, file, format=None):
        """
            Load the datastructure from a RDF file.
            If not format is provided, then rdflib is used to guess the format.
        """
        if not os.path.isfile(file):
            raise Error("Cannot find RDF file to load: {}".format(file))
        if format is None:
            format = guess_format(file)
        # use a temporary graph to load from a RDF file
        g = Graph()
        g.parse(file, format=format)
        for s, p, o in g.triples((None, None, None)):
            # load RDF triples in the dictionary, then index it
            triple = self._dictionary.insert_triple(strip_uri(s.n3()), strip_uri(p.n3()), strip_uri(o.n3()))
            self._indexes["spo"].insert(triple, len(self._triples))
            self._indexes["sop"].insert((triple[0], triple[2], triple[1]), len(self._triples))
            self._indexes["osp"].insert((triple[2], triple[0], triple[1]), len(self._triples))
            self._indexes["ops"].insert((triple[2], triple[1], triple[0]), len(self._triples))
            self._indexes["pso"].insert((triple[1], triple[0], triple[2]), len(self._triples))
            self._indexes["pos"].insert((triple[1], triple[2], triple[0]), len(self._triples))
            self._triples.append(triple)

    def __loadFromCache(self, file):
        """Load the datastructure from a serialized cache"""
        with open(file, 'rb') as f:
            data = pickle.load(f)
            self._dictionary = data["dictionary"]
            self._triples = data["triples"]
            self._indexes = data["indexes"]

    def __saveToCache(self, path):
        """Save the datastructure using the pickle protocol"""
        with open(path, 'wb') as f:
            savedData = {"dictionary": self._dictionary, "indexes": self._indexes, "triples": self._triples}
            pickle.dump(savedData, f, pickle.HIGHEST_PROTOCOL)

    def __purgeCache(self, filename):
        """Purge a previous version of the cache"""
        fpattern = "{}.v*.cache".format(os.path.basename(filename))
        dir = os.path.dirname(filename)
        for file in os.listdir(dir):
            if fnmatch.fnmatch(file, fpattern):
                os.remove("{}/{}".format(dir, file))
