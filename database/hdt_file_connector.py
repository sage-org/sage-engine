# hdt_file_connector.py
# Author: Thomas MINIER - MIT License 2017-2018
from hdt import HDTDocument
from database.db_connector import DatabaseConnector
import os.path


class HDTFileConnector(DatabaseConnector):
    """A HDTFileConnector search for RDF triples in a HDT file"""

    def __init__(self, file):
        self._hdt = HDTDocument(file)

    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        return self._hdt.search_triples(subject, predicate, obj, offset=offset, limit=limit)

    def from_config(config):
        """Build a HDTFileFactory from a config file"""
        if not os.path.isfile(config["file"]):
            raise Error("Configuration file not found: {}".format(config["file"]))
        return HDTFileConnector(config["file"])
