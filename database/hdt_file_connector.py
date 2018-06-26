# hdt_file_connector.py
# Author: Thomas MINIER - MIT License 2017-2018
from hdt import HDTDocument
from database.db_connector import DatabaseConnector
import os.path


class HDTFileConnector(DatabaseConnector):
    """A HDTFileConnector search for RDF triples in a HDT file"""

    def __init__(self, file):
        super(HDTFileConnector, self).__init__()
        self._hdt = HDTDocument(file)

    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        """
            Get an iterator over all RDF triples matching a triple pattern.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - object ``string`` - Object of the triple pattern
                - limit ``int=0`` ``optional`` -  LIMIT modifier, i.e., maximum number of RDF triples to read
                - offset ``int=0`` ``optional`` -  OFFSET modifier, i.e., number of RDF triples to skip

            Returns:
                A Python iterator over RDF triples matching the given triples pattern
        """
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        return self._hdt.search_triples(subject, predicate, obj, offset=offset, limit=limit)

    def from_config(config):
        """Build a HDTFileFactory from a config file"""
        if not os.path.isfile(config["file"]):
            raise Exception("Configuration file not found: {}".format(config["file"]))
        return HDTFileConnector(config["file"])
