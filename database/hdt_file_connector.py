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

    def search(self, subject, predicate, obj, offset=None):
        """
            Get an iterator over all RDF triples matching a triple pattern.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - object ``string`` - Object of the triple pattern
                - offset ``string=None`` ``optional`` -  OFFSET ID used to resume scan

            Returns:
                A Python iterator over RDF triples matching the given triples pattern
        """
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        offset = 0 if offset is None else int(float(offset))
        return self._hdt.search_triples(subject, predicate, obj, offset=offset)

    @property
    def nb_triples(self):
        return self._hdt.total_triples

    @property
    def nb_subjects(self):
        """Get the number of subjects in the database"""
        return self._hdt.nb_subjects

    @property
    def nb_predicates(self):
        """Get the number of predicates in the database"""
        return self._hdt.nb_predicates

    @property
    def nb_objects(self):
        """Get the number of objects in the database"""
        return self._hdt.nb_objects

    def from_config(config):
        """Build a HDTFileFactory from a config file"""
        if not os.path.isfile(config["file"]):
            raise Exception("Configuration file not found: {}".format(config["file"]))
        return HDTFileConnector(config["file"])
