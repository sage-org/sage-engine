# hdt_file_connector.py
# Author: Thomas MINIER - MIT License 2017-2018
from hdt import HDTDocument
from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator
import os.path


class HDTIterator(DBIterator):
    """An HDTIterator implements a DBIterator for a triple pattern evaluated using an HDT file"""

    def __init__(self, source, pattern, start_offset=0):
        super(HDTIterator, self).__init__(pattern)
        self._source = source
        self._start_offset = start_offset

    def last_read(self):
        """Return the ID of the last element read"""
        return str(self._source.nb_reads + self._start_offset)

    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        return next(self._source)

    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        return self._source.has_next()


class HDTFileConnector(DatabaseConnector):
    """A HDTFileConnector search for RDF triples in a HDT file"""

    def __init__(self, file):
        super(HDTFileConnector, self).__init__()
        self._hdt = HDTDocument(file)

    def search(self, subject, predicate, obj, last_read=None):
        """
            Get an iterator over all RDF triples matching a triple pattern.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - object ``string`` - Object of the triple pattern
                - last_read ``string=None`` ``optional`` -  OFFSET ID used to resume scan

            Returns:
                A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern
        """
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        # convert None & empty string to offset = 0
        offset = 0 if last_read is None or last_read == '' else int(float(last_read))
        pattern = {'subject': subject, 'predicate': predicate, 'object': obj}
        iterator, card = self._hdt.search_triples(subject, predicate, obj, offset=offset)
        return HDTIterator(iterator, pattern, start_offset=offset), card

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
