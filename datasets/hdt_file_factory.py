# hdt_file_factory.py
# Author: Thomas MINIER - MIT License 2017-2018
from hdt import HDTDocument
from rdflib import URIRef
from datasets.fragment_factory import FragmentFactory


class HDTFileFactory(FragmentFactory):
    """A HDTFileFactory builds LDF fragments from a raw RDF file (N-Triples, NQuads, Trig, etc)"""

    def __init__(self, file, triplesPerPage=100):
        self._hdt = HDTDocument(file)
        self._triplesPerPage = triplesPerPage

    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        return self._hdt.search_triples(subject, predicate, obj, offset=offset, limit=limit)

    def search_triples_ids(self, subject, predicate, obj, limit=0, offset=0):
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        return self._hdt.search_triples_ids(subject, predicate, obj, offset=offset, limit=limit)

    def tripleid_to_string(self, subject, predicate, obj):
        return self._hdt.tripleid_to_string(subject, predicate, obj)

    def from_config(config):
        """Build a HDTFileFactory from a config file"""
        # TODO add safeguard
        return HDTFileFactory(config['file'], config['pageSize'])
